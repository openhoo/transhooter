import "server-only";
import { isIP } from "node:net";
import {
  type BearerRegistration,
  ComposeBearerVerifier,
  type ConfiguredWebApplication,
  createConfiguredWebApplication,
  type InternalPrincipalVerifier,
  KubernetesServiceAccountVerifier,
  type ServiceJwtVerifier,
} from "@transhooter/server-core";
import { createPrismaDatabase, type PrismaDatabase } from "@transhooter/server-core/persistence";
import Redis from "ioredis";
import { createRemoteJWKSet, jwtVerify } from "jose";
import { type WebConfig, webConfig } from "./config";
import {
  AesGcmMagicLinkTokenSealer,
  cryptoPrimitives,
  liveKitAdapters,
  S3ArchiveAdapter,
  SmtpMailAdapter,
} from "./runtime-adapters";

async function ensureRedisConnected(redis: Redis): Promise<void> {
  if (redis.status === "wait") {
    await redis.connect();
  }
}
interface ApplicationResources {
  readonly application: ConfiguredWebApplication;
  dispose(): Promise<void>;
}

let resources: ApplicationResources | undefined;

function disposeApplicationResources(
  database: PrismaDatabase,
  redis: Redis,
  mail: SmtpMailAdapter,
  storage: S3ArchiveAdapter,
): () => Promise<void> {
  let disposal: Promise<void> | undefined;
  return async () => {
    if (disposal) {
      return disposal;
    }
    disposal = Promise.allSettled([
      database.disconnect(),
      Promise.resolve().then(() => redis.disconnect()),
      Promise.resolve().then(() => mail.close()),
      Promise.resolve().then(() => storage.destroy()),
    ]).then((results) => {
      const failures = results.flatMap((result) =>
        result.status === "rejected" ? [result.reason] : [],
      );
      if (failures.length === 1) {
        throw failures[0];
      }
      if (failures.length > 1) {
        throw new AggregateError(failures, "Failed to dispose web application resources");
      }
    });
    try {
      await disposal;
    } catch (error) {
      disposal = undefined;
      throw error;
    }
  };
}

type InternalVerifierPrimitives = {
  hashing: {
    sha256(value: Uint8Array | string): string;
  };
  clock: {
    now(): Date;
  };
};

export const INTERNAL_SERVICE_DEFINITIONS = Object.freeze({
  web: Object.freeze({
    service: "web" as const,
    token: null,
    permissions: Object.freeze([]),
  }),
  controlWorker: Object.freeze({
    service: "control-worker" as const,
    token: "controlWorker" as const,
    permissions: Object.freeze([
      "egress-layout:read",
      "archive:recording",
      "archive:finalize",
      "delete:drain",
    ]),
  }),
  translationWorker: Object.freeze({
    service: "translation-worker" as const,
    token: "translationWorker" as const,
    permissions: Object.freeze(["capability:write", "heartbeat:write", "provider-attempt:write"]),
  }),
  spoolDrainer: Object.freeze({
    service: "spool-drainer" as const,
    token: "spoolDrainer" as const,
    permissions: Object.freeze([
      "checkpoint:write",
      "worker-recovery:read",
      "worker-recovery:write",
    ]),
  }),
  languageRefresh: Object.freeze({
    service: "language-refresh" as const,
    token: null,
    permissions: Object.freeze(["capability:write"]),
  }),
  capabilityPublisher: Object.freeze({
    service: "capability-publisher" as const,
    token: null,
    permissions: Object.freeze(["capability:write"]),
  }),
});

export function composeBearerRegistrations(
  config: WebConfig,
  primitives: InternalVerifierPrimitives,
): BearerRegistration[] {
  const registrations: BearerRegistration[] = [];
  for (const definition of Object.values(INTERNAL_SERVICE_DEFINITIONS)) {
    if (definition.token === null) {
      continue;
    }
    const token = config.internalTokens[definition.token];
    if (token) {
      registrations.push({
        service: definition.service,
        tokenHash: primitives.hashing.sha256(token),
        subject: `compose:${definition.service}`,
        permissions: definition.permissions,
        notBefore: new Date(0),
        expiresAt: new Date("9999-12-31T23:59:59.999Z"),
      });
    }
  }
  return registrations;
}

export type ClientIpBoundary =
  | Readonly<{ mode: "direct-local" }>
  | Readonly<{ mode: "trusted-header"; headerName: string }>;

export function trustedClientIp(request: Request, boundary: ClientIpBoundary): string {
  if (boundary.mode === "direct-local") {
    return "direct-local";
  }
  const address = request.headers.get(boundary.headerName)?.trim();
  if (!address || address.includes(",") || isIP(address) === 0) {
    return "unknown";
  }
  return address.toLowerCase();
}

export function kubernetesServiceAccountSubjects(
  config: Pick<WebConfig, "podNamespace" | "internalServiceAccountPrefix">,
) {
  if (!config.podNamespace || !config.internalServiceAccountPrefix) {
    throw new Error("Internal Kubernetes service account identity is not configured");
  }
  const prefix = `system:serviceaccount:${config.podNamespace}:${config.internalServiceAccountPrefix}-`;
  return Object.fromEntries(
    Object.values(INTERNAL_SERVICE_DEFINITIONS).map((definition) => [
      `${prefix}${definition.service}`,
      {
        service: definition.service,
        permissions: definition.permissions,
      },
    ]),
  );
}

function kubernetesPrincipalVerifier(
  config: WebConfig,
  primitives: InternalVerifierPrimitives,
): InternalPrincipalVerifier {
  if (
    !config.internalJwtIssuer ||
    !config.internalJwtAudience ||
    !config.podNamespace ||
    !config.internalServiceAccountPrefix
  ) {
    throw new Error("Internal JWT verification is not configured");
  }
  const issuer = config.internalJwtIssuer;
  const audience = config.internalJwtAudience;
  const jwks = createRemoteJWKSet(new URL(`${issuer.replace(/\/$/, "")}/openid/v1/jwks`));
  const jwt: ServiceJwtVerifier = {
    async verify(token) {
      const verified = await jwtVerify(token, jwks, { issuer, audience });
      if (
        !verified.payload.iss ||
        !verified.payload.sub ||
        !verified.payload.aud ||
        !verified.payload.exp
      ) {
        throw new Error("Service account token is missing required claims");
      }
      return {
        issuer: verified.payload.iss,
        audience: verified.payload.aud,
        subject: verified.payload.sub,
        expiresAt: new Date(verified.payload.exp * 1000),
      };
    },
  };
  return new KubernetesServiceAccountVerifier(
    jwt,
    issuer,
    audience,
    kubernetesServiceAccountSubjects(config),
    primitives.clock,
  );
}

function internalPrincipalVerifier(
  config: WebConfig,
  primitives: InternalVerifierPrimitives,
): InternalPrincipalVerifier {
  const registrations = composeBearerRegistrations(config, primitives);
  const serviceVerifier =
    registrations.length > 0
      ? new ComposeBearerVerifier(registrations, primitives.hashing, primitives.clock)
      : kubernetesPrincipalVerifier(config, primitives);
  const layoutVerifier = new ComposeBearerVerifier(
    [
      {
        service: "control-worker",
        tokenHash: primitives.hashing.sha256(config.egressLayoutSigningKey),
        subject: "egress-layout-signature",
        permissions: ["egress-layout:read"],
        notBefore: new Date(0),
        expiresAt: new Date("9999-12-31T23:59:59.999Z"),
      },
    ],
    primitives.hashing,
    primitives.clock,
  );
  return {
    async verify(requestHeaders) {
      try {
        return await layoutVerifier.verify(requestHeaders);
      } catch {
        return serviceVerifier.verify(requestHeaders);
      }
    },
  };
}

export function configuredApplication(): ConfiguredWebApplication {
  if (resources) {
    return resources.application;
  }

  const config = webConfig();
  const database = createPrismaDatabase({
    connectionString: config.databaseUrl,
    pool: {
      max: 20,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 5_000,
    },
  });
  const redis = new Redis(config.redisUrl, {
    lazyConnect: true,
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
  });
  const mail = new SmtpMailAdapter(config.smtpUrl, config.publicUrl);
  const storage = new S3ArchiveAdapter(config);
  const dispose = disposeApplicationResources(database, redis, mail, storage);
  try {
    const livekit = liveKitAdapters(config);
    const primitives = cryptoPrimitives();
    const application = createConfiguredWebApplication({
      database: database.client,
      mail,
      storage,
      livekitRooms: livekit.rooms,
      livekitTokens: livekit.tokens,
      webhookVerifier: livekit.webhookVerifier,
      internalPrincipalVerifier: internalPrincipalVerifier(config, primitives),
      ...primitives,
      authSecrets: { rateLimitKey: config.sessionSecret },
      magicLinkTokenSealer: new AesGcmMagicLinkTokenSealer(config.magicLinkSealKeyring),
      publicBaseUrl: config.publicUrl,
      clientIp: (request) =>
        trustedClientIp(
          request,
          config.trustedClientIpHeader
            ? { mode: "trusted-header", headerName: config.trustedClientIpHeader }
            : { mode: "direct-local" },
        ),
      readiness: async () => {
        await database.readiness();
        await ensureRedisConnected(redis);
        const [, mailReady, storageReady, liveKitReady] = await Promise.all([
          redis.ping(),
          mail.verify(),
          storage.ready(),
          livekit.ready(),
        ]);
        return mailReady && storageReady && liveKitReady;
      },
    });
    resources = { application, dispose };
    return application;
  } catch (error) {
    void dispose().catch(() => undefined);
    throw error;
  }
}

export async function disposeConfiguredApplication(): Promise<void> {
  const ownedResources = resources;
  if (!ownedResources) {
    return;
  }
  await ownedResources.dispose();
  if (resources === ownedResources) {
    resources = undefined;
  }
}
