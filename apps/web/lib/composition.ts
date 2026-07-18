import "server-only";
import {
  type BearerRegistration,
  ComposeBearerVerifier,
  type ConfiguredWebApplication,
  createConfiguredWebApplication,
  type InternalPrincipalVerifier,
  KubernetesServiceAccountVerifier,
  type ServiceJwtVerifier,
} from "@transhooter/server-core";
import * as schema from "@transhooter/server-core/persistence";
import { drizzle } from "drizzle-orm/node-postgres";
import Redis from "ioredis";
import { createRemoteJWKSet, jwtVerify } from "jose";
import { Pool } from "pg";
import { type WebConfig, webConfig } from "./config";
import {
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
let application: ConfiguredWebApplication | undefined;

type InternalVerifierPrimitives = {
  hashing: {
    sha256(value: Uint8Array | string): string;
  };
  clock: {
    now(): Date;
  };
};

const servicePermissions = Object.freeze({
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
    permissions: Object.freeze([
      "capability:write",
      "heartbeat:write",
      "checkpoint:write",
      "archive:finalize",
    ]),
  }),
  spoolDrainer: Object.freeze({
    service: "spool-drainer" as const,
    token: "spoolDrainer" as const,
    permissions: Object.freeze(["checkpoint:write", "archive:finalize"]),
  }),
  languageRefresh: Object.freeze({
    service: "language-refresh" as const,
    token: null,
    permissions: Object.freeze(["capability:write"]),
  }),
});

function composeBearerRegistrations(
  config: WebConfig,
  primitives: InternalVerifierPrimitives,
): BearerRegistration[] {
  const registrations: BearerRegistration[] = [];
  for (const definition of Object.values(servicePermissions)) {
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

function kubernetesPrincipalVerifier(
  config: WebConfig,
  primitives: InternalVerifierPrimitives,
): InternalPrincipalVerifier {
  if (!config.internalJwtIssuer || !config.internalJwtAudience || !config.podNamespace) {
    throw new Error("Internal JWT verification is not configured");
  }
  const issuer = config.internalJwtIssuer;
  const audience = config.internalJwtAudience;
  const jwks = createRemoteJWKSet(new URL(`${issuer.replace(/\/$/, "")}/openid/v1/jwks`));
  const prefix = `system:serviceaccount:${config.podNamespace}:`;
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
  const subjects = Object.fromEntries(
    Object.values(servicePermissions).map((definition) => [
      `${prefix}${definition.service}`,
      {
        service: definition.service,
        permissions: definition.permissions,
      },
    ]),
  );
  return new KubernetesServiceAccountVerifier(jwt, issuer, audience, subjects, primitives.clock);
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
  if (application) {
    return application;
  }

  const config = webConfig();
  const pool = new Pool({
    connectionString: config.databaseUrl,
    max: 20,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 5_000,
  });
  const database = drizzle(pool, { schema });
  const redis = new Redis(config.redisUrl, {
    lazyConnect: true,
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
  });
  const mail = new SmtpMailAdapter(config.smtpUrl, config.publicUrl);
  const storage = new S3ArchiveAdapter(config);
  const livekit = liveKitAdapters(config);
  const primitives = cryptoPrimitives();

  application = createConfiguredWebApplication({
    database,
    mail,
    storage,
    livekitRooms: livekit.rooms,
    livekitTokens: livekit.tokens,
    egress: livekit.egress,
    webhookVerifier: livekit.webhookVerifier,
    internalPrincipalVerifier: internalPrincipalVerifier(config, primitives),
    ...primitives,
    authSecrets: { rateLimitKey: config.sessionSecret },
    publicBaseUrl: config.publicUrl,
    clientIp: (request) =>
      request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ||
      request.headers.get("x-real-ip") ||
      "unknown",
    readiness: async () => {
      const databaseResult = await pool.query<{ value: number }>("SELECT 1 AS value");
      if (databaseResult.rows[0]?.value !== 1) {
        return false;
      }
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
  return application;
}
