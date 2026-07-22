import "server-only";
import { readFileSync } from "node:fs";
import { z } from "zod";

function hasSmtpSource(value: {
  SMTP_URL?: string | undefined;
  SMTP_URL_FILE?: string | undefined;
}): boolean {
  return value.SMTP_URL !== undefined || value.SMTP_URL_FILE !== undefined;
}

function hasRequiredKmsKey(value: {
  ARCHIVE_REQUIRE_KMS: "true" | "false";
  S3_KMS_KEY_ID?: string | undefined;
}): boolean {
  return value.ARCHIVE_REQUIRE_KMS !== "true" || value.S3_KMS_KEY_ID !== undefined;
}

function hasInternalAuthentication(value: {
  INTERNAL_CONTROL_TOKEN_FILE?: string | undefined;
  INTERNAL_TRANSLATION_TOKEN_FILE?: string | undefined;
  INTERNAL_SPOOL_DRAINER_TOKEN_FILE?: string | undefined;
  INTERNAL_JWT_ISSUER?: string | undefined;
  INTERNAL_JWT_AUDIENCE?: string | undefined;
  POD_NAMESPACE?: string | undefined;
}): boolean {
  const hasBearerFiles =
    value.INTERNAL_CONTROL_TOKEN_FILE !== undefined &&
    value.INTERNAL_TRANSLATION_TOKEN_FILE !== undefined &&
    value.INTERNAL_SPOOL_DRAINER_TOKEN_FILE !== undefined;
  const hasJwtConfiguration =
    value.INTERNAL_JWT_ISSUER !== undefined &&
    value.INTERNAL_JWT_AUDIENCE !== undefined &&
    value.POD_NAMESPACE !== undefined;
  return hasBearerFiles || hasJwtConfiguration;
}

const MagicLinkSealKeyringSchema = z
  .object({
    currentKeyId: z.string().min(1),
    keys: z.record(z.string().min(1), z.string()),
  })
  .strict();

export type MagicLinkSealKeyring = Readonly<{
  currentKeyId: string;
  keys: ReadonlyMap<string, Uint8Array>;
}>;

const EnvironmentSchema = z
  .object({
    APP_ENV: z.enum(["development", "test", "production"]),
    PROVIDER_PROFILE: z.string().regex(/^[a-z0-9]+(?:-[a-z0-9]+)*$/u),
    DATABASE_URL_FILE: z.string().min(1),
    REDIS_URL_FILE: z.string().min(1),
    S3_ENDPOINT: z.url(),
    S3_PUBLIC_ENDPOINT: z.url(),
    S3_REGION: z.string().min(1),
    S3_BUCKET: z.string().min(1),
    PUBLIC_LIVEKIT_URL: z.url(),
    LIVEKIT_INTERNAL_URL: z
      .url()
      .refine((value) => ["http:", "https:", "ws:", "wss:"].includes(new URL(value).protocol), {
        message: "LIVEKIT_INTERNAL_URL must use http, https, ws, or wss",
      }),
    LIVEKIT_CREDENTIALS_FILE: z.string().min(1),
    SESSION_SECRET_FILE: z.string().min(1),
    CSRF_SECRET_FILE: z.string().min(1),
    MAGIC_LINK_SEAL_KEYS_FILE: z.string().min(1),
    INTERNAL_CONTROL_TOKEN_FILE: z.string().min(1).optional(),
    INTERNAL_TRANSLATION_TOKEN_FILE: z.string().min(1).optional(),
    INTERNAL_SPOOL_DRAINER_TOKEN_FILE: z.string().min(1).optional(),
    EGRESS_LAYOUT_SIGNING_KEY_FILE: z.string().min(1),
    SMTP_URL: z.string().min(1).optional(),
    SMTP_URL_FILE: z.string().min(1).optional(),
    PUBLIC_BASE_URL: z.url(),
    S3_CREDENTIALS_FILE: z.string().min(1).optional(),
    S3_KMS_KEY_ID: z.string().min(1).optional(),
    ARCHIVE_REQUIRE_KMS: z.enum(["true", "false"]).default("false"),
    INTERNAL_JWT_ISSUER: z.string().min(1).optional(),
    INTERNAL_JWT_AUDIENCE: z.string().min(1).optional(),
    INTERNAL_SERVICE_ACCOUNT_TOKEN_FILE: z.string().min(1).optional(),
    POD_NAMESPACE: z.string().min(1).optional(),
    TRUSTED_CLIENT_IP_HEADER: z
      .string()
      .regex(/^x-[a-z0-9-]+$/iu)
      .transform((header) => header.toLowerCase())
      .refine((header) => header !== "x-forwarded-for" && header !== "x-real-ip", {
        message: "TRUSTED_CLIENT_IP_HEADER must be a dedicated boundary header",
      })
      .optional(),
  })
  .refine(hasSmtpSource, {
    message: "SMTP_URL or SMTP_URL_FILE is required",
  })
  .refine(hasRequiredKmsKey, {
    message: "S3_KMS_KEY_ID is required when ARCHIVE_REQUIRE_KMS=true",
  })
  .refine(hasInternalAuthentication, {
    message:
      "Per-caller internal Bearers or JWT issuer/audience/namespace configuration is required",
  });

export type WebConfig = {
  appEnv: "development" | "test" | "production";
  providerProfile: string;
  databaseUrl: string;
  redisUrl: string;
  s3Endpoint: string;
  s3PublicEndpoint: string;
  s3Region: string;
  s3Bucket: string;
  s3Credentials: string | null;
  s3KmsKeyId: string | null;
  archiveRequireKms: boolean;
  liveKitBrowserUrl: string;
  liveKitInternalUrl: string;
  liveKitCredentials: string;
  sessionSecret: string;
  csrfSecret: string;
  egressLayoutSigningKey: string;
  magicLinkSealKeyring: MagicLinkSealKeyring;
  internalTokens: Readonly<{
    controlWorker: string | null;
    translationWorker: string | null;
    spoolDrainer: string | null;
  }>;
  smtpUrl: string;
  publicUrl: string;
  internalJwtIssuer: string | null;
  internalJwtAudience: string | null;
  internalServiceAccountToken: string | null;
  podNamespace: string | null;
  trustedClientIpHeader: string | null;
};

let cached: WebConfig | undefined;

function readSecret(path: string): string {
  const value = readFileSync(path, "utf8").trim();
  if (!value) {
    throw new Error(`Secret file ${path} is empty`);
  }
  return value;
}

function readOptionalSecret(path: string | undefined): string | null {
  return path ? readSecret(path) : null;
}

export function parseMagicLinkSealKeyring(serialized: string): MagicLinkSealKeyring {
  let document: unknown;
  try {
    document = JSON.parse(serialized);
  } catch {
    throw new Error("Magic-link seal keyring is not valid JSON");
  }
  const parsed = MagicLinkSealKeyringSchema.parse(document);
  const entries = Object.entries(parsed.keys);
  if (entries.length === 0 || !Object.hasOwn(parsed.keys, parsed.currentKeyId)) {
    throw new Error("Magic-link seal keyring must contain its current key");
  }
  const keys = new Map<string, Uint8Array>();
  for (const [keyId, encoded] of entries) {
    if (!/^[A-Za-z0-9+/]{43}=$/u.test(encoded)) {
      throw new Error(`Magic-link seal key ${keyId} must be canonical base64`);
    }
    const key = Buffer.from(encoded, "base64");
    if (key.length !== 32 || key.toString("base64") !== encoded) {
      throw new Error(`Magic-link seal key ${keyId} must decode to 32 bytes`);
    }
    keys.set(keyId, new Uint8Array(key));
  }
  return Object.freeze({
    currentKeyId: parsed.currentKeyId,
    keys,
  });
}

export function parseWebEnvironment(environment: NodeJS.ProcessEnv) {
  return EnvironmentSchema.parse(environment);
}

export function webConfig(): WebConfig {
  if (cached) {
    return cached;
  }

  const environment = parseWebEnvironment(process.env);
  const smtpUrl = environment.SMTP_URL_FILE
    ? readSecret(environment.SMTP_URL_FILE)
    : environment.SMTP_URL;
  if (!smtpUrl) {
    throw new Error("SMTP_URL or SMTP_URL_FILE is required");
  }

  const internalTokens: WebConfig["internalTokens"] = {
    controlWorker: readOptionalSecret(environment.INTERNAL_CONTROL_TOKEN_FILE),
    translationWorker: readOptionalSecret(environment.INTERNAL_TRANSLATION_TOKEN_FILE),
    spoolDrainer: readOptionalSecret(environment.INTERNAL_SPOOL_DRAINER_TOKEN_FILE),
  };

  cached = {
    appEnv: environment.APP_ENV,
    providerProfile: environment.PROVIDER_PROFILE,
    databaseUrl: readSecret(environment.DATABASE_URL_FILE),
    redisUrl: readSecret(environment.REDIS_URL_FILE),
    s3Endpoint: environment.S3_ENDPOINT,
    s3PublicEndpoint: environment.S3_PUBLIC_ENDPOINT,
    s3Region: environment.S3_REGION,
    s3Bucket: environment.S3_BUCKET,
    s3Credentials: readOptionalSecret(environment.S3_CREDENTIALS_FILE),
    s3KmsKeyId: environment.S3_KMS_KEY_ID ?? null,
    archiveRequireKms: environment.ARCHIVE_REQUIRE_KMS === "true",
    liveKitBrowserUrl: environment.PUBLIC_LIVEKIT_URL,
    liveKitInternalUrl: environment.LIVEKIT_INTERNAL_URL,
    liveKitCredentials: readSecret(environment.LIVEKIT_CREDENTIALS_FILE),
    sessionSecret: readSecret(environment.SESSION_SECRET_FILE),
    csrfSecret: readSecret(environment.CSRF_SECRET_FILE),
    magicLinkSealKeyring: parseMagicLinkSealKeyring(
      readSecret(environment.MAGIC_LINK_SEAL_KEYS_FILE),
    ),
    internalTokens,
    egressLayoutSigningKey: readSecret(environment.EGRESS_LAYOUT_SIGNING_KEY_FILE),
    smtpUrl,
    publicUrl: environment.PUBLIC_BASE_URL,
    internalJwtIssuer: environment.INTERNAL_JWT_ISSUER ?? null,
    internalJwtAudience: environment.INTERNAL_JWT_AUDIENCE ?? null,
    internalServiceAccountToken: readOptionalSecret(
      environment.INTERNAL_SERVICE_ACCOUNT_TOKEN_FILE,
    ),
    podNamespace: environment.POD_NAMESPACE ?? null,
    trustedClientIpHeader: environment.TRUSTED_CLIENT_IP_HEADER ?? null,
  };
  return cached;
}
