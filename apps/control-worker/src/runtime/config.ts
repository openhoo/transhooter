import { readFile } from "node:fs/promises";
import { z } from "zod";

const environmentSchema = z.object({
  APP_ENV: z.enum(["development", "test", "production"]),
  DATABASE_URL_FILE: z.string().min(1),
  REDIS_URL_FILE: z.string().min(1),
  LIVEKIT_INTERNAL_URL: z.url(),
  LIVEKIT_CREDENTIALS_FILE: z.string().min(1),
  INTERNAL_TOKEN_FILE: z.string().min(1),
  EGRESS_LAYOUT_SIGNING_KEY_FILE: z.string().min(1),
  EGRESS_LAYOUT_URL: z.url().default("http://web:3000/egress-layout"),
  S3_ENDPOINT: z.url(),
  S3_BUCKET: z.string().min(1),
  S3_REGION: z.string().min(1),
  S3_CREDENTIALS_FILE: z.string().min(1),
  INSTANCE_ID: z.uuid(),
  POLL_INTERVAL_MS: z.coerce.number().int().min(50).max(30_000).default(500),
  HEALTH_PORT: z.coerce.number().int().min(1).max(65_535).default(8080),
  TEST_FAULT_CONTROL_FILE: z.string().min(1).optional(),
});

const livekitCredentialsSchema = z
  .object({
    apiKey: z.string().min(1),
    apiSecret: z.string().min(1),
  })
  .strict();

const s3CredentialsSchema = z
  .object({
    accessKeyId: z.string().min(1),
    secretAccessKey: z.string().min(1),
  })
  .strict();

interface LivekitRuntimeConfig {
  readonly url: string;
  readonly apiKey: string;
  readonly apiSecret: string;
}

interface S3RuntimeConfig {
  readonly endpoint: string;
  readonly bucket: string;
  readonly region: string;
  readonly accessKey: string;
  readonly secretKey: string;
  readonly forcePathStyle: boolean;
}

export interface RuntimeConfig {
  readonly appEnv: "development" | "test" | "production";
  readonly databaseUrl: string;
  readonly redisUrl: string;
  readonly workerId: string;
  readonly pollIntervalMs: number;
  readonly healthPort: number;
  readonly livekit: LivekitRuntimeConfig;
  readonly egressLayoutUrl: string;
  readonly internalToken: () => Promise<string>;
  readonly egressLayoutSigningKey: string;
  readonly testFaultControlFile: string | null;
  readonly s3: S3RuntimeConfig;
}

export async function loadConfig(environment: NodeJS.ProcessEnv): Promise<RuntimeConfig> {
  const parsedEnvironment = environmentSchema.parse(environment);
  if (
    parsedEnvironment.APP_ENV !== "test" &&
    parsedEnvironment.TEST_FAULT_CONTROL_FILE !== undefined
  ) {
    throw new Error("test fault controls require APP_ENV=test");
  }

  const [databaseUrlValue, redisUrlValue, livekitValue, egressLayoutSigningKey, s3Value] =
    await Promise.all([
      readSecret(parsedEnvironment.DATABASE_URL_FILE),
      readSecret(parsedEnvironment.REDIS_URL_FILE),
      readJsonSecret(parsedEnvironment.LIVEKIT_CREDENTIALS_FILE),
      readSecret(parsedEnvironment.EGRESS_LAYOUT_SIGNING_KEY_FILE),
      readJsonSecret(parsedEnvironment.S3_CREDENTIALS_FILE),
    ]);

  const livekitCredentials = livekitCredentialsSchema.parse(livekitValue);
  const s3Credentials = s3CredentialsSchema.parse(s3Value);

  return {
    appEnv: parsedEnvironment.APP_ENV,
    databaseUrl: z.url().parse(databaseUrlValue),
    redisUrl: z.url().parse(redisUrlValue),
    workerId: parsedEnvironment.INSTANCE_ID,
    pollIntervalMs: parsedEnvironment.POLL_INTERVAL_MS,
    healthPort: parsedEnvironment.HEALTH_PORT,
    egressLayoutUrl: parsedEnvironment.EGRESS_LAYOUT_URL,
    testFaultControlFile:
      parsedEnvironment.APP_ENV === "test"
        ? (parsedEnvironment.TEST_FAULT_CONTROL_FILE ?? null)
        : null,
    internalToken: async () => readSecret(parsedEnvironment.INTERNAL_TOKEN_FILE),
    egressLayoutSigningKey,
    livekit: {
      url: parsedEnvironment.LIVEKIT_INTERNAL_URL,
      apiKey: livekitCredentials.apiKey,
      apiSecret: livekitCredentials.apiSecret,
    },
    s3: {
      endpoint: parsedEnvironment.S3_ENDPOINT,
      bucket: parsedEnvironment.S3_BUCKET,
      region: parsedEnvironment.S3_REGION,
      accessKey: s3Credentials.accessKeyId,
      secretKey: s3Credentials.secretAccessKey,
      forcePathStyle: true,
    },
  };
}

async function readSecret(path: string): Promise<string> {
  const value = (await readFile(path, "utf8")).trim();
  if (value.length === 0) {
    throw new Error(`secret file is empty: ${path}`);
  }

  return value;
}

async function readJsonSecret(path: string): Promise<unknown> {
  const value = await readSecret(path);
  return JSON.parse(value) as unknown;
}
