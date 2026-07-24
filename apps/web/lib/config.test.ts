import { expect, mock, test } from "bun:test";

mock.module("server-only", () => ({}));

// Import after replacing Next's server-only guard so the pure environment parser can run in Bun.
const { parseMagicLinkSealKeyring, parseWebEnvironment } = await import("./server/config");

const baseEnvironment: NodeJS.ProcessEnv = {
  NODE_ENV: "production",
  APP_ENV: "production",
  PROVIDER_PROFILE: "google-eu",
  DATABASE_URL_FILE: "/run/secrets/database-url",
  REDIS_URL_FILE: "/run/secrets/redis-url",
  S3_ENDPOINT: "https://s3.example.test",
  S3_PUBLIC_ENDPOINT: "https://s3.example.test",
  S3_REGION: "eu-central-1",
  S3_BUCKET: "transhooter",
  PUBLIC_LIVEKIT_URL: "wss://livekit.example.test",
  LIVEKIT_INTERNAL_URL: "ws://livekit.example.test",
  LIVEKIT_CREDENTIALS_FILE: "/run/secrets/livekit-credentials",
  SESSION_SECRET_FILE: "/run/secrets/session-secret",
  CSRF_SECRET_FILE: "/run/secrets/csrf-secret",
  MAGIC_LINK_SEAL_KEYS_FILE: "/run/secrets/magic-link-seal-keys",
  EGRESS_LAYOUT_SIGNING_KEY_FILE: "/run/secrets/egress-layout-signing-key",
  SMTP_URL: "smtp://mail.example.test:2525",
  PUBLIC_BASE_URL: "https://app.example.test",
  ARCHIVE_REQUIRE_KMS: "false",
  INTERNAL_JWT_ISSUER: "https://kubernetes.default.svc",
  INTERNAL_JWT_AUDIENCE: "transhooter-internal",
  POD_NAMESPACE: "transhooter",
  INTERNAL_SERVICE_ACCOUNT_PREFIX: "transhooter",
};

test("Kubernetes JWT authentication requires the service-account prefix", () => {
  const parsed = parseWebEnvironment(baseEnvironment);

  expect(parsed.INTERNAL_JWT_ISSUER).toBe("https://kubernetes.default.svc");
  expect(parsed.INTERNAL_JWT_AUDIENCE).toBe("transhooter-internal");
  expect(parsed.POD_NAMESPACE).toBe("transhooter");
  expect(parsed.INTERNAL_SERVICE_ACCOUNT_PREFIX).toBe("transhooter");

  const { INTERNAL_SERVICE_ACCOUNT_PREFIX: _prefix, ...withoutPrefix } = baseEnvironment;
  expect(() => parseWebEnvironment(withoutPrefix)).toThrow(
    "Per-caller internal Bearers or JWT issuer/audience/namespace/service-account-prefix configuration is required",
  );
});

test("bearer authentication omits and ignores the Kubernetes service-account prefix", () => {
  const bearerEnvironment: NodeJS.ProcessEnv = {
    ...baseEnvironment,
    INTERNAL_CONTROL_TOKEN_FILE: "/run/secrets/internal-control-token",
    INTERNAL_TRANSLATION_TOKEN_FILE: "/run/secrets/internal-translation-token",
    INTERNAL_SPOOL_DRAINER_TOKEN_FILE: "/run/secrets/internal-spool-drainer-token",
  };
  const { INTERNAL_SERVICE_ACCOUNT_PREFIX: _prefix, ...withoutPrefix } = bearerEnvironment;

  expect(parseWebEnvironment(withoutPrefix).INTERNAL_SERVICE_ACCOUNT_PREFIX).toBeUndefined();
  expect(
    parseWebEnvironment({
      ...bearerEnvironment,
      INTERNAL_SERVICE_ACCOUNT_PREFIX: "ignored-prefix",
    }).INTERNAL_SERVICE_ACCOUNT_PREFIX,
  ).toBeUndefined();
});

test("production supports direct-local admission when no trusted ingress boundary is configured", () => {
  expect(parseWebEnvironment(baseEnvironment).TRUSTED_CLIENT_IP_HEADER).toBe(undefined);
});

test("trusted client IP header is validated and normalized", () => {
  expect(
    parseWebEnvironment({
      ...baseEnvironment,
      TRUSTED_CLIENT_IP_HEADER: "X-Transhooter-Client-IP",
    }).TRUSTED_CLIENT_IP_HEADER,
  ).toBe("x-transhooter-client-ip");
  expect(() =>
    parseWebEnvironment({
      ...baseEnvironment,
      TRUSTED_CLIENT_IP_HEADER: "x-forwarded-for, x-real-ip",
    }),
  ).toThrow();
  expect(() =>
    parseWebEnvironment({
      ...baseEnvironment,
      TRUSTED_CLIENT_IP_HEADER: "x-forwarded-for",
    }),
  ).toThrow("TRUSTED_CLIENT_IP_HEADER must be a dedicated boundary header");
});

test("development and test use direct-local mode without a boundary header", () => {
  expect(
    parseWebEnvironment({ ...baseEnvironment, APP_ENV: "test" }).TRUSTED_CLIENT_IP_HEADER,
  ).toBe(undefined);
});

test("magic-link seal keyrings require a present current 32-byte key and validate every key", () => {
  const oldKey = Buffer.alloc(32, 1).toString("base64");
  const currentKey = Buffer.alloc(32, 2).toString("base64");
  const parsed = parseMagicLinkSealKeyring(
    JSON.stringify({
      currentKeyId: "current",
      keys: { old: oldKey, current: currentKey },
    }),
  );
  expect(parsed.currentKeyId).toBe("current");
  expect(parsed.keys.get("old")).toEqual(new Uint8Array(Buffer.alloc(32, 1)));
  expect(parsed.keys.get("current")).toEqual(new Uint8Array(Buffer.alloc(32, 2)));

  expect(() =>
    parseMagicLinkSealKeyring(
      JSON.stringify({ currentKeyId: "missing", keys: { current: currentKey } }),
    ),
  ).toThrow("must contain its current key");
  expect(() =>
    parseMagicLinkSealKeyring(
      JSON.stringify({
        currentKeyId: "current",
        keys: { current: currentKey, invalid: Buffer.alloc(31).toString("base64") },
      }),
    ),
  ).toThrow(/invalid.*canonical base64|invalid.*32 bytes/);
});
