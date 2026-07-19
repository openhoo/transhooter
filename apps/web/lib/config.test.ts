import { expect, mock, test } from "bun:test";

mock.module("server-only", () => ({}));

// Import after replacing Next's server-only guard so the pure environment parser can run in Bun.
const { parseWebEnvironment } = await import("./config");

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
  EGRESS_LAYOUT_SIGNING_KEY_FILE: "/run/secrets/egress-layout-signing-key",
  SMTP_URL: "smtp://mail.example.test:2525",
  PUBLIC_BASE_URL: "https://app.example.test",
  ARCHIVE_REQUIRE_KMS: "false",
  INTERNAL_JWT_ISSUER: "https://kubernetes.default.svc",
  INTERNAL_JWT_AUDIENCE: "transhooter-internal",
  POD_NAMESPACE: "transhooter",
};

test("production requires an explicit trusted client IP boundary header", () => {
  expect(() => parseWebEnvironment(baseEnvironment)).toThrow(
    "TRUSTED_CLIENT_IP_HEADER is required in production",
  );
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
