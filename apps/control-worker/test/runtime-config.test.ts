import { test } from "bun:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadConfig } from "../src/runtime/config";

test("projected internal bearer is reloaded after file rotation", async () => {
  const directory = await mkdtemp(join(tmpdir(), "transhooter-config-"));
  const secret = async (name: string, value: string): Promise<string> => {
    const path = join(directory, name);
    await writeFile(path, value, "utf8");
    return path;
  };

  try {
    const internalTokenPath = await secret("internal-token", "first");
    const config = await loadConfig({
      APP_ENV: "test",
      DATABASE_URL_FILE: await secret("database-url", "postgres://db/test"),
      REDIS_URL_FILE: await secret("redis-url", "redis://redis:6379"),
      LIVEKIT_INTERNAL_URL: "http://livekit:7880",
      LIVEKIT_CREDENTIALS_FILE: await secret(
        "livekit.json",
        JSON.stringify({ apiKey: "key", apiSecret: "secret" }),
      ),
      INTERNAL_TOKEN_FILE: internalTokenPath,
      EGRESS_LAYOUT_SIGNING_KEY_FILE: await secret("layout-key", "layout-secret"),
      S3_ENDPOINT: "http://minio:9000",
      S3_BUCKET: "archive",
      S3_REGION: "eu-central-1",
      S3_CREDENTIALS_FILE: await secret(
        "s3.json",
        JSON.stringify({ accessKeyId: "access", secretAccessKey: "secret" }),
      ),
      INSTANCE_ID: "50000000-0000-4000-8000-000000000001",
    });

    assert.equal(await config.internalToken(), "first");
    await writeFile(internalTokenPath, "second", "utf8");
    assert.equal(await config.internalToken(), "second");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
