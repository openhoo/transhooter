import { test } from "bun:test";
import assert from "node:assert/strict";
import { S3ArchiveVersionDeleter } from "../src/adapters/s3-archive";

function archive() {
  return new S3ArchiveVersionDeleter("archive", {
    endpoint: "http://minio:9000",
    region: "eu-central-1",
    accessKey: "access",
    secretKey: "secret",
    forcePathStyle: true,
  });
}

const expected = {
  key: "v1/meetings/id/object",
  versionId: "version",
  size: 12,
  checksum: "checksum",
};

test("missing S3 object versions verify as reconciliation gaps", async () => {
  for (const error of [
    { name: "NoSuchVersion", $metadata: { httpStatusCode: 404 } },
    { name: "NoSuchKey" },
  ]) {
    const adapter = archive();
    Object.assign(adapter, {
      client: {
        send: async () => {
          throw error;
        },
      },
    });

    assert.equal(await adapter.verifyObject(expected), false);
  }
});

test("S3 lookup failures other than missing objects remain operational failures", async () => {
  for (const error of [
    { name: "S3ServiceException", $metadata: { httpStatusCode: 404 } },
    { name: "NotFound", $metadata: { httpStatusCode: 404 } },
    { name: "NoSuchBucket", $metadata: { httpStatusCode: 404 } },
    { name: "AuthorizationHeaderMalformed", $metadata: { httpStatusCode: 400 } },
    { name: "AccessDenied", $metadata: { httpStatusCode: 403 } },
    { name: "InternalError", $metadata: { httpStatusCode: 500 } },
    { name: "ServiceUnavailable", $metadata: { httpStatusCode: 503 } },
  ]) {
    const adapter = archive();
    Object.assign(adapter, {
      client: {
        send: async () => {
          throw error;
        },
      },
    });

    await assert.rejects(
      () => adapter.verifyObject(expected),
      (caught) => caught === error,
    );
  }
});
