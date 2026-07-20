import { describe, expect, mock, test } from "bun:test";
import { createHash } from "node:crypto";
import type { WebConfig } from "./config";

mock.module("server-only", () => ({}));

type CommandInput = Record<string, unknown>;
type SentCommand = { kind: string; input: CommandInput };

const sent: SentCommand[] = [];
let respond: (command: SentCommand) => unknown = () => ({});

class MockCommand {
  constructor(
    readonly kind: string,
    readonly input: CommandInput,
  ) {}
}

function command(kind: string) {
  return class extends MockCommand {
    constructor(input: CommandInput) {
      super(kind, input);
    }
  };
}

mock.module("@aws-sdk/client-s3", () => ({
  AbortMultipartUploadCommand: command("AbortMultipartUpload"),
  DeleteObjectsCommand: command("DeleteObjects"),
  GetObjectCommand: command("GetObject"),
  HeadBucketCommand: command("HeadBucket"),
  HeadObjectCommand: command("HeadObject"),
  ListMultipartUploadsCommand: command("ListMultipartUploads"),
  ListObjectVersionsCommand: command("ListObjectVersions"),
  PutObjectCommand: command("PutObject"),
  PutObjectLegalHoldCommand: command("PutObjectLegalHold"),
  S3Client: class {
    async send(command: MockCommand) {
      const record = { kind: command.kind, input: command.input };
      sent.push(record);
      return respond(record);
    }
  },
}));

// This import must follow the module mocks so the adapter captures the test S3 client.
const { S3ArchiveAdapter } = await import("./runtime-adapters");

const config = {
  appEnv: "test",
  providerProfile: "fixture",
  databaseUrl: "postgres://test",
  redisUrl: "redis://test",
  s3Endpoint: "https://s3.example.test",
  s3PublicEndpoint: "https://public-s3.example.test",
  s3Region: "eu-central-1",
  s3Bucket: "archive",
  s3Credentials: null,
  s3KmsKeyId: null,
  archiveRequireKms: false,
  liveKitBrowserUrl: "wss://livekit.example.test",
  liveKitInternalUrl: "ws://livekit.example.test",
  liveKitCredentials: "{}",
  sessionSecret: "session",
  csrfSecret: "csrf",
  egressLayoutSigningKey: "egress",
  magicLinkSealKeyring: {
    currentKeyId: "test",
    keys: new Map([["test", new Uint8Array(32)]]),
  },
  internalTokens: { controlWorker: null, translationWorker: null, spoolDrainer: null },
  smtpUrl: "smtp://mail.example.test",
  publicUrl: "https://app.example.test",
  internalJwtIssuer: null,
  internalJwtAudience: null,
  internalServiceAccountToken: null,
  podNamespace: null,
  trustedClientIpHeader: null,
} satisfies WebConfig;

const consultationId = "10000000-0000-4000-8000-000000000001";

describe("S3 archive integrity and pagination", () => {
  test("rejects a caller checksum mismatch before uploading", async () => {
    sent.length = 0;
    respond = () => {
      throw new Error("upload must not be attempted");
    };
    const adapter = new S3ArchiveAdapter(config);

    await expect(
      adapter.putCreateOnce({
        key: "v1/meetings/test/archive.json",
        body: new TextEncoder().encode("archive"),
        contentType: "application/json",
        checksum: "0".repeat(64),
      }),
    ).rejects.toThrow("caller-provided checksum");
    expect(sent).toHaveLength(0);
  });

  test("uploads only after matching the computed SHA-256", async () => {
    sent.length = 0;
    respond = () => ({ VersionId: "version-1", ChecksumCRC64NVME: "crc" });
    const adapter = new S3ArchiveAdapter(config);
    const body = new TextEncoder().encode("archive");

    await adapter.putCreateOnce({
      key: "v1/meetings/test/archive.json",
      body,
      contentType: "application/json",
      checksum: createHash("sha256").update(body).digest("hex"),
    });

    expect(sent).toHaveLength(1);
    expect(sent[0]?.kind).toBe("PutObject");
  });

  test("round-trips both version markers without skipping or repeating boundaries", async () => {
    sent.length = 0;
    const pages = [
      {
        Versions: [{ Key: "same-key", VersionId: "v3" }],
        DeleteMarkers: [{ Key: "deleted-key", VersionId: "delete-v1" }],
        IsTruncated: true,
        NextKeyMarker: "same-key",
        NextVersionIdMarker: "v3",
      },
      {
        Versions: [
          { Key: "same-key", VersionId: "v2" },
          { Key: "same-key", VersionId: "v1" },
        ],
        IsTruncated: false,
      },
    ];
    respond = () => pages.shift() ?? {};
    const adapter = new S3ArchiveAdapter(config);

    const first = await adapter.listMeetingVersions(consultationId);
    expect(first.cursor).not.toBeNull();
    expect(first.cursor).not.toContain("same-key");
    const second = await adapter.listMeetingVersions(consultationId, first.cursor ?? undefined);

    expect(sent[1]?.input.KeyMarker).toBe("same-key");
    expect(sent[1]?.input.VersionIdMarker).toBe("v3");
    expect([...first.versions, ...second.versions]).toEqual([
      { key: "same-key", versionId: "v3" },
      { key: "deleted-key", versionId: "delete-v1" },
      { key: "same-key", versionId: "v2" },
      { key: "same-key", versionId: "v1" },
    ]);
    expect(second.cursor).toBeNull();
  });

  test("discovers multipart uploads beyond the first 1,000 using both markers", async () => {
    sent.length = 0;
    const firstUploads = Array.from({ length: 1_000 }, (_, index) => ({
      Key: `v1/meetings/${consultationId}/${String(index).padStart(4, "0")}`,
      UploadId: `upload-${String(index)}`,
    }));
    const pages = [
      {
        Uploads: firstUploads,
        IsTruncated: true,
        NextKeyMarker: firstUploads[999]?.Key,
        NextUploadIdMarker: firstUploads[999]?.UploadId,
      },
      {
        Uploads: [
          { Key: `v1/meetings/${consultationId}/1000`, UploadId: "upload-1000" },
          { Key: `v1/meetings/${consultationId}/1001`, UploadId: "upload-1001" },
        ],
        IsTruncated: false,
      },
    ];
    respond = () => pages.shift() ?? {};
    const adapter = new S3ArchiveAdapter(config);

    const uploads = await adapter.listMultipart(consultationId);

    expect(uploads).toHaveLength(1_002);
    expect(sent).toHaveLength(2);
    expect(sent[1]?.input.KeyMarker).toBe(firstUploads[999]?.Key);
    expect(sent[1]?.input.UploadIdMarker).toBe(firstUploads[999]?.UploadId);
    expect(uploads[1_001]).toEqual({
      key: `v1/meetings/${consultationId}/1001`,
      uploadId: "upload-1001",
    });
  });
});
