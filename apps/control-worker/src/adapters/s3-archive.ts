import { createHash } from "node:crypto";
import {
  AbortMultipartUploadCommand,
  ChecksumAlgorithm,
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListMultipartUploadsCommand,
  ListObjectVersionsCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import type { Uuid } from "../orchestration/model";
import type { ArchiveVersionDeleter } from "./livekit-effects";

interface S3ArchiveConfig {
  readonly endpoint: string;
  readonly region: string;
  readonly accessKey: string;
  readonly secretKey: string;
  readonly forcePathStyle: boolean;
}

interface CreateOnceInput {
  readonly key: string;
  readonly body: Uint8Array;
  readonly contentType: string;
  readonly sha256: string;
}

interface StoredObject {
  readonly versionId: string;
  readonly size: number;
  readonly checksum: string;
}

export interface DiscoveredStoredObject extends StoredObject {
  readonly key: string;
  readonly contentType: string;
  readonly sha256: string;
}

interface VersionIdentifier {
  readonly Key: string;
  readonly VersionId: string;
}

export class S3ArchiveVersionDeleter implements ArchiveVersionDeleter {
  private readonly client: S3Client;

  constructor(
    private readonly bucket: string,
    config: S3ArchiveConfig,
  ) {
    this.client = new S3Client({
      endpoint: config.endpoint,
      region: config.region,
      forcePathStyle: config.forcePathStyle,
      maxAttempts: 2,
      requestHandler: new NodeHttpHandler({
        connectionTimeout: 3_000,
        requestTimeout: 10_000,
        socketTimeout: 10_000,
        throwOnRequestTimeout: true,
      }),
      credentials: {
        accessKeyId: config.accessKey,
        secretAccessKey: config.secretKey,
      },
    });
  }

  async readiness(): Promise<void> {
    await this.client.send(
      new ListObjectVersionsCommand({
        Bucket: this.bucket,
        Prefix: "v1/readiness/",
        MaxKeys: 1,
      }),
    );
  }

  async verifyObject(input: {
    key: string;
    versionId: string;
    size: number;
    checksum: string;
  }): Promise<boolean> {
    try {
      const head = await this.client.send(
        new HeadObjectCommand({
          Bucket: this.bucket,
          Key: input.key,
          VersionId: input.versionId,
          ChecksumMode: "ENABLED",
        }),
      );
      return head.ContentLength === input.size && objectChecksum(head) === input.checksum;
    } catch (error) {
      if (isMissingObjectVersion(error)) {
        return false;
      }
      throw error;
    }
  }

  async readObject(input: { key: string; versionId: string }): Promise<Uint8Array> {
    const result = await this.client.send(
      new GetObjectCommand({
        Bucket: this.bucket,
        Key: input.key,
        VersionId: input.versionId,
      }),
    );
    if (result.Body === undefined) {
      throw new Error("S3 object body is missing");
    }

    return result.Body.transformToByteArray();
  }

  async putCreateOnce(input: CreateOnceInput): Promise<StoredObject> {
    try {
      const result = await this.client.send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: input.key,
          Body: input.body,
          ContentType: input.contentType,
          Metadata: { sha256: input.sha256 },
          IfNoneMatch: "*",
          ChecksumAlgorithm: ChecksumAlgorithm.CRC64NVME,
        }),
      );
      if (result.VersionId === undefined || result.ChecksumCRC64NVME === undefined) {
        throw new Error("S3 create-once response lacks version or CRC64NVME");
      }

      return {
        versionId: result.VersionId,
        size: input.body.byteLength,
        checksum: result.ChecksumCRC64NVME,
      };
    } catch (error) {
      if (httpStatusCode(error) !== 412) {
        throw error;
      }

      return this.adoptExistingObject(input);
    }
  }

  async discoverObjects(prefix: string): Promise<readonly DiscoveredStoredObject[]> {
    const versions = await this.listObjectVersions(prefix);
    const discovered: DiscoveredStoredObject[] = [];
    for (const version of versions) {
      const head = await this.client.send(
        new HeadObjectCommand({
          Bucket: this.bucket,
          Key: version.Key,
          VersionId: version.VersionId,
          ChecksumMode: "ENABLED",
        }),
      );
      const checksum = objectChecksum(head);
      const sha256 =
        head.Metadata?.sha256 ??
        createHash("sha256")
          .update(
            await this.readObject({
              key: version.Key,
              versionId: version.VersionId,
            }),
          )
          .digest("hex");
      discovered.push({
        key: version.Key,
        versionId: version.VersionId,
        size: Number(head.ContentLength),
        checksum,
        contentType: head.ContentType ?? "application/octet-stream",
        sha256,
      });
    }
    return discovered;
  }

  async drain(consultationId: Uuid): Promise<boolean> {
    const prefix = `v1/meetings/${consultationId}/`;
    for (;;) {
      const uploadCount = await this.abortUploads(prefix);
      const objects = await this.listVersionIdentifiers(prefix, 1_000);
      if (objects.length === 0 && uploadCount === 0) {
        break;
      }

      if (objects.length > 0) {
        await this.deleteVersions(objects);
      }
    }

    for (let scan = 0; scan < 2; scan += 1) {
      if (!(await this.isPrefixEmpty(prefix))) {
        return false;
      }
    }

    return true;
  }

  destroy(): void {
    this.client.destroy();
  }

  private async adoptExistingObject(input: CreateOnceInput): Promise<StoredObject> {
    const head = await this.client.send(
      new HeadObjectCommand({
        Bucket: this.bucket,
        Key: input.key,
        ChecksumMode: "ENABLED",
      }),
    );
    if (
      head.VersionId === undefined ||
      head.ChecksumCRC64NVME === undefined ||
      head.Metadata?.sha256 !== input.sha256
    ) {
      throw new Error(`create-once object collision: ${input.key}`);
    }

    return {
      versionId: head.VersionId,
      size: Number(head.ContentLength),
      checksum: head.ChecksumCRC64NVME,
    };
  }

  private async abortUploads(prefix: string): Promise<number> {
    const uploads = await this.client.send(
      new ListMultipartUploadsCommand({
        Bucket: this.bucket,
        Prefix: prefix,
      }),
    );
    for (const upload of uploads.Uploads ?? []) {
      if (upload.Key === undefined || upload.UploadId === undefined) {
        continue;
      }

      await this.client.send(
        new AbortMultipartUploadCommand({
          Bucket: this.bucket,
          Key: upload.Key,
          UploadId: upload.UploadId,
        }),
      );
    }

    return uploads.Uploads?.length ?? 0;
  }

  private async listVersionIdentifiers(
    prefix: string,
    maxKeys: number,
  ): Promise<VersionIdentifier[]> {
    const listed = await this.client.send(
      new ListObjectVersionsCommand({
        Bucket: this.bucket,
        Prefix: prefix,
        MaxKeys: maxKeys,
      }),
    );
    return [...(listed.Versions ?? []), ...(listed.DeleteMarkers ?? [])].flatMap((entry) =>
      entry.Key === undefined || entry.VersionId === undefined
        ? []
        : [{ Key: entry.Key, VersionId: entry.VersionId }],
    );
  }

  private async listObjectVersions(prefix: string): Promise<VersionIdentifier[]> {
    const versions: VersionIdentifier[] = [];
    let keyMarker: string | undefined;
    let versionIdMarker: string | undefined;
    for (;;) {
      const listed = await this.client.send(
        new ListObjectVersionsCommand({
          Bucket: this.bucket,
          Prefix: prefix,
          KeyMarker: keyMarker,
          VersionIdMarker: versionIdMarker,
        }),
      );
      versions.push(
        ...(listed.Versions ?? []).flatMap((entry) =>
          entry.Key === undefined || entry.VersionId === undefined
            ? []
            : [{ Key: entry.Key, VersionId: entry.VersionId }],
        ),
      );
      if (!listed.IsTruncated) {
        return versions;
      }
      if (listed.NextKeyMarker === undefined) {
        throw new Error("S3 version listing omitted its continuation marker");
      }
      keyMarker = listed.NextKeyMarker;
      versionIdMarker = listed.NextVersionIdMarker;
    }
  }
  private async deleteVersions(objects: VersionIdentifier[]): Promise<void> {
    const deleted = await this.client.send(
      new DeleteObjectsCommand({
        Bucket: this.bucket,
        Delete: {
          Objects: objects,
          Quiet: true,
        },
      }),
    );
    const errorCount = deleted.Errors?.length ?? 0;
    if (errorCount > 0) {
      throw new Error(`S3 rejected ${String(errorCount)} version deletions`);
    }
  }

  private async isPrefixEmpty(prefix: string): Promise<boolean> {
    const [versions, uploads] = await Promise.all([
      this.client.send(
        new ListObjectVersionsCommand({
          Bucket: this.bucket,
          Prefix: prefix,
          MaxKeys: 1,
        }),
      ),
      this.client.send(
        new ListMultipartUploadsCommand({
          Bucket: this.bucket,
          Prefix: prefix,
          MaxUploads: 1,
        }),
      ),
    ]);
    return (
      (versions.Versions?.length ?? 0) === 0 &&
      (versions.DeleteMarkers?.length ?? 0) === 0 &&
      (uploads.Uploads?.length ?? 0) === 0
    );
  }
}
function objectChecksum(
  head: Readonly<
    Partial<
      Record<
        "ChecksumCRC64NVME" | "ChecksumCRC32C" | "ChecksumCRC32" | "ChecksumSHA256",
        string | undefined
      >
    >
  >,
): string {
  if (head.ChecksumCRC64NVME !== undefined) {
    return head.ChecksumCRC64NVME;
  }
  if (head.ChecksumCRC32C !== undefined) {
    return `CRC32C:${head.ChecksumCRC32C}`;
  }
  if (head.ChecksumCRC32 !== undefined) {
    return `CRC32:${head.ChecksumCRC32}`;
  }
  if (head.ChecksumSHA256 !== undefined) {
    return `SHA256:${head.ChecksumSHA256}`;
  }
  throw new Error("S3 object does not expose a checksum");
}

function isMissingObjectVersion(error: unknown): boolean {
  if (typeof error !== "object" || error === null || !("name" in error)) {
    return false;
  }
  return error.name === "NoSuchKey" || error.name === "NoSuchVersion";
}

function httpStatusCode(error: unknown): number | undefined {
  if (typeof error !== "object" || error === null || !("$metadata" in error)) {
    return undefined;
  }

  const metadata = error.$metadata;
  if (
    typeof metadata !== "object" ||
    metadata === null ||
    !("httpStatusCode" in metadata) ||
    typeof metadata.httpStatusCode !== "number"
  ) {
    return undefined;
  }

  return metadata.httpStatusCode;
}
