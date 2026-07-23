import "server-only";
import { createCipheriv, createDecipheriv, createHash, randomBytes, randomUUID } from "node:crypto";
import {
  AbortMultipartUploadCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadBucketCommand,
  HeadObjectCommand,
  ListMultipartUploadsCommand,
  ListObjectVersionsCommand,
  PutObjectCommand,
  PutObjectLegalHoldCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { DataPacket_Kind } from "@livekit/protocol";
import type { MagicLinkPurpose } from "@transhooter/contracts";
import {
  type ActiveMagicLink,
  DomainError,
  type LiveKitRoomPort,
  type LiveKitTokenPort,
  type MagicLinkRecord,
  type MagicLinkTokenSealer,
  type MailPort,
  type ObjectStoragePort,
  type UUID,
  type VerifiedWebhook,
  type WebhookVerifier,
} from "@transhooter/server-core";
import {
  AccessToken,
  authorizeHeader,
  RoomServiceClient,
  WebhookReceiver,
} from "livekit-server-sdk";
import nodemailer from "nodemailer";
import { z } from "zod";
import {
  normalizedEgressWebhookKind,
  normalizeEgressRequestSource,
} from "../shared/livekit-webhook";
import type { WebConfig } from "./config";

function magicLinkTokenAad(record: MagicLinkRecord): Buffer {
  return Buffer.from(
    JSON.stringify([
      "transhooter.magic-link-token.v1",
      record.id,
      record.userId,
      record.purpose,
      record.consultationId,
      record.sessionId,
    ]),
    "utf8",
  );
}

export class AesGcmMagicLinkTokenSealer implements MagicLinkTokenSealer {
  constructor(private readonly keyring: WebConfig["magicLinkSealKeyring"]) {}

  seal(rawToken: Uint8Array, record: MagicLinkRecord) {
    const key = this.keyring.keys.get(this.keyring.currentKeyId);
    if (!key) {
      throw new Error("Current magic-link seal key is unavailable");
    }
    const nonce = randomBytes(12);
    const cipher = createCipheriv("aes-256-gcm", key, nonce);
    cipher.setAAD(magicLinkTokenAad(record));
    const ciphertext = Buffer.concat([cipher.update(rawToken), cipher.final()]);
    const authenticationTag = cipher.getAuthTag();
    return {
      sealedRawToken: Buffer.concat([nonce, authenticationTag, ciphertext]).toString("base64url"),
      keyId: this.keyring.currentKeyId,
    };
  }

  open(link: ActiveMagicLink): Uint8Array {
    const key = this.keyring.keys.get(link.keyId);
    if (!key) {
      throw new Error("Magic-link seal key is unavailable");
    }
    if (!/^[A-Za-z0-9_-]+$/u.test(link.sealedRawToken)) {
      throw new Error("Magic-link ciphertext is malformed");
    }
    const payload = Buffer.from(link.sealedRawToken, "base64url");
    if (payload.length <= 28 || payload.toString("base64url") !== link.sealedRawToken) {
      throw new Error("Magic-link ciphertext is malformed");
    }
    const decipher = createDecipheriv("aes-256-gcm", key, payload.subarray(0, 12));
    decipher.setAAD(magicLinkTokenAad(link.record));
    decipher.setAuthTag(payload.subarray(12, 28));
    return new Uint8Array(Buffer.concat([decipher.update(payload.subarray(28)), decipher.final()]));
  }
}

const LiveKitCredentialsSchema = z.object({
  apiKey: z.string().min(1),
  apiSecret: z.string().min(1),
});
const S3CredentialsSchema = z.object({
  accessKeyId: z.string().min(1),
  secretAccessKey: z.string().min(1),
  sessionToken: z.string().min(1).optional(),
});
type S3Credentials =
  | { accessKeyId: string; secretAccessKey: string; sessionToken: string }
  | { accessKeyId: string; secretAccessKey: string };
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(",")}]`;
  }

  const entries = Object.entries(value).sort(([left], [right]) => left.localeCompare(right));
  const properties = entries.map(
    ([key, child]) => `${JSON.stringify(key)}:${canonicalJson(child)}`,
  );
  return `{${properties.join(",")}}`;
}

export function cryptoPrimitives() {
  return {
    clock: {
      now: () => new Date(),
    },
    ids: {
      uuid: () => randomUUID() as UUID,
    },
    tokens: {
      bytes: (length: number) => randomBytes(length),
    },
    hashing: {
      sha256: (value: Uint8Array | string) => createHash("sha256").update(value).digest("hex"),
      sha256Text: (value: string) => createHash("sha256").update(value).digest("hex"),
      sha256Canonical: (value: unknown) =>
        createHash("sha256").update(canonicalJson(value)).digest("hex"),
    },
  };
}

export class SmtpMailAdapter implements MailPort {
  readonly #transport;

  constructor(
    smtpUrl: string,
    private readonly publicUrl: string,
  ) {
    this.#transport = nodemailer.createTransport(smtpUrl);
  }

  async sendMagicLink(input: {
    to: string;
    purpose: MagicLinkPurpose;
    url: string;
    expiresAt: Date;
  }): Promise<void> {
    const purpose =
      input.purpose === "sign_in"
        ? "Sign in"
        : input.purpose === "consultation_invite"
          ? "Join consultation"
          : "Confirm archive action";
    await this.#transport.sendMail({
      to: input.to,
      from: `Transhooter <no-reply@${new URL(this.publicUrl).hostname}>`,
      subject: `${purpose} · Transhooter`,
      text: `${purpose}: ${input.url}\n\nThis private link expires at ${input.expiresAt.toISOString()}. Opening it does not consume it; finish verification in the browser.`,
      headers: { "X-Auto-Response-Suppress": "All" },
    });
  }

  async verify(): Promise<boolean> {
    return this.#transport.verify();
  }

  close(): void {
    this.#transport.close();
  }
}

function parseS3Credentials(serialized: string | null): S3Credentials | undefined {
  if (!serialized) {
    return undefined;
  }
  const parsed = S3CredentialsSchema.parse(JSON.parse(serialized) as unknown);
  if (parsed.sessionToken) {
    return {
      accessKeyId: parsed.accessKeyId,
      secretAccessKey: parsed.secretAccessKey,
      sessionToken: parsed.sessionToken,
    };
  }
  return {
    accessKeyId: parsed.accessKeyId,
    secretAccessKey: parsed.secretAccessKey,
  };
}

function isNotFoundError(error: unknown): boolean {
  if (!error || typeof error !== "object" || !("$metadata" in error)) {
    return false;
  }
  const metadata = error.$metadata;
  return Boolean(
    metadata &&
      typeof metadata === "object" &&
      "httpStatusCode" in metadata &&
      metadata.httpStatusCode === 404,
  );
}

type VersionCursor = {
  keyMarker: string;
  versionIdMarker: string | null;
};

function encodeVersionCursor(cursor: VersionCursor): string {
  return Buffer.from(JSON.stringify(cursor), "utf8").toString("base64url");
}

function decodeVersionCursor(cursor: string): VersionCursor {
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8")) as unknown;
  } catch {
    throw new Error("Invalid S3 version cursor");
  }
  if (
    !parsed ||
    typeof parsed !== "object" ||
    !("keyMarker" in parsed) ||
    typeof parsed.keyMarker !== "string" ||
    !("versionIdMarker" in parsed) ||
    (parsed.versionIdMarker !== null && typeof parsed.versionIdMarker !== "string")
  ) {
    throw new Error("Invalid S3 version cursor");
  }
  return {
    keyMarker: parsed.keyMarker,
    versionIdMarker: parsed.versionIdMarker,
  };
}

export class S3ArchiveAdapter implements ObjectStoragePort {
  readonly #client: S3Client;
  readonly #publicClient: S3Client;
  readonly #credentials: S3Credentials | undefined;

  constructor(private readonly config: WebConfig) {
    this.#credentials = parseS3Credentials(config.s3Credentials);
    const base = {
      region: config.s3Region,
      endpoint: config.s3Endpoint,
      forcePathStyle: true as const,
    };
    this.#client = new S3Client(
      this.#credentials ? { ...base, credentials: this.#credentials } : base,
    );
    const publicBase = {
      region: config.s3Region,
      endpoint: config.s3PublicEndpoint,
      forcePathStyle: true as const,
    };
    this.#publicClient = new S3Client(
      this.#credentials ? { ...publicBase, credentials: this.#credentials } : publicBase,
    );
  }

  destroy(): void {
    this.#client.destroy();
    this.#publicClient.destroy();
  }

  async putCreateOnce(input: {
    key: string;
    body: Uint8Array;
    contentType: string;
    checksum: string;
  }) {
    const sha256 = createHash("sha256").update(input.body).digest("hex");
    if (input.checksum !== sha256) {
      throw new Error("Archive body SHA-256 does not match the caller-provided checksum");
    }
    const kmsOptions = this.config.archiveRequireKms
      ? {
          ServerSideEncryption: "aws:kms" as const,
          SSEKMSKeyId: this.config.s3KmsKeyId ?? undefined,
          BucketKeyEnabled: true,
        }
      : {};
    const result = await this.#client.send(
      new PutObjectCommand({
        Bucket: this.config.s3Bucket,
        Key: input.key,
        Body: input.body,
        ContentType: input.contentType,
        Metadata: { sha256 },
        IfNoneMatch: "*",
        ChecksumAlgorithm: "CRC64NVME",
        ...kmsOptions,
      }),
    );
    if (!result.VersionId || !result.ChecksumCRC64NVME) {
      throw new Error("S3 did not return a version and CRC64NVME checksum");
    }
    return {
      versionId: result.VersionId,
      size: input.body.byteLength,
      checksum: result.ChecksumCRC64NVME,
    };
  }

  async head(key: string): Promise<{
    versionId: string;
    size: number;
    checksum: string;
    sha256: string;
  } | null> {
    try {
      const result = await this.#client.send(
        new HeadObjectCommand({
          Bucket: this.config.s3Bucket,
          Key: key,
          ChecksumMode: "ENABLED",
        }),
      );
      if (
        !result.VersionId ||
        result.ContentLength === undefined ||
        !result.ChecksumCRC64NVME ||
        !result.Metadata?.sha256
      ) {
        throw new Error("S3 object metadata is incomplete");
      }
      return {
        versionId: result.VersionId,
        size: result.ContentLength,
        checksum: result.ChecksumCRC64NVME,
        sha256: result.Metadata.sha256,
      };
    } catch (error) {
      if (isNotFoundError(error)) {
        return null;
      }
      throw error;
    }
  }

  async verify(input: {
    key: string;
    versionId: string;
    size: number;
    checksum: string;
  }): Promise<boolean> {
    const result = await this.#client.send(
      new HeadObjectCommand({
        Bucket: this.config.s3Bucket,
        Key: input.key,
        VersionId: input.versionId,
        ChecksumMode: "ENABLED",
      }),
    );
    return result.ContentLength === input.size && result.ChecksumCRC64NVME === input.checksum;
  }

  async listMeetingVersions(consultationId: UUID, cursor?: string) {
    const markers = cursor ? decodeVersionCursor(cursor) : undefined;
    const result = await this.#client.send(
      new ListObjectVersionsCommand({
        Bucket: this.config.s3Bucket,
        Prefix: `v1/meetings/${consultationId}/`,
        KeyMarker: markers?.keyMarker,
        VersionIdMarker: markers?.versionIdMarker ?? undefined,
      }),
    );
    const versions = [...(result.Versions ?? []), ...(result.DeleteMarkers ?? [])].flatMap(
      (version) =>
        version.Key && version.VersionId
          ? [{ key: version.Key, versionId: version.VersionId }]
          : [],
    );
    if (!result.IsTruncated) {
      return { versions, cursor: null };
    }
    if (!result.NextKeyMarker) {
      throw new Error("S3 version listing omitted its next key marker");
    }
    return {
      versions,
      cursor: encodeVersionCursor({
        keyMarker: result.NextKeyMarker,
        versionIdMarker: result.NextVersionIdMarker ?? null,
      }),
    };
  }

  async deleteVersions(versions: readonly { key: string; versionId: string }[]): Promise<void> {
    if (versions.length === 0) {
      return;
    }
    const result = await this.#client.send(
      new DeleteObjectsCommand({
        Bucket: this.config.s3Bucket,
        Delete: {
          Quiet: false,
          Objects: versions.map(({ key, versionId }) => ({ Key: key, VersionId: versionId })),
        },
      }),
    );
    const errorCount = result.Errors?.length ?? 0;
    if (errorCount > 0) {
      throw new Error(`S3 rejected ${String(errorCount)} version deletions`);
    }
  }

  async listMultipart(consultationId: UUID) {
    const uploads: { key: string; uploadId: string }[] = [];
    let keyMarker: string | undefined;
    let uploadIdMarker: string | undefined;
    for (;;) {
      const result = await this.#client.send(
        new ListMultipartUploadsCommand({
          Bucket: this.config.s3Bucket,
          Prefix: `v1/meetings/${consultationId}/`,
          KeyMarker: keyMarker,
          UploadIdMarker: uploadIdMarker,
        }),
      );
      uploads.push(
        ...(result.Uploads ?? []).flatMap((upload) =>
          upload.Key && upload.UploadId ? [{ key: upload.Key, uploadId: upload.UploadId }] : [],
        ),
      );
      if (!result.IsTruncated) {
        return uploads;
      }
      if (!result.NextKeyMarker || !result.NextUploadIdMarker) {
        throw new Error("S3 multipart listing omitted its continuation markers");
      }
      keyMarker = result.NextKeyMarker;
      uploadIdMarker = result.NextUploadIdMarker;
    }
  }

  async abortMultipart(key: string, uploadId: string): Promise<void> {
    await this.#client.send(
      new AbortMultipartUploadCommand({
        Bucket: this.config.s3Bucket,
        Key: key,
        UploadId: uploadId,
      }),
    );
  }

  async presignGet(key: string, versionId: string, expiresSeconds: 300): Promise<string> {
    const command = new GetObjectCommand({
      Bucket: this.config.s3Bucket,
      Key: key,
      VersionId: versionId,
      ChecksumMode: "ENABLED",
    });
    return getSignedUrl(this.#publicClient, command, { expiresIn: expiresSeconds });
  }

  async setLegalHold(key: string, versionId: string, enabled: boolean): Promise<void> {
    await this.#client.send(
      new PutObjectLegalHoldCommand({
        Bucket: this.config.s3Bucket,
        Key: key,
        VersionId: versionId,
        LegalHold: { Status: enabled ? "ON" : "OFF" },
      }),
    );
  }

  async ready(): Promise<boolean> {
    await this.#client.send(new HeadBucketCommand({ Bucket: this.config.s3Bucket }));
    return true;
  }
}

function createRoomsPort(roomsClient: RoomServiceClient): LiveKitRoomPort {
  return {
    async findRoomByName(name) {
      const room = (await roomsClient.listRooms([name]))[0];
      return room ? { name: room.name, metadata: room.metadata } : null;
    },
    async createRoom(input) {
      const room = await roomsClient.createRoom({
        name: input.name,
        metadata: input.metadata,
        emptyTimeout: 300,
        departureTimeout: 120,
      });
      return { sid: room.sid };
    },
    async closeRoom(name) {
      await roomsClient.deleteRoom(name);
    },
    async listAllowedParticipants(roomName) {
      const participants = await roomsClient.listParticipants(roomName);
      return participants
        .filter(
          (participant) =>
            UUID_PATTERN.test(participant.identity) &&
            (participant.attributes["consultation.role"] === "employee" ||
              participant.attributes["consultation.role"] === "customer"),
        )
        .map((participant) => participant.identity);
    },
    async updateParticipant(input) {
      await roomsClient.updateParticipant(input.roomName, input.identity, {
        permission: {
          canSubscribe: true,
          canPublish: input.canPublish,
          canPublishData: false,
        },
      });
    },
    async sendStatus(roomName, packet, destinations) {
      await roomsClient.sendData(
        roomName,
        new TextEncoder().encode(JSON.stringify(packet)),
        DataPacket_Kind.RELIABLE,
        {
          topic: "consultation.status.v1",
          destinationIdentities: [...destinations],
        },
      );
    },
    async removeParticipant(roomName, identity) {
      await roomsClient.removeParticipant(roomName, identity);
    },
  };
}

function createTokenPort(credentials: z.infer<typeof LiveKitCredentialsSchema>): LiveKitTokenPort {
  return {
    async issue(input) {
      const token = new AccessToken(credentials.apiKey, credentials.apiSecret, {
        identity: input.identity,
        ttl: input.ttlSeconds,
        attributes: input.attributes,
      });
      token.addGrant({
        room: input.roomName,
        roomJoin: true,
        canPublish: false,
        canPublishData: false,
        canSubscribe: true,
      });
      return token.toJwt();
    },
  };
}

function createWebhookVerifier(webhookReceiver: WebhookReceiver): WebhookVerifier {
  return {
    async verify(rawBody, headers): Promise<VerifiedWebhook> {
      try {
        const event = await webhookReceiver.receive(
          new TextDecoder().decode(rawBody),
          headers[authorizeHeader.toLowerCase()] ?? headers.authorization,
        );
        let kind: VerifiedWebhook["kind"] = "ignored";
        if (event.event === "participant_joined") {
          kind = "participant_joined";
        } else if (event.event === "participant_left") {
          kind = "participant_left";
        } else {
          kind = normalizedEgressWebhookKind(event.event, event.egressInfo?.status) ?? "ignored";
        }

        const egressSource = normalizeEgressRequestSource(event.egressInfo);
        const roomName = event.room?.name ?? event.egressInfo?.roomName ?? egressSource?.roomName;
        if (!event.id) {
          throw new Error("LiveKit webhook event ID is missing");
        }
        if (!event.createdAt) {
          throw new Error("LiveKit webhook createdAt is missing");
        }
        if (!roomName && kind === "participant_joined") {
          throw new Error("LiveKit participant_joined webhook roomName is missing");
        }
        if (!roomName && kind === "participant_left") {
          throw new Error("LiveKit participant_left webhook roomName is missing");
        }

        let binding: { consultationId: UUID; generation: number } | undefined;
        if (event.room?.metadata) {
          let metadata: unknown;
          try {
            metadata = JSON.parse(event.room.metadata) as unknown;
          } catch {
            throw new Error("LiveKit room metadata is not valid JSON");
          }
          binding = z
            .object({
              consultationId: z.uuid(),
              generation: z.number().int().nonnegative(),
            })
            .parse(metadata);
        }

        const isParticipantEvent = kind === "participant_joined" || kind === "participant_left";
        const participantIdentity =
          isParticipantEvent && event.participant?.identity
            ? event.participant.identity
            : undefined;
        return {
          id: event.id,
          occurredAt: new Date(Number(event.createdAt) * 1000),
          kind,
          ...(roomName ? { roomName } : {}),
          ...(binding ? binding : {}),
          ...(participantIdentity ? { identity: participantIdentity as UUID } : {}),
          ...(event.egressInfo?.egressId ? { egressId: event.egressInfo.egressId } : {}),
          ...(egressSource ? { egressSource } : {}),
          payload: event,
        };
      } catch (error) {
        let reason:
          | "authorization_header_empty"
          | "body_checksum_mismatch"
          | "missing_event_id"
          | "missing_created_at"
          | "missing_room_name_participant_joined"
          | "missing_room_name_participant_left"
          | "invalid_room_metadata"
          | "zod_validation_failed"
          | "json_parse_failed"
          | "unknown" = "unknown";
        if (error instanceof Error && error.message === "authorization header is empty") {
          reason = "authorization_header_empty";
        } else if (
          error instanceof Error &&
          error.message === "sha256 checksum of body does not match"
        ) {
          reason = "body_checksum_mismatch";
        } else if (
          error instanceof Error &&
          error.message === "LiveKit webhook event ID is missing"
        ) {
          reason = "missing_event_id";
        } else if (
          error instanceof Error &&
          error.message === "LiveKit webhook createdAt is missing"
        ) {
          reason = "missing_created_at";
        } else if (
          error instanceof Error &&
          error.message === "LiveKit participant_joined webhook roomName is missing"
        ) {
          reason = "missing_room_name_participant_joined";
        } else if (
          error instanceof Error &&
          error.message === "LiveKit participant_left webhook roomName is missing"
        ) {
          reason = "missing_room_name_participant_left";
        } else if (
          error instanceof Error &&
          error.message === "LiveKit room metadata is not valid JSON"
        ) {
          reason = "invalid_room_metadata";
        } else if (
          error instanceof z.ZodError ||
          (error instanceof Error && error.name === "ZodError")
        ) {
          reason = "zod_validation_failed";
        } else if (
          error instanceof SyntaxError ||
          (error instanceof Error && error.name === "SyntaxError")
        ) {
          reason = "json_parse_failed";
        }
        console.warn("LiveKit webhook rejected", { reason });
        throw new DomainError("INVALID_WEBHOOK");
      }
    },
  };
}

export function liveKitAdapters(config: WebConfig): {
  rooms: LiveKitRoomPort;
  tokens: LiveKitTokenPort;
  webhookVerifier: WebhookVerifier;
  ready: () => Promise<boolean>;
} {
  const credentials = LiveKitCredentialsSchema.parse(
    JSON.parse(config.liveKitCredentials) as unknown,
  );
  const apiUrl = new URL(config.liveKitInternalUrl);
  switch (apiUrl.protocol) {
    case "wss:":
      apiUrl.protocol = "https:";
      break;
    case "ws:":
      apiUrl.protocol = "http:";
      break;
    case "https:":
    case "http:":
      break;
    default:
      throw new Error("LIVEKIT_INTERNAL_URL must use http, https, ws, or wss");
  }
  const roomsClient = new RoomServiceClient(
    apiUrl.toString(),
    credentials.apiKey,
    credentials.apiSecret,
  );
  const webhookReceiver = new WebhookReceiver(credentials.apiKey, credentials.apiSecret);

  return {
    rooms: createRoomsPort(roomsClient),
    tokens: createTokenPort(credentials),
    webhookVerifier: createWebhookVerifier(webhookReceiver),
    ready: async () => {
      await roomsClient.listRooms([]);
      return true;
    },
  };
}
