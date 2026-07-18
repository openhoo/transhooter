import "server-only";
import { createHash, randomBytes, randomUUID } from "node:crypto";
import {
  AbortMultipartUploadCommand,
  DeleteObjectsCommand,
  GetObjectAttributesCommand,
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
import {
  DataPacket_Kind,
  EgressStatus,
  S3Upload,
  SegmentedFileOutput,
  SegmentedFileProtocol,
} from "@livekit/protocol";
import type {
  EgressPort,
  LiveKitRoomPort,
  LiveKitTokenPort,
  MailPort,
  ObjectStoragePort,
  UUID,
  VerifiedWebhook,
  WebhookVerifier,
} from "@transhooter/server-core";
import { AccessToken, EgressClient, RoomServiceClient, WebhookReceiver } from "livekit-server-sdk";
import nodemailer from "nodemailer";
import { z } from "zod";
import type { WebConfig } from "./config";
import { normalizedEgressWebhookKind } from "./livekit-webhook";

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
    purpose: "sign_in" | "consultation_invite" | "archive_delete_reauth";
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

  async putCreateOnce(input: {
    key: string;
    body: Uint8Array;
    contentType: string;
    checksum: string;
  }) {
    const sha256 = createHash("sha256").update(input.body).digest("hex");
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
      new GetObjectAttributesCommand({
        Bucket: this.config.s3Bucket,
        Key: input.key,
        VersionId: input.versionId,
        ObjectAttributes: ["ObjectSize", "Checksum"],
      }),
    );
    return (
      result.ObjectSize === input.size && result.Checksum?.ChecksumCRC64NVME === input.checksum
    );
  }

  async listMeetingVersions(consultationId: UUID, cursor?: string) {
    const result = await this.#client.send(
      new ListObjectVersionsCommand({
        Bucket: this.config.s3Bucket,
        Prefix: `v1/meetings/${consultationId}/`,
        KeyMarker: cursor,
      }),
    );
    const versions = (result.Versions ?? []).flatMap((version) =>
      version.Key && version.VersionId ? [{ key: version.Key, versionId: version.VersionId }] : [],
    );
    return {
      versions,
      cursor: result.IsTruncated ? (result.NextKeyMarker ?? null) : null,
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
    const result = await this.#client.send(
      new ListMultipartUploadsCommand({
        Bucket: this.config.s3Bucket,
        Prefix: `v1/meetings/${consultationId}/`,
      }),
    );
    return (result.Uploads ?? []).flatMap((upload) =>
      upload.Key && upload.UploadId ? [{ key: upload.Key, uploadId: upload.UploadId }] : [],
    );
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

function createSegmentedOutput(config: WebConfig): (prefix: string) => SegmentedFileOutput {
  const credentials = config.s3Credentials
    ? S3CredentialsSchema.parse(JSON.parse(config.s3Credentials) as unknown)
    : null;
  const s3 = new S3Upload({
    accessKey: credentials?.accessKeyId ?? "",
    secret: credentials?.secretAccessKey ?? "",
    sessionToken: credentials?.sessionToken ?? "",
    region: config.s3Region,
    endpoint: config.s3Endpoint,
    bucket: config.s3Bucket,
    forcePathStyle: true,
  });
  return (prefix) =>
    new SegmentedFileOutput({
      protocol: SegmentedFileProtocol.HLS_PROTOCOL,
      filenamePrefix: prefix,
      playlistName: "index.m3u8",
      segmentDuration: 2,
      output: { case: "s3", value: s3 },
    });
}

function createEgressPort(
  egressClient: EgressClient,
  output: (prefix: string) => SegmentedFileOutput,
): EgressPort {
  return {
    async startRoomComposite(input) {
      const info = await egressClient.startRoomCompositeEgress(
        input.roomName,
        output(input.outputPrefix),
        { customBaseUrl: input.layoutUrl },
      );
      return {
        egressId: info.egressId,
        state: EgressStatus[info.status],
      };
    },
    async startParticipant(input) {
      const info = await egressClient.startParticipantEgress(input.roomName, input.identity, {
        segments: output(input.outputPrefix),
      });
      return {
        egressId: info.egressId,
        state: EgressStatus[info.status],
      };
    },
    async get(egressId) {
      const info = (await egressClient.listEgress({ egressId }))[0];
      if (!info) {
        throw new Error(`Unknown Egress ${egressId}`);
      }
      return {
        egressId: info.egressId,
        state: EgressStatus[info.status],
        output: info,
      };
    },
    async stop(egressId) {
      await egressClient.stopEgress(egressId);
    },
  };
}

function createWebhookVerifier(webhookReceiver: WebhookReceiver): WebhookVerifier {
  return {
    async verify(rawBody, headers): Promise<VerifiedWebhook> {
      const event = await webhookReceiver.receive(
        new TextDecoder().decode(rawBody),
        headers.authorization,
      );
      let kind: VerifiedWebhook["kind"] = "ignored";
      if (event.event === "participant_joined") {
        kind = "participant_joined";
      } else if (event.event === "participant_left") {
        kind = "participant_left";
      } else {
        kind = normalizedEgressWebhookKind(event.event, event.egressInfo?.status) ?? "ignored";
      }

      const roomName = event.room?.name ?? event.egressInfo?.roomName;
      if (!event.id || !event.createdAt || !roomName) {
        throw new Error("Incomplete LiveKit webhook");
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
        isParticipantEvent && event.participant?.identity ? event.participant.identity : undefined;
      return {
        id: event.id,
        occurredAt: new Date(Number(event.createdAt) * 1000),
        kind,
        roomName,
        ...(binding ? binding : {}),
        ...(participantIdentity ? { identity: participantIdentity as UUID } : {}),
        ...(event.egressInfo?.egressId ? { egressId: event.egressInfo.egressId } : {}),
        payload: event,
      };
    },
  };
}

export function liveKitAdapters(config: WebConfig): {
  rooms: LiveKitRoomPort;
  tokens: LiveKitTokenPort;
  egress: EgressPort;
  webhookVerifier: WebhookVerifier;
  ready: () => Promise<boolean>;
} {
  const credentials = LiveKitCredentialsSchema.parse(
    JSON.parse(config.liveKitCredentials) as unknown,
  );
  const apiUrl = new URL(config.liveKitInternalUrl);
  apiUrl.protocol = apiUrl.protocol === "wss:" ? "https:" : "http:";
  const roomsClient = new RoomServiceClient(
    apiUrl.toString(),
    credentials.apiKey,
    credentials.apiSecret,
  );
  const egressClient = new EgressClient(
    apiUrl.toString(),
    credentials.apiKey,
    credentials.apiSecret,
  );
  const webhookReceiver = new WebhookReceiver(credentials.apiKey, credentials.apiSecret);

  return {
    rooms: createRoomsPort(roomsClient),
    tokens: createTokenPort(credentials),
    egress: createEgressPort(egressClient, createSegmentedOutput(config)),
    webhookVerifier: createWebhookVerifier(webhookReceiver),
    ready: async () => {
      await roomsClient.listRooms([]);
      return true;
    },
  };
}
