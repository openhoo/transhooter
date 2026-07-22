import { createHash } from "node:crypto";
import { SendDataRequest } from "@livekit/protocol";
import { deterministicRoomName } from "@transhooter/server-core/rooms";
import { AccessToken, DataPacket_Kind } from "livekit-server-sdk";
import type { Effect, Uuid } from "../../orchestration/model";

export interface LiveKitConfig {
  readonly url: string;
  readonly apiKey: string;
  readonly apiSecret: string;
  readonly s3: {
    readonly accessKey: string;
    readonly secretKey: string;
    readonly endpoint: string;
    readonly bucket: string;
    readonly region: string;
    readonly forcePathStyle: boolean;
  };
  readonly internalToken: () => Promise<string>;
  readonly egressLayoutSigningKey: string;
  readonly egressLayoutUrl: string;
}

export interface ArchiveVersionDeleter {
  readiness(): Promise<void>;
  verifyObject(input: {
    key: string;
    versionId: string;
    size: number;
    checksum: string;
  }): Promise<boolean>;
  readObject(input: { key: string; versionId: string }): Promise<Uint8Array>;
  putCreateOnce(input: {
    key: string;
    body: Uint8Array;
    contentType: string;
    sha256: string;
  }): Promise<{ versionId: string; size: number; checksum: string }>;
  discoverObjects(prefix: string): Promise<
    readonly {
      key: string;
      versionId: string;
      size: number;
      checksum: string;
      contentType: string;
      sha256: string;
    }[]
  >;
  drain(consultationId: Uuid): Promise<boolean>;
}

export function twirpBytes(message: { toJson(): unknown }): Uint8Array {
  return Buffer.from(JSON.stringify(message.toJson()), "utf8");
}

export function requiredString(value: Readonly<Record<string, unknown>>, key: string): string {
  const field = value[key];
  if (typeof field !== "string" || field.length === 0) {
    throw new Error(`${key} must be a non-empty string`);
  }
  return field;
}

export function requiredOptionalString(
  value: Readonly<Record<string, unknown>>,
  key: string,
): string | undefined {
  const field = value[key];
  if (field === undefined) return undefined;
  if (typeof field !== "string" || field.length === 0) {
    throw new Error(`${key} must be a non-empty string`);
  }
  return field;
}

export function requiredNumber(value: Readonly<Record<string, unknown>>, key: string): number {
  const field = value[key];
  if (typeof field !== "number" || !Number.isFinite(field)) {
    throw new Error(`${key} must be a number`);
  }
  return field;
}

export function asRecord(value: unknown): Readonly<Record<string, unknown>> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Readonly<Record<string, unknown>>;
}

export function metadataAdoptionId(metadata: string): string | null {
  try {
    const parsed: unknown = JSON.parse(metadata);
    const adoptionId = asRecord(parsed).adoptionId;
    return typeof adoptionId === "string" ? adoptionId : null;
  } catch {
    return null;
  }
}

export function roomNameFor(effect: Effect, request: Readonly<Record<string, unknown>>): string {
  if (
    (effect.kind === "PARTICIPANT_EGRESS" || effect.kind === "PARTICIPANT_REMOVE") &&
    typeof request.resourceRoomName === "string"
  ) {
    return request.resourceRoomName;
  }
  return typeof request.roomName === "string"
    ? request.roomName
    : deterministicRoomName(effect.consultationId, effect.generation);
}

function optionalStringArray(value: unknown): string[] {
  if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) return [];
  return value;
}

function withStatusCommon(
  effect: Effect,
  request: Readonly<Record<string, unknown>>,
): Readonly<Record<string, unknown>> {
  const payload: Record<string, unknown> = { ...request };
  delete payload.destinationIdentities;
  delete payload.topic;
  delete payload.roomName;
  delete payload.adoptionId;
  return {
    schemaVersion: 1,
    consultationId: effect.consultationId,
    generation: effect.generation,
    ...payload,
    occurredAtMs: requiredNumber(request, "occurredAtMs"),
  };
}

export function canonicalStatusRequest(
  effect: Effect,
  request: Readonly<Record<string, unknown>>,
  roomName: string,
): Uint8Array {
  const payload = Buffer.from(JSON.stringify(withStatusCommon(effect, request)));
  return twirpBytes(
    new SendDataRequest({
      room: roomName,
      data: payload,
      kind: DataPacket_Kind.RELIABLE,
      destinationSids: [],
      destinationIdentities:
        request.reasonCode === "SHUTDOWN" ? [] : optionalStringArray(request.destinationIdentities),
      topic: requiredString(request, "topic"),
      nonce: createHash("sha256").update(effect.id).digest().subarray(0, 16),
    }),
  );
}

export async function sendPersistedStatus(
  config: LiveKitConfig,
  roomName: string,
  body: Uint8Array,
  effectId: Uuid,
): Promise<void> {
  const token = new AccessToken(config.apiKey, config.apiSecret, { identity: effectId });
  token.addGrant({ roomAdmin: true, room: roomName });
  const endpoint = new URL(config.url);
  endpoint.protocol =
    endpoint.protocol === "wss:"
      ? "https:"
      : endpoint.protocol === "ws:"
        ? "http:"
        : endpoint.protocol;
  endpoint.pathname = "/twirp/livekit.RoomService/SendData";
  endpoint.search = "";
  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      authorization: `Bearer ${await token.toJwt()}`,
      "content-type": "application/json;charset=UTF-8",
    },
    body: Buffer.from(body),
  });
  if (!response.ok) {
    throw new Error(`LiveKit SendData rejected with status ${String(response.status)}`);
  }
}
