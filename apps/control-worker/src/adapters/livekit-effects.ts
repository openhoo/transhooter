import { AgentDispatchClient, EgressClient, RoomServiceClient } from "livekit-server-sdk";
import type { Effect, Uuid } from "../orchestration/model";
import { canonicalRequest as encodeCanonicalRequest } from "../orchestration/model";
import type { Adoption, RemoteEffects, RemoteResult } from "../orchestration/remote";
import { EgressEffects } from "./livekit-effects/egress";
import { ParticipantEffects } from "./livekit-effects/participants";
import { RoomDispatchEffects } from "./livekit-effects/rooms-dispatch";
import {
  type ArchiveVersionDeleter,
  canonicalStatusRequest,
  type LiveKitConfig,
  requiredString,
  roomNameFor,
  sendPersistedStatus,
} from "./livekit-effects/shared";

export { egressStatusName, isViableEgressAdoption } from "./livekit-effects/egress";
export type { ArchiveVersionDeleter, LiveKitConfig } from "./livekit-effects/shared";

export class LiveKitEffects implements RemoteEffects {
  private readonly rooms: RoomServiceClient;
  private readonly dispatch: AgentDispatchClient;
  private readonly egress: EgressClient;
  private readonly roomDispatchEffects: RoomDispatchEffects;
  private readonly egressEffects: EgressEffects;
  private readonly participantEffects: ParticipantEffects;

  constructor(
    private readonly config: LiveKitConfig,
    private readonly archive: ArchiveVersionDeleter,
  ) {
    this.rooms = new RoomServiceClient(config.url, config.apiKey, config.apiSecret);
    this.dispatch = new AgentDispatchClient(config.url, config.apiKey, config.apiSecret);
    this.egress = new EgressClient(config.url, config.apiKey, config.apiSecret);
    const thisEffects = this;
    this.roomDispatchEffects = new RoomDispatchEffects({
      get rooms() {
        return thisEffects.rooms;
      },
      get dispatch() {
        return thisEffects.dispatch;
      },
      get egress() {
        return thisEffects.egress;
      },
    });
    this.egressEffects = new EgressEffects(() => this.egress, config);
    this.participantEffects = new ParticipantEffects(() => this.rooms);
  }

  async readiness(): Promise<void> {
    await Promise.all([
      this.rooms.listRooms([]),
      this.egress.listEgress({}),
      this.archive.readiness(),
    ]);
  }

  async adopt(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
  ): Promise<Adoption | null> {
    if (
      effect.kind === "ROOM_CREATE" ||
      effect.kind === "ROOM_DRAIN" ||
      effect.kind === "ROOM_DELETE" ||
      effect.kind === "WORKER_DISPATCH" ||
      effect.kind === "DISPATCH_DELETE"
    ) {
      return this.roomDispatchEffects.adopt(effect, request);
    }
    if (
      effect.kind === "ROOM_COMPOSITE_EGRESS" ||
      effect.kind === "PARTICIPANT_EGRESS" ||
      effect.kind === "EGRESS_STOP"
    ) {
      return this.egressEffects.adopt(effect, request);
    }
    if (effect.kind === "PARTICIPANT_GRANT" || effect.kind === "PARTICIPANT_REMOVE") {
      return this.participantEffects.adopt(effect, request);
    }
    if (effect.kind === "STATUS_PACKET") {
      const roomName = requiredString(request, "roomName");
      if ((await this.rooms.listRooms([roomName])).length === 0) {
        return {
          remoteId: effect.id,
          matchesRequest: true,
          terminal: true,
          result: { sent: false, skipped: "room_absent" },
        };
      }
    }
    return null;
  }

  canonicalRequest(effect: Effect, request: Readonly<Record<string, unknown>>): Uint8Array {
    const roomName = roomNameFor(effect, request);
    if (
      effect.kind === "ROOM_CREATE" ||
      effect.kind === "ROOM_DRAIN" ||
      effect.kind === "ROOM_DELETE" ||
      effect.kind === "WORKER_DISPATCH" ||
      effect.kind === "DISPATCH_DELETE"
    ) {
      return this.roomDispatchEffects.canonical(effect, request, roomName);
    }
    if (
      effect.kind === "ROOM_COMPOSITE_EGRESS" ||
      effect.kind === "PARTICIPANT_EGRESS" ||
      effect.kind === "EGRESS_STOP"
    ) {
      return this.egressEffects.canonical(effect, request, roomName);
    }
    if (effect.kind === "PARTICIPANT_GRANT" || effect.kind === "PARTICIPANT_REMOVE") {
      return this.participantEffects.canonical(effect, request, roomName);
    }
    if (effect.kind === "STATUS_PACKET") {
      return canonicalStatusRequest(effect, request, roomName);
    }
    if (effect.kind === "ARCHIVE_RECONCILE" || effect.kind === "ARCHIVE_DELETE") {
      return encodeCanonicalRequest({ kind: effect.kind, roomName, request }).bytes;
    }
    return encodeCanonicalRequest({ kind: effect.kind, roomName, request }).bytes;
  }

  async execute(effect: Effect, request: Readonly<Record<string, unknown>>): Promise<RemoteResult> {
    const roomName = roomNameFor(effect, request);
    if (
      effect.kind === "ROOM_CREATE" ||
      effect.kind === "ROOM_DRAIN" ||
      effect.kind === "ROOM_DELETE" ||
      effect.kind === "WORKER_DISPATCH" ||
      effect.kind === "DISPATCH_DELETE"
    ) {
      return this.roomDispatchEffects.execute(effect, request, roomName);
    }
    if (
      effect.kind === "ROOM_COMPOSITE_EGRESS" ||
      effect.kind === "PARTICIPANT_EGRESS" ||
      effect.kind === "EGRESS_STOP"
    ) {
      return this.egressEffects.execute(effect, request, roomName);
    }
    if (effect.kind === "PARTICIPANT_GRANT" || effect.kind === "PARTICIPANT_REMOVE") {
      return this.participantEffects.execute(effect, request, roomName);
    }
    if (effect.kind === "STATUS_PACKET") {
      if (effect.requestBytes === null) {
        throw new Error("persisted SendData request bytes are missing");
      }
      await sendPersistedStatus(this.config, roomName, effect.requestBytes, effect.id);
      return { remoteId: effect.id, result: { sent: true } };
    }
    if (effect.kind === "ARCHIVE_RECONCILE") {
      throw new Error("archive reconciliation must execute through durable reconciliation store");
    }
    throw new Error(`unsupported remote effect ${effect.kind}`);
  }

  async compensate(effect: Effect): Promise<void> {
    let remoteId = effect.remoteId;
    if (remoteId === null) {
      const adoption = await this.adopt(effect, effect.plan);
      if (adoption === null || !adoption.matchesRequest) return;
      remoteId = adoption.remoteId;
    }
    if (
      effect.kind === "ROOM_CREATE" ||
      effect.kind === "ROOM_DELETE" ||
      effect.kind === "WORKER_DISPATCH"
    ) {
      await this.roomDispatchEffects.compensate(effect, remoteId);
      return;
    }
    if (
      effect.kind === "ROOM_COMPOSITE_EGRESS" ||
      effect.kind === "PARTICIPANT_EGRESS" ||
      effect.kind === "EGRESS_STOP"
    ) {
      await this.egressEffects.compensate(remoteId);
      return;
    }
    if (effect.kind === "PARTICIPANT_GRANT") {
      await this.participantEffects.compensate(effect);
    }
  }

  async areHumansAbsent(roomName: string, identities: readonly Uuid[]): Promise<boolean> {
    const allowed = new Set(identities);
    const rooms = await this.rooms.listRooms([roomName]);
    if (rooms.length === 0) return true;
    return !(await this.rooms.listParticipants(roomName)).some((participant) =>
      allowed.has(participant.identity),
    );
  }

  async notifyArchiveRecording(consultationId: Uuid): Promise<void> {
    await this.callInternal("archive-recording", consultationId);
  }

  async notifyDeleteDrain(
    consultationId: Uuid,
    writeEpoch: number,
    reason: string,
  ): Promise<boolean> {
    if (reason.trim().length === 0) {
      throw new Error("delete drain reason must be nonblank");
    }
    return (
      (await this.callInternal("delete-drain", consultationId, { writeEpoch, reason })) === true
    );
  }

  private async callInternal(
    operation: "archive-recording" | "delete-drain",
    consultationId: Uuid,
    extra: Readonly<Record<string, unknown>> = {},
  ): Promise<unknown> {
    const endpoint = new URL(`/api/internal/${operation}`, this.config.egressLayoutUrl);
    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        authorization: `Bearer ${await this.config.internalToken()}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ consultationId, ...extra }),
    });
    if (!response.ok) {
      throw new Error(`internal ${operation} rejected with status ${String(response.status)}`);
    }
    return response.status === 204 ? null : await response.json();
  }

  async verifyArchiveObject(input: {
    key: string;
    versionId: string;
    size: number;
    checksum: string;
  }): Promise<boolean> {
    return this.archive.verifyObject(input);
  }

  async readArchiveObject(input: { key: string; versionId: string }): Promise<Uint8Array> {
    return this.archive.readObject(input);
  }

  async putArchiveObject(input: {
    key: string;
    body: Uint8Array;
    contentType: string;
    sha256: string;
  }): Promise<{ versionId: string; size: number; checksum: string }> {
    return this.archive.putCreateOnce(input);
  }

  async discoverArchiveObjects(prefix: string) {
    return this.archive.discoverObjects(prefix);
  }

  async drainArchive(consultationId: Uuid): Promise<boolean> {
    return this.archive.drain(consultationId);
  }
}
