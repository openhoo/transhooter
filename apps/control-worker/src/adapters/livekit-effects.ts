import { createHash, createHmac } from "node:crypto";
import type { EgressInfo } from "@livekit/protocol";
import {
  CreateAgentDispatchRequest,
  CreateRoomRequest,
  DeleteAgentDispatchRequest,
  DeleteRoomRequest,
  ParticipantPermission,
  RoomParticipantIdentity,
  SendDataRequest,
  StopEgressRequest,
  UpdateParticipantRequest,
} from "@livekit/protocol";
import { deterministicRoomName } from "@transhooter/server-core/rooms";
import {
  AccessToken,
  AgentDispatchClient,
  DataPacket_Kind,
  EgressClient,
  EgressStatus,
  RoomServiceClient,
  S3Upload,
  SegmentedFileOutput,
  SegmentedFileProtocol,
  TrackSource,
} from "livekit-server-sdk";
import type { Effect, Uuid } from "../orchestration/model";
import { canonicalRequest as encodeCanonicalRequest } from "../orchestration/model";
import type { Adoption, RemoteEffects, RemoteResult } from "../orchestration/remote";

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
  }): Promise<{
    versionId: string;
    size: number;
    checksum: string;
  }>;
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

type EgressStatusName =
  | "EGRESS_STARTING"
  | "EGRESS_ACTIVE"
  | "EGRESS_ENDING"
  | "EGRESS_COMPLETE"
  | "EGRESS_FAILED"
  | "EGRESS_ABORTED"
  | "EGRESS_LIMIT_REACHED";

type EgressTerminalResult = {
  readonly egressId: string;
  readonly status: EgressStatusName;
  readonly error: string;
  readonly errorCode: number;
  readonly details: string;
  readonly fileResults: EgressInfo["fileResults"];
  readonly segmentResults: EgressInfo["segmentResults"];
};

export class LiveKitEffects implements RemoteEffects {
  private readonly rooms: RoomServiceClient;
  private readonly egress: EgressClient;
  private readonly dispatch: AgentDispatchClient;

  constructor(
    private readonly config: LiveKitConfig,
    private readonly archive: ArchiveVersionDeleter,
  ) {
    this.rooms = new RoomServiceClient(config.url, config.apiKey, config.apiSecret);
    this.egress = new EgressClient(config.url, config.apiKey, config.apiSecret);
    this.dispatch = new AgentDispatchClient(config.url, config.apiKey, config.apiSecret);
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
      effect.kind === "ROOM_DELETE"
    ) {
      return this.adoptRoomEffect(effect, request);
    }
    if (effect.kind === "WORKER_DISPATCH" || effect.kind === "DISPATCH_DELETE") {
      return this.adoptDispatchEffect(effect, request);
    }
    if (
      effect.kind === "ROOM_COMPOSITE_EGRESS" ||
      effect.kind === "PARTICIPANT_EGRESS" ||
      effect.kind === "EGRESS_STOP"
    ) {
      return this.adoptEgressEffect(effect, request);
    }
    if (effect.kind === "PARTICIPANT_GRANT" || effect.kind === "PARTICIPANT_REMOVE") {
      return this.adoptParticipantEffect(effect, request);
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
      effect.kind === "ROOM_DELETE"
    ) {
      return this.canonicalRoomRequest(effect, request, roomName);
    }
    if (effect.kind === "WORKER_DISPATCH" || effect.kind === "DISPATCH_DELETE") {
      return this.canonicalDispatchRequest(effect, request, roomName);
    }
    if (
      effect.kind === "ROOM_COMPOSITE_EGRESS" ||
      effect.kind === "PARTICIPANT_EGRESS" ||
      effect.kind === "EGRESS_STOP"
    ) {
      return this.canonicalEgressRequest(effect, request, roomName);
    }
    if (effect.kind === "PARTICIPANT_GRANT" || effect.kind === "PARTICIPANT_REMOVE") {
      return this.canonicalParticipantRequest(effect, request, roomName);
    }
    if (effect.kind === "STATUS_PACKET") {
      return this.canonicalStatusRequest(effect, request, roomName);
    }
    if (effect.kind === "ARCHIVE_RECONCILE" || effect.kind === "ARCHIVE_DELETE") {
      return this.canonicalArchiveRequest(effect, request, roomName);
    }
    return encodeCanonicalRequest({ kind: effect.kind, roomName, request }).bytes;
  }

  async execute(effect: Effect, request: Readonly<Record<string, unknown>>): Promise<RemoteResult> {
    const roomName = roomNameFor(effect, request);

    if (
      effect.kind === "ROOM_CREATE" ||
      effect.kind === "ROOM_DRAIN" ||
      effect.kind === "ROOM_DELETE"
    ) {
      return this.executeRoomEffect(effect, request, roomName);
    }
    if (effect.kind === "WORKER_DISPATCH" || effect.kind === "DISPATCH_DELETE") {
      return this.executeDispatchEffect(effect, request, roomName);
    }
    if (
      effect.kind === "ROOM_COMPOSITE_EGRESS" ||
      effect.kind === "PARTICIPANT_EGRESS" ||
      effect.kind === "EGRESS_STOP"
    ) {
      return this.executeEgressEffect(effect, request, roomName);
    }
    if (effect.kind === "PARTICIPANT_GRANT" || effect.kind === "PARTICIPANT_REMOVE") {
      return this.executeParticipantEffect(effect, request, roomName);
    }
    if (effect.kind === "STATUS_PACKET") {
      return this.executeStatusEffect(effect, roomName);
    }
    if (effect.kind === "ARCHIVE_RECONCILE" || effect.kind === "ARCHIVE_DELETE") {
      return this.executeArchiveEffect(effect);
    }
    throw new Error(`unsupported remote effect ${effect.kind}`);
  }

  async compensate(effect: Effect): Promise<void> {
    let remoteId = effect.remoteId;
    if (remoteId === null) {
      const adoption = await this.adopt(effect, effect.plan);
      if (adoption === null || !adoption.matchesRequest) {
        return;
      }
      remoteId = adoption.remoteId;
    }

    if (effect.kind === "WORKER_DISPATCH") {
      const roomName = roomNameFor(effect, effect.plan);
      const found = (await this.dispatch.listDispatch(roomName)).find(({ id }) => id === remoteId);
      if (found !== undefined) {
        await this.dispatch.deleteDispatch(remoteId, roomName);
      }
      return;
    }
    if (
      effect.kind === "ROOM_COMPOSITE_EGRESS" ||
      effect.kind === "PARTICIPANT_EGRESS" ||
      effect.kind === "EGRESS_STOP"
    ) {
      const found = (await this.egress.listEgress({ egressId: remoteId }))[0];
      if (found !== undefined && !isTerminalEgress(found.status)) {
        await this.egress.stopEgress(remoteId);
      }
      return;
    }

    if (effect.kind === "PARTICIPANT_GRANT") {
      const roomName = requiredString(effect.plan, "roomName");
      const participantIdentity = requiredString(effect.plan, "participantIdentity");
      const participant = await this.rooms
        .getParticipant(roomName, participantIdentity)
        .catch(() => undefined);
      if (participant !== undefined) {
        await this.rooms.updateParticipant(roomName, participantIdentity, {
          permission: {
            canSubscribe: false,
            canPublish: false,
            canPublishData: false,
            canPublishSources: [],
          },
        });
      }
      return;
    }

    if (effect.kind === "ROOM_CREATE" || effect.kind === "ROOM_DELETE") {
      const roomName = roomNameFor(effect, effect.plan);
      if ((await this.rooms.listRooms([roomName])).length > 0) {
        await this.rooms.deleteRoom(roomName);
      }
    }
  }

  private async adoptRoomEffect(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
  ): Promise<Adoption | null> {
    const roomName = requiredString(request, "roomName");

    if (effect.kind === "ROOM_CREATE") {
      const room = (await this.rooms.listRooms([roomName]))[0];
      if (room === undefined) {
        return null;
      }
      return {
        remoteId: room.sid,
        matchesRequest: metadataAdoptionId(room.metadata) === effect.id,
        terminal: false,
      };
    }

    if (effect.kind === "ROOM_DRAIN") {
      if ((await this.rooms.listRooms([roomName])).length > 0) {
        return null;
      }
      const terminals = await this.egress.listEgress({ roomName });
      if (terminals.some((info) => !isTerminalEgress(info.status))) {
        return null;
      }
      return {
        remoteId: effect.id,
        matchesRequest: true,
        terminal: true,
        result: {
          roomClosed: true,
          adopted: true,
          egressTerminals: terminals,
        },
      };
    }

    const rooms = await this.rooms.listRooms([roomName]);
    return rooms.length === 0
      ? { remoteId: effect.id, matchesRequest: true, terminal: true }
      : null;
  }

  private async adoptDispatchEffect(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
  ): Promise<Adoption | null> {
    if (effect.kind === "WORKER_DISPATCH") {
      const roomName = requiredString(request, "roomName");
      const dispatches = await this.dispatch.listDispatch(roomName);
      const found = dispatches.find(
        (candidate) => metadataAdoptionId(candidate.metadata) === effect.id,
      );
      return found === undefined
        ? null
        : { remoteId: found.id, matchesRequest: true, terminal: false };
    }

    const dispatchId = requiredString(request, "dispatchId");
    const roomName = requiredString(request, "roomName");
    if ((await this.rooms.listRooms([roomName])).length === 0) {
      return { remoteId: dispatchId, matchesRequest: true, terminal: true };
    }
    const found = (await this.dispatch.listDispatch(roomName)).find(({ id }) => id === dispatchId);
    return found === undefined
      ? { remoteId: dispatchId, matchesRequest: true, terminal: true }
      : null;
  }

  private async adoptEgressEffect(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
  ): Promise<Adoption | null> {
    if (effect.kind === "ROOM_COMPOSITE_EGRESS" || effect.kind === "PARTICIPANT_EGRESS") {
      const roomName = roomNameFor(effect, request);
      const found = (await this.egress.listEgress({ roomName })).find(
        (candidate) =>
          egressMatches(candidate, effect, request) && isViableEgressAdoption(candidate.status),
      );
      return found === undefined
        ? null
        : {
            remoteId: found.egressId,
            matchesRequest: true,
            terminal: false,
            result: { egressId: found.egressId, status: egressStatusName(found.status) },
          };
    }

    const egressId = requiredString(request, "egressId");
    const found = (await this.egress.listEgress({ egressId }))[0];
    if (found === undefined || !isTerminalEgress(found.status)) {
      return null;
    }
    return {
      remoteId: egressId,
      matchesRequest: true,
      terminal: true,
      result: terminalEgressResult(found),
    };
  }

  private async adoptParticipantEffect(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
  ): Promise<Adoption | null> {
    if (effect.kind === "PARTICIPANT_GRANT") {
      const roomName = requiredString(request, "roomName");
      const participantIdentity = requiredString(request, "participantIdentity");
      const participant = await this.rooms
        .getParticipant(roomName, participantIdentity)
        .catch(() => undefined);
      if (participant === undefined || !hasExactCaptureGrant(participant.permission)) {
        return null;
      }
      return {
        remoteId: participant.sid,
        matchesRequest: true,
        terminal: false,
      };
    }
    const participantIdentity = requiredString(request, "participantIdentity");
    const roomName = roomNameFor(effect, request);

    const participant = (await this.rooms.listParticipants(roomName)).find(
      ({ identity }) => identity === participantIdentity,
    );
    return participant === undefined
      ? { remoteId: effect.id, matchesRequest: true, terminal: true }
      : null;
  }

  private canonicalRoomRequest(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
    roomName: string,
  ): Uint8Array {
    if (effect.kind === "ROOM_CREATE") {
      return twirpBytes(
        new CreateRoomRequest({
          name: roomName,
          emptyTimeout: requiredNumber(request, "emptyTimeout"),
          metadata: JSON.stringify({ ...asRecord(request.metadata), adoptionId: effect.id }),
        }),
      );
    }
    if (effect.kind === "ROOM_DELETE") {
      return twirpBytes(new DeleteRoomRequest({ room: roomName }));
    }
    return encodeCanonicalRequest({
      kind: effect.kind,
      roomName,
      request,
      orderedOperations: [
        "list-and-delete-worker-dispatches",
        "list-query-and-stop-egress",
        "list-and-remove-standard-humans",
        "delete-room",
      ],
    }).bytes;
  }

  private canonicalDispatchRequest(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
    roomName: string,
  ): Uint8Array {
    if (effect.kind === "WORKER_DISPATCH") {
      return twirpBytes(
        new CreateAgentDispatchRequest({
          room: roomName,
          agentName: requiredString(request, "agentName"),
          metadata: JSON.stringify({ ...asRecord(request.metadata), adoptionId: effect.id }),
        }),
      );
    }
    return twirpBytes(
      new DeleteAgentDispatchRequest({
        dispatchId: requiredString(request, "dispatchId"),
        room: roomName,
      }),
    );
  }

  private canonicalEgressRequest(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
    roomName: string,
  ): Uint8Array {
    if (effect.kind === "EGRESS_STOP") {
      return twirpBytes(
        new StopEgressRequest({
          egressId: requiredString(request, "egressId"),
        }),
      );
    }

    const intent = {
      kind: effect.kind,
      roomName,
      egressIdentity: effect.id,
      consultationId: effect.consultationId,
      generation: effect.generation,
      output: {
        format: "segmented_hls",
        segmentDurationSeconds: 2,
      },
      ...(effect.kind === "ROOM_COMPOSITE_EGRESS"
        ? {
            render: {
              layout: "speaker",
              audioOnly: false,
              videoOnly: false,
            },
          }
        : {
            participantIdentity: requiredString(request, "participantIdentity"),
            screenShare: false,
          }),
    };
    return encodeCanonicalRequest(intent).bytes;
  }

  private canonicalParticipantRequest(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
    roomName: string,
  ): Uint8Array {
    if (effect.kind === "PARTICIPANT_GRANT") {
      return twirpBytes(
        new UpdateParticipantRequest({
          room: roomName,
          identity: requiredString(request, "participantIdentity"),
          attributes: {},
          permission: new ParticipantPermission({
            canSubscribe: true,
            canPublish: true,
            canPublishData: false,
            canPublishSources: [TrackSource.MICROPHONE, TrackSource.CAMERA],
          }),
        }),
      );
    }
    return twirpBytes(
      new RoomParticipantIdentity({
        room: roomName,
        identity: requiredString(request, "participantIdentity"),
      }),
    );
  }

  private canonicalStatusRequest(
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
          request.reasonCode === "SHUTDOWN"
            ? []
            : optionalStringArray(request.destinationIdentities),
        topic: requiredString(request, "topic"),
        nonce: createHash("sha256").update(effect.id).digest().subarray(0, 16),
      }),
    );
  }

  private canonicalArchiveRequest(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
    roomName: string,
  ): Uint8Array {
    return encodeCanonicalRequest({ kind: effect.kind, roomName, request }).bytes;
  }

  private async executeRoomEffect(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
    roomName: string,
  ): Promise<RemoteResult> {
    if (effect.kind === "ROOM_CREATE") {
      const room = await this.rooms.createRoom({
        name: roomName,
        emptyTimeout: requiredNumber(request, "emptyTimeout"),
        metadata: JSON.stringify({ ...asRecord(request.metadata), adoptionId: effect.id }),
      });
      return { remoteId: room.sid, result: { sid: room.sid, name: room.name } };
    }
    if (effect.kind === "ROOM_DELETE") {
      await this.rooms.deleteRoom(roomName);
      return { remoteId: effect.id, result: { deleted: true, roomClosed: true } };
    }
    return this.executeRoomDrain(effect, roomName);
  }

  private async executeRoomDrain(effect: Effect, roomName: string): Promise<RemoteResult> {
    await this.deleteWorkerDispatches(roomName);
    await this.stopActiveEgresses(roomName);
    const terminalEgress = await this.requireTerminalEgresses(roomName);
    await this.removeStandardHumans(roomName);
    await this.rooms.deleteRoom(roomName);
    return {
      remoteId: effect.id,
      result: {
        roomClosed: true,
        egressTerminals: terminalEgress.map(terminalEgressResult),
      },
    };
  }

  private async deleteWorkerDispatches(roomName: string): Promise<void> {
    for (const job of await this.dispatch.listDispatch(roomName)) {
      await this.dispatch.deleteDispatch(job.id, roomName);
    }
  }

  private async stopActiveEgresses(roomName: string): Promise<void> {
    for (const info of await this.egress.listEgress({ roomName, active: true })) {
      await this.egress.stopEgress(info.egressId);
    }
  }

  private async requireTerminalEgresses(roomName: string): Promise<EgressInfo[]> {
    const terminalEgress = await this.egress.listEgress({ roomName });
    if (terminalEgress.some((info) => !isTerminalEgress(info.status))) {
      throw new Error("Egress drain has not reached terminal state");
    }
    return terminalEgress;
  }

  private async removeStandardHumans(roomName: string): Promise<void> {
    const participants = await this.rooms.listParticipants(roomName);
    for (const participant of participants) {
      const role = participant.attributes["consultation.role"];
      if (role === "employee" || role === "customer") {
        await this.rooms.removeParticipant(roomName, participant.identity);
      }
    }
  }

  private async executeDispatchEffect(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
    roomName: string,
  ): Promise<RemoteResult> {
    if (effect.kind === "WORKER_DISPATCH") {
      const created = await this.dispatch.createDispatch(
        roomName,
        requiredString(request, "agentName"),
        { metadata: JSON.stringify({ ...asRecord(request.metadata), adoptionId: effect.id }) },
      );
      return { remoteId: created.id, result: { dispatchId: created.id } };
    }
    const dispatchId = requiredString(request, "dispatchId");
    await this.dispatch.deleteDispatch(dispatchId, roomName);
    return { remoteId: dispatchId, result: { deleted: true } };
  }

  private async executeEgressEffect(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
    roomName: string,
  ): Promise<RemoteResult> {
    if (effect.kind === "EGRESS_STOP") {
      const info = await this.egress.stopEgress(requiredString(request, "egressId"));
      if (!isTerminalEgress(info.status)) {
        throw new Error("Egress stop has not reached terminal state");
      }
      return {
        remoteId: info.egressId,
        result: terminalEgressResult(info),
      };
    }

    const output = this.egressOutput(effect, request);
    const started =
      effect.kind === "ROOM_COMPOSITE_EGRESS"
        ? await this.egress.startRoomCompositeEgress(roomName, output, {
            layout: "speaker",
            customBaseUrl: this.signedLayoutUrl(effect),
          })
        : await this.egress.startParticipantEgress(
            roomName,
            requiredString(request, "participantIdentity"),
            { segments: output },
            { screenShare: false },
          );
    if (!isViableEgressAdoption(started.status)) {
      throw new Error(
        `Egress start returned non-viable status ${egressStatusName(started.status)}`,
      );
    }
    return {
      remoteId: started.egressId,
      result: { egressId: started.egressId, status: egressStatusName(started.status) },
    };
  }

  private async executeParticipantEffect(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
    roomName: string,
  ): Promise<RemoteResult> {
    if (effect.kind === "PARTICIPANT_GRANT") {
      const participant = await this.rooms.updateParticipant(
        roomName,
        requiredString(request, "participantIdentity"),
        {
          permission: {
            canSubscribe: true,
            canPublish: true,
            canPublishData: false,
            canPublishSources: [TrackSource.MICROPHONE, TrackSource.CAMERA],
          },
        },
      );
      return { remoteId: participant.sid, result: { participantSid: participant.sid } };
    }
    await this.rooms.removeParticipant(roomName, requiredString(request, "participantIdentity"));
    return { remoteId: effect.id, result: { removed: true } };
  }

  private async executeStatusEffect(effect: Effect, roomName: string): Promise<RemoteResult> {
    if (effect.requestBytes === null) {
      throw new Error("persisted SendData request bytes are missing");
    }
    await this.sendPersistedStatus(roomName, effect.requestBytes, effect.id);
    return { remoteId: effect.id, result: { sent: true } };
  }

  private async executeArchiveEffect(effect: Effect): Promise<RemoteResult> {
    if (effect.kind === "ARCHIVE_RECONCILE") {
      throw new Error("archive reconciliation must execute through durable reconciliation store");
    }
    throw new Error(`unsupported remote effect ${effect.kind}`);
  }

  private egressOutput(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
  ): SegmentedFileOutput {
    return this.segmentedOutput(`${requiredString(request, "outputPrefix")}/${effect.id}`);
  }

  async areHumansAbsent(roomName: string, identities: readonly Uuid[]): Promise<boolean> {
    const allowed = new Set(identities);

    const rooms = await this.rooms.listRooms([roomName]);
    if (rooms.length === 0) {
      return true;
    }
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
  private signedLayoutUrl(effect: Effect): string {
    const expires = Date.now() + 10 * 60_000;
    const message = `${effect.consultationId}\n${String(effect.generation)}\n${String(expires)}`;
    const signature = createHmac("sha256", this.config.egressLayoutSigningKey)
      .update(message)
      .digest("hex");
    const url = new URL(this.config.egressLayoutUrl);
    url.searchParams.set("consultationId", effect.consultationId);
    url.searchParams.set("generation", String(effect.generation));
    url.searchParams.set("expires", String(expires));
    url.searchParams.set("signature", signature);
    return url.toString();
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
  }): Promise<{
    versionId: string;
    size: number;
    checksum: string;
  }> {
    return this.archive.putCreateOnce(input);
  }

  async discoverArchiveObjects(prefix: string) {
    return this.archive.discoverObjects(prefix);
  }
  async drainArchive(consultationId: Uuid): Promise<boolean> {
    return this.archive.drain(consultationId);
  }

  private async sendPersistedStatus(
    roomName: string,
    body: Uint8Array,
    effectId: Uuid,
  ): Promise<void> {
    const token = new AccessToken(this.config.apiKey, this.config.apiSecret, {
      identity: effectId,
    });
    token.addGrant({ roomAdmin: true, room: roomName });
    const endpoint = new URL(this.config.url);
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

  private segmentedOutput(prefix: string): SegmentedFileOutput {
    return new SegmentedFileOutput({
      protocol: SegmentedFileProtocol.HLS_PROTOCOL,
      filenamePrefix: prefix,
      playlistName: `${prefix}/index.m3u8`,
      livePlaylistName: `${prefix}/live.m3u8`,
      segmentDuration: 2,
      output: {
        case: "s3",
        value: new S3Upload({
          accessKey: this.config.s3.accessKey,
          secret: this.config.s3.secretKey,
          endpoint: this.config.s3.endpoint,
          bucket: this.config.s3.bucket,
          region: this.config.s3.region,
          forcePathStyle: this.config.s3.forcePathStyle,
        }),
      },
    });
  }
}

function twirpBytes(message: { toJson(): unknown }): Uint8Array {
  return Buffer.from(JSON.stringify(message.toJson()), "utf8");
}

function isTerminalEgress(status: EgressStatus): boolean {
  return (
    status === EgressStatus.EGRESS_COMPLETE ||
    status === EgressStatus.EGRESS_FAILED ||
    status === EgressStatus.EGRESS_ABORTED ||
    status === EgressStatus.EGRESS_LIMIT_REACHED
  );
}

export function isViableEgressAdoption(status: EgressStatus): boolean {
  return status === EgressStatus.EGRESS_STARTING || status === EgressStatus.EGRESS_ACTIVE;
}

function hasExactCaptureGrant(
  permission:
    | {
        readonly canSubscribe?: boolean;
        readonly canPublish?: boolean;
        readonly canPublishData?: boolean;
        readonly canPublishSources?: readonly TrackSource[];
      }
    | undefined,
): boolean {
  if (
    permission?.canSubscribe !== true ||
    permission.canPublish !== true ||
    permission.canPublishData !== false
  ) {
    return false;
  }
  const sources = permission.canPublishSources ?? [];
  return (
    sources.length === 2 &&
    sources.includes(TrackSource.MICROPHONE) &&
    sources.includes(TrackSource.CAMERA)
  );
}

function requiredString(value: Readonly<Record<string, unknown>>, key: string): string {
  const field = value[key];
  if (typeof field !== "string" || field.length === 0) {
    throw new Error(`${key} must be a non-empty string`);
  }
  return field;
}
function requiredOptionalString(
  value: Readonly<Record<string, unknown>>,
  key: string,
): string | undefined {
  const field = value[key];
  if (field === undefined) {
    return undefined;
  }
  if (typeof field !== "string" || field.length === 0) {
    throw new Error(`${key} must be a non-empty string`);
  }
  return field;
}
function requiredNumber(value: Readonly<Record<string, unknown>>, key: string): number {
  const field = value[key];
  if (typeof field !== "number" || !Number.isFinite(field)) {
    throw new Error(`${key} must be a number`);
  }
  return field;
}
function asRecord(value: unknown): Readonly<Record<string, unknown>> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  return value as Readonly<Record<string, unknown>>;
}

function optionalStringArray(value: unknown): string[] {
  if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) {
    return [];
  }
  return value;
}
function metadataAdoptionId(metadata: string): string | null {
  try {
    const parsed: unknown = JSON.parse(metadata);
    const adoptionId = asRecord(parsed).adoptionId;
    return typeof adoptionId === "string" ? adoptionId : null;
  } catch {
    return null;
  }
}
function egressMatches(
  info: { readonly roomName?: string; readonly request: unknown },
  effect: Effect,
  request: Readonly<Record<string, unknown>>,
): boolean {
  const roomName = roomNameFor(effect, request);
  const participantIdentity = requiredOptionalString(request, "participantIdentity");
  const encoded = JSON.stringify(info.request);
  return (
    info.roomName === roomName &&
    encoded.includes(effect.id) &&
    (participantIdentity === undefined || encoded.includes(participantIdentity))
  );
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

function roomNameFor(effect: Effect, request: Readonly<Record<string, unknown>>): string {
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

function terminalEgressResult(info: EgressInfo): EgressTerminalResult {
  return {
    egressId: info.egressId,
    status: egressStatusName(info.status),
    error: info.error,
    errorCode: info.errorCode,
    details: info.details,
    fileResults: info.fileResults,
    segmentResults: info.segmentResults,
  };
}

export function egressStatusName(status: EgressStatus): EgressStatusName {
  switch (status) {
    case EgressStatus.EGRESS_STARTING:
      return "EGRESS_STARTING";
    case EgressStatus.EGRESS_ACTIVE:
      return "EGRESS_ACTIVE";
    case EgressStatus.EGRESS_ENDING:
      return "EGRESS_ENDING";
    case EgressStatus.EGRESS_COMPLETE:
      return "EGRESS_COMPLETE";
    case EgressStatus.EGRESS_FAILED:
      return "EGRESS_FAILED";
    case EgressStatus.EGRESS_ABORTED:
      return "EGRESS_ABORTED";
    case EgressStatus.EGRESS_LIMIT_REACHED:
      return "EGRESS_LIMIT_REACHED";
    default:
      throw new Error(`unsupported LiveKit Egress status ${String(status)}`);
  }
}
