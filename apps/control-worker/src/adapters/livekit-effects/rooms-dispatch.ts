import {
  CreateAgentDispatchRequest,
  CreateRoomRequest,
  DeleteAgentDispatchRequest,
  DeleteRoomRequest,
} from "@livekit/protocol";
import type { AgentDispatchClient, EgressClient, RoomServiceClient } from "livekit-server-sdk";
import type { Effect } from "../../orchestration/model";
import { canonicalRequest as encodeCanonicalRequest } from "../../orchestration/model";
import type { Adoption, RemoteResult } from "../../orchestration/remote";
import { isTerminalEgress, terminalEgressResult } from "./egress";
import {
  asRecord,
  metadataAdoptionId,
  requiredNumber,
  requiredString,
  roomNameFor,
  twirpBytes,
} from "./shared";

interface LiveKitClients {
  readonly rooms: RoomServiceClient;
  readonly dispatch: AgentDispatchClient;
  readonly egress: EgressClient;
}

export class RoomDispatchEffects {
  constructor(private readonly clients: LiveKitClients) {}

  async adopt(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
  ): Promise<Adoption | null> {
    if (effect.kind === "ROOM_CREATE") {
      const room = (await this.clients.rooms.listRooms([requiredString(request, "roomName")]))[0];
      return room === undefined
        ? null
        : {
            remoteId: room.sid,
            matchesRequest: metadataAdoptionId(room.metadata) === effect.id,
            terminal: false,
          };
    }
    if (effect.kind === "ROOM_DRAIN") {
      const roomName = requiredString(request, "roomName");
      if ((await this.clients.rooms.listRooms([roomName])).length > 0) return null;
      const terminals = await this.clients.egress.listEgress({ roomName });
      if (terminals.some((info) => !isTerminalEgress(info.status))) return null;
      return {
        remoteId: effect.id,
        matchesRequest: true,
        terminal: true,
        result: { roomClosed: true, adopted: true, egressTerminals: terminals },
      };
    }
    if (effect.kind === "ROOM_DELETE") {
      const rooms = await this.clients.rooms.listRooms([requiredString(request, "roomName")]);
      return rooms.length === 0
        ? { remoteId: effect.id, matchesRequest: true, terminal: true }
        : null;
    }
    if (effect.kind === "WORKER_DISPATCH") {
      const roomName = requiredString(request, "roomName");
      const found = (await this.clients.dispatch.listDispatch(roomName)).find(
        (candidate) => metadataAdoptionId(candidate.metadata) === effect.id,
      );
      return found === undefined
        ? null
        : { remoteId: found.id, matchesRequest: true, terminal: false };
    }
    const dispatchId = requiredString(request, "dispatchId");
    const roomName = requiredString(request, "roomName");
    if ((await this.clients.rooms.listRooms([roomName])).length === 0) {
      return { remoteId: dispatchId, matchesRequest: true, terminal: true };
    }
    const found = (await this.clients.dispatch.listDispatch(roomName)).find(
      ({ id }) => id === dispatchId,
    );
    return found === undefined
      ? { remoteId: dispatchId, matchesRequest: true, terminal: true }
      : null;
  }

  canonical(
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
    if (effect.kind === "ROOM_DRAIN") {
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

  async execute(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
    roomName: string,
  ): Promise<RemoteResult> {
    if (effect.kind === "ROOM_CREATE") {
      const room = await this.clients.rooms.createRoom({
        name: roomName,
        emptyTimeout: requiredNumber(request, "emptyTimeout"),
        metadata: JSON.stringify({ ...asRecord(request.metadata), adoptionId: effect.id }),
      });
      return { remoteId: room.sid, result: { sid: room.sid, name: room.name } };
    }
    if (effect.kind === "ROOM_DELETE") {
      await this.clients.rooms.deleteRoom(roomName);
      return { remoteId: effect.id, result: { deleted: true, roomClosed: true } };
    }
    if (effect.kind === "ROOM_DRAIN") {
      for (const job of await this.clients.dispatch.listDispatch(roomName)) {
        await this.clients.dispatch.deleteDispatch(job.id, roomName);
      }
      for (const info of await this.clients.egress.listEgress({ roomName, active: true })) {
        await this.clients.egress.stopEgress(info.egressId);
      }
      const terminalEgress = await this.clients.egress.listEgress({ roomName });
      if (terminalEgress.some((info) => !isTerminalEgress(info.status))) {
        throw new Error("Egress drain has not reached terminal state");
      }
      for (const participant of await this.clients.rooms.listParticipants(roomName)) {
        const role = participant.attributes["consultation.role"];
        if (role === "employee" || role === "customer") {
          await this.clients.rooms.removeParticipant(roomName, participant.identity);
        }
      }
      await this.clients.rooms.deleteRoom(roomName);
      return {
        remoteId: effect.id,
        result: {
          roomClosed: true,
          egressTerminals: terminalEgress.map(terminalEgressResult),
        },
      };
    }
    if (effect.kind === "WORKER_DISPATCH") {
      const created = await this.clients.dispatch.createDispatch(
        roomName,
        requiredString(request, "agentName"),
        { metadata: JSON.stringify({ ...asRecord(request.metadata), adoptionId: effect.id }) },
      );
      return { remoteId: created.id, result: { dispatchId: created.id } };
    }
    const dispatchId = requiredString(request, "dispatchId");
    await this.clients.dispatch.deleteDispatch(dispatchId, roomName);
    return { remoteId: dispatchId, result: { deleted: true } };
  }

  async compensate(effect: Effect, remoteId: string): Promise<void> {
    if (effect.kind === "WORKER_DISPATCH") {
      const roomName = roomNameFor(effect, effect.plan);
      const found = (await this.clients.dispatch.listDispatch(roomName)).find(
        ({ id }) => id === remoteId,
      );
      if (found !== undefined) await this.clients.dispatch.deleteDispatch(remoteId, roomName);
      return;
    }
    if (effect.kind === "ROOM_CREATE" || effect.kind === "ROOM_DELETE") {
      const roomName = roomNameFor(effect, effect.plan);
      if ((await this.clients.rooms.listRooms([roomName])).length > 0)
        await this.clients.rooms.deleteRoom(roomName);
    }
  }
}
