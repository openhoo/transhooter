import {
  ParticipantPermission,
  RoomParticipantIdentity,
  UpdateParticipantRequest,
} from "@livekit/protocol";
import { type RoomServiceClient, TrackSource } from "livekit-server-sdk";
import type { Effect } from "../../orchestration/model";
import type { Adoption, RemoteResult } from "../../orchestration/remote";
import { requiredString, roomNameFor, twirpBytes } from "./shared";

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

export class ParticipantEffects {
  constructor(private readonly getRooms: () => RoomServiceClient) {}

  async adopt(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
  ): Promise<Adoption | null> {
    if (effect.kind === "PARTICIPANT_GRANT") {
      const roomName = requiredString(request, "roomName");
      const participantIdentity = requiredString(request, "participantIdentity");
      const participant = await this.getRooms()
        .getParticipant(roomName, participantIdentity)
        .catch(() => undefined);
      if (participant === undefined || !hasExactCaptureGrant(participant.permission)) return null;
      return { remoteId: participant.sid, matchesRequest: true, terminal: false };
    }
    const participantIdentity = requiredString(request, "participantIdentity");
    const roomName = roomNameFor(effect, request);
    const participant = (await this.getRooms().listParticipants(roomName)).find(
      ({ identity }) => identity === participantIdentity,
    );
    return participant === undefined
      ? { remoteId: effect.id, matchesRequest: true, terminal: true }
      : null;
  }

  canonical(
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

  async execute(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
    roomName: string,
  ): Promise<RemoteResult> {
    if (effect.kind === "PARTICIPANT_GRANT") {
      const participant = await this.getRooms().updateParticipant(
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
    await this.getRooms().removeParticipant(
      roomName,
      requiredString(request, "participantIdentity"),
    );
    return { remoteId: effect.id, result: { removed: true } };
  }

  async compensate(effect: Effect): Promise<void> {
    const roomName = requiredString(effect.plan, "roomName");
    const participantIdentity = requiredString(effect.plan, "participantIdentity");
    const participant = await this.getRooms()
      .getParticipant(roomName, participantIdentity)
      .catch(() => undefined);
    if (participant !== undefined) {
      await this.getRooms().updateParticipant(roomName, participantIdentity, {
        permission: {
          canSubscribe: false,
          canPublish: false,
          canPublishData: false,
          canPublishSources: [],
        },
      });
    }
  }
}
