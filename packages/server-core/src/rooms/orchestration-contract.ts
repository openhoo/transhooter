import { createHash } from "node:crypto";
import { DomainError, type UUID } from "../domain/model";

export const ORCHESTRATION_TOPICS = {
  provisioningRequested: "consultation.provisioning.requested",
  effectPlan: "orchestration.effect.plan",
  effectApplied: "orchestration.effect.applied",
  finalizationRequested: "consultation.finalization.requested",
} as const;

export type OrchestrationEffectKind =
  | "ROOM_CREATE"
  | "ROOM_COMPOSITE_EGRESS"
  | "WORKER_DISPATCH"
  | "PARTICIPANT_EGRESS"
  | "PARTICIPANT_GRANT"
  | "STATUS_PACKET"
  | "ROOM_DRAIN"
  | "DISPATCH_DELETE"
  | "PARTICIPANT_REMOVE"
  | "EGRESS_STOP"
  | "ROOM_DELETE"
  | "ARCHIVE_RECONCILE"
  | "ARCHIVE_DELETE";

export interface ProvisioningRequested {
  consultationId: UUID;
  generation: number;
  subjectId: UUID;
}

export interface ParticipantEgressEffectRequest {
  roomName: string;
  resourceRoomName: string;
  participantIdentity: UUID;
  outputPrefix: string;
  resourceGeneration: number;
}

export interface ParticipantRemoveEffectRequest {
  roomName: string;
  resourceRoomName: string;
  participantIdentity: UUID;
  resourceGeneration: number;
}

type GenericEffectKind = Exclude<
  OrchestrationEffectKind,
  "PARTICIPANT_EGRESS" | "PARTICIPANT_REMOVE"
>;

export type EffectPlanRequested = ProvisioningRequested &
  (
    | {
        kind: "PARTICIPANT_EGRESS";
        request: ParticipantEgressEffectRequest;
      }
    | {
        kind: "PARTICIPANT_REMOVE";
        request: ParticipantRemoveEffectRequest;
      }
    | {
        kind: GenericEffectKind;
        request: Readonly<Record<string, unknown>>;
      }
  );

export function deterministicUuid(namespace: UUID, name: string): UUID {
  const namespaceBytes = Buffer.from(namespace.replaceAll("-", ""), "hex");
  if (namespaceBytes.length !== 16) {
    throw new DomainError("INVALID_UUID_NAMESPACE");
  }

  const hash = createHash("sha1").update(namespaceBytes).update(name).digest().subarray(0, 16);
  const versionFiveByte = (hash.readUInt8(6) & 0x0f) | 0x50;
  const rfcVariantByte = (hash.readUInt8(8) & 0x3f) | 0x80;
  hash.writeUInt8(versionFiveByte, 6);
  hash.writeUInt8(rfcVariantByte, 8);

  const hex = hash.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export function deterministicRoomName(consultationId: UUID, generation: number): UUID {
  return deterministicUuid(consultationId, `${String(generation)}:room`);
}
