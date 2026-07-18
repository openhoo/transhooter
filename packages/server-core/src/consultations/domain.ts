import type { RoomProviderSelection } from "@transhooter/contracts";
import {
  type ArchiveState,
  type ConsultationState,
  DomainError,
  type Instant,
  type ParticipantRole,
  type UUID,
} from "../domain/model";

export interface ConsentRecord {
  readonly version: 1;
  readonly copyHash: string;
  readonly snapshotHash: string;
  readonly consentedAt: Instant;
}

export interface ParticipantSlot {
  readonly id: UUID;
  readonly role: ParticipantRole;
  readonly userId: UUID;
  readonly livekitIdentity: UUID;
  readonly displayName: string | null;
  readonly language: string | null;
  readonly consent: ConsentRecord | null;
  readonly present: boolean;
  readonly eventWatermark: string | null;
  readonly eventOccurredAt: Instant | null;
  readonly publicationGranted: boolean;
  readonly participantEgressId: string | null;
}

export interface Consultation {
  readonly id: UUID;
  readonly state: ConsultationState;
  readonly archiveState: ArchiveState;
  readonly providerProfileId: UUID;
  readonly providerProfileRevision: number;
  readonly participants: readonly [ParticipantSlot, ParticipantSlot];
  readonly providerSelection: RoomProviderSelection | null;
  readonly snapshotHash: string | null;
  readonly generation: number;
  readonly roomName: string | null;
  readonly roomSid: string | null;
  readonly dispatchId: string | null;
  readonly compositeEgressId: string | null;
  readonly workerIdentity: UUID | null;
  readonly readyDeadlineAt: Instant | null;
  readonly finalizeDeadlineAt: Instant | null;
  readonly bothAbsentSince: Instant | null;
  readonly admissionFencedAt: Instant | null;
  readonly createdAt: Instant;
  readonly updatedAt: Instant;
}

type JoinEligibility =
  | "eligible"
  | "WAITING_FOR_PREFERENCES"
  | "CONSENT_REQUIRED"
  | "SNAPSHOT_CHANGED";

function hasParticipant(value: Consultation, participantId: UUID): boolean {
  return value.participants.some((slot) => slot.id === participantId);
}

export function assertTwoSlots(value: Consultation): void {
  const roles = new Set(value.participants.map((slot) => slot.role));
  if (roles.size !== 2 || !roles.has("employee") || !roles.has("customer")) {
    throw new DomainError("INVALID_PARTICIPANTS");
  }

  const participantIds = new Set(value.participants.map((slot) => slot.id));
  if (participantIds.size !== 2) {
    throw new DomainError("INVALID_PARTICIPANTS");
  }
}

export function slotForUser(value: Consultation, userId: UUID): ParticipantSlot {
  const slot = value.participants.find((candidate) => candidate.userId === userId);
  if (!slot) {
    throw new DomainError("FORBIDDEN");
  }
  return slot;
}

export function withPreferences(
  value: Consultation,
  participantId: UUID,
  displayName: string,
  language: string,
  now: Instant,
): Consultation {
  if (value.state !== "invited") {
    throw new DomainError("PREFERENCES_LOCKED");
  }

  const cleanName = displayName.trim();
  if (!cleanName || cleanName.length > 120 || !language.trim()) {
    throw new DomainError("INVALID_PREFERENCES");
  }
  if (!hasParticipant(value, participantId)) {
    throw new DomainError("FORBIDDEN");
  }

  const participants = value.participants.map(
    (slot): ParticipantSlot =>
      slot.id === participantId
        ? { ...slot, displayName: cleanName, language, consent: null }
        : { ...slot, consent: null },
  ) as [ParticipantSlot, ParticipantSlot];

  return {
    ...value,
    participants,
    providerSelection: null,
    snapshotHash: null,
    updatedAt: now,
  };
}

export function withProviderSelection(
  value: Consultation,
  selection: RoomProviderSelection,
  snapshotHash: string,
  now: Instant,
): Consultation {
  if (value.state !== "invited") {
    throw new DomainError("INVALID_STATE");
  }
  if (value.participants.some((slot) => slot.language === null)) {
    throw new DomainError("WAITING_FOR_PREFERENCES");
  }
  if (!snapshotHash) {
    throw new DomainError("INVALID_SNAPSHOT");
  }

  const participants = value.participants.map((slot) => ({
    ...slot,
    consent: null,
  })) as [ParticipantSlot, ParticipantSlot];

  return {
    ...value,
    providerSelection: selection,
    snapshotHash,
    participants,
    updatedAt: now,
  };
}

export function withConsent(
  value: Consultation,
  participantId: UUID,
  snapshotHash: string,
  copyHash: string,
  now: Instant,
): Consultation {
  if (value.state !== "invited") {
    throw new DomainError("INVALID_STATE");
  }
  if (!value.snapshotHash || value.snapshotHash !== snapshotHash) {
    throw new DomainError("SNAPSHOT_CHANGED");
  }
  if (!hasParticipant(value, participantId)) {
    throw new DomainError("FORBIDDEN");
  }

  const participants = value.participants.map(
    (slot): ParticipantSlot =>
      slot.id === participantId
        ? {
            ...slot,
            consent: {
              version: 1,
              copyHash,
              snapshotHash,
              consentedAt: now,
            },
          }
        : slot,
  ) as [ParticipantSlot, ParticipantSlot];

  return { ...value, participants, updatedAt: now };
}

export function joinEligibility(value: Consultation): JoinEligibility {
  if (value.participants.some((slot) => slot.language === null)) {
    return "WAITING_FOR_PREFERENCES";
  }
  if (!value.snapshotHash || !value.providerSelection) {
    return "WAITING_FOR_PREFERENCES";
  }
  if (value.participants.some((slot) => slot.consent === null)) {
    return "CONSENT_REQUIRED";
  }
  if (value.participants.some((slot) => slot.consent?.snapshotHash !== value.snapshotHash)) {
    return "SNAPSHOT_CHANGED";
  }
  return "eligible";
}

export function beginProvisioning(
  value: Consultation,
  roomName: string,
  now: Instant,
): Consultation {
  if (value.state === "ready" || value.state === "active") {
    return value;
  }
  if (value.state !== "invited") {
    throw new DomainError("INVALID_STATE");
  }

  const eligibility = joinEligibility(value);
  if (eligibility !== "eligible") {
    throw new DomainError(eligibility);
  }

  return {
    ...value,
    state: "ready",
    generation: value.generation + 1,
    roomName,
    readyDeadlineAt: new Date(now.getTime() + 300_000),
    updatedAt: now,
  };
}

export function grantCapture(
  value: Consultation,
  participantId: UUID,
  egressId: string,
  now: Instant,
): Consultation {
  if (value.state !== "ready" && value.state !== "active") {
    throw new DomainError("INVALID_STATE");
  }
  if (!hasParticipant(value, participantId)) {
    throw new DomainError("FORBIDDEN");
  }

  const participants = value.participants.map(
    (slot): ParticipantSlot =>
      slot.id === participantId
        ? {
            ...slot,
            participantEgressId: egressId,
            publicationGranted: true,
          }
        : slot,
  ) as [ParticipantSlot, ParticipantSlot];

  return { ...value, participants, state: "active", updatedAt: now };
}

export function beginFinalization(value: Consultation, now: Instant): Consultation {
  if (value.state === "finalizing" || value.state === "ended") {
    return value;
  }
  if (value.state !== "active") {
    throw new DomainError("INVALID_STATE");
  }

  const archiveState =
    value.archiveState === "pending" || value.archiveState === "recording"
      ? "reconciling"
      : value.archiveState;

  return {
    ...value,
    state: "finalizing",
    archiveState,
    finalizeDeadlineAt: new Date(now.getTime() + 900_000),
    updatedAt: now,
  };
}

export function beginFencedRoomFinalization(value: Consultation, now: Instant): Consultation {
  if (value.state !== "active") {
    throw new DomainError("INVALID_STATE");
  }
  return {
    ...value,
    state: "finalizing",
    archiveState: "reconciling",
    generation: value.generation + 1,
    finalizeDeadlineAt: new Date(now.getTime() + 900_000),
    updatedAt: now,
  };
}

export function cancelBeforeStart(value: Consultation, now: Instant): Consultation {
  if (value.state === "cancelled") {
    return value;
  }
  if (value.state !== "invited" && value.state !== "ready") {
    throw new DomainError("INVALID_STATE");
  }
  return {
    ...value,
    state: "cancelled",
    generation: value.generation + 1,
    updatedAt: now,
  };
}

export function finishConsultation(value: Consultation, now: Instant): Consultation {
  if (value.state === "ended") {
    return value;
  }
  if (value.state !== "finalizing") {
    throw new DomainError("INVALID_STATE");
  }
  return { ...value, state: "ended", updatedAt: now };
}
