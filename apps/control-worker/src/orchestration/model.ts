import { createHash } from "node:crypto";
import type { ConsultationState, ExternalEffectState } from "@transhooter/contracts";
import type { OrchestrationEffectKind } from "@transhooter/server-core/rooms";

export type { ConsultationState };
export type EffectState = ExternalEffectState;
export type Uuid = string;
export const EFFECT_KINDS = [
  "ROOM_CREATE",
  "ROOM_COMPOSITE_EGRESS",
  "WORKER_DISPATCH",
  "PARTICIPANT_EGRESS",
  "PARTICIPANT_GRANT",
  "STATUS_PACKET",
  "ROOM_DRAIN",
  "DISPATCH_DELETE",
  "PARTICIPANT_REMOVE",
  "EGRESS_STOP",
  "ROOM_DELETE",
  "ARCHIVE_RECONCILE",
  "ARCHIVE_DELETE",
] as const satisfies readonly (
  | OrchestrationEffectKind
  | "DISPATCH_DELETE"
  | "PARTICIPANT_REMOVE"
)[];
export type EffectKind = (typeof EFFECT_KINDS)[number];

export interface Effect {
  readonly id: Uuid;
  readonly consultationId: Uuid;
  readonly generation: number;
  readonly kind: EffectKind;
  readonly subjectId: Uuid;
  readonly occurrenceKey: string;
  readonly plan: Readonly<Record<string, unknown>>;
  readonly state: EffectState;
  readonly requestBytes: Uint8Array | null;
  readonly requestSha256: string | null;
  readonly remoteId: string | null;
  /** Durable result recorded before an applied effect is terminally settled. */
  readonly appliedResult?: unknown;
  readonly attempt: number;
  readonly leaseOwner: Uuid | null;
  readonly leaseExpiresAt: Date | null;
}

export type PlannedEffect = Omit<
  Effect,
  | "state"
  | "requestBytes"
  | "requestSha256"
  | "appliedResult"
  | "remoteId"
  | "attempt"
  | "leaseOwner"
  | "leaseExpiresAt"
>;

export interface WorkerReservation {
  readonly consultationId: Uuid;
  readonly generation: number;
  readonly workerId: Uuid;
  readonly epoch: number;
  readonly heartbeatAt: Date;
  readonly leaseExpiresAt: Date;
  readonly acceptingLoad: boolean;
}

export interface Deadline {
  readonly consultationId: Uuid;
  readonly generation: number;
  readonly kind: "ready" | "absence" | "finalize" | "archive-reconcile";
  readonly dueAt: Date;
}

export interface VerifiedWebhook {
  readonly eventId: string;
  readonly occurredAtMs: number;
  readonly consultationId: Uuid;
  readonly generation: number;
  readonly participantId: Uuid | null;
  readonly kind: "PARTICIPANT_JOINED" | "PARTICIPANT_LEFT" | "EGRESS_ACTIVE" | "EGRESS_TERMINAL";
  readonly egressId: string | null;
  readonly egressStatus: string | null;
  readonly rawSha256: string;
}

export interface OutboxItem {
  readonly id: Uuid;
  readonly aggregateId: Uuid;
  readonly type: string;
  readonly generation: number;
  readonly payload: unknown;
  readonly attempts: number;
}

export interface ClaimOptions {
  readonly owner: Uuid;
  readonly now: Date;
  readonly leaseMs: number;
  readonly limit: number;
}

export interface ReconciliationExpectation {
  readonly id: Uuid;
  readonly objectClass: string;
  readonly causalKey: string;
  readonly sampleStart: number | null;
  readonly sampleEnd: number | null;
  readonly fulfilledObjectId: Uuid | null;
}

export interface ArchivedObject {
  readonly id: Uuid;
  readonly objectClass: string;
  readonly key: string;
  readonly versionId: string;
  readonly size: number;
  readonly sha256: string;
  readonly s3Checksum: string;
  readonly contentType: string;
}

export interface ReconciliationProviderAttempt {
  readonly attemptId: Uuid;
  readonly stage: string;
}

export interface ReconciliationProviderGap {
  readonly attemptId: Uuid;
  readonly stage: string;
  readonly provider: string;
  readonly directionId: Uuid;
  readonly operationId: Uuid;
  readonly attemptNumber: number;
  readonly outcome: string;
  readonly errorKind: string | null;
  readonly acceptedInputWatermark: number | null;
  readonly receivedOutputWatermark: number | null;
  readonly emittedOutputWatermark: number | null;
  readonly retryDecision: unknown;
}

export interface ReconciliationDirection {
  readonly mode: "same_language" | "translated";
  readonly destinationParticipantId: Uuid;
  readonly emittedOutput: number;
}

export interface ReconciliationSnapshot {
  readonly archiveId: Uuid;
  readonly state: "reconciling";
  readonly reconciliationDeadlineAt: Date;
  readonly roomClose: unknown;
  readonly workerTerminal: unknown;
  readonly egressResults: readonly unknown[];
  readonly providerAttempts: readonly ReconciliationProviderAttempt[];
  readonly providerGaps: readonly ReconciliationProviderGap[];
  readonly directions: readonly ReconciliationDirection[];
  readonly expectations: readonly ReconciliationExpectation[];
  readonly objects: readonly ArchivedObject[];
}

export interface FinalInventoryObject {
  readonly id: Uuid;
  readonly versionId: string;
  readonly size: number;
  readonly checksum: string;
}

export interface DerivedArchiveObject {
  readonly id: Uuid;
  readonly objectClass: string;
  readonly key: string;
  readonly versionId: string;
  readonly size: number;
  readonly sha256: string;
  readonly checksum: string;
  readonly contentType: string;
}

export interface RoomDrainPlan {
  readonly egressIds: readonly string[];
  readonly participantIds: readonly Uuid[];
  readonly dispatchIds: readonly string[];
  readonly roomCreated: boolean;
  readonly resourceRoomName: string | null;
}

export interface CapacityDimension {
  readonly key: string;
  readonly capacity: number;
  readonly units: number;
}

export type FinalizationAdmission = "admitted" | ConsultationState | null;

export type AppliedTransition = "applied" | "rejected";

export interface DurableStore {
  readiness(): Promise<void>;

  claimOutbox(options: ClaimOptions): Promise<readonly OutboxItem[]>;
  completeOutbox(id: Uuid, owner: Uuid): Promise<void>;
  retryOutbox(id: Uuid, owner: Uuid, error: string, nextAt: Date): Promise<void>;
  claimDeadlines(options: ClaimOptions): Promise<readonly Deadline[]>;
  completeDeadline(deadline: Deadline, owner: Uuid): Promise<void>;
  claimStaleReservations(options: ClaimOptions): Promise<readonly WorkerReservation[]>;
  preparePendingArchiveDeletes(options: ClaimOptions): Promise<void>;

  claimEffects(options: ClaimOptions): Promise<readonly Effect[]>;
  persistCalling(
    effectId: Uuid,
    owner: Uuid,
    requestBytes: Uint8Array,
    requestSha256: string,
  ): Promise<Effect | null>;
  renewEffectLease(effectId: Uuid, owner: Uuid, leaseExpiresAt: Date): Promise<boolean>;
  markApplied(
    effectId: Uuid,
    owner: Uuid,
    remoteId: string | null,
    result: unknown,
  ): Promise<AppliedTransition>;
  markDone(effectId: Uuid, owner: Uuid): Promise<void>;
  markFailed(effectId: Uuid, owner: Uuid, error: string, retryAt: Date | null): Promise<void>;
  markCompensating(effectId: Uuid, owner: Uuid, reason: string): Promise<void>;
  scheduleEffect(input: PlannedEffect): Promise<void>;

  currentGeneration(consultationId: Uuid): Promise<number | null>;
  consultationState(consultationId: Uuid): Promise<ConsultationState | null>;
  applyVerifiedWebhook(event: VerifiedWebhook): Promise<boolean>;
  isStandardHuman(consultationId: Uuid, participantId: Uuid): Promise<boolean>;
  markCaptureReady(
    consultationId: Uuid,
    generation: number,
    participantIdentity: Uuid,
    participantEgressId: string,
  ): Promise<"active" | null>;
  presenceEpoch(consultationId: Uuid, generation: number): Promise<number | null>;
  admitFinalization(
    consultationId: Uuid,
    generation: number,
    presenceEpoch: number,
    now: Date,
  ): Promise<FinalizationAdmission>;
  humanIdentities(consultationId: Uuid): Promise<readonly [Uuid, Uuid]>;
  seedDeadlines(consultationId: Uuid, generation: number): Promise<void>;
  roomDrainPlan(consultationId: Uuid, generation: number): Promise<RoomDrainPlan>;
  completeRoomDrain(consultationId: Uuid, generation: number): Promise<void>;

  reserveWorker(consultationId: Uuid, generation: number): Promise<WorkerReservation>;
  workerReservation(consultationId: Uuid, generation: number): Promise<WorkerReservation | null>;
  heartbeat(workerId: Uuid, epoch: number, now: Date, leaseExpiresAt: Date): Promise<boolean>;
  fenceWorkerAndPlanFailure(
    reservation: WorkerReservation,
    owner: Uuid,
    reason: string,
    effects: readonly PlannedEffect[],
  ): Promise<boolean>;
  fenceWorkerAndScheduleCancellation(
    consultationId: Uuid,
    cleanupGeneration: number,
    resourceGeneration: number,
    owner: Uuid,
    reason: string,
    effects: readonly PlannedEffect[],
  ): Promise<void>;
  workerDispatchMetadata(consultationId: Uuid, generation: number): Promise<unknown>;
  capacityDimensions(consultationId: Uuid): Promise<readonly CapacityDimension[]>;

  planFailureEffects(
    consultationId: Uuid,
    generation: number,
    reason: string,
    effects: readonly PlannedEffect[],
  ): Promise<void>;
  reconciliationSnapshot(
    consultationId: Uuid,
    cleanupGeneration: number,
    resourceGeneration: number,
  ): Promise<ReconciliationSnapshot | null>;
  completeReconciliation(
    effect: Effect,
    owner: Uuid,
    now: Date,
    snapshot: ReconciliationSnapshot,
    inventory: Readonly<Record<string, unknown>>,
    sha256: string,
    finalObject: FinalInventoryObject,
    derivedObjects: readonly DerivedArchiveObject[],
  ): Promise<boolean>;
  finishArchiveDeletionIfEmpty(
    consultationId: Uuid,
    generation: number,
    writeEpoch: number,
  ): Promise<boolean>;
}

export interface Clock {
  now(): Date;
}

export const systemClock: Clock = {
  now: () => new Date(),
};

function stable(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(stable);
  }
  if (value !== null && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, nestedValue]) => [key, stable(nestedValue)]);
    return Object.fromEntries(entries);
  }
  return value;
}

export function canonicalRequest(value: unknown): { bytes: Uint8Array; sha256: string } {
  const bytes = Buffer.from(JSON.stringify(stable(value)), "utf8");
  const sha256 = createHash("sha256").update(bytes).digest("hex");
  return { bytes, sha256 };
}
