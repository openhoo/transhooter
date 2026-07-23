import { createPrismaDatabase, type PrismaDatabase } from "@transhooter/server-core/persistence";
import type {
  AppliedTransition,
  ClaimOptions,
  ConsultationState,
  Deadline,
  DerivedArchiveObject,
  DurableStore,
  Effect,
  FinalInventoryObject,
  OutboxItem,
  PlannedEffect,
  ReconciliationSnapshot,
  Uuid,
  VerifiedWebhook,
  WorkerReservation,
} from "../orchestration/model";
import * as archiveOperations from "./postgres-store/archives";
import * as consultationOperations from "./postgres-store/consultations";
import * as effectOperations from "./postgres-store/effects";
import type { CapacityDimension } from "./postgres-store/shared";

export { persistSupervisorTerminalCheckpoints } from "./postgres-store/consultations";

export class PostgresStore implements DurableStore {
  private constructor(private readonly database: PrismaDatabase) {}

  static connect(url: string): PostgresStore {
    return new PostgresStore(
      createPrismaDatabase({
        connectionString: url,
        pool: { max: 10 },
      }),
    );
  }

  async close(): Promise<void> {
    await this.database.disconnect();
  }

  async readiness(): Promise<void> {
    await this.database.readiness();
  }

  async claimOutbox(options: ClaimOptions): Promise<readonly OutboxItem[]> {
    return effectOperations.claimOutbox(this.database.client, options);
  }

  async completeOutbox(id: Uuid, owner: Uuid): Promise<void> {
    return effectOperations.completeOutbox(this.database.client, id, owner);
  }

  async retryOutbox(id: Uuid, owner: Uuid, error: string, nextAt: Date): Promise<void> {
    return effectOperations.retryOutbox(this.database.client, id, owner, error, nextAt);
  }

  async claimEffects(options: ClaimOptions): Promise<readonly Effect[]> {
    return effectOperations.claimEffects(this.database.client, options);
  }

  async persistCalling(
    effectId: Uuid,
    owner: Uuid,
    requestBytes: Uint8Array,
    requestSha256: string,
  ): Promise<Effect | null> {
    return effectOperations.persistCalling(
      this.database.client,
      effectId,
      owner,
      requestBytes,
      requestSha256,
    );
  }

  async markApplied(
    effectId: Uuid,
    owner: Uuid,
    remoteId: string | null,
    result: unknown,
  ): Promise<AppliedTransition> {
    return effectOperations.markApplied(this.database.client, effectId, owner, remoteId, result);
  }

  async markDone(effectId: Uuid, owner: Uuid): Promise<void> {
    return effectOperations.markDone(this.database.client, effectId, owner);
  }

  async markFailed(
    effectId: Uuid,
    owner: Uuid,
    error: string,
    retryAt: Date | null,
  ): Promise<void> {
    return effectOperations.markFailed(this.database.client, effectId, owner, error, retryAt);
  }

  async renewEffectLease(effectId: Uuid, owner: Uuid, leaseExpiresAt: Date): Promise<boolean> {
    return effectOperations.renewEffectLease(this.database.client, effectId, owner, leaseExpiresAt);
  }

  async markCompensating(effectId: Uuid, owner: Uuid, reason: string): Promise<void> {
    return effectOperations.markCompensating(this.database.client, effectId, owner, reason);
  }

  async scheduleEffect(input: PlannedEffect): Promise<void> {
    return effectOperations.scheduleEffect(this.database.client, input);
  }

  async currentGeneration(consultationId: Uuid): Promise<number | null> {
    return consultationOperations.currentGeneration(this.database.client, consultationId);
  }
  async claimDeadlines(options: ClaimOptions): Promise<readonly Deadline[]> {
    return consultationOperations.claimDeadlines(this.database.client, options);
  }
  async completeDeadline(deadline: Deadline, owner: Uuid): Promise<void> {
    return consultationOperations.completeDeadline(this.database.client, deadline, owner);
  }
  async claimStaleReservations(options: ClaimOptions): Promise<readonly WorkerReservation[]> {
    return consultationOperations.claimStaleReservations(this.database.client, options);
  }
  async heartbeat(
    workerId: Uuid,
    epoch: number,
    now: Date,
    leaseExpiresAt: Date,
  ): Promise<boolean> {
    return consultationOperations.heartbeat(
      this.database.client,
      workerId,
      epoch,
      now,
      leaseExpiresAt,
    );
  }
  async reserveWorker(consultationId: Uuid, generation: number): Promise<WorkerReservation> {
    return consultationOperations.reserveWorker(this.database.client, consultationId, generation);
  }
  async applyVerifiedWebhook(event: VerifiedWebhook): Promise<boolean> {
    return consultationOperations.applyVerifiedWebhook(this.database.client, event);
  }
  async presenceEpoch(consultationId: Uuid, generation: number): Promise<number | null> {
    return consultationOperations.presenceEpoch(this.database.client, consultationId, generation);
  }
  async admitFinalization(
    consultationId: Uuid,
    generation: number,
    presenceEpoch: number,
    now: Date,
  ) {
    return consultationOperations.admitFinalization(
      this.database.client,
      consultationId,
      generation,
      presenceEpoch,
      now,
    );
  }
  async isStandardHuman(consultationId: Uuid, participantId: Uuid): Promise<boolean> {
    return consultationOperations.isStandardHuman(
      this.database.client,
      consultationId,
      participantId,
    );
  }
  async markCaptureReady(
    consultationId: Uuid,
    generation: number,
    participantIdentity: Uuid,
    participantEgressId: string,
  ) {
    return consultationOperations.markCaptureReady(
      this.database.client,
      consultationId,
      generation,
      participantIdentity,
      participantEgressId,
    );
  }
  async consultationState(consultationId: Uuid): Promise<ConsultationState | null> {
    return consultationOperations.consultationState(this.database.client, consultationId);
  }
  async workerReservation(
    consultationId: Uuid,
    generation: number,
  ): Promise<WorkerReservation | null> {
    return consultationOperations.workerReservation(
      this.database.client,
      consultationId,
      generation,
    );
  }
  async workerDispatchMetadata(consultationId: Uuid, generation: number): Promise<unknown> {
    return consultationOperations.workerDispatchMetadata(
      this.database.client,
      consultationId,
      generation,
    );
  }
  async planFailureEffects(
    consultationId: Uuid,
    generation: number,
    reason: string,
    effects: readonly PlannedEffect[],
  ): Promise<void> {
    return consultationOperations.planFailureEffects(
      this.database.client,
      consultationId,
      generation,
      reason,
      effects,
    );
  }
  async fenceWorkerAndPlanFailure(
    reservation: WorkerReservation,
    owner: Uuid,
    reason: string,
    effects: readonly PlannedEffect[],
  ): Promise<boolean> {
    return consultationOperations.fenceWorkerAndPlanFailure(
      this.database.client,
      reservation,
      owner,
      reason,
      effects,
    );
  }
  async fenceWorkerAndScheduleCancellation(
    consultationId: Uuid,
    cleanupGeneration: number,
    resourceGeneration: number,
    owner: Uuid,
    reason: string,
    effects: readonly PlannedEffect[],
  ): Promise<void> {
    return consultationOperations.fenceWorkerAndScheduleCancellation(
      this.database.client,
      consultationId,
      cleanupGeneration,
      resourceGeneration,
      owner,
      reason,
      effects,
    );
  }
  async humanIdentities(consultationId: Uuid): Promise<readonly [Uuid, Uuid]> {
    return consultationOperations.humanIdentities(this.database.client, consultationId);
  }
  async seedDeadlines(consultationId: Uuid, generation: number): Promise<void> {
    return consultationOperations.seedDeadlines(this.database.client, consultationId, generation);
  }
  async roomDrainPlan(consultationId: Uuid, generation: number) {
    return consultationOperations.roomDrainPlan(this.database.client, consultationId, generation);
  }
  async completeRoomDrain(consultationId: Uuid, generation: number): Promise<void> {
    return consultationOperations.completeRoomDrain(
      this.database.client,
      consultationId,
      generation,
    );
  }
  async capacityDimensions(consultationId: Uuid): Promise<readonly CapacityDimension[]> {
    return consultationOperations.capacityDimensions(this.database.client, consultationId);
  }
  async preparePendingArchiveDeletes(_options: ClaimOptions): Promise<void> {
    return archiveOperations.preparePendingArchiveDeletes(this.database.client);
  }
  async reconciliationSnapshot(
    consultationId: Uuid,
    cleanupGeneration: number,
    resourceGeneration: number,
  ): Promise<ReconciliationSnapshot | null> {
    return archiveOperations.reconciliationSnapshot(
      this.database.client,
      consultationId,
      cleanupGeneration,
      resourceGeneration,
    );
  }
  async completeReconciliation(
    effect: Effect,
    owner: Uuid,
    now: Date,
    snapshot: ReconciliationSnapshot,
    inventory: Readonly<Record<string, unknown>>,
    sha256: string,
    finalObject: FinalInventoryObject,
    derivedObjects: readonly DerivedArchiveObject[],
  ): Promise<boolean> {
    return archiveOperations.completeReconciliation(
      this.database.client,
      effect,
      owner,
      now,
      snapshot,
      inventory,
      sha256,
      finalObject,
      derivedObjects,
    );
  }
  async finishArchiveDeletionIfEmpty(
    consultationId: Uuid,
    generation: number,
    writeEpoch: number,
  ): Promise<boolean> {
    return archiveOperations.finishArchiveDeletionIfEmpty(
      this.database.client,
      consultationId,
      generation,
      writeEpoch,
    );
  }
}
