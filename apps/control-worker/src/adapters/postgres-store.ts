import { sql } from "drizzle-orm";
import { drizzle, type PostgresJsDatabase } from "drizzle-orm/postgres-js";
import postgres, { type Sql } from "postgres";
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
  private readonly db: PostgresJsDatabase;
  private constructor(private readonly client: Sql) {
    this.db = drizzle({ client });
  }

  static connect(url: string): PostgresStore {
    const client = postgres(url, {
      max: 10,
      prepare: false,
      transform: { undefined: null },
    });
    return new PostgresStore(client);
  }

  async close(): Promise<void> {
    await this.client.end({ timeout: 5 });
  }

  async readiness(): Promise<void> {
    await this.db.execute(sql`SELECT 1`);
  }

  async claimOutbox(options: ClaimOptions): Promise<readonly OutboxItem[]> {
    return effectOperations.claimOutbox(this.db, this.client, options);
  }

  async completeOutbox(id: Uuid, owner: Uuid): Promise<void> {
    return effectOperations.completeOutbox(this.db, this.client, id, owner);
  }

  async retryOutbox(id: Uuid, owner: Uuid, error: string, nextAt: Date): Promise<void> {
    return effectOperations.retryOutbox(this.db, this.client, id, owner, error, nextAt);
  }

  async claimEffects(options: ClaimOptions): Promise<readonly Effect[]> {
    return effectOperations.claimEffects(this.db, this.client, options);
  }

  async persistCalling(
    effectId: Uuid,
    owner: Uuid,
    requestBytes: Uint8Array,
    requestSha256: string,
  ): Promise<Effect | null> {
    return effectOperations.persistCalling(
      this.db,
      this.client,
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
    return effectOperations.markApplied(this.db, this.client, effectId, owner, remoteId, result);
  }

  async markDone(effectId: Uuid, owner: Uuid): Promise<void> {
    return effectOperations.markDone(this.db, this.client, effectId, owner);
  }

  async markFailed(
    effectId: Uuid,
    owner: Uuid,
    error: string,
    retryAt: Date | null,
  ): Promise<void> {
    return effectOperations.markFailed(this.db, this.client, effectId, owner, error, retryAt);
  }

  async renewEffectLease(effectId: Uuid, owner: Uuid, leaseExpiresAt: Date): Promise<boolean> {
    return effectOperations.renewEffectLease(this.db, this.client, effectId, owner, leaseExpiresAt);
  }

  async markCompensating(effectId: Uuid, owner: Uuid, reason: string): Promise<void> {
    return effectOperations.markCompensating(this.db, this.client, effectId, owner, reason);
  }

  async scheduleEffect(input: PlannedEffect): Promise<void> {
    return effectOperations.scheduleEffect(this.db, this.client, input);
  }

  async currentGeneration(consultationId: Uuid): Promise<number | null> {
    return consultationOperations.currentGeneration(this.db, this.client, consultationId);
  }

  async claimDeadlines(options: ClaimOptions): Promise<readonly Deadline[]> {
    return consultationOperations.claimDeadlines(this.db, this.client, options);
  }

  async completeDeadline(deadline: Deadline, owner: Uuid): Promise<void> {
    return consultationOperations.completeDeadline(this.db, this.client, deadline, owner);
  }

  async claimStaleReservations(options: ClaimOptions): Promise<readonly WorkerReservation[]> {
    return consultationOperations.claimStaleReservations(this.db, this.client, options);
  }

  async heartbeat(
    workerId: Uuid,
    epoch: number,
    now: Date,
    leaseExpiresAt: Date,
  ): Promise<boolean> {
    return consultationOperations.heartbeat(
      this.db,
      this.client,
      workerId,
      epoch,
      now,
      leaseExpiresAt,
    );
  }

  async reserveWorker(consultationId: Uuid, generation: number): Promise<WorkerReservation> {
    return consultationOperations.reserveWorker(this.db, this.client, consultationId, generation);
  }

  async applyVerifiedWebhook(event: VerifiedWebhook): Promise<boolean> {
    return consultationOperations.applyVerifiedWebhook(this.db, this.client, event);
  }

  async presenceEpoch(consultationId: Uuid, generation: number): Promise<number | null> {
    return consultationOperations.presenceEpoch(this.db, this.client, consultationId, generation);
  }

  async admitFinalization(
    consultationId: Uuid,
    generation: number,
    presenceEpoch: number,
    now: Date,
  ): Promise<"admitted" | ConsultationState | null> {
    return consultationOperations.admitFinalization(
      this.db,
      this.client,
      consultationId,
      generation,
      presenceEpoch,
      now,
    );
  }

  async isStandardHuman(consultationId: Uuid, participantId: Uuid): Promise<boolean> {
    return consultationOperations.isStandardHuman(
      this.db,
      this.client,
      consultationId,
      participantId,
    );
  }

  async markCaptureReady(
    consultationId: Uuid,
    generation: number,
    participantIdentity: Uuid,
    participantEgressId: string,
  ): Promise<"active" | null> {
    return consultationOperations.markCaptureReady(
      this.db,
      this.client,
      consultationId,
      generation,
      participantIdentity,
      participantEgressId,
    );
  }

  async consultationState(consultationId: Uuid): Promise<ConsultationState | null> {
    return consultationOperations.consultationState(this.db, this.client, consultationId);
  }

  async workerReservation(
    consultationId: Uuid,
    generation: number,
  ): Promise<WorkerReservation | null> {
    return consultationOperations.workerReservation(
      this.db,
      this.client,
      consultationId,
      generation,
    );
  }

  async workerDispatchMetadata(consultationId: Uuid, generation: number): Promise<unknown> {
    return consultationOperations.workerDispatchMetadata(
      this.db,
      this.client,
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
      this.db,
      this.client,
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
      this.db,
      this.client,
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
      this.db,
      this.client,
      consultationId,
      cleanupGeneration,
      resourceGeneration,
      owner,
      reason,
      effects,
    );
  }

  async humanIdentities(consultationId: Uuid): Promise<readonly [Uuid, Uuid]> {
    return consultationOperations.humanIdentities(this.db, this.client, consultationId);
  }

  async seedDeadlines(consultationId: Uuid, generation: number): Promise<void> {
    return consultationOperations.seedDeadlines(this.db, this.client, consultationId, generation);
  }

  async roomDrainPlan(
    consultationId: Uuid,
    generation: number,
  ): Promise<{
    readonly egressIds: readonly string[];
    readonly participantIds: readonly Uuid[];
    readonly dispatchIds: readonly string[];
    readonly roomCreated: boolean;
    readonly resourceRoomName: string | null;
  }> {
    return consultationOperations.roomDrainPlan(this.db, this.client, consultationId, generation);
  }

  async completeRoomDrain(consultationId: Uuid, generation: number): Promise<void> {
    return consultationOperations.completeRoomDrain(
      this.db,
      this.client,
      consultationId,
      generation,
    );
  }

  async capacityDimensions(consultationId: Uuid): Promise<readonly CapacityDimension[]> {
    return consultationOperations.capacityDimensions(this.db, this.client, consultationId);
  }

  async preparePendingArchiveDeletes(): Promise<void> {
    return archiveOperations.preparePendingArchiveDeletes(this.db, this.client);
  }

  async reconciliationSnapshot(
    consultationId: Uuid,
    cleanupGeneration: number,
    resourceGeneration: number,
  ): Promise<ReconciliationSnapshot | null> {
    return archiveOperations.reconciliationSnapshot(
      this.db,
      this.client,
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
      this.db,
      this.client,
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
      this.db,
      this.client,
      consultationId,
      generation,
      writeEpoch,
    );
  }
}
