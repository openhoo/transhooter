import { WorkerJobMetadataSchema } from "@transhooter/contracts";
import {
  deterministicRoomName,
  deterministicUuid,
  ORCHESTRATION_TOPICS,
} from "@transhooter/server-core/rooms";
import { z } from "zod";
import {
  type Clock,
  type ConsultationState,
  canonicalRequest,
  type Deadline,
  type DurableStore,
  EFFECT_KINDS,
  type EffectKind,
  type OutboxItem,
  type PlannedEffect,
  type Uuid,
  type VerifiedWebhook,
  type WorkerReservation,
} from "./model";
import type { RemoteEffects } from "./remote";

const uuid = z.uuid();
const effectKindSchema = z.enum(EFFECT_KINDS);
const webhookSchema = z.object({
  eventId: z.string().min(1),
  occurredAtMs: z.number().int().nonnegative(),
  consultationId: uuid,
  generation: z.number().int().nonnegative(),
  participantId: uuid.nullable(),
  kind: z.enum(["PARTICIPANT_JOINED", "PARTICIPANT_LEFT", "EGRESS_ACTIVE", "EGRESS_TERMINAL"]),
  egressId: z.string().nullable(),
  egressStatus: z.string().nullable(),
  rawSha256: z.string().regex(/^[a-f0-9]{64}$/),
});
const planSchema = z.object({
  consultationId: uuid,
  generation: z.number().int().nonnegative(),
  subjectId: uuid,
  kind: effectKindSchema,
  request: z.record(z.string(), z.unknown()),
});
const lifecycleRequestSchema = planSchema.pick({
  consultationId: true,
  generation: true,
  subjectId: true,
});
const lifecycleSchema = z.object({
  consultationId: uuid,
  generation: z.number().int().nonnegative(),
  subjectId: uuid,
  kind: effectKindSchema,
  humanIdentities: z.array(uuid).max(2).optional(),
  participantEgressId: z.string().nullable().optional(),
  resourceGeneration: z.number().int().nonnegative().optional(),
});
const finalizationSchema = lifecycleRequestSchema.extend({
  shutdownAtMs: z.number().int().nonnegative(),
});
const cancellationSchema = z.object({
  consultationId: uuid,
  generation: z.number().int().nonnegative(),
  resourceGeneration: z.number().int().nonnegative(),
});
const heartbeatSchema = z.object({
  workerId: uuid,
  epoch: z.number().int().nonnegative(),
  leaseSeconds: z.number().int().min(5).max(120),
});
const captureRequestSchema = z.object({
  participantIdentity: uuid,
});
const archiveFailureSchema = z.object({
  reasonCode: z.literal("ARCHIVE_FAILED"),
  egressId: z.string().min(1),
  resourceGeneration: z.number().int().nonnegative(),
});
const sameLanguageBypassSchema = z.object({
  consultationId: uuid,
  generation: z.number().int().nonnegative(),
  sourceParticipantId: uuid,
  destinationParticipantId: uuid,
});
const workerOrEgressFailureSchema = z.object({
  consultationId: uuid,
  generation: z.number().int(),
  subjectId: uuid,
  reason: z.string().min(1),
  humanIdentities: z.array(uuid).length(2),
});

export interface CoordinatorOptions {
  readonly owner: Uuid;
  readonly leaseMs: number;
  readonly batchSize: number;
}

interface CapacityCoordination {
  reserve(
    key: string,
    capacity: number,
    units: number,
    ttlMs: number,
    reservationId: string,
  ): Promise<boolean>;
  renew(key: string, ttlMs: number, reservationId: string): Promise<boolean>;
  release(key: string, reservationId: string): Promise<void>;
}

interface CapacityReservation {
  readonly key: string;
  readonly capacity: number;
  readonly units: number;
  readonly reservationId: string;
}

export class Coordinator {
  constructor(
    private readonly store: DurableStore,
    private readonly clock: Clock,
    private readonly options: CoordinatorOptions,
    private readonly remote: Pick<RemoteEffects, "areHumansAbsent" | "notifyArchiveRecording">,
    private readonly capacity: CapacityCoordination,
  ) {}

  async tick(): Promise<number> {
    const claim = {
      owner: this.options.owner,
      now: this.clock.now(),
      leaseMs: this.options.leaseMs,
      limit: this.options.batchSize,
    };
    const [outbox, deadlines, staleReservations] = await Promise.all([
      this.store.claimOutbox(claim),
      this.store.claimDeadlines(claim),
      this.store.claimStaleReservations(claim),
      this.store.preparePendingArchiveDeletes(claim),
    ]);

    await Promise.all([
      ...outbox.map(async (item) => this.consumeOutbox(item)),
      ...deadlines.map(async (deadline) => this.consumeDeadline(deadline)),
      ...staleReservations.map(async (reservation) => this.recoverWorker(reservation)),
    ]);

    return outbox.length + deadlines.length + staleReservations.length;
  }

  private async consumeOutbox(item: OutboxItem): Promise<void> {
    try {
      await this.dispatchOutbox(item);
      await this.store.completeOutbox(item.id, this.options.owner);
    } catch (error) {
      const delay = Math.min(60_000, 500 * 2 ** Math.min(item.attempts, 7));
      const message = error instanceof Error ? error.message : "outbox handler failed";
      await this.store.retryOutbox(
        item.id,
        this.options.owner,
        message,
        new Date(this.clock.now().getTime() + delay),
      );
    }
  }

  private async dispatchOutbox(item: OutboxItem): Promise<void> {
    if (item.type === "worker.heartbeat") {
      await this.handleHeartbeat(item);
      return;
    }
    if (item.type === "livekit.webhook.verified") {
      await this.consumeWebhook(webhookSchema.parse(item.payload));
      return;
    }
    if (item.type === ORCHESTRATION_TOPICS.effectPlan) {
      const planned = planSchema.parse(item.payload);
      await this.plan(
        planned.consultationId,
        planned.generation,
        planned.kind,
        planned.subjectId,
        planned.request,
      );
      return;
    }
    if (item.type === ORCHESTRATION_TOPICS.provisioningRequested) {
      await this.handleProvisioningRequested(item);
      return;
    }
    if (item.type === ORCHESTRATION_TOPICS.finalizationRequested) {
      await this.handleFinalizationRequested(item);
      return;
    }
    if (item.type === "consultation.cancelled") {
      await this.handleCancellation(item);
      return;
    }
    if (item.type === ORCHESTRATION_TOPICS.effectApplied) {
      await this.handleEffectApplied(item);
      return;
    }
    if (item.type === "room.capture_requested") {
      await this.handleCaptureRequested(item);
      return;
    }
    if (item.type === "archive.failed") {
      await this.handleArchiveFailed(item);
      return;
    }
    if (item.type === "translation.same-language-bypass") {
      await this.handleSameLanguageBypass(item);
      return;
    }
    if (item.type === "worker.failure" || item.type === "egress.failure") {
      await this.handleWorkerOrEgressFailure(item);
    }
  }

  private async handleHeartbeat(item: OutboxItem): Promise<void> {
    const heartbeat = heartbeatSchema.parse(item.payload);
    const ttlMs = heartbeat.leaseSeconds * 1_000;
    const now = this.clock.now();
    const leaseExpiresAt = new Date(now.getTime() + ttlMs);
    const reservations = await this.capacityReservations(item.aggregateId);
    const renewed = await Promise.all(
      reservations.map(async ({ key, reservationId }) =>
        this.capacity.renew(key, ttlMs, reservationId),
      ),
    );
    if (renewed.some((acceptedReservation) => !acceptedReservation)) {
      throw new Error("worker heartbeat capacity reservation expired");
    }
    const accepted = await this.store.heartbeat(
      heartbeat.workerId,
      heartbeat.epoch,
      now,
      leaseExpiresAt,
    );
    if (!accepted) {
      throw new Error("worker heartbeat rejected by epoch fence");
    }
  }

  private async handleProvisioningRequested(item: OutboxItem): Promise<void> {
    const lifecycle = lifecycleRequestSchema.parse(item.payload);
    const reservations = await this.capacityReservations(lifecycle.consultationId);
    const acquired: CapacityReservation[] = [];
    for (const reservation of reservations) {
      let reserved: boolean;
      try {
        reserved = await this.capacity.reserve(
          reservation.key,
          reservation.capacity,
          reservation.units,
          20 * 60_000,
          reservation.reservationId,
        );
      } catch (error) {
        const rollback = await Promise.allSettled(
          [...acquired, reservation].map(async ({ key, reservationId }) => {
            await this.capacity.release(key, reservationId);
          }),
        );
        const rollbackFailures = rollback.flatMap((result) =>
          result.status === "rejected" ? [result.reason] : [],
        );
        if (rollbackFailures.length > 0) {
          const primaryMessage = error instanceof Error ? error.message : String(error);
          const rollbackMessage = rollbackFailures
            .map((failure) => (failure instanceof Error ? failure.message : String(failure)))
            .join("; ");
          throw new AggregateError(
            [error, ...rollbackFailures],
            `${primaryMessage}; capacity rollback failed: ${rollbackMessage}`,
          );
        }
        throw error;
      }
      if (!reserved) {
        const exhaustion = new Error(`worker capacity is exhausted for ${reservation.key}`);
        const rollback = await Promise.allSettled(
          acquired.map(async ({ key, reservationId }) => {
            await this.capacity.release(key, reservationId);
          }),
        );
        const rollbackFailures = rollback.flatMap((result) =>
          result.status === "rejected" ? [result.reason] : [],
        );
        if (rollbackFailures.length > 0) {
          const rollbackMessage = rollbackFailures
            .map((failure) => (failure instanceof Error ? failure.message : String(failure)))
            .join("; ");
          throw new AggregateError(
            [exhaustion, ...rollbackFailures],
            `${exhaustion.message}; capacity rollback failed: ${rollbackMessage}`,
          );
        }
        throw exhaustion;
      }
      acquired.push(reservation);
    }

    try {
      await this.store.reserveWorker(lifecycle.consultationId, lifecycle.generation);
      await this.plan(
        lifecycle.consultationId,
        lifecycle.generation,
        "ROOM_CREATE",
        lifecycle.subjectId,
        {
          emptyTimeout: 300,
          metadata: {
            consultationId: lifecycle.consultationId,
            generation: lifecycle.generation,
          },
        },
      );
      await this.store.seedDeadlines(lifecycle.consultationId, lifecycle.generation);
    } catch (error) {
      await this.rollbackCapacity(acquired, error);
    }
  }

  private async handleFinalizationRequested(item: OutboxItem): Promise<void> {
    const lifecycle = finalizationSchema.parse(item.payload);
    const identities = await this.store.humanIdentities(lifecycle.consultationId);
    const status = this.effectInput(
      lifecycle.consultationId,
      lifecycle.generation,
      "STATUS_PACKET",
      lifecycle.subjectId,
      {
        topic: "consultation.status.v1",
        reasonCode: "SHUTDOWN",
        state: "finalizing",
        shutdownAtMs: lifecycle.shutdownAtMs,
        destinationIdentities: identities,
      },
      item.id,
    );
    await this.store.scheduleEffect(status);
    await this.scheduleDrainEffects(
      lifecycle.consultationId,
      lifecycle.generation,
      lifecycle.generation,
      lifecycle.subjectId,
      status.id,
      lifecycle.shutdownAtMs,
    );
    await this.store.seedDeadlines(lifecycle.consultationId, lifecycle.generation);
  }

  private async handleCancellation(item: OutboxItem): Promise<void> {
    const cancellation = cancellationSchema.parse(item.payload);
    await this.requireGeneration(cancellation.consultationId, item);
    if (cancellation.resourceGeneration >= cancellation.generation) {
      throw new Error("cancellation resource generation must precede cleanup generation");
    }
    const effects = await this.drainEffects(
      cancellation.consultationId,
      cancellation.generation,
      cancellation.resourceGeneration,
      cancellation.consultationId,
      null,
      this.clock.now().getTime(),
      true,
    );
    await this.store.fenceWorkerAndScheduleCancellation(
      cancellation.consultationId,
      cancellation.generation,
      cancellation.resourceGeneration,
      this.options.owner,
      "consultation cancelled",
      effects,
    );
  }

  private async handleEffectApplied(item: OutboxItem): Promise<void> {
    const lifecycle = lifecycleSchema.parse(item.payload);
    if (lifecycle.kind === "ROOM_DELETE") {
      await this.releaseCapacity(lifecycle.consultationId);
      await this.store.completeRoomDrain(lifecycle.consultationId, lifecycle.generation);
      await this.plan(
        lifecycle.consultationId,
        lifecycle.generation,
        "ARCHIVE_RECONCILE",
        lifecycle.consultationId,
        {
          forceIncomplete: false,
          resourceGeneration: lifecycle.resourceGeneration ?? lifecycle.generation,
        },
      );
      return;
    }
    if (lifecycle.kind === "ROOM_CREATE") {
      await this.plan(
        lifecycle.consultationId,
        lifecycle.generation,
        "ROOM_COMPOSITE_EGRESS",
        lifecycle.subjectId,
        {
          outputPrefix: `v1/meetings/${lifecycle.consultationId}/media/composite/${String(lifecycle.generation)}`,
          layoutExpiresAtMs: this.clock.now().getTime() + 10 * 60_000,
        },
      );
      return;
    }
    if (lifecycle.kind === "PARTICIPANT_EGRESS") {
      await this.planParticipantGrant(lifecycle);
      return;
    }
    if (lifecycle.kind === "PARTICIPANT_GRANT") {
      await this.planCaptureReady(lifecycle, item.id);
    }
  }

  private async capacityReservations(
    consultationId: Uuid,
  ): Promise<readonly CapacityReservation[]> {
    const dimensions = await this.store.capacityDimensions(consultationId);
    return dimensions.map((dimension, index) => ({
      ...dimension,
      reservationId: `${consultationId}:${String(index)}`,
    }));
  }

  private async releaseCapacity(consultationId: Uuid): Promise<void> {
    const reservations = await this.capacityReservations(consultationId);
    await Promise.all(
      reservations.map(async ({ key, reservationId }) => {
        await this.capacity.release(key, reservationId);
      }),
    );
  }
  private async rollbackCapacity(
    reservations: readonly CapacityReservation[],
    primaryError: unknown,
  ): Promise<never> {
    const rollback = await Promise.allSettled(
      reservations.map(async ({ key, reservationId }) => {
        await this.capacity.release(key, reservationId);
      }),
    );
    const failures = rollback.flatMap((result) =>
      result.status === "rejected" ? [result.reason] : [],
    );
    if (failures.length === 0) {
      throw primaryError;
    }
    const primaryMessage =
      primaryError instanceof Error ? primaryError.message : String(primaryError);
    const rollbackMessage = failures
      .map((failure) => (failure instanceof Error ? failure.message : String(failure)))
      .join("; ");
    throw new AggregateError(
      [primaryError, ...failures],
      `${primaryMessage}; capacity rollback failed: ${rollbackMessage}`,
    );
  }

  private async planParticipantGrant(lifecycle: z.infer<typeof lifecycleSchema>): Promise<void> {
    await this.requirePublishableState(
      lifecycle.consultationId,
      "participant capture requires ready or active consultation",
    );
    const participantEgressId = lifecycle.participantEgressId;
    if (participantEgressId === undefined || participantEgressId === null) {
      throw new Error("participant capture is missing Egress identity");
    }
    await this.plan(
      lifecycle.consultationId,
      lifecycle.generation,
      "PARTICIPANT_GRANT",
      lifecycle.subjectId,
      {
        participantIdentity: lifecycle.subjectId,
        canPublish: true,
        canPublishData: false,
        trackSource: ["microphone", "camera"],
        barrierEgressId: participantEgressId,
      },
    );
  }
  private async planCaptureReady(
    lifecycle: z.infer<typeof lifecycleSchema>,
    occurrenceIdentity: string,
  ): Promise<void> {
    const participantEgressId = lifecycle.participantEgressId;
    if (participantEgressId === undefined || participantEgressId === null) {
      throw new Error("capture barrier is missing participant Egress identity");
    }
    const state = await this.store.markCaptureReady(
      lifecycle.consultationId,
      lifecycle.generation,
      lifecycle.subjectId,
      participantEgressId,
    );
    if (state === null) {
      throw new Error("capture-ready is fenced from the current consultation generation");
    }
    await this.plan(
      lifecycle.consultationId,
      lifecycle.generation,
      "STATUS_PACKET",
      lifecycle.subjectId,
      {
        topic: "consultation.status.v1",
        reasonCode: "CAPTURE_READY",
        state,
        subjectParticipantId: lifecycle.subjectId,
        participantEgressId,
        shutdownAtMs: null,
        destinationIdentities: [lifecycle.subjectId],
      },
      occurrenceIdentity,
    );
  }

  private async handleCaptureRequested(item: OutboxItem): Promise<void> {
    const capture = captureRequestSchema.parse(item.payload);
    const admitted = await this.store.isStandardHuman(
      item.aggregateId,
      capture.participantIdentity,
    );
    if (!admitted) {
      throw new Error("capture request participant is not an admitted human");
    }
    await this.requirePublishableState(
      item.aggregateId,
      "capture request consultation is not publishable",
    );
    const generation = await this.requireGeneration(item.aggregateId, item);
    await this.plan(
      item.aggregateId,
      generation,
      "PARTICIPANT_EGRESS",
      capture.participantIdentity,
      {
        participantIdentity: capture.participantIdentity,
        outputPrefix: `v1/meetings/${item.aggregateId}/media/participants/${capture.participantIdentity}/${String(item.generation)}`,
        segmentedHls: true,
      },
    );
  }

  private async handleArchiveFailed(item: OutboxItem): Promise<void> {
    const failure = archiveFailureSchema.parse(item.payload);
    const generation = await this.requireGeneration(item.aggregateId, item);
    if (failure.resourceGeneration >= generation) {
      throw new Error("archive failure resource generation must precede cleanup generation");
    }
    const identities = await this.store.humanIdentities(item.aggregateId);
    const state = await this.store.consultationState(item.aggregateId);
    const failureState = requireFailureState(
      state,
      "archive failure consultation is not finalizing",
    );
    const effects = await this.failureEffects(
      item.aggregateId,
      generation,
      failure.resourceGeneration,
      item.aggregateId,
      failureState,
      identities,
      item.id,
    );
    await this.store.planFailureEffects(item.aggregateId, generation, failure.reasonCode, effects);
  }

  private async handleSameLanguageBypass(item: OutboxItem): Promise<void> {
    const bypass = sameLanguageBypassSchema.parse(item.payload);
    const state = await this.requirePublishableState(
      bypass.consultationId,
      "same-language bypass requires ready or active consultation",
    );
    await this.plan(
      bypass.consultationId,
      bypass.generation,
      "STATUS_PACKET",
      bypass.destinationParticipantId,
      {
        topic: "consultation.status.v1",
        reasonCode: "SAME_LANGUAGE_BYPASS",
        state,
        sourceParticipantId: bypass.sourceParticipantId,
        destinationParticipantId: bypass.destinationParticipantId,
        shutdownAtMs: null,
        destinationIdentities: [bypass.destinationParticipantId],
      },
      item.id,
    );
  }

  private async handleWorkerOrEgressFailure(item: OutboxItem): Promise<void> {
    const failure = workerOrEgressFailureSchema.parse(item.payload);
    const state = requireFailureState(
      await this.store.consultationState(failure.consultationId),
      "archive failure requires active or finalizing consultation",
    );
    const effects = await this.failureEffects(
      failure.consultationId,
      failure.generation,
      failure.generation,
      failure.subjectId,
      state,
      failure.humanIdentities,
      item.id,
    );

    if (item.type === "worker.failure") {
      const reservation = await this.store.workerReservation(
        failure.consultationId,
        failure.generation,
      );
      if (reservation === null || reservation.workerId !== failure.subjectId) {
        throw new Error("worker failure has no matching reservation");
      }
      await this.store.fenceWorkerAndPlanFailure(
        reservation,
        this.options.owner,
        failure.reason,
        effects,
      );
      return;
    }
    await this.store.planFailureEffects(
      failure.consultationId,
      failure.generation,
      failure.reason,
      effects,
    );
  }

  private async consumeWebhook(event: VerifiedWebhook): Promise<void> {
    const applied = await this.store.applyVerifiedWebhook(event);
    if (!applied) {
      return;
    }
    const generation = await this.store.currentGeneration(event.consultationId);
    if (generation === null || generation !== event.generation) {
      return;
    }

    if (event.kind === "PARTICIPANT_JOINED" && event.participantId !== null) {
      await this.handleParticipantJoined(event, generation);
      return;
    }
    if (event.kind === "EGRESS_ACTIVE" && event.participantId === null) {
      await this.handleCompositeEgressActive(event, generation);
      return;
    }
    if (event.kind === "EGRESS_ACTIVE" && event.participantId !== null) {
      await this.handleParticipantEgressActive(event, generation);
      return;
    }
    if (
      event.kind === "EGRESS_TERMINAL" &&
      egressTerminalOutcome(event.egressStatus) === "failed"
    ) {
      await this.handleTerminalEgressFailure(event, generation);
    }
  }

  private async handleParticipantJoined(event: VerifiedWebhook, generation: number): Promise<void> {
    const participantId = event.participantId;
    if (participantId === null) {
      return;
    }
    if (!(await this.store.isStandardHuman(event.consultationId, participantId))) {
      return;
    }
    await this.plan(event.consultationId, generation, "PARTICIPANT_EGRESS", participantId, {
      participantIdentity: participantId,
      outputPrefix: `v1/meetings/${event.consultationId}/media/participants/${participantId}/${String(generation)}`,
      segmentedHls: true,
    });
  }

  private async handleCompositeEgressActive(
    event: VerifiedWebhook,
    generation: number,
  ): Promise<void> {
    await this.remote.notifyArchiveRecording(event.consultationId);
    const metadata = WorkerJobMetadataSchema.parse(
      await this.store.workerDispatchMetadata(event.consultationId, generation),
    );
    await this.plan(event.consultationId, generation, "WORKER_DISPATCH", metadata.workerIdentity, {
      agentName: "translation-worker",
      metadata,
      roomCompositeEgressId: event.egressId,
    });
  }

  private async handleParticipantEgressActive(
    event: VerifiedWebhook,
    generation: number,
  ): Promise<void> {
    const participantId = event.participantId;
    if (participantId === null) {
      return;
    }
    if (!(await this.store.isStandardHuman(event.consultationId, participantId))) {
      return;
    }
    await this.plan(event.consultationId, generation, "PARTICIPANT_GRANT", participantId, {
      participantIdentity: participantId,
      canPublish: true,
      canPublishData: false,
      trackSource: ["microphone", "camera"],
      barrierEgressId: event.egressId,
    });
  }

  private async handleTerminalEgressFailure(
    event: VerifiedWebhook,
    generation: number,
  ): Promise<void> {
    const [humanIdentities, state] = await Promise.all([
      this.store.humanIdentities(event.consultationId),
      this.store.consultationState(event.consultationId),
    ]);
    const failureState = requireFailureState(state, "Egress failure state is not dispatchable");
    const subjectId = event.participantId ?? event.consultationId;
    const effects = await this.failureEffects(
      event.consultationId,
      generation,
      generation,
      subjectId,
      failureState,
      humanIdentities,
      event.eventId,
    );
    const reason = `terminal Egress ${event.egressId ?? "unknown"} failed`;
    await this.store.planFailureEffects(event.consultationId, generation, reason, effects);
  }

  private async consumeDeadline(deadline: Deadline): Promise<void> {
    const generation = await this.store.currentGeneration(deadline.consultationId);
    if (generation !== deadline.generation) {
      await this.store.completeDeadline(deadline, this.options.owner);
      return;
    }
    let complete = true;
    if (deadline.kind === "archive-reconcile") {
      await this.plan(
        deadline.consultationId,
        deadline.generation,
        "ARCHIVE_RECONCILE",
        deadline.consultationId,
        { forceIncomplete: true },
      );
    } else {
      complete = await this.handleRoomDeadline(deadline);
    }
    if (complete) {
      await this.store.completeDeadline(deadline, this.options.owner);
    }
  }

  private async handleRoomDeadline(deadline: Deadline): Promise<boolean> {
    if (deadline.kind === "absence") {
      const roomName = deterministicRoomName(deadline.consultationId, deadline.generation);
      const identities = await this.store.humanIdentities(deadline.consultationId);
      if (!(await this.remote.areHumansAbsent(roomName, identities))) {
        return false;
      }
    }

    const humanIdentities = await this.store.humanIdentities(deadline.consultationId);
    const state =
      deadline.kind === "absence"
        ? await this.store.admitFinalization(
            deadline.consultationId,
            deadline.generation,
            this.clock.now(),
          )
        : await this.store.consultationState(deadline.consultationId);
    if (
      (deadline.kind === "ready" && state !== "ready") ||
      (deadline.kind === "finalize" && state !== "finalizing") ||
      (deadline.kind === "absence" && state !== "finalizing")
    ) {
      return true;
    }
    const nowMs = this.clock.now().getTime();
    const notBeforeMs = state === "finalizing" ? nowMs + 5_000 : nowMs;
    let statusId: Uuid | null = null;

    if (state === "finalizing") {
      const status = this.effectInput(
        deadline.consultationId,
        deadline.generation,
        "STATUS_PACKET",
        deadline.consultationId,
        {
          topic: "consultation.status.v1",
          reasonCode: "SHUTDOWN",
          state,
          shutdownAtMs: notBeforeMs,
          destinationIdentities: humanIdentities,
        },
        `${deadline.kind}:${deadline.dueAt.toISOString()}`,
      );
      statusId = status.id;
      await this.store.scheduleEffect(status);
    }
    await this.scheduleDrainEffects(
      deadline.consultationId,
      deadline.generation,
      deadline.generation,
      deadline.consultationId,
      statusId,
      notBeforeMs,
    );
    return true;
  }

  private async requireGeneration(consultationId: Uuid, item: OutboxItem): Promise<number> {
    const current = await this.store.currentGeneration(consultationId);
    if (current === null || current !== item.generation) {
      throw new Error("outbox generation is fenced");
    }
    return current;
  }

  private async requirePublishableState(
    consultationId: Uuid,
    message: string,
  ): Promise<"ready" | "active"> {
    const state = await this.store.consultationState(consultationId);
    if (state !== "ready" && state !== "active") {
      throw new Error(message);
    }
    return state;
  }

  private async recoverWorker(reservation: WorkerReservation): Promise<void> {
    const [humanIdentities, state, cleanupGeneration] = await Promise.all([
      this.store.humanIdentities(reservation.consultationId),
      this.store.consultationState(reservation.consultationId),
      this.store.currentGeneration(reservation.consultationId),
    ]);
    if ((state !== "active" && state !== "finalizing") || cleanupGeneration === null) {
      return;
    }
    const effects =
      cleanupGeneration === reservation.generation
        ? await this.failureEffects(
            reservation.consultationId,
            cleanupGeneration,
            reservation.generation,
            reservation.workerId,
            state,
            humanIdentities,
            `worker-epoch:${String(reservation.epoch)}`,
          )
        : [];
    await this.store.fenceWorkerAndPlanFailure(
      reservation,
      this.options.owner,
      "heartbeat expired",
      effects,
    );
  }

  private async failureEffects(
    consultationId: Uuid,
    cleanupGeneration: number,
    resourceGeneration: number,
    subjectId: Uuid,
    state: "active" | "finalizing",
    destinationIdentities: readonly Uuid[],
    occurrenceIdentity: string,
  ): Promise<readonly PlannedEffect[]> {
    const shutdownAtMs = this.clock.now().getTime() + 10_000;
    const status = this.effectInput(
      consultationId,
      cleanupGeneration,
      "STATUS_PACKET",
      subjectId,
      {
        topic: "consultation.status.v1",
        reasonCode: "ARCHIVE_FAILED",
        state,
        shutdownAtMs,
        resourceGeneration,
        destinationIdentities,
      },
      occurrenceIdentity,
    );
    const drain = await this.drainEffects(
      consultationId,
      cleanupGeneration,
      resourceGeneration,
      subjectId,
      status.id,
      shutdownAtMs,
      false,
    );
    return [status, ...drain];
  }

  private async scheduleDrainEffects(
    consultationId: Uuid,
    cleanupGeneration: number,
    resourceGeneration: number,
    subjectId: Uuid,
    statusId: Uuid | null,
    notBeforeMs: number,
  ): Promise<void> {
    const effects = await this.drainEffects(
      consultationId,
      cleanupGeneration,
      resourceGeneration,
      subjectId,
      statusId,
      notBeforeMs,
      false,
    );
    await Promise.all(effects.map(async (effect) => this.store.scheduleEffect(effect)));
  }

  private async drainEffects(
    consultationId: Uuid,
    cleanupGeneration: number,
    resourceGeneration: number,
    subjectId: Uuid,
    statusId: Uuid | null,
    notBeforeMs: number,
    noOpWhenEmpty: boolean,
  ): Promise<readonly PlannedEffect[]> {
    const drain = await this.store.roomDrainPlan(consultationId, resourceGeneration);
    const dependency = statusId === null ? {} : { dependsOnEffectId: statusId };
    const resource = { resourceGeneration };
    const egressStops = drain.egressIds.map((egressId) =>
      this.effectInput(
        consultationId,
        cleanupGeneration,
        "EGRESS_STOP",
        deterministicUuid(consultationId, `${String(resourceGeneration)}:egress-stop:${egressId}`),
        { egressId, ...resource, ...dependency, notBeforeMs },
      ),
    );
    const participantRemovals =
      !noOpWhenEmpty || drain.roomCreated
        ? drain.participantIds.map((participantId) =>
            this.effectInput(
              consultationId,
              cleanupGeneration,
              "PARTICIPANT_REMOVE",
              participantId,
              {
                participantIdentity: participantId,
                ...resource,
                ...dependency,
                notBeforeMs,
              },
            ),
          )
        : [];
    const roomCleanup = [...egressStops, ...participantRemovals];
    const dispatchStops = drain.dispatchIds.map((dispatchId) =>
      this.effectInput(
        consultationId,
        cleanupGeneration,
        "DISPATCH_DELETE",
        deterministicUuid(consultationId, `${String(resourceGeneration)}:dispatch:${dispatchId}`),
        {
          dispatchId,
          ...resource,
          ...dependency,
          waitForWorkerTerminal: true,
          workerTerminalGeneration: resourceGeneration,
          notBeforeMs,
        },
      ),
    );
    if (
      noOpWhenEmpty &&
      !drain.roomCreated &&
      egressStops.length === 0 &&
      dispatchStops.length === 0
    ) {
      return [];
    }
    const roomDelete = this.effectInput(
      consultationId,
      cleanupGeneration,
      "ROOM_DELETE",
      subjectId,
      {
        ...resource,
        ...dependency,
        dependsOnEffectIds: roomCleanup.map(({ id }) => id),
        waitForWorkerTerminal: true,
        workerTerminalGeneration: resourceGeneration,
        notBeforeMs,
      },
      `room-cleanup:${String(resourceGeneration)}`,
    );
    return [...roomCleanup, ...dispatchStops, roomDelete];
  }

  private effectInput(
    consultationId: Uuid,
    generation: number,
    kind: EffectKind,
    subjectId: Uuid,
    request: Readonly<Record<string, unknown>>,
    occurrenceIdentity?: string,
  ): PlannedEffect {
    // The transport timestamp is deliberately excluded. An explicit durable event
    // identity makes retries stable while allowing repeated status transitions.
    const occurrenceKey = `${kind}:${canonicalRequest(occurrenceIdentity ?? request).sha256}`;
    const id = deterministicUuid(
      consultationId,
      `${String(generation)}:${kind}:${subjectId}:${occurrenceKey}`,
    );
    const roomName = deterministicRoomName(
      consultationId,
      typeof request.resourceGeneration === "number" ? request.resourceGeneration : generation,
    );
    const occurredAtMs =
      kind === "STATUS_PACKET" ? (request.occurredAtMs ?? this.clock.now().getTime()) : undefined;
    const timestamp = occurredAtMs === undefined ? {} : { occurredAtMs };
    return {
      id,
      consultationId,
      generation,
      kind,
      subjectId,
      occurrenceKey,
      plan: {
        ...request,
        roomName,
        ...timestamp,
        adoptionId: id,
      },
    };
  }

  private async plan(
    consultationId: Uuid,
    generation: number,
    kind: EffectKind,
    subjectId: Uuid,
    request: Readonly<Record<string, unknown>>,
    occurrenceIdentity?: string,
  ): Promise<void> {
    const effect = this.effectInput(
      consultationId,
      generation,
      kind,
      subjectId,
      request,
      occurrenceIdentity,
    );
    await this.store.scheduleEffect(effect);
  }
}

function requireFailureState(
  state: ConsultationState | null,
  message: string,
): "active" | "finalizing" {
  if (state !== "active" && state !== "finalizing") {
    throw new Error(message);
  }
  return state;
}

function egressTerminalOutcome(status: string | null): "complete" | "failed" {
  if (status === "EGRESS_COMPLETE") {
    return "complete";
  }
  if (
    status === "EGRESS_FAILED" ||
    status === "EGRESS_ABORTED" ||
    status === "EGRESS_LIMIT_REACHED"
  ) {
    return "failed";
  }
  throw new Error(`unrecognized terminal Egress status: ${status ?? "missing"}`);
}
