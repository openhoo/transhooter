import {
  beginFencedRoomFinalization,
  type Consultation,
  finishConsultation,
  type ParticipantSlot,
} from "../consultations/domain";
import {
  type Clock,
  DomainError,
  type IdGenerator,
  type TokenHasher,
  type UUID,
} from "../domain/model";
import type {
  AuditPort,
  ConsultationRepository,
  EffectRepository,
  EgressEventEarlySource,
  EgressPort,
  LiveKitRoomPort,
  OutboxMessage,
  Transaction,
} from "../ports/index";
import {
  deterministicUuid,
  type EffectPlanRequested,
  ORCHESTRATION_TOPICS,
} from "./orchestration-contract";

export interface VerifiedWebhook {
  id: string;
  consultationId?: UUID;
  generation?: number;
  occurredAt: Date;
  kind:
    | "participant_joined"
    | "participant_left"
    | "egress_active"
    | "egress_complete"
    | "egress_failed"
    | "ignored";
  roomName?: string;
  identity?: UUID;
  egressId?: string;
  egressSource?: EgressEventEarlySource;
  payload: unknown;
}

export interface WebhookVerifier {
  verify(
    rawBody: Uint8Array,
    headers: Readonly<Record<string, string | undefined>>,
  ): Promise<VerifiedWebhook>;
}

interface ResolvedWebhookTarget {
  consultationId: UUID;
  generation: number;
  roomName: string;
  earlySubject?: { participantId: UUID | null };
}

type ResolvedWebhook = VerifiedWebhook & {
  roomName: string;
  earlySubject?: { participantId: UUID | null };
};

interface AbsenceCandidate {
  roomName: string;
  generation: number;
  fencedAt: Date;
  participantIds: UUID[];
}

export class RoomService {
  constructor(
    private readonly consultations: ConsultationRepository,
    private readonly effects: EffectRepository,
    private readonly livekit: LiveKitRoomPort,
    _egress: EgressPort,
    private readonly verifier: WebhookVerifier,
    _audit: AuditPort,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
    private readonly hasher: TokenHasher,
  ) {}

  async acceptWebhook(
    rawBody: Uint8Array,
    headers: Readonly<Record<string, string | undefined>>,
  ): Promise<boolean> {
    const event = await this.verifier.verify(rawBody, headers);
    const target = await this.resolveWebhookTarget(event);
    if (target === null) {
      return true;
    }
    const resolvedEvent: ResolvedWebhook = {
      ...event,
      roomName: target.roomName,
      ...(target.earlySubject ? { earlySubject: target.earlySubject } : {}),
    };

    return this.consultations.transaction(async (tx) => {
      const rawSha256 = this.hasher.sha256(rawBody);
      const inboxAccepted = await this.effects.acceptInbox(
        "livekit",
        resolvedEvent.id,
        resolvedEvent.occurredAt,
        rawSha256,
        webhookInboxPayload(resolvedEvent, target),
        tx,
      );
      if (!inboxAccepted) {
        return false;
      }

      const current = await this.requiredLocked(target.consultationId, tx);
      const isAllowedIdentity =
        resolvedEvent.identity !== undefined &&
        current.participants.some((slot) => slot.livekitIdentity === resolvedEvent.identity);

      if (current.roomName !== resolvedEvent.roomName || current.generation !== target.generation) {
        if (
          resolvedEvent.kind === "participant_joined" &&
          isAllowedIdentity &&
          resolvedEvent.identity
        ) {
          await this.enqueueParticipantRemoval(
            tx,
            current,
            resolvedEvent.roomName,
            resolvedEvent.identity,
            target.generation,
          );
        }
        return true;
      }

      const next = await this.applyCurrentWebhook(current, resolvedEvent, rawSha256, tx);
      if (next !== current) {
        await this.save(current, next, tx);
      }
      return true;
    });
  }

  async reconcileAbsence(consultationId: UUID): Promise<boolean> {
    const candidate = await this.claimAbsenceCandidate(consultationId);
    if (!candidate) {
      return false;
    }

    const present = await this.livekit.listAllowedParticipants(candidate.roomName);
    const allowedParticipantIsPresent = present.some((identity) =>
      candidate.participantIds.includes(identity),
    );
    if (allowedParticipantIsPresent) {
      await this.clearMatchingAbsenceFence(consultationId, candidate);
      return false;
    }

    return this.finalizeMatchingAbsenceCandidate(consultationId, candidate);
  }

  async forceFinalizeAtDeadline(consultationId: UUID): Promise<void> {
    await this.consultations.transaction(async (tx) => {
      const current = await this.requiredLocked(consultationId, tx);
      if (
        current.state !== "finalizing" ||
        !current.finalizeDeadlineAt ||
        current.finalizeDeadlineAt > this.clock.now()
      ) {
        return;
      }

      const next = finishConsultation(current, this.clock.now());
      await this.save(current, next, tx);
      await this.enqueue(tx, next, "archive.reconciliation_forced", { consultationId });
    });
  }

  private async resolveWebhookTarget(
    event: VerifiedWebhook,
  ): Promise<ResolvedWebhookTarget | null> {
    if (event.kind === "ignored") {
      return null;
    }
    if (
      event.kind === "egress_active" ||
      event.kind === "egress_complete" ||
      event.kind === "egress_failed"
    ) {
      if (!event.egressId) {
        throw new DomainError("INVALID_WEBHOOK");
      }
      const binding = await this.consultations.resolveEgressEvent(
        event.egressId,
        event.egressSource,
      );
      if (!binding) {
        throw new DomainError("EGRESS_NOT_BOUND");
      }
      if (event.roomName !== undefined && binding.roomName !== event.roomName) {
        throw new DomainError("INVALID_WEBHOOK");
      }
      return {
        consultationId: binding.consultationId,
        generation: binding.generation,
        roomName: binding.roomName,
        ...(binding.earlySubject ? { earlySubject: binding.earlySubject } : {}),
      };
    }
    if (
      event.consultationId === undefined ||
      event.generation === undefined ||
      event.roomName === undefined
    ) {
      throw new DomainError("INVALID_WEBHOOK");
    }
    return {
      consultationId: event.consultationId,
      generation: event.generation,
      roomName: event.roomName,
    };
  }

  private async applyCurrentWebhook(
    current: Consultation,
    event: ResolvedWebhook,
    rawSha256: string,
    tx: Transaction,
  ): Promise<Consultation> {
    if (event.kind === "participant_joined" || event.kind === "participant_left") {
      return this.applyParticipantWebhook(current, event, tx);
    }
    if (
      event.kind === "egress_active" ||
      event.kind === "egress_complete" ||
      event.kind === "egress_failed"
    ) {
      return this.applyEgressWebhook(current, event, rawSha256, tx);
    }
    return current;
  }

  private async applyParticipantWebhook(
    current: Consultation,
    event: ResolvedWebhook,
    tx: Transaction,
  ): Promise<Consultation> {
    if (!event.identity) {
      throw new DomainError("INVALID_WEBHOOK");
    }
    const participant = requiredParticipant(current, event.identity);
    if (event.kind === "participant_joined" && current.admissionFencedAt) {
      await this.enqueueParticipantRemoval(
        tx,
        current,
        event.roomName,
        event.identity,
        current.generation,
      );
      return current;
    }

    const joined = event.kind === "participant_joined";
    const next = applyPresence(
      current,
      event.identity,
      event.id,
      joined,
      event.occurredAt,
      this.clock.now(),
    );
    if (joined && next !== current) {
      await this.enqueue(tx, current, ORCHESTRATION_TOPICS.effectPlan, {
        consultationId: current.id,
        generation: current.generation,
        subjectId: participant.id,
        kind: "PARTICIPANT_EGRESS",
        request: {
          roomName: event.roomName,
          resourceRoomName: event.roomName,
          participantIdentity: event.identity,
          outputPrefix: `v1/meetings/${current.id}/media/participants/${participant.id}/${String(current.generation)}`,
          segmentedHls: true,
          resourceGeneration: current.generation,
          compensationIntent: "EGRESS_STOP",
        },
      } satisfies EffectPlanRequested);
    }
    return next;
  }

  private async enqueueParticipantRemoval(
    tx: Transaction,
    consultation: Consultation,
    roomName: string,
    participantIdentity: UUID,
    resourceGeneration: number,
  ): Promise<void> {
    await this.enqueue(tx, consultation, ORCHESTRATION_TOPICS.effectPlan, {
      consultationId: consultation.id,
      generation: consultation.generation,
      subjectId: deterministicUuid(
        consultation.id,
        `participant-remove:${String(resourceGeneration)}:${roomName}:${participantIdentity}`,
      ),
      kind: "PARTICIPANT_REMOVE",
      request: {
        roomName,
        resourceRoomName: roomName,
        participantIdentity,
        resourceGeneration,
        compensationIntent: "REMOVE_PARTICIPANT",
      },
    } satisfies EffectPlanRequested);
  }

  private async applyEgressWebhook(
    current: Consultation,
    event: ResolvedWebhook,
    rawSha256: string,
    tx: Transaction,
  ): Promise<Consultation> {
    if (!event.egressId) {
      throw new DomainError("INVALID_WEBHOOK");
    }
    const binding =
      (await this.consultations.resolveCurrentEgressSubject(
        current.id,
        current.generation,
        event.egressId,
        tx,
      )) ?? event.earlySubject;
    if (!binding) {
      return current;
    }

    const active = event.kind === "egress_active";
    await this.enqueue(tx, current, "livekit.webhook.verified", {
      eventId: event.id,
      occurredAtMs: event.occurredAt.getTime(),
      consultationId: current.id,
      generation: current.generation,
      participantId: binding.participantId,
      kind: active ? "EGRESS_ACTIVE" : "EGRESS_TERMINAL",
      egressId: event.egressId,
      egressStatus: active
        ? "EGRESS_ACTIVE"
        : event.kind === "egress_complete"
          ? "EGRESS_COMPLETE"
          : "EGRESS_FAILED",
      rawSha256,
    });

    if (event.kind !== "egress_failed" || current.state !== "active") {
      return current;
    }

    const next = beginFencedRoomFinalization(current, this.clock.now());
    await this.enqueue(tx, next, "archive.failed", {
      reasonCode: "ARCHIVE_FAILED",
      egressId: event.egressId,
      resourceGeneration: current.generation,
    });
    return next;
  }

  private async claimAbsenceCandidate(consultationId: UUID): Promise<AbsenceCandidate | null> {
    return this.consultations.transaction(async (tx) => {
      const locked = await this.requiredLocked(consultationId, tx);
      if (!locked.roomName || !locked.bothAbsentSince || locked.state !== "active") {
        return null;
      }
      if (!locked.admissionFencedAt) {
        const now = this.clock.now();
        const fenced = {
          ...locked,
          admissionFencedAt: now,
          updatedAt: now,
        };
        await this.save(locked, fenced, tx);
        return null;
      }
      if (this.clock.now().getTime() - locked.admissionFencedAt.getTime() < 600_000) {
        return null;
      }
      return {
        roomName: locked.roomName,
        generation: locked.generation,
        fencedAt: locked.admissionFencedAt,
        participantIds: locked.participants.map((slot) => slot.livekitIdentity),
      };
    });
  }

  private async clearMatchingAbsenceFence(
    consultationId: UUID,
    candidate: AbsenceCandidate,
  ): Promise<void> {
    await this.consultations.transaction(async (tx) => {
      const locked = await this.requiredLocked(consultationId, tx);
      if (!matchesAbsenceCandidate(locked, candidate)) {
        return;
      }
      const now = this.clock.now();
      await this.save(locked, { ...locked, admissionFencedAt: null, updatedAt: now }, tx);
    });
  }

  private async finalizeMatchingAbsenceCandidate(
    consultationId: UUID,
    candidate: AbsenceCandidate,
  ): Promise<boolean> {
    return this.consultations.transaction(async (tx) => {
      const locked = await this.requiredLocked(consultationId, tx);
      if (!matchesAbsenceCandidate(locked, candidate)) {
        return false;
      }
      const next = beginFencedRoomFinalization(locked, this.clock.now());
      await this.save(locked, next, tx);
      await this.enqueue(tx, next, "consultation.finalization_requested", {
        reason: "both_absent",
      });
      return true;
    });
  }

  private async save(previous: Consultation, next: Consultation, tx: Transaction): Promise<void> {
    if (!(await this.consultations.save(next, previous.updatedAt, tx))) {
      throw new DomainError("CONCURRENT_MODIFICATION");
    }
  }

  private async enqueue(
    tx: Transaction,
    consultation: Consultation,
    topic: string,
    payload: unknown,
  ): Promise<void> {
    const message: OutboxMessage = {
      id: this.ids.uuid(),
      topic,
      aggregateId: consultation.id,
      generation: consultation.generation,
      payload,
      availableAt: this.clock.now(),
      attempts: 0,
    };
    await this.effects.enqueue(message, tx);
  }

  private async requiredLocked(id: UUID, tx: Transaction): Promise<Consultation> {
    const value = await this.consultations.lock(id, tx);
    if (!value) {
      throw new DomainError("NOT_FOUND");
    }
    return value;
  }
}

function webhookInboxPayload(
  event: ResolvedWebhook,
  target: ResolvedWebhookTarget,
): Record<string, unknown> {
  return {
    consultationId: target.consultationId,
    generation: target.generation,
    kind: event.kind,
    roomName: event.roomName,
    identity: event.identity ?? null,
    egressId: event.egressId ?? null,
    occurredAtMs: event.occurredAt.getTime(),
  };
}

function requiredParticipant(consultation: Consultation, identity: UUID): ParticipantSlot {
  const slot = consultation.participants.find(
    (candidate) => candidate.livekitIdentity === identity,
  );
  if (!slot) {
    throw new DomainError("FORBIDDEN");
  }
  return slot;
}

function matchesAbsenceCandidate(consultation: Consultation, candidate: AbsenceCandidate): boolean {
  return (
    consultation.state === "active" &&
    consultation.generation === candidate.generation &&
    consultation.admissionFencedAt?.getTime() === candidate.fencedAt.getTime()
  );
}

function applyPresence(
  current: Consultation,
  identity: UUID,
  eventId: string,
  present: boolean,
  occurredAt: Date,
  processedAt: Date,
): Consultation {
  const matchingSlot = current.participants.find((slot) => slot.livekitIdentity === identity);
  if (!matchingSlot) {
    throw new DomainError("FORBIDDEN_PARTICIPANT");
  }

  const storedEventTime = matchingSlot.eventOccurredAt?.getTime() ?? -1;
  const incomingEventTime = occurredAt.getTime();
  const isOlder = storedEventTime > incomingEventTime;
  const isDuplicateOrEarlierAtSameTime =
    storedEventTime === incomingEventTime &&
    matchingSlot.eventWatermark !== null &&
    matchingSlot.eventWatermark >= eventId;
  if (isOlder || isDuplicateOrEarlierAtSameTime) {
    return current;
  }

  const participants = current.participants.map(
    (slot): ParticipantSlot =>
      slot.livekitIdentity === identity
        ? {
            ...slot,
            present,
            eventWatermark: eventId,
            eventOccurredAt: occurredAt,
          }
        : slot,
  ) as [ParticipantSlot, ParticipantSlot];
  const areBothAbsent = participants.every((slot) => !slot.present);
  return {
    ...current,
    participants,
    bothAbsentSince: areBothAbsent ? (current.bothAbsentSince ?? occurredAt) : null,
    updatedAt: processedAt,
  };
}
