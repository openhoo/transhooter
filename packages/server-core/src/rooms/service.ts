import type { StatusPacket } from "@transhooter/contracts";
import {
  beginFencedRoomFinalization,
  type Consultation,
  finishConsultation,
  grantCapture,
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
  EgressPort,
  LiveKitRoomPort,
  OutboxMessage,
  Transaction,
} from "../ports/index";

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
  roomName: string;
  identity?: UUID;
  egressId?: string;
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
}

interface ParticipantRevocation {
  roomName: string;
  identity: UUID;
}

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
    private readonly egress: EgressPort,
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

    const revocation: { value: ParticipantRevocation | null } = { value: null };
    const accepted = await this.consultations.transaction(async (tx) => {
      const rawSha256 = this.hasher.sha256(rawBody);
      const inboxAccepted = await this.effects.acceptInbox(
        "livekit",
        event.id,
        event.occurredAt,
        rawSha256,
        webhookInboxPayload(event, target),
        tx,
      );
      if (!inboxAccepted) {
        return false;
      }

      const current = await this.requiredLocked(target.consultationId, tx);
      const isAllowedIdentity =
        event.identity !== undefined &&
        current.participants.some((slot) => slot.livekitIdentity === event.identity);

      if (current.roomName !== event.roomName || current.generation !== target.generation) {
        if (event.kind === "participant_joined" && isAllowedIdentity && event.identity) {
          revocation.value = {
            roomName: event.roomName,
            identity: event.identity,
          };
        }
        return true;
      }

      const next = await this.applyCurrentWebhook(current, event, rawSha256, tx, revocation);
      if (next !== current) {
        await this.save(current, next, tx);
      }
      return true;
    });

    if (revocation.value) {
      await this.livekit.removeParticipant(revocation.value.roomName, revocation.value.identity);
    }
    return accepted;
  }

  async executeCaptureBarrier(consultationId: UUID, participantIdentity: UUID): Promise<void> {
    const initial = await this.requireCaptureCandidate(consultationId, participantIdentity);
    const slot = requiredParticipant(initial, participantIdentity);
    const egressResult = await this.adoptOrStartParticipantEgress(
      initial,
      slot,
      participantIdentity,
    );

    try {
      await this.persistCaptureBinding(initial, slot, egressResult.egressId);
    } catch (error) {
      await this.egress.stop(egressResult.egressId);
      throw error;
    }

    try {
      await this.livekit.updateParticipant({
        roomName: requiredRoomName(initial),
        identity: participantIdentity,
        canPublish: true,
        canPublishData: false,
      });
    } catch (error) {
      await this.egress.stop(egressResult.egressId);
      await this.clearCaptureBinding(
        consultationId,
        initial.generation,
        slot.id,
        egressResult.egressId,
      );
      throw error;
    }

    let updated: Consultation;
    try {
      updated = await this.persistCaptureGrant(
        consultationId,
        initial.generation,
        slot.id,
        egressResult.egressId,
      );
    } catch (error) {
      await this.livekit.updateParticipant({
        roomName: requiredRoomName(initial),
        identity: participantIdentity,
        canPublish: false,
        canPublishData: false,
      });
      await this.egress.stop(egressResult.egressId);
      await this.clearCaptureBinding(
        consultationId,
        initial.generation,
        slot.id,
        egressResult.egressId,
      );
      throw error;
    }

    await this.sendCaptureReadyStatus(updated, slot, egressResult.egressId, participantIdentity);
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
    if (event.consultationId !== undefined && event.generation !== undefined) {
      return {
        consultationId: event.consultationId,
        generation: event.generation,
      };
    }
    if (event.kind === "ignored") {
      return null;
    }
    if (!event.egressId) {
      throw new DomainError("INVALID_WEBHOOK");
    }

    const binding = await this.consultations.resolveEgressEvent(event.egressId);
    if (!binding) {
      throw new DomainError("EGRESS_NOT_BOUND");
    }
    if (binding.roomName !== event.roomName) {
      throw new DomainError("INVALID_WEBHOOK");
    }
    return {
      consultationId: binding.consultationId,
      generation: binding.generation,
    };
  }

  private async applyCurrentWebhook(
    current: Consultation,
    event: VerifiedWebhook,
    rawSha256: string,
    tx: Transaction,
    revocation: { value: ParticipantRevocation | null },
  ): Promise<Consultation> {
    if (event.kind === "participant_joined" || event.kind === "participant_left") {
      return this.applyParticipantWebhook(current, event, tx, revocation);
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
    event: VerifiedWebhook,
    tx: Transaction,
    revocation: { value: ParticipantRevocation | null },
  ): Promise<Consultation> {
    if (!event.identity) {
      throw new DomainError("INVALID_WEBHOOK");
    }
    if (event.kind === "participant_joined" && current.admissionFencedAt) {
      revocation.value = {
        roomName: event.roomName,
        identity: event.identity,
      };
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
      await this.enqueue(tx, current, "room.capture_requested", {
        participantIdentity: event.identity,
      });
    }
    return next;
  }

  private async applyEgressWebhook(
    current: Consultation,
    event: VerifiedWebhook,
    rawSha256: string,
    tx: Transaction,
  ): Promise<Consultation> {
    if (!event.egressId) {
      throw new DomainError("INVALID_WEBHOOK");
    }
    const binding = await this.consultations.resolveCurrentEgressSubject(
      current.id,
      current.generation,
      event.egressId,
      tx,
    );
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

  private async requireCaptureCandidate(
    consultationId: UUID,
    participantIdentity: UUID,
  ): Promise<Consultation> {
    const consultation = await this.consultations.get(consultationId);
    if (
      !consultation?.roomName ||
      consultation.admissionFencedAt ||
      (consultation.state !== "ready" && consultation.state !== "active")
    ) {
      throw new DomainError("INVALID_STATE");
    }
    requiredParticipant(consultation, participantIdentity);
    return consultation;
  }

  private async adoptOrStartParticipantEgress(
    consultation: Consultation,
    slot: ParticipantSlot,
    participantIdentity: UUID,
  ) {
    const adopted = slot.participantEgressId
      ? await this.egress.get(slot.participantEgressId)
      : null;
    const result =
      adopted?.state === "EGRESS_ACTIVE"
        ? adopted
        : await this.egress.startParticipant({
            roomName: requiredRoomName(consultation),
            identity: participantIdentity,
            outputPrefix: `v1/meetings/${consultation.id}/media/participants/${slot.id}/`,
            requestIdentity: `${consultation.id}:${String(consultation.generation)}:participant_capture:${slot.id}`,
          });
    if (result.state !== "EGRESS_ACTIVE") {
      throw new DomainError("EGRESS_NOT_ACTIVE");
    }
    return result;
  }

  private async persistCaptureBinding(
    initial: Consultation,
    slot: ParticipantSlot,
    egressId: string,
  ): Promise<void> {
    await this.consultations.transaction(async (tx) => {
      const current = await this.requiredLocked(initial.id, tx);
      if (current.generation !== initial.generation || current.admissionFencedAt) {
        throw new DomainError("FENCED_GENERATION");
      }
      const participants = current.participants.map((candidate) =>
        candidate.id === slot.id ? { ...candidate, participantEgressId: egressId } : candidate,
      ) as [ParticipantSlot, ParticipantSlot];
      const next = {
        ...current,
        participants,
        updatedAt: this.clock.now(),
      };
      await this.save(current, next, tx);
    });
  }

  private async persistCaptureGrant(
    consultationId: UUID,
    generation: number,
    participantId: UUID,
    egressId: string,
  ): Promise<Consultation> {
    return this.consultations.transaction(async (tx) => {
      const current = await this.requiredLocked(consultationId, tx);
      if (current.generation !== generation || current.admissionFencedAt) {
        throw new DomainError("FENCED_GENERATION");
      }
      const next = grantCapture(current, participantId, egressId, this.clock.now());
      await this.save(current, next, tx);
      await this.enqueue(tx, next, "room.capture_ready", {
        participantId,
        participantEgressId: egressId,
      });
      return next;
    });
  }

  private async sendCaptureReadyStatus(
    consultation: Consultation,
    slot: ParticipantSlot,
    egressId: string,
    participantIdentity: UUID,
  ): Promise<void> {
    const packet = {
      schemaVersion: 1,
      consultationId: consultation.id,
      generation: consultation.generation,
      occurredAtMs: this.clock.now().getTime(),
      state: consultation.state === "active" ? "active" : "ready",
      reasonCode: "CAPTURE_READY",
      subjectParticipantId: slot.id,
      participantEgressId: egressId,
      shutdownAtMs: null,
    } as StatusPacket;
    await this.livekit.sendStatus(requiredRoomName(consultation), packet, [participantIdentity]);
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

  private async clearCaptureBinding(
    consultationId: UUID,
    generation: number,
    participantId: UUID,
    egressId: string,
  ): Promise<void> {
    await this.consultations.transaction((tx) =>
      this.consultations.clearParticipantEgressBinding(
        consultationId,
        generation,
        participantId,
        egressId,
        tx,
      ),
    );
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
  event: VerifiedWebhook,
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

function requiredRoomName(consultation: Consultation): string {
  if (!consultation.roomName) {
    throw new DomainError("INVALID_STATE");
  }
  return consultation.roomName;
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
