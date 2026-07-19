import { RoomProviderSelectionSchema } from "@transhooter/contracts";
import { type Clock, DomainError, type IdGenerator, type UUID } from "../domain/model";
import type {
  AuditPort,
  AuthRepository,
  ConsultationRepository,
  EffectRepository,
  LiveKitTokenPort,
  OutboxMessage,
  ProviderSnapshotPort,
  Transaction,
} from "../ports/index";
import { deterministicRoomName, ORCHESTRATION_TOPICS } from "../rooms/orchestration-contract";
import {
  beginFinalization,
  beginProvisioning,
  type Consultation,
  cancelBeforeStart,
  joinEligibility,
  type ParticipantSlot,
  slotForUser,
  withConsent,
  withPreferences,
  withProviderSelection,
} from "./domain";

export const CONSENT_COPY =
  "By joining, I agree that this consultation, including my audio/video, live captions, translations, synthesized interpretation, and data sent to and returned by the listed speech, translation, and voice providers, " +
  "will be recorded and stored until an administrator deletes it. Media is encrypted in transit but is not end-to-end encrypted; the self-hosted translation and recording services receive decrypted media to translate and record it.";

export interface ConsultationHasher {
  sha256Canonical(value: unknown): string;
  sha256Text(value: string): string;
}

interface CreateConsultationInput {
  employeeUserId: UUID;
  customerUserId: UUID;
  providerProfileId: string;
}

type SnapshotParticipant = {
  id: UUID;
  language: string;
};

type SnapshotParticipants = [SnapshotParticipant, SnapshotParticipant];

type JoinResult =
  | { status: "ready"; consultation: Consultation }
  | { status: "PROVISIONING"; consultation: Consultation };

interface JoinTransactionResult {
  current: Consultation;
  newlyPlanned: boolean;
}

interface EndResult {
  consultation: Consultation;
  generation: number;
  shutdownAtMs: number;
}

export class ConsultationService {
  constructor(
    private readonly consultations: ConsultationRepository,
    private readonly effects: EffectRepository,
    private readonly snapshots: ProviderSnapshotPort,
    private readonly tokens: LiveKitTokenPort,
    private readonly audit: AuditPort,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
    private readonly hash: ConsultationHasher,
    private readonly auth: AuthRepository,
  ) {}

  async create(input: CreateConsultationInput): Promise<Consultation> {
    return this.consultations.transaction(async (tx) => {
      const now = this.clock.now();
      const profile = await this.snapshots.currentEnabledRevision(input.providerProfileId, tx);
      const id = this.ids.uuid();
      const employee = this.newParticipant("employee", input.employeeUserId);
      const customer = this.newParticipant("customer", input.customerUserId);
      const value: Consultation = {
        id,
        state: "invited",
        archiveState: "pending",
        providerProfileId: profile.profileId,
        providerProfileRevision: profile.revision,
        participants: [employee, customer],
        providerSelection: null,
        snapshotHash: null,
        generation: 0,
        roomName: null,
        roomSid: null,
        dispatchId: null,
        compositeEgressId: null,
        workerIdentity: null,
        readyDeadlineAt: null,
        finalizeDeadlineAt: null,
        bothAbsentSince: null,
        admissionFencedAt: null,
        createdAt: now,
        updatedAt: now,
      };

      await this.consultations.create(value, tx);
      await this.audit.append(
        {
          id: this.ids.uuid(),
          aggregateId: id,
          actorId: input.employeeUserId,
          kind: "consultation.created",
          occurredAt: now,
          details: {
            providerProfileId: input.providerProfileId,
            providerProfileRevision: profile.revision,
          },
        },
        tx,
      );
      return value;
    });
  }

  async get(consultationId: UUID, userId: UUID): Promise<Consultation> {
    const value = await this.consultations.get(consultationId);
    if (!value) {
      throw new DomainError("NOT_FOUND");
    }
    slotForUser(value, userId);
    return value;
  }

  async list(userId: UUID): Promise<readonly Consultation[]> {
    return this.consultations.listForUser(userId);
  }

  async setPreferences(
    consultationId: UUID,
    userId: UUID,
    displayName: string,
    language: string,
  ): Promise<Consultation> {
    return this.consultations.transaction(async (tx) => {
      const current = await this.requiredLocked(consultationId, tx);
      const slot = slotForUser(current, userId);
      let next = withPreferences(current, slot.id, displayName, language, this.clock.now());
      const [first, second] = next.participants;

      if (first.language !== null && second.language !== null) {
        const participants: SnapshotParticipants = [
          { id: first.id, language: first.language },
          { id: second.id, language: second.language },
        ];
        const resolved = await this.snapshots.resolve(next.providerProfileId, participants, tx);
        const selection = RoomProviderSelectionSchema.parse(resolved.selection);
        next = withProviderSelection(
          {
            ...next,
            providerProfileRevision: resolved.profileRevision,
          },
          selection,
          resolved.hash,
          this.clock.now(),
        );
      }

      await this.save(current, next, tx);
      await this.audit.append(
        {
          id: this.ids.uuid(),
          aggregateId: next.id,
          actorId: userId,
          kind: "consultation.preferences_changed",
          occurredAt: this.clock.now(),
          details: { participantId: slot.id, language },
        },
        tx,
      );
      return next;
    });
  }

  async consent(consultationId: UUID, userId: UUID, snapshotHash: string): Promise<Consultation> {
    return this.consultations.transaction(async (tx) => {
      const current = await this.requiredLocked(consultationId, tx);
      const slot = slotForUser(current, userId);
      const next = withConsent(
        current,
        slot.id,
        snapshotHash,
        this.hash.sha256Text(CONSENT_COPY),
        this.clock.now(),
      );

      await this.save(current, next, tx);
      await this.audit.append(
        {
          id: this.ids.uuid(),
          aggregateId: next.id,
          actorId: userId,
          kind: "consultation.consent_recorded",
          occurredAt: this.clock.now(),
          details: {
            participantId: slot.id,
            snapshotHash,
            version: 1,
          },
        },
        tx,
      );
      return next;
    });
  }

  async join(consultationId: UUID, userId: UUID): Promise<JoinResult> {
    const preflight = await this.consultations.get(consultationId);
    if (!preflight) {
      throw new DomainError("NOT_FOUND");
    }
    slotForUser(preflight, userId);

    if (preflight.state !== "ready" && preflight.state !== "active") {
      if (!preflight.providerSelection || !preflight.snapshotHash) {
        throw new DomainError("WAITING_FOR_PREFERENCES");
      }
      await this.snapshots.assertFreshAndHealthy(preflight.providerSelection);
    }

    const preflightHash = preflight.snapshotHash;
    const result = await this.consultations.transaction<JoinTransactionResult>(async (tx) => {
      const current = await this.requiredLocked(consultationId, tx);
      if (current.state === "ready" || current.state === "active") {
        return { current, newlyPlanned: false };
      }

      const eligibility = joinEligibility(current);
      if (eligibility !== "eligible") {
        throw new DomainError(eligibility);
      }
      if (!current.providerSelection || !current.snapshotHash) {
        throw new DomainError("WAITING_FOR_PREFERENCES");
      }
      if (current.snapshotHash !== preflightHash) {
        throw new DomainError("SNAPSHOT_CHANGED");
      }

      const generation = current.generation + 1;
      const roomName = deterministicRoomName(current.id, generation);
      const next = {
        ...beginProvisioning(current, roomName, this.clock.now()),
        workerIdentity: current.workerIdentity ?? this.ids.uuid(),
      };

      await this.save(current, next, tx);
      await this.enqueue(tx, next, ORCHESTRATION_TOPICS.provisioningRequested, {
        consultationId: next.id,
        generation: next.generation,
        subjectId: next.id,
      });
      return { current: next, newlyPlanned: true };
    });

    return result.newlyPlanned
      ? { status: "PROVISIONING", consultation: result.current }
      : { status: "ready", consultation: result.current };
  }

  async issueLiveKitToken(consultationId: UUID, userId: UUID): Promise<string> {
    return this.consultations.transaction(async (tx) => {
      const current = await this.requiredLocked(consultationId, tx);
      if (current.admissionFencedAt) {
        throw new DomainError("ADMISSION_FENCED");
      }
      if (current.state !== "ready" && current.state !== "active") {
        throw new DomainError("INVALID_STATE");
      }

      const slot = slotForUser(current, userId);
      if (
        !current.roomName ||
        !current.snapshotHash ||
        slot.consent?.snapshotHash !== current.snapshotHash
      ) {
        throw new DomainError("CONSENT_REQUIRED");
      }
      if (!current.roomSid || !current.compositeEgressId || !current.dispatchId) {
        throw new DomainError("PROVISIONING");
      }

      return this.tokens.issue({
        identity: slot.livekitIdentity,
        roomName: current.roomName,
        ttlSeconds: 600,
        attributes: {
          "consultation.id": current.id,
          "consultation.role": slot.role,
          "consultation.language": slot.language ?? "",
        },
        grants: {
          roomJoin: true,
          canPublish: false,
          canPublishData: false,
          canSubscribe: true,
        },
      });
    });
  }

  async end(consultationId: UUID, actorId: UUID): Promise<EndResult> {
    return this.consultations.transaction(async (tx) => {
      const current = await this.requiredLocked(consultationId, tx);
      const actor = slotForUser(current, actorId);
      if (actor.role !== "employee") {
        throw new DomainError("FORBIDDEN");
      }
      if (current.state === "finalizing" || current.state === "ended") {
        return {
          consultation: current,
          generation: current.generation,
          shutdownAtMs: current.updatedAt.getTime() + 5_000,
        };
      }

      const next = beginFinalization(current, this.clock.now());
      const shutdownAtMs = this.clock.now().getTime() + 5_000;
      await this.save(current, next, tx);
      await this.enqueue(tx, next, ORCHESTRATION_TOPICS.finalizationRequested, {
        consultationId: next.id,
        generation: next.generation,
        subjectId: next.id,
        shutdownAtMs,
      });
      return {
        consultation: next,
        generation: next.generation,
        shutdownAtMs,
      };
    });
  }

  async cancel(consultationId: UUID, actorId: UUID): Promise<Consultation> {
    return this.consultations.transaction(async (tx) => {
      const current = await this.requiredLocked(consultationId, tx);
      const actor = slotForUser(current, actorId);
      if (actor.role !== "employee") {
        throw new DomainError("FORBIDDEN");
      }
      if (current.state === "cancelled") {
        return current;
      }

      const resourceGeneration = current.generation;
      const next = cancelBeforeStart(current, this.clock.now());
      await this.save(current, next, tx);
      await this.auth.revokeConsultationLinks(current.id, this.clock.now(), tx);
      await this.enqueue(tx, next, "consultation.cancelled", {
        consultationId: next.id,
        generation: next.generation,
        resourceGeneration,
      });
      return next;
    });
  }

  private newParticipant(role: ParticipantSlot["role"], userId: UUID): ParticipantSlot {
    const id = this.ids.uuid();
    return {
      id,
      role,
      userId,
      livekitIdentity: id,
      displayName: null,
      language: null,
      consent: null,
      present: false,
      eventWatermark: null,
      eventOccurredAt: null,
      publicationGranted: false,
      participantEgressId: null,
    };
  }

  private async requiredLocked(id: UUID, tx: Transaction): Promise<Consultation> {
    const value = await this.consultations.lock(id, tx);
    if (!value) {
      throw new DomainError("NOT_FOUND");
    }
    return value;
  }

  private async save(previous: Consultation, next: Consultation, tx: Transaction): Promise<void> {
    const saved = await this.consultations.save(next, previous.updatedAt, tx);
    if (!saved) {
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
}
