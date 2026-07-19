import { describe, expect, it, mock } from "bun:test";
import type { Consultation, ParticipantSlot } from "../src/consultations/domain";
import type { ConsultationRepository, EffectRepository, Transaction } from "../src/ports/index";
import { RoomService, type VerifiedWebhook } from "../src/rooms/service";

const ID = "00000000-0000-4000-8000-000000000001";
const OTHER = "00000000-0000-4000-8000-000000000002";
const PROFILE_ID = "00000000-0000-4000-8000-000000000003";
const ROOM_NAME = "00000000-0000-4000-8000-000000000010:2:room";
const NOW = new Date("2026-01-01T00:00:00Z");
const transaction = { opaque: Symbol("tx") } satisfies Transaction;

function participant(
  id: string,
  role: "employee" | "customer",
  overrides: Partial<ParticipantSlot> = {},
): ParticipantSlot {
  return {
    id,
    userId: id,
    role,
    livekitIdentity: id,
    displayName: role === "employee" ? "E" : "C",
    language: role === "employee" ? "en-US" : "de-DE",
    consent: null,
    present: true,
    eventWatermark: null,
    eventOccurredAt: null,
    publicationGranted: true,
    participantEgressId: null,
    ...overrides,
  };
}

function consultation(overrides: Partial<Consultation> = {}): Consultation {
  return {
    id: ID,
    state: "active",
    archiveState: "recording",
    providerProfileId: PROFILE_ID,
    providerProfileRevision: 1,
    participants: [
      participant(ID, "employee", {
        eventWatermark: "z",
        eventOccurredAt: NOW,
        participantEgressId: "egress-p",
      }),
      participant(OTHER, "customer"),
    ],
    providerSelection: null,
    snapshotHash: null,
    generation: 2,
    roomName: ROOM_NAME,
    roomSid: "RM_1",
    dispatchId: "DP_1",
    compositeEgressId: "egress-room",
    workerIdentity: null,
    readyDeadlineAt: null,
    finalizeDeadlineAt: null,
    bothAbsentSince: null,
    admissionFencedAt: null,
    createdAt: NOW,
    updatedAt: NOW,
    ...overrides,
  };
}

function webhookFixture(event: VerifiedWebhook) {
  const value = consultation();
  const save = mock(async () => true);
  const enqueue = mock(async () => undefined);
  const consultations = {
    transaction: async <T>(work: (value: Transaction) => Promise<T>) => work(transaction),
    lock: async () => value,
    save,
    resolveCurrentEgressSubject: async () => ({ participantId: ID }),
  } as unknown as ConsultationRepository;
  const effects = {
    acceptInbox: async () => true,
    enqueue,
  } as unknown as EffectRepository;
  const service = new RoomService(
    consultations,
    effects,
    {} as never,
    {} as never,
    { verify: async () => event },
    {} as never,
    { now: () => NOW },
    { uuid: () => OTHER },
    { sha256: () => "a".repeat(64) },
  );

  return { service, save, enqueue };
}

describe("RoomService webhook fences", () => {
  it("does not enqueue capture or rewrite state for a stale participant event", async () => {
    const fixture = webhookFixture({
      id: "a",
      consultationId: ID,
      generation: 2,
      occurredAt: new Date(NOW.getTime() - 1),
      kind: "participant_joined",
      roomName: ROOM_NAME,
      identity: ID,
      payload: {},
    });

    await expect(fixture.service.acceptWebhook(new Uint8Array(), {})).resolves.toBe(true);

    expect(fixture.enqueue).not.toHaveBeenCalled();
    expect(fixture.save).not.toHaveBeenCalled();
  });

  it("derives the Egress participant from persisted binding", async () => {
    const fixture = webhookFixture({
      id: "e",
      consultationId: ID,
      generation: 2,
      occurredAt: NOW,
      kind: "egress_active",
      roomName: ROOM_NAME,
      egressId: "egress-p",
      payload: {},
    });

    await fixture.service.acceptWebhook(new Uint8Array(), {});

    expect(fixture.enqueue).toHaveBeenCalledWith(
      expect.objectContaining({
        payload: expect.objectContaining({ participantId: ID }),
      }),
      transaction,
    );
  });

  it("clears a stopped participant Egress binding when publication permission fails", async () => {
    const value = consultation();
    const clearParticipantEgressBinding = mock(async () => true);
    const stop = mock(async () => undefined);
    const consultations = {
      get: async () => value,
      transaction: async <T>(work: (entry: Transaction) => Promise<T>) => work(transaction),
      lock: async () => value,
      save: async () => true,
      clearParticipantEgressBinding,
    } as unknown as ConsultationRepository;
    const effects = {
      enqueue: async () => undefined,
    } as unknown as EffectRepository;
    const service = new RoomService(
      consultations,
      effects,
      {
        updateParticipant: async () => {
          throw new Error("grant denied");
        },
      } as never,
      {
        get: async () => null,
        startParticipant: async () => ({
          egressId: "egress-new",
          state: "EGRESS_ACTIVE",
        }),
        stop,
      } as never,
      {
        verify: async () => {
          throw new Error("unused");
        },
      },
      {} as never,
      { now: () => NOW },
      { uuid: () => OTHER },
      { sha256: () => "a".repeat(64) },
    );

    await expect(service.executeCaptureBarrier(ID, OTHER)).rejects.toThrowError(/grant denied/);

    expect(stop).toHaveBeenCalledWith("egress-new");
    expect(clearParticipantEgressBinding).toHaveBeenCalledWith(
      ID,
      2,
      OTHER,
      "egress-new",
      transaction,
    );
  });

  it("persists an admission fence and waits for old ten-minute tokens before absence finalization", async () => {
    let now = NOW;
    const initial = consultation();
    let value: Consultation = {
      ...initial,
      bothAbsentSince: NOW,
      participants: [
        { ...initial.participants[0], present: false },
        { ...initial.participants[1], present: false },
      ],
    };
    const listAllowedParticipants = mock(async () => []);
    const consultations = {
      transaction: async <T>(work: (entry: Transaction) => Promise<T>) => work(transaction),
      lock: async () => value,
      save: async (next: Consultation) => {
        value = next;
        return true;
      },
    } as unknown as ConsultationRepository;
    const service = new RoomService(
      consultations,
      { enqueue: async () => undefined } as unknown as EffectRepository,
      { listAllowedParticipants } as never,
      {} as never,
      {
        verify: async () => {
          throw new Error("unused");
        },
      },
      {} as never,
      { now: () => now },
      { uuid: () => OTHER },
      { sha256: () => "a".repeat(64) },
    );

    await expect(service.reconcileAbsence(ID)).resolves.toBe(false);

    expect(value.admissionFencedAt).toEqual(NOW);
    expect(listAllowedParticipants).not.toHaveBeenCalled();

    now = new Date(NOW.getTime() + 600_000);
    await expect(service.reconcileAbsence(ID)).resolves.toBe(true);

    expect(listAllowedParticipants).toHaveBeenCalledTimes(1);
    expect(value.state).toBe("finalizing");
  });

  it("rolls back the durable admission fence when an allowed participant is observed", async () => {
    const old = new Date(NOW.getTime() - 600_000);
    const initial = consultation();
    let value: Consultation = {
      ...initial,
      bothAbsentSince: old,
      admissionFencedAt: old,
      participants: [
        { ...initial.participants[0], present: false },
        { ...initial.participants[1], present: false },
      ],
    };
    const consultations = {
      transaction: async <T>(work: (entry: Transaction) => Promise<T>) => work(transaction),
      lock: async () => value,
      save: async (next: Consultation) => {
        value = next;
        return true;
      },
    } as unknown as ConsultationRepository;
    const service = new RoomService(
      consultations,
      { enqueue: async () => undefined } as unknown as EffectRepository,
      { listAllowedParticipants: async () => [ID] } as never,
      {} as never,
      {
        verify: async () => {
          throw new Error("unused");
        },
      },
      {} as never,
      { now: () => NOW },
      { uuid: () => OTHER },
      { sha256: () => "a".repeat(64) },
    );

    await expect(service.reconcileAbsence(ID)).resolves.toBe(false);

    expect(value.admissionFencedAt).toBeNull();
    expect(value.state).toBe("active");
  });

  it("revokes a join made with a pre-fence token without granting capture", async () => {
    const current = consultation({ admissionFencedAt: NOW });
    const removeParticipant = mock(async () => undefined);
    const enqueue = mock(async () => undefined);
    const repository = {
      transaction: async <T>(work: (entry: Transaction) => Promise<T>) => work(transaction),
      lock: async () => current,
      save: async () => true,
    } as unknown as ConsultationRepository;
    const service = new RoomService(
      repository,
      {
        acceptInbox: async () => true,
        enqueue,
      } as unknown as EffectRepository,
      { removeParticipant } as never,
      {} as never,
      {
        verify: async () => ({
          id: "join",
          consultationId: ID,
          generation: 2,
          occurredAt: NOW,
          kind: "participant_joined" as const,
          roomName: ROOM_NAME,
          identity: ID,
          payload: {},
        }),
      },
      {} as never,
      { now: () => NOW },
      { uuid: () => OTHER },
      { sha256: () => "a".repeat(64) },
    );

    await service.acceptWebhook(new Uint8Array(), {});

    expect(removeParticipant).toHaveBeenCalledWith(ROOM_NAME, ID);
    expect(enqueue).not.toHaveBeenCalled();
  });
  it("revokes a stale-generation join from the room named by the event", async () => {
    const staleRoom = "00000000-0000-4000-8000-000000000099";
    const removeParticipant = mock(async () => undefined);
    const current = consultation();
    const service = new RoomService(
      {
        transaction: async <T>(work: (entry: Transaction) => Promise<T>) => work(transaction),
        lock: async () => current,
      } as unknown as ConsultationRepository,
      { acceptInbox: async () => true } as unknown as EffectRepository,
      { removeParticipant } as never,
      {} as never,
      {
        verify: async () => ({
          id: "stale-join",
          consultationId: ID,
          generation: 1,
          occurredAt: NOW,
          kind: "participant_joined" as const,
          roomName: staleRoom,
          identity: ID,
          payload: {},
        }),
      },
      {} as never,
      { now: () => NOW },
      { uuid: () => OTHER },
      { sha256: () => "a".repeat(64) },
    );

    await service.acceptWebhook(new Uint8Array(), {});

    expect(removeParticipant).toHaveBeenCalledWith(staleRoom, ID);
  });

  it("rechecks the admission fence before persisting a capture grant", async () => {
    let value = consultation();
    const clearParticipantEgressBinding = mock(async () => true);
    const stop = mock(async () => undefined);
    const updateParticipant = mock(async (input: { canPublish: boolean }) => {
      if (input.canPublish) {
        value = { ...value, admissionFencedAt: NOW };
      }
    });
    const service = new RoomService(
      {
        get: async () => value,
        transaction: async <T>(work: (entry: Transaction) => Promise<T>) => work(transaction),
        lock: async () => value,
        save: async (next: Consultation) => {
          value = next;
          return true;
        },
        clearParticipantEgressBinding,
      } as unknown as ConsultationRepository,
      { enqueue: async () => undefined } as unknown as EffectRepository,
      { updateParticipant } as never,
      {
        get: async () => null,
        startParticipant: async () => ({
          egressId: "egress-new",
          state: "EGRESS_ACTIVE",
        }),
        stop,
      } as never,
      {
        verify: async () => {
          throw new Error("unused");
        },
      },
      {} as never,
      { now: () => NOW },
      { uuid: () => OTHER },
      { sha256: () => "a".repeat(64) },
    );

    await expect(service.executeCaptureBarrier(ID, OTHER)).rejects.toThrowError(
      /FENCED_GENERATION/,
    );

    expect(updateParticipant).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ identity: OTHER, canPublish: false }),
    );
    expect(stop).toHaveBeenCalledWith("egress-new");
    expect(clearParticipantEgressBinding).toHaveBeenCalledWith(
      ID,
      2,
      OTHER,
      "egress-new",
      transaction,
    );
  });
});
