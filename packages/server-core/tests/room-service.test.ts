import { describe, expect, it, mock } from "bun:test";
import type { Consultation, ParticipantSlot } from "../src/consultations/domain";
import type { ConsultationRepository, EffectRepository, Transaction } from "../src/ports/index";
import { deterministicUuid } from "../src/rooms/orchestration-contract";
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

function webhookFixture(
  event: VerifiedWebhook,
  options: {
    resolveEgressEvent?: ConsultationRepository["resolveEgressEvent"];
    resolveCurrentEgressSubject?: ConsultationRepository["resolveCurrentEgressSubject"];
  } = {},
) {
  const value = consultation();
  const save = mock(async () => true);
  const enqueue = mock(async () => undefined);
  const resolveEgressEvent = mock(
    options.resolveEgressEvent ??
      (async () => ({
        consultationId: ID,
        generation: 2,
        roomName: ROOM_NAME,
      })),
  );
  const resolveCurrentEgressSubject =
    options.resolveCurrentEgressSubject ?? (async () => ({ participantId: ID }));
  const consultations = {
    transaction: async <T>(work: (value: Transaction) => Promise<T>) => work(transaction),
    lock: async () => value,
    save,
    resolveCurrentEgressSubject,
    resolveEgressEvent,
  } as unknown as ConsultationRepository;
  const acceptInbox = mock(async () => true);
  const effects = {
    acceptInbox,
    enqueue,
  } as unknown as EffectRepository;
  const service = new RoomService(
    consultations,
    effects,
    {} as never,
    { verify: async () => event },
    { now: () => NOW },
    { uuid: () => OTHER },
    { sha256: () => "a".repeat(64) },
  );

  return { service, save, enqueue, acceptInbox, resolveEgressEvent };
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

  it("journals participant capture with immutable resource identities", async () => {
    const fixture = webhookFixture({
      id: "joined-customer",
      consultationId: ID,
      generation: 2,
      occurredAt: NOW,
      kind: "participant_joined",
      roomName: ROOM_NAME,
      identity: OTHER,
      payload: {},
    });

    await fixture.service.acceptWebhook(new Uint8Array(), {});

    expect(fixture.enqueue).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: "orchestration.effect.plan",
        aggregateId: ID,
        generation: 2,
        payload: {
          consultationId: ID,
          generation: 2,
          subjectId: OTHER,
          kind: "PARTICIPANT_EGRESS",
          request: {
            roomName: ROOM_NAME,
            resourceRoomName: ROOM_NAME,
            participantIdentity: OTHER,
            outputPrefix: `v1/meetings/${ID}/media/participants/${OTHER}/2`,
            resourceGeneration: 2,
          },
        },
      }),
      transaction,
    );
    expect(fixture.save).toHaveBeenCalledTimes(1);
  });

  it("resolves a roomless Egress event from its persisted binding before applying it", async () => {
    const fixture = webhookFixture({
      id: "e",
      occurredAt: NOW,
      kind: "egress_active",
      egressId: "egress-p",
      payload: {},
    });

    await fixture.service.acceptWebhook(new Uint8Array(), {});

    expect(fixture.resolveEgressEvent).toHaveBeenCalledWith("egress-p", undefined);
    expect(fixture.acceptInbox).toHaveBeenCalledWith(
      "livekit",
      "e",
      NOW,
      "a".repeat(64),
      expect.objectContaining({
        consultationId: ID,
        generation: 2,
        roomName: ROOM_NAME,
      }),
      transaction,
    );
    expect(fixture.enqueue).toHaveBeenCalledWith(
      expect.objectContaining({
        aggregateId: ID,
        generation: 2,
        payload: expect.objectContaining({ consultationId: ID, participantId: ID }),
      }),
      transaction,
    );
  });

  it("accepts an early composite Egress event and enqueues its null participant subject", async () => {
    const egressSource = { kind: "room_composite", roomName: ROOM_NAME } as const;
    const fixture = webhookFixture(
      {
        id: "early-composite",
        occurredAt: NOW,
        kind: "egress_active",
        roomName: ROOM_NAME,
        egressId: "early-room-egress",
        egressSource,
        payload: {},
      },
      {
        resolveEgressEvent: async () => ({
          consultationId: ID,
          generation: 2,
          roomName: ROOM_NAME,
          earlySubject: { participantId: null },
        }),
        resolveCurrentEgressSubject: async () => null,
      },
    );

    await expect(fixture.service.acceptWebhook(new Uint8Array(), {})).resolves.toBe(true);

    expect(fixture.resolveEgressEvent).toHaveBeenCalledWith("early-room-egress", egressSource);
    expect(fixture.enqueue).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: "livekit.webhook.verified",
        payload: expect.objectContaining({
          kind: "EGRESS_ACTIVE",
          egressId: "early-room-egress",
          participantId: null,
        }),
      }),
      transaction,
    );
  });

  it("uses only the early participant identity fenced by repository resolution", async () => {
    const egressSource = {
      kind: "participant",
      roomName: ROOM_NAME,
      identity: OTHER,
    } as const;
    const fixture = webhookFixture(
      {
        id: "early-participant",
        occurredAt: NOW,
        kind: "egress_active",
        roomName: ROOM_NAME,
        egressId: "early-participant-egress",
        egressSource,
        payload: {},
      },
      {
        resolveEgressEvent: async () => ({
          consultationId: ID,
          generation: 2,
          roomName: ROOM_NAME,
          earlySubject: { participantId: OTHER },
        }),
        resolveCurrentEgressSubject: async () => null,
      },
    );

    await expect(fixture.service.acceptWebhook(new Uint8Array(), {})).resolves.toBe(true);
    expect(fixture.resolveEgressEvent).toHaveBeenCalledWith(
      "early-participant-egress",
      egressSource,
    );
    expect(fixture.enqueue).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: "livekit.webhook.verified",
        payload: expect.objectContaining({ participantId: OTHER }),
      }),
      transaction,
    );
  });

  it.each(["no pending match", "ambiguous pending matches"])(
    "rejects an early Egress event with %s",
    async () => {
      const egressSource = { kind: "room_composite", roomName: ROOM_NAME } as const;
      const fixture = webhookFixture(
        {
          id: "unbound-early",
          occurredAt: NOW,
          kind: "egress_active",
          roomName: ROOM_NAME,
          egressId: "unbound-egress",
          egressSource,
          payload: {},
        },
        { resolveEgressEvent: async () => null },
      );

      await expect(fixture.service.acceptWebhook(new Uint8Array(), {})).rejects.toMatchObject({
        code: "EGRESS_NOT_BOUND",
      });
      expect(fixture.resolveEgressEvent).toHaveBeenCalledWith("unbound-egress", egressSource);
      expect(fixture.acceptInbox).not.toHaveBeenCalled();
      expect(fixture.enqueue).not.toHaveBeenCalled();
    },
  );

  it("rejects a supplied Egress room that conflicts with its nested source binding", async () => {
    const fixture = webhookFixture({
      id: "e-mismatch",
      occurredAt: NOW,
      kind: "egress_active",
      roomName: "different-room",
      egressId: "egress-p",
      egressSource: { kind: "room_composite", roomName: ROOM_NAME },
      payload: {},
    });

    await expect(fixture.service.acceptWebhook(new Uint8Array(), {})).rejects.toMatchObject({
      code: "INVALID_WEBHOOK",
    });
    expect(fixture.resolveEgressEvent).toHaveBeenCalledWith("egress-p", {
      kind: "room_composite",
      roomName: ROOM_NAME,
    });
    expect(fixture.acceptInbox).not.toHaveBeenCalled();
    expect(fixture.enqueue).not.toHaveBeenCalled();
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
      {
        verify: async () => {
          throw new Error("unused");
        },
      },
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
      {
        verify: async () => {
          throw new Error("unused");
        },
      },
      { now: () => NOW },
      { uuid: () => OTHER },
      { sha256: () => "a".repeat(64) },
    );

    await expect(service.reconcileAbsence(ID)).resolves.toBe(false);

    expect(value.admissionFencedAt).toBeNull();
    expect(value.state).toBe("active");
  });

  it("journals a pre-fence participant removal in the webhook transaction", async () => {
    const current = consultation({ admissionFencedAt: NOW });
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
      { now: () => NOW },
      { uuid: () => OTHER },
      { sha256: () => "a".repeat(64) },
    );

    await service.acceptWebhook(new Uint8Array(), {});

    expect(enqueue).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: "orchestration.effect.plan",
        generation: 2,
        payload: expect.objectContaining({
          consultationId: ID,
          generation: 2,
          subjectId: deterministicUuid(ID, `participant-remove:2:${ROOM_NAME}:${ID}`),
          kind: "PARTICIPANT_REMOVE",
          request: {
            roomName: ROOM_NAME,
            resourceRoomName: ROOM_NAME,
            participantIdentity: ID,
            resourceGeneration: 2,
          },
        }),
      }),
      transaction,
    );
  });

  it("journals stale-generation removal against the event room as current-generation cleanup", async () => {
    const staleRoom = "00000000-0000-4000-8000-000000000099";
    const enqueue = mock(async () => undefined);
    const current = consultation();
    const service = new RoomService(
      {
        transaction: async <T>(work: (entry: Transaction) => Promise<T>) => work(transaction),
        lock: async () => current,
      } as unknown as ConsultationRepository,
      { acceptInbox: async () => true, enqueue } as unknown as EffectRepository,
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
      { now: () => NOW },
      { uuid: () => OTHER },
      { sha256: () => "a".repeat(64) },
    );

    await service.acceptWebhook(new Uint8Array(), {});

    expect(enqueue).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: "orchestration.effect.plan",
        generation: 2,
        payload: expect.objectContaining({
          generation: 2,
          kind: "PARTICIPANT_REMOVE",
          request: expect.objectContaining({
            roomName: staleRoom,
            resourceRoomName: staleRoom,
            participantIdentity: ID,
            resourceGeneration: 1,
          }),
        }),
      }),
      transaction,
    );
  });
});
