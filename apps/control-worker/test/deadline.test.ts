import { test } from "bun:test";
import assert from "node:assert/strict";
import { Coordinator } from "../src/orchestration/coordinator";
import type { DurableStore, PlannedEffect } from "../src/orchestration/model";

const consultationId = "30000000-0000-4000-8000-000000000001";
const owner = "30000000-0000-4000-8000-000000000002";
const employeeIdentity = "30000000-0000-4000-8000-000000000003";
const customerIdentity = "30000000-0000-4000-8000-000000000004";

test("absence deadline queries LiveKit and does not finalize while a human is present", async () => {
  // Arrange
  const planned: PlannedEffect[] = [];
  let queried = false;
  let completed = false;
  const absenceDeadline = {
    consultationId,
    generation: 2,
    kind: "absence" as const,
    dueAt: new Date(0),
  };
  const store = {
    claimOutbox: async () => [],
    claimStaleReservations: async () => [],
    claimDeadlines: async () => [absenceDeadline],
    preparePendingArchiveDeletes: async () => undefined,
    currentGeneration: async () => 2,
    consultationState: async () => "active" as const,
    presenceEpoch: async () => 4,
    humanIdentities: async () => [employeeIdentity, customerIdentity] as const,
    completeDeadline: async () => {
      completed = true;
    },
    scheduleEffect: async (effect: PlannedEffect) => {
      planned.push(effect);
    },
  } as unknown as DurableStore;
  const coordinator = new Coordinator(
    store,
    { now: () => new Date(5_000) },
    {
      owner,
      leaseMs: 1_000,
      batchSize: 2,
    },
    {
      areHumansAbsent: async () => {
        queried = true;
        return false;
      },
    },
    {
      reserve: async () => true,
      renew: async () => true,
      release: async () => undefined,
    },
  );

  // Act
  await coordinator.tick();

  // Assert
  assert.equal(queried, true);
  assert.equal(completed, false);
  assert.deepEqual(planned, []);
});

test("stale ready and finalize deadlines are gated by consultation state", async () => {
  for (const [kind, state] of [
    ["ready", "ended"],
    ["finalize", "ended"],
  ] as const) {
    let completions = 0;
    const planned: PlannedEffect[] = [];
    const deadline = {
      consultationId,
      generation: 2,
      kind,
      dueAt: new Date(0),
    };
    const store = {
      claimOutbox: async () => [],
      claimStaleReservations: async () => [],
      claimDeadlines: async () => [deadline],
      preparePendingArchiveDeletes: async () => undefined,
      currentGeneration: async () => 2,
      presenceEpoch: async () => 4,
      admitFinalization: async () => state,
      consultationState: async () => state,
      humanIdentities: async () => [employeeIdentity, customerIdentity] as const,
      completeDeadline: async () => {
        completions += 1;
      },
      scheduleEffect: async (effect: PlannedEffect) => {
        planned.push(effect);
      },
    } as unknown as DurableStore;
    const coordinator = new Coordinator(
      store,
      { now: () => new Date(5_000) },
      { owner, leaseMs: 1_000, batchSize: 2 },
      {
        areHumansAbsent: async () => true,
      },
      {
        reserve: async () => true,
        renew: async () => true,
        release: async () => undefined,
      },
    );

    await coordinator.tick();

    assert.equal(completions, 1, `${kind} terminal deadline must complete as stale`);
    assert.deepEqual(planned, [], `${kind} deadline must not drain ${state} consultation`);
  }
});

test("ready deadline finalizes while a human remains present", async () => {
  const planned: PlannedEffect[] = [];
  let completions = 0;
  let queried = false;
  const deadline = {
    consultationId,
    generation: 2,
    kind: "ready" as const,
    dueAt: new Date(0),
  };
  const store = {
    claimOutbox: async () => [],
    claimStaleReservations: async () => [],
    claimDeadlines: async () => [deadline],
    preparePendingArchiveDeletes: async () => undefined,
    currentGeneration: async () => 2,
    consultationState: async () => "ready" as const,
    presenceEpoch: async () => 9,
    humanIdentities: async () => [employeeIdentity, customerIdentity] as const,
    admitFinalization: async (
      admittedConsultationId: string,
      generation: number,
      observedPresenceEpoch: number,
    ) => {
      assert.equal(admittedConsultationId, consultationId);
      assert.equal(generation, 2);
      assert.equal(observedPresenceEpoch, 9);
      return "admitted" as const;
    },
    completeDeadline: async () => {
      completions += 1;
    },
    scheduleEffect: async (effect: PlannedEffect) => {
      planned.push(effect);
    },
    roomDrainPlan: async () => ({
      dispatchIds: [],
      egressIds: [],
      participantIds: [],
      roomCreated: false,
      resourceRoomName: null,
    }),
  } as unknown as DurableStore;
  const coordinator = new Coordinator(
    store,
    { now: () => new Date(5_000) },
    { owner, leaseMs: 1_000, batchSize: 2 },
    {
      areHumansAbsent: async () => {
        queried = true;
        return false;
      },
    },
    {
      reserve: async () => true,
      renew: async () => true,
      release: async () => undefined,
    },
  );

  await coordinator.tick();

  assert.equal(queried, false);
  assert.equal(completions, 1);
  assert.deepEqual(
    planned.map(({ kind }) => kind),
    ["STATUS_PACKET", "ROOM_DELETE"],
  );
  assert.equal(Object.hasOwn(planned[0]?.plan ?? {}, "destinationIdentities"), false);
});

test("ready deadline remains claimable when finalization loses its presence fence", async () => {
  const planned: PlannedEffect[] = [];
  let completions = 0;
  const deadline = {
    consultationId,
    generation: 2,
    kind: "ready" as const,
    dueAt: new Date(0),
  };
  const store = {
    claimOutbox: async () => [],
    claimStaleReservations: async () => [],
    claimDeadlines: async () => [deadline],
    preparePendingArchiveDeletes: async () => undefined,
    currentGeneration: async () => 2,
    consultationState: async () => "ready" as const,
    presenceEpoch: async () => 9,
    humanIdentities: async () => [employeeIdentity, customerIdentity] as const,
    admitFinalization: async (
      _consultationId: string,
      _generation: number,
      observedPresenceEpoch: number,
    ) => {
      assert.equal(observedPresenceEpoch, 9);
      return "ready" as const;
    },
    completeDeadline: async () => {
      completions += 1;
    },
    scheduleEffect: async (effect: PlannedEffect) => {
      planned.push(effect);
    },
  } as unknown as DurableStore;
  const coordinator = new Coordinator(
    store,
    { now: () => new Date(5_000) },
    { owner, leaseMs: 1_000, batchSize: 2 },
    { areHumansAbsent: async () => false },
    {
      reserve: async () => true,
      renew: async () => true,
      release: async () => undefined,
    },
  );

  await coordinator.tick();

  assert.equal(completions, 0);
  assert.deepEqual(planned, []);
});
