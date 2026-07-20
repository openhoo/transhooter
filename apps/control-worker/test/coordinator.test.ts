import { test } from "bun:test";
import assert from "node:assert/strict";
import { WorkerJobMetadataSchema } from "@transhooter/contracts";
import { deterministicRoomName } from "@transhooter/server-core/rooms";
import { Coordinator } from "../src/orchestration/coordinator";
import type { Deadline, DurableStore, Effect, OutboxItem } from "../src/orchestration/model";

const consultationId = "20000000-0000-4000-8000-000000000001";
const participantId = "20000000-0000-4000-8000-000000000002";
const owner = "20000000-0000-4000-8000-000000000003";
const secondParticipantId = "20000000-0000-4000-8000-000000000004";
const workerIdentity = "20000000-0000-4000-8000-000000000005";
type PlannedEffect = Omit<
  Effect,
  | "state"
  | "requestBytes"
  | "requestSha256"
  | "remoteId"
  | "attempt"
  | "leaseOwner"
  | "leaseExpiresAt"
>;

const metadata = {
  schemaVersion: 1 as const,
  consultationId,
  generation: 7,
  roomName: "20000000-0000-4000-8000-000000000006",
  workerIdentity,
  workerEpoch: 3,
  writeEpoch: 0,
  expectedParticipantIds: [participantId, secondParticipantId],
  expectedLivekitIdentities: [
    "20000000-0000-4000-8000-000000000007",
    "20000000-0000-4000-8000-000000000008",
  ],
  providerSelection: {
    profileId: "google-eu",
    profileRevision: 1,
    capabilityHash: "a".repeat(64),
    participantIds: [participantId, secondParticipantId],
    directions: [
      {
        mode: "same_language",
        sourceParticipantId: participantId,
        destinationParticipantId: secondParticipantId,
        capabilityRowId: "20000000-0000-4000-8000-000000000009",
        bypass: true,
        stt: {
          provider: "fixture",
          endpoint: "https://fixture.invalid/stt",
          region: "test",
          model: "fixture",
          adapterBuild: "test",
          policy: "test",
          credential: {
            reference: "fixture",
            version: "1",
          },
          limits: {
            sessions: 4,
          },
          locale: "en-US",
          encoding: "LINEAR16",
        },
      },
      {
        mode: "same_language",
        sourceParticipantId: secondParticipantId,
        destinationParticipantId: participantId,
        capabilityRowId: "20000000-0000-4000-8000-000000000010",
        bypass: true,
        stt: {
          provider: "fixture",
          endpoint: "https://fixture.invalid/stt",
          region: "test",
          model: "fixture",
          adapterBuild: "test",
          policy: "test",
          credential: {
            reference: "fixture",
            version: "1",
          },
          limits: {
            sessions: 4,
          },
          locale: "en-US",
          encoding: "LINEAR16",
        },
      },
    ],
  },
  snapshotHash: "b".repeat(64),
};

async function run(
  payload: OutboxItem,
  standardHuman = true,
  drainPlan = {
    dispatchIds: ["AD_1"],
    egressIds: ["EG_1"],
    participantIds: [participantId],
    roomCreated: true,
    resourceRoomName: null,
  },
  onCancellationFence: (
    cleanupGeneration: number,
    resourceGeneration: number,
    effects: readonly PlannedEffect[],
  ) => void = () => undefined,
) {
  const planned: PlannedEffect[] = [];
  const store = {
    claimOutbox: async () => [payload],
    claimDeadlines: async () => [],
    claimStaleReservations: async () => [],
    preparePendingArchiveDeletes: async () => undefined,
    completeOutbox: async () => undefined,
    retryOutbox: async () => undefined,
    currentGeneration: async () => 7,
    applyVerifiedWebhook: async () => true,
    isStandardHuman: async () => standardHuman,
    consultationState: async () => "active" as const,
    presenceEpoch: async () => 0,
    admitFinalization: async () => "admitted" as const,
    markCaptureReady: async () => "active" as const,
    humanIdentities: async () => [participantId, "20000000-0000-4000-8000-000000000004"] as const,
    seedDeadlines: async () => undefined,
    roomDrainPlan: async () => drainPlan,
    completeRoomDrain: async () => undefined,
    workerDispatchMetadata: async () => metadata,
    capacityDimensions: async () => [],
    planFailureEffects: async (
      _consultationId: string,
      _generation: number,
      _reason: string,
      effects: readonly [PlannedEffect, PlannedEffect],
    ) => {
      planned.push(...effects);
    },
    fenceWorkerAndScheduleCancellation: async (
      _consultationId: string,
      cleanupGeneration: number,
      resourceGeneration: number,
      _owner: string,
      _reason: string,
      effects: readonly PlannedEffect[],
    ) => {
      onCancellationFence(cleanupGeneration, resourceGeneration, effects);
      planned.push(...effects);
    },
    scheduleEffect: async (effect: PlannedEffect) => {
      planned.push(effect);
    },
  } as unknown as DurableStore;
  const clock = {
    now: () => new Date(5_000),
  };
  const options = {
    owner,
    leaseMs: 1_000,
    batchSize: 4,
  };
  const remote = {
    areHumansAbsent: async () => true,
    notifyArchiveRecording: async () => undefined,
  };
  const coordination = {
    reserve: async () => true,
    renew: async () => true,
    release: async () => undefined,
  };
  const coordinator = new Coordinator(store, clock, options, remote, coordination);

  await coordinator.tick();

  return planned;
}

test("absence deadline remains claimable while humans are still present", async () => {
  const deadline: Deadline = {
    consultationId,
    generation: 7,
    kind: "absence",
    dueAt: new Date(4_000),
  };
  let completions = 0;
  const store = {
    claimOutbox: async () => [],
    claimDeadlines: async () => [deadline],
    claimStaleReservations: async () => [],
    preparePendingArchiveDeletes: async () => undefined,
    currentGeneration: async () => 7,
    consultationState: async () => "active" as const,
    presenceEpoch: async () => 3,
    humanIdentities: async () => [participantId, secondParticipantId],
    completeDeadline: async () => {
      completions += 1;
    },
  } as unknown as DurableStore;
  const coordinator = new Coordinator(
    store,
    { now: () => new Date(5_000) },
    { owner, leaseMs: 1_000, batchSize: 4 },
    {
      areHumansAbsent: async () => false,
      notifyArchiveRecording: async () => undefined,
    },
    {
      reserve: async () => true,
      renew: async () => true,
      release: async () => undefined,
    },
  );

  assert.equal(await coordinator.tick(), 1);
  assert.equal(completions, 0);
});

test("an active participant Egress converges on the publication grant", async () => {
  const planned = await run({
    id: "20000000-0000-4000-8000-000000000004",
    aggregateId: consultationId,
    type: "livekit.webhook.verified",
    attempts: 0,
    payload: {
      eventId: "evt-1",
      occurredAtMs: 10,
      consultationId,
      generation: 7,
      participantId,
      kind: "EGRESS_ACTIVE",
      egressId: "EG_1",
      egressStatus: "EGRESS_ACTIVE",
      rawSha256: "a".repeat(64),
    },
  });
  assert.equal(planned.length, 1);
  assert.equal(planned[0]?.kind, "PARTICIPANT_GRANT");
  assert.equal(planned[0]?.plan.barrierEgressId, "EG_1");
});

test("publication grant marks the accepted capture path ready", async () => {
  const planned = await run({
    id: "20000000-0000-4000-8000-000000000005",
    aggregateId: consultationId,
    type: "orchestration.effect.applied",
    attempts: 0,
    payload: {
      consultationId,
      generation: 7,
      subjectId: participantId,
      kind: "PARTICIPANT_GRANT",
      participantEgressId: "EG_1",
    },
  });
  assert.equal(planned.length, 1);
  assert.equal(planned[0]?.kind, "STATUS_PACKET");
  assert.equal(planned[0]?.plan.reasonCode, "CAPTURE_READY");
  assert.deepEqual(planned[0]?.plan.destinationIdentities, [participantId]);
  assert.equal(planned[0]?.plan.participantEgressId, "EG_1");
});

test("status occurrences coexist when transition evidence differs", async () => {
  const base = {
    id: "20000000-0000-4000-8000-000000000005",
    aggregateId: consultationId,
    type: "orchestration.effect.applied",
    attempts: 0,
  };
  const first = await run({
    ...base,
    payload: {
      consultationId,
      generation: 7,
      subjectId: participantId,
      kind: "PARTICIPANT_GRANT",
      participantEgressId: "EG_1",
    },
  });
  const second = await run({
    ...base,
    id: "20000000-0000-4000-8000-000000000019",
    payload: {
      consultationId,
      generation: 7,
      subjectId: participantId,
      kind: "PARTICIPANT_GRANT",
      participantEgressId: "EG_2",
    },
  });

  assert.notEqual(first[0]?.occurrenceKey, second[0]?.occurrenceKey);
  assert.notEqual(first[0]?.id, second[0]?.id);
});

test("identical logical status occurrence has a stable dedupe identity", async () => {
  const item = {
    id: "20000000-0000-4000-8000-000000000005",
    aggregateId: consultationId,
    type: "orchestration.effect.applied",
    attempts: 0,
    payload: {
      consultationId,
      generation: 7,
      subjectId: participantId,
      kind: "PARTICIPANT_GRANT",
      participantEgressId: "EG_1",
    },
  };
  const first = await run(item);
  const retried = await run(item);

  assert.equal(first[0]?.occurrenceKey, retried[0]?.occurrenceKey);
  assert.equal(first[0]?.id, retried[0]?.id);
});

test("an accepted participant Egress unlocks the publication grant", async () => {
  const planned = await run({
    id: "20000000-0000-4000-8000-000000000018",
    aggregateId: consultationId,
    type: "orchestration.effect.applied",
    attempts: 0,
    payload: {
      consultationId,
      generation: 7,
      subjectId: participantId,
      kind: "PARTICIPANT_EGRESS",
      participantEgressId: "EG_STARTING",
    },
  });
  assert.equal(planned.length, 1);
  assert.equal(planned[0]?.kind, "PARTICIPANT_GRANT");
  assert.equal(planned[0]?.plan.barrierEgressId, "EG_STARTING");
  assert.deepEqual(planned[0]?.plan.trackSource, ["microphone", "camera"]);
});

test("an unknown UUID participant never starts capture Egress", async () => {
  const planned = await run(
    {
      id: "20000000-0000-4000-8000-000000000006",
      aggregateId: consultationId,
      type: "livekit.webhook.verified",
      attempts: 0,
      payload: {
        eventId: "evt-outsider",
        occurredAtMs: 11,
        consultationId,
        generation: 7,
        participantId: "20000000-0000-4000-8000-000000000099",
        kind: "PARTICIPANT_JOINED",
        egressId: null,
        egressStatus: null,
        rawSha256: "b".repeat(64),
      },
    },
    false,
  );
  assert.deepEqual(planned, []);
});

test("finalization broadcasts shutdown so worker terminal evidence unlocks cleanup", async () => {
  const planned = await run({
    id: "20000000-0000-4000-8000-000000000012",
    aggregateId: consultationId,
    type: "consultation.finalization.requested",
    attempts: 0,
    payload: {
      consultationId,
      generation: 7,
      subjectId: consultationId,
      shutdownAtMs: 12_000,
    },
  });
  const dispatchDelete = planned[3];
  const roomDelete = planned[4];
  assert.ok(dispatchDelete);
  assert.ok(roomDelete);
  assert.deepEqual(
    planned.map(({ kind }) => kind),
    ["STATUS_PACKET", "EGRESS_STOP", "PARTICIPANT_REMOVE", "DISPATCH_DELETE", "ROOM_DELETE"],
  );
  assert.equal(Object.hasOwn(planned[0]?.plan ?? {}, "destinationIdentities"), false);
  for (const effect of planned.slice(1)) {
    assert.equal(effect.plan.notBeforeMs, 12_000);
  }
  assert.equal(dispatchDelete.plan.dependsOnEffectId, planned[0]?.id);
  assert.equal(dispatchDelete.plan.waitForWorkerTerminal, true);
  assert.deepEqual(
    roomDelete.plan.dependsOnEffectIds,
    planned.slice(1, 3).map(({ id }) => id),
  );
  assert.equal(roomDelete.plan.waitForWorkerTerminal, true);
  assert.equal(dispatchDelete.plan.workerTerminalGeneration, 7);
  assert.equal(roomDelete.plan.workerTerminalGeneration, 7);
});

test("cancellation fences prior ownership and targets prior-generation resources", async () => {
  const fences: Array<{
    cleanupGeneration: number;
    resourceGeneration: number;
  }> = [];
  const item: OutboxItem = {
    id: "20000000-0000-4000-8000-000000000020",
    aggregateId: consultationId,
    generation: 7,
    type: "consultation.cancelled",
    attempts: 0,
    payload: {
      consultationId,
      generation: 7,
      resourceGeneration: 6,
    },
  };
  const persistedResourceRoomName = "persisted-prior-generation-room";
  const planned = await run(
    item,
    true,
    {
      dispatchIds: ["AD_1"],
      egressIds: ["EG_1"],
      participantIds: [participantId],
      roomCreated: true,
      resourceRoomName: persistedResourceRoomName,
    },
    (cleanupGeneration, resourceGeneration) => {
      fences.push({ cleanupGeneration, resourceGeneration });
    },
  );

  assert.deepEqual(fences, [{ cleanupGeneration: 7, resourceGeneration: 6 }]);
  assert.deepEqual(
    planned.map(({ kind }) => kind),
    ["EGRESS_STOP", "PARTICIPANT_REMOVE", "DISPATCH_DELETE", "ROOM_DELETE"],
  );
  for (const effect of planned) {
    assert.equal(effect.generation, 7);
    assert.equal(effect.plan.resourceGeneration, 6);
    assert.equal(effect.plan.roomName, persistedResourceRoomName);
    assert.equal(effect.plan.resourceRoomName, persistedResourceRoomName);
  }
  const dispatchDelete = planned[2];
  const roomDelete = planned[3];
  assert.equal(dispatchDelete?.plan.workerTerminalGeneration, 6);
  assert.equal(roomDelete?.plan.workerTerminalGeneration, 6);
  assert.deepEqual(
    roomDelete?.plan.dependsOnEffectIds,
    planned.slice(0, 2).map(({ id }) => id),
  );

  const redelivered = await run(item, true, {
    dispatchIds: ["AD_1"],
    egressIds: ["EG_1"],
    participantIds: [participantId],
    roomCreated: true,
    resourceRoomName: persistedResourceRoomName,
  });
  assert.deepEqual(
    redelivered.map(({ id }) => id),
    planned.map(({ id }) => id),
  );
  const partiallySettledRedelivery = await run(item, true, {
    dispatchIds: ["AD_1"],
    egressIds: [],
    participantIds: [participantId],
    roomCreated: true,
    resourceRoomName: persistedResourceRoomName,
  });
  assert.equal(
    partiallySettledRedelivery.find(({ kind }) => kind === "ROOM_DELETE")?.id,
    roomDelete?.id,
  );
});

test("invited cancellation with no admitted resources is idempotent no-op cleanup", async () => {
  let fenceCalls = 0;
  const planned = await run(
    {
      id: "20000000-0000-4000-8000-000000000021",
      aggregateId: consultationId,
      generation: 7,
      type: "consultation.cancelled",
      attempts: 0,
      payload: {
        consultationId,
        generation: 7,
        resourceGeneration: 6,
      },
    },
    true,
    {
      dispatchIds: [],
      egressIds: [],
      participantIds: [participantId, secondParticipantId],
      roomCreated: false,
      resourceRoomName: null,
    },
    () => {
      fenceCalls += 1;
    },
  );
  assert.deepEqual(planned, []);
  assert.equal(fenceCalls, 1);
});

test("room deletion immediately starts archive reconciliation", async () => {
  const planned = await run({
    id: "20000000-0000-4000-8000-000000000018",
    aggregateId: consultationId,
    type: "orchestration.effect.applied",
    attempts: 0,
    payload: {
      consultationId,
      generation: 7,
      subjectId: consultationId,
      kind: "ROOM_DELETE",
      resourceGeneration: 6,
    },
  });
  assert.equal(planned.length, 1);
  assert.equal(planned[0]?.kind, "ARCHIVE_RECONCILE");
  assert.equal(planned[0]?.plan.forceIncomplete, false);
  assert.equal(planned[0]?.plan.resourceGeneration, 6);
});

test("archive deadline reuses the immediate reconciliation effect identity", async () => {
  const immediate = await run({
    id: "20000000-0000-4000-8000-000000000018",
    aggregateId: consultationId,
    type: "orchestration.effect.applied",
    attempts: 0,
    payload: {
      consultationId,
      generation: 7,
      subjectId: consultationId,
      kind: "ROOM_DELETE",
      resourceGeneration: 6,
    },
  });
  const deadline: Deadline = {
    consultationId,
    generation: 7,
    kind: "archive-reconcile",
    dueAt: new Date(4_000),
  };
  const planned: PlannedEffect[] = [];
  const store = {
    claimOutbox: async () => [],
    claimDeadlines: async () => [deadline],
    claimStaleReservations: async () => [],
    preparePendingArchiveDeletes: async () => undefined,
    currentGeneration: async () => 7,
    scheduleEffect: async (effect: PlannedEffect) => {
      planned.push(effect);
    },
    completeDeadline: async () => undefined,
  } as unknown as DurableStore;
  const coordinator = new Coordinator(
    store,
    { now: () => new Date(5_000) },
    { owner, leaseMs: 1_000, batchSize: 4 },
    {
      areHumansAbsent: async () => true,
      notifyArchiveRecording: async () => undefined,
    },
    {
      reserve: async () => true,
      renew: async () => true,
      release: async () => undefined,
    },
  );

  await coordinator.tick();

  assert.equal(planned.length, 1);
  assert.equal(planned[0]?.id, immediate[0]?.id);
  assert.equal(planned[0]?.occurrenceKey, immediate[0]?.occurrenceKey);
  assert.equal(planned[0]?.plan.forceIncomplete, true);
});

test("capture requests create generation-fenced participant Egress", async () => {
  const planned = await run({
    id: "20000000-0000-4000-8000-000000000013",
    aggregateId: consultationId,
    generation: 7,
    type: "room.capture_requested",
    attempts: 0,
    payload: {
      participantIdentity: participantId,
    },
  });
  assert.equal(planned.length, 1);
  const participantEgress = planned[0];
  assert.ok(participantEgress);
  assert.equal(participantEgress.kind, "PARTICIPANT_EGRESS");
  assert.equal(participantEgress.generation, 7);
});

test("successful Egress terminal does not trigger archive-failure shutdown", async () => {
  const planned = await run({
    id: "20000000-0000-4000-8000-000000000014",
    aggregateId: consultationId,
    generation: 7,
    type: "livekit.webhook.verified",
    attempts: 0,
    payload: {
      eventId: "evt-complete",
      occurredAtMs: 12,
      consultationId,
      generation: 7,
      participantId: null,
      kind: "EGRESS_TERMINAL",
      egressId: "EG_DONE",
      egressStatus: "EGRESS_COMPLETE",
      rawSha256: "c".repeat(64),
    },
  });
  assert.deepEqual(planned, []);
});

test("failed Egress terminal schedules archive-failure status", async () => {
  const planned = await run({
    id: "20000000-0000-4000-8000-000000000015",
    aggregateId: consultationId,
    generation: 7,
    type: "livekit.webhook.verified",
    attempts: 0,
    payload: {
      eventId: "evt-failed",
      occurredAtMs: 13,
      consultationId,
      generation: 7,
      participantId: null,
      kind: "EGRESS_TERMINAL",
      egressId: "EG_FAILED",
      egressStatus: "EGRESS_FAILED",
      rawSha256: "d".repeat(64),
    },
  });
  const archiveFailureStatus = planned[0];
  assert.ok(archiveFailureStatus);
  assert.equal(archiveFailureStatus.kind, "STATUS_PACKET");
  assert.equal(archiveFailureStatus.plan.reasonCode, "ARCHIVE_FAILED");
});

test("archive failure drains the fenced resource generation", async () => {
  const planned = await run({
    id: "20000000-0000-4000-8000-000000000019",
    aggregateId: consultationId,
    generation: 7,
    type: "archive.failed",
    attempts: 0,
    payload: {
      reasonCode: "ARCHIVE_FAILED",
      egressId: "EG_FAILED",
      resourceGeneration: 6,
    },
  });
  const status = planned.find(({ kind }) => kind === "STATUS_PACKET");
  const roomDelete = planned.find(({ kind }) => kind === "ROOM_DELETE");
  assert.ok(status);
  assert.ok(roomDelete);
  assert.equal(status.generation, 7);
  assert.equal(status.plan.resourceGeneration, 6);
  assert.equal(status.plan.roomName, deterministicRoomName(consultationId, 6));
  assert.equal(roomDelete.generation, 7);
  assert.equal(roomDelete.plan.resourceGeneration, 6);
  assert.equal(roomDelete.plan.workerTerminalGeneration, 6);
});

test("accepted Room Composite dispatches the full canonical worker metadata verbatim", async () => {
  const planned = await run({
    id: "20000000-0000-4000-8000-000000000016",
    aggregateId: consultationId,
    generation: 7,
    type: "orchestration.effect.applied",
    attempts: 0,
    payload: {
      consultationId,
      generation: 7,
      subjectId: consultationId,
      kind: "ROOM_COMPOSITE_EGRESS",
      participantEgressId: "EG_COMPOSITE_STARTING",
    },
  });
  assert.equal(planned.length, 1);
  const workerDispatch = planned[0];
  assert.ok(workerDispatch);
  assert.equal(workerDispatch.kind, "WORKER_DISPATCH");
  assert.equal(workerDispatch.subjectId, workerIdentity);
  assert.equal(workerDispatch.plan.roomCompositeEgressId, "EG_COMPOSITE_STARTING");
  assert.deepEqual(WorkerJobMetadataSchema.parse(workerDispatch.plan.metadata), metadata);
});

test("Room Composite ACTIVE is acknowledgement-only after accepted-effect dispatch", async () => {
  const planned = await run({
    id: "20000000-0000-4000-8000-000000000020",
    aggregateId: consultationId,
    generation: 7,
    type: "livekit.webhook.verified",
    attempts: 0,
    payload: {
      eventId: "evt-composite-active",
      occurredAtMs: 14,
      consultationId,
      generation: 7,
      participantId: null,
      kind: "EGRESS_ACTIVE",
      egressId: "EG_COMPOSITE",
      egressStatus: "EGRESS_ACTIVE",
      rawSha256: "e".repeat(64),
    },
  });
  assert.equal(planned.length, 0);
});

test("capacity rollback releases only reservations acquired before exhaustion", async () => {
  const released: Array<readonly [string, string]> = [];
  let reservationAttempt = 0;
  let retryMessage = "";
  const provisioning: OutboxItem = {
    id: "20000000-0000-4000-8000-000000000019",
    aggregateId: consultationId,
    type: "consultation.provisioning.requested",
    generation: 7,
    attempts: 0,
    payload: {
      consultationId,
      generation: 7,
      subjectId: consultationId,
    },
  };
  const store = {
    claimOutbox: async () => [provisioning],
    claimDeadlines: async () => [],
    claimStaleReservations: async () => [],
    preparePendingArchiveDeletes: async () => undefined,
    completeOutbox: async () => undefined,
    retryOutbox: async (_id: string, _owner: string, error: string) => {
      retryMessage = error;
    },
    capacityDimensions: async () => [
      { key: "first", capacity: 4, units: 1 },
      { key: "second", capacity: 4, units: 1 },
      { key: "third", capacity: 4, units: 1 },
    ],
  } as unknown as DurableStore;
  const coordinator = new Coordinator(
    store,
    { now: () => new Date(5_000) },
    { owner, leaseMs: 1_000, batchSize: 4 },
    {
      areHumansAbsent: async () => true,
      notifyArchiveRecording: async () => undefined,
    },
    {
      reserve: async () => {
        reservationAttempt += 1;
        return reservationAttempt !== 2;
      },
      renew: async () => true,
      release: async (key, reservationId) => {
        released.push([key, reservationId]);
      },
    },
  );

  await coordinator.tick();

  assert.deepEqual(released, [["first", `${consultationId}:0`]]);
  assert.equal(retryMessage, "worker capacity is exhausted for second");
});

test("capacity rollback includes an ambiguously failed reservation", async () => {
  const released: Array<readonly [string, string]> = [];
  let reservationAttempt = 0;
  let retryMessage = "";
  const provisioning: OutboxItem = {
    id: "20000000-0000-4000-8000-000000000020",
    aggregateId: consultationId,
    type: "consultation.provisioning.requested",
    generation: 7,
    attempts: 0,
    payload: {
      consultationId,
      generation: 7,
      subjectId: consultationId,
    },
  };
  const store = {
    claimOutbox: async () => [provisioning],
    claimDeadlines: async () => [],
    claimStaleReservations: async () => [],
    preparePendingArchiveDeletes: async () => undefined,
    completeOutbox: async () => undefined,
    retryOutbox: async (_id: string, _owner: string, error: string) => {
      retryMessage = error;
    },
    capacityDimensions: async () => [
      { key: "first", capacity: 4, units: 1 },
      { key: "second", capacity: 4, units: 1 },
      { key: "third", capacity: 4, units: 1 },
    ],
  } as unknown as DurableStore;
  const coordinator = new Coordinator(
    store,
    { now: () => new Date(5_000) },
    { owner, leaseMs: 1_000, batchSize: 4 },
    {
      areHumansAbsent: async () => true,
      notifyArchiveRecording: async () => undefined,
    },
    {
      reserve: async () => {
        reservationAttempt += 1;
        if (reservationAttempt === 2) {
          throw new Error("capacity reserve failed");
        }
        return true;
      },
      renew: async () => true,
      release: async (key, reservationId) => {
        released.push([key, reservationId]);
      },
    },
  );

  await coordinator.tick();

  assert.equal(reservationAttempt, 2);
  assert.deepEqual(released, [
    ["first", `${consultationId}:0`],
    ["second", `${consultationId}:1`],
  ]);
  assert.equal(retryMessage, "capacity reserve failed");
});

test("capacity rolls back when worker reservation fails after acquisition", async () => {
  const released: Array<readonly [string, string]> = [];
  let retryMessage = "";
  const provisioning: OutboxItem = {
    id: "20000000-0000-4000-8000-000000000021",
    aggregateId: consultationId,
    type: "consultation.provisioning.requested",
    generation: 7,
    attempts: 0,
    payload: {
      consultationId,
      generation: 7,
      subjectId: consultationId,
    },
  };
  const store = {
    claimOutbox: async () => [provisioning],
    claimDeadlines: async () => [],
    claimStaleReservations: async () => [],
    preparePendingArchiveDeletes: async () => undefined,
    completeOutbox: async () => undefined,
    retryOutbox: async (_id: string, _owner: string, error: string) => {
      retryMessage = error;
    },
    capacityDimensions: async () => [
      { key: "stt", capacity: 4, units: 1 },
      { key: "tts", capacity: 4, units: 1 },
    ],
    reserveWorker: async () => {
      throw new Error("worker reservation failed");
    },
  } as unknown as DurableStore;
  const coordinator = new Coordinator(
    store,
    { now: () => new Date(5_000) },
    { owner, leaseMs: 1_000, batchSize: 4 },
    {
      areHumansAbsent: async () => true,
      notifyArchiveRecording: async () => undefined,
    },
    {
      reserve: async () => true,
      renew: async () => true,
      release: async (key, reservationId) => {
        released.push([key, reservationId]);
      },
    },
  );

  await coordinator.tick();

  assert.deepEqual(released, [
    ["stt", `${consultationId}:0`],
    ["tts", `${consultationId}:1`],
  ]);
  assert.equal(retryMessage, "worker reservation failed");
});

test("accepted worker heartbeats renew every active capacity reservation", async () => {
  const renewed: Array<readonly [string, number, string]> = [];
  const heartbeat: OutboxItem = {
    id: "20000000-0000-4000-8000-000000000022",
    aggregateId: consultationId,
    type: "worker.heartbeat",
    generation: 7,
    attempts: 0,
    payload: {
      workerId: "20000000-0000-4000-8000-000000000023",
      epoch: 7,
      leaseSeconds: 30,
    },
  };
  const store = {
    claimOutbox: async () => [heartbeat],
    claimDeadlines: async () => [],
    claimStaleReservations: async () => [],
    preparePendingArchiveDeletes: async () => undefined,
    heartbeat: async () => true,
    capacityDimensions: async () => [
      { key: "stt", capacity: 4, units: 1 },
      { key: "tts", capacity: 4, units: 1 },
    ],
    completeOutbox: async () => undefined,
    retryOutbox: async () => undefined,
  } as unknown as DurableStore;
  const coordinator = new Coordinator(
    store,
    { now: () => new Date(5_000) },
    { owner, leaseMs: 1_000, batchSize: 4 },
    {
      areHumansAbsent: async () => true,
      notifyArchiveRecording: async () => undefined,
    },
    {
      reserve: async () => true,
      renew: async (key, ttlMs, reservationId) => {
        renewed.push([key, ttlMs, reservationId]);
        return true;
      },
      release: async () => undefined,
    },
  );

  await coordinator.tick();

  assert.deepEqual(renewed, [
    ["stt", 30_000, `${consultationId}:0`],
    ["tts", 30_000, `${consultationId}:1`],
  ]);
});

test("expired capacity reservations prevent durable worker heartbeat persistence", async () => {
  const calls: string[] = [];
  const heartbeat: OutboxItem = {
    id: "20000000-0000-4000-8000-000000000024",
    aggregateId: consultationId,
    type: "worker.heartbeat",
    generation: 7,
    attempts: 0,
    payload: {
      workerId: "20000000-0000-4000-8000-000000000025",
      epoch: 6,
      leaseSeconds: 30,
    },
  };
  const store = {
    claimOutbox: async () => [heartbeat],
    claimDeadlines: async () => [],
    claimStaleReservations: async () => [],
    preparePendingArchiveDeletes: async () => undefined,
    heartbeat: async () => {
      calls.push("heartbeat");
      return true;
    },
    capacityDimensions: async () => {
      calls.push("capacity");
      return [{ key: "stt", capacity: 4, units: 1 }];
    },
    completeOutbox: async () => undefined,
    retryOutbox: async () => undefined,
  } as unknown as DurableStore;
  const coordinator = new Coordinator(
    store,
    { now: () => new Date(5_000) },
    { owner, leaseMs: 1_000, batchSize: 4 },
    {
      areHumansAbsent: async () => true,
      notifyArchiveRecording: async () => undefined,
    },
    {
      reserve: async () => true,
      renew: async () => {
        calls.push("renew");
        return false;
      },
      release: async () => undefined,
    },
  );

  await coordinator.tick();

  assert.deepEqual(calls, ["capacity", "renew"]);
});
