import { test } from "bun:test";
import assert from "node:assert/strict";
import { EffectRunner } from "../src/orchestration/effect-runner";
import type { DurableStore, Effect, ReconciliationSnapshot } from "../src/orchestration/model";
import type { RemoteEffects } from "../src/orchestration/remote";

const consultationId = "40000000-0000-4000-8000-000000000001";
const archiveObjectId = "40000000-0000-4000-8000-000000000005";

const effect: Effect = {
  id: "40000000-0000-4000-8000-000000000002",
  consultationId,
  generation: 3,
  kind: "ARCHIVE_RECONCILE",
  subjectId: consultationId,
  occurrenceKey: "ARCHIVE_RECONCILE:test",
  plan: { forceIncomplete: true, resourceGeneration: 2 },
  state: "planned",
  requestBytes: null,
  requestSha256: null,
  remoteId: null,
  attempt: 0,
  leaseOwner: null,
  leaseExpiresAt: null,
};

const snapshot: ReconciliationSnapshot = {
  archiveId: "40000000-0000-4000-8000-000000000003",
  state: "reconciling",
  reconciliationDeadlineAt: new Date(30_000),
  roomClose: { terminal: true },
  workerTerminal: { terminal: true },
  egressResults: [{ terminal: true, outputPrefix: `v1/meetings/${consultationId}/audio` }],
  providerGaps: [],
  expectations: [
    {
      id: "40000000-0000-4000-8000-000000000004",
      objectClass: "audio",
      causalKey: "one",
      sampleStart: 0,
      sampleEnd: 480,
      fulfilledObjectId: archiveObjectId,
    },
  ],
  objects: [
    {
      id: archiveObjectId,
      objectClass: "audio",
      key: `v1/meetings/${consultationId}/audio/one.pcm`,
      versionId: "v1",
      size: 960,
      sha256: "a".repeat(64),
      s3Checksum: "crc",
      contentType: "audio/L16",
    },
  ],
};

test("reconciliation verifies ledger objects before create-once final inventory", async () => {
  // Arrange
  const calls: string[] = [];
  const store = {
    claimEffects: async () => [effect],
    currentGeneration: async () => 3,
    persistCalling: async () => ({
      ...effect,
      state: "calling" as const,
      requestBytes: new Uint8Array(),
      requestSha256: "a".repeat(64),
    }),
    renewEffectLease: async () => true,
    reconciliationSnapshot: async (
      _consultationId: string,
      cleanupGeneration: number,
      resourceGeneration: number,
    ) => {
      calls.push(`snapshot:${String(cleanupGeneration)}:${String(resourceGeneration)}`);
      return snapshot;
    },
    completeReconciliation: async (
      _id: string,
      _snapshot: ReconciliationSnapshot,
      inventory: Readonly<Record<string, unknown>>,
    ) => {
      calls.push(`complete:${inventory.status}`);
      return true;
    },
    markDone: async () => {
      calls.push("done");
    },
    markFailed: async () => {
      calls.push("failed");
    },
  } as unknown as DurableStore;
  const remote = {
    discoverArchiveObjects: async () => [],
    verifyArchiveObject: async () => {
      calls.push("verify");
      return true;
    },
    putArchiveObject: async () => {
      calls.push("put");
      return {
        versionId: "final-v1",
        size: 100,
        checksum: "final-crc",
      };
    },
  } as unknown as RemoteEffects;
  const runner = new EffectRunner(
    store,
    remote,
    { now: () => new Date(1_000) },
    {
      owner: "40000000-0000-4000-8000-000000000009",
      leaseMs: 1_000,
      batchSize: 1,
    },
  );

  // Act
  await runner.tick();

  // Assert
  assert.deepEqual(calls, ["snapshot:3:2", "verify", "put", "complete:complete", "done"]);
});

test("reconciliation resolves planned Egress expectations from discovered immutable objects", async () => {
  const outputPrefix = `v1/meetings/${consultationId}/media/composite/3`;
  const calls: string[] = [];
  const egressSnapshot: ReconciliationSnapshot = {
    ...snapshot,
    egressResults: [{ terminal: true, outputPrefix }],
    expectations: [
      {
        id: "40000000-0000-4000-8000-000000000006",
        objectClass: "room_composite",
        causalKey: outputPrefix,
        sampleStart: null,
        sampleEnd: null,
        fulfilledObjectId: null,
      },
    ],
    objects: [],
  };
  const store = {
    claimEffects: async () => [effect],
    currentGeneration: async () => 3,
    persistCalling: async () => ({
      ...effect,
      state: "calling" as const,
      requestBytes: new Uint8Array(),
      requestSha256: "a".repeat(64),
    }),
    renewEffectLease: async () => true,
    reconciliationSnapshot: async () => egressSnapshot,
    completeReconciliation: async (
      _id: string,
      _snapshot: ReconciliationSnapshot,
      inventory: Readonly<Record<string, unknown>>,
    ) => {
      calls.push(`complete:${inventory.status}`);
      return true;
    },
    markDone: async () => {
      calls.push("done");
    },
    markFailed: async () => {
      calls.push("failed");
    },
  } as unknown as DurableStore;
  const remote = {
    discoverArchiveObjects: async () => [
      {
        key: `${outputPrefix}/${effect.id}/playlist.m3u8`,
        versionId: "egress-v1",
        size: 100,
        sha256: "b".repeat(64),
        checksum: "egress-crc",
        contentType: "application/vnd.apple.mpegurl",
      },
    ],
    verifyArchiveObject: async () => true,
    putArchiveObject: async () => ({
      versionId: "final-v1",
      size: 100,
      checksum: "final-crc",
    }),
  } as unknown as RemoteEffects;
  const runner = new EffectRunner(
    store,
    remote,
    { now: () => new Date(1_000) },
    {
      owner: "40000000-0000-4000-8000-000000000009",
      leaseMs: 1_000,
      batchSize: 1,
    },
  );

  await runner.tick();

  assert.deepEqual(calls, ["complete:complete", "done"]);
});

test("reconciliation records unresolved leaf provider attempts as explicit gaps", async () => {
  const inventories: Readonly<Record<string, unknown>>[] = [];
  const providerGapSnapshot: ReconciliationSnapshot = {
    ...snapshot,
    reconciliationDeadlineAt: new Date(0),
    providerGaps: [
      {
        attemptId: "40000000-0000-4000-8000-000000000010",
        stage: "tts",
        provider: "google",
        directionId: "40000000-0000-4000-8000-000000000011",
        operationId: "40000000-0000-4000-8000-000000000012",
        attemptNumber: 1,
        outcome: "failed",
        errorKind: "rate_limit",
        acceptedInputWatermark: 1,
        receivedOutputWatermark: 1,
        emittedOutputWatermark: 0,
        retryDecision: { action: "do_not_retry", reason: "unsafe committed output" },
      },
    ],
  };
  const store = {
    claimEffects: async () => [effect],
    currentGeneration: async () => 3,
    persistCalling: async () => ({
      ...effect,
      state: "calling" as const,
      requestBytes: new Uint8Array(),
      requestSha256: "a".repeat(64),
    }),
    renewEffectLease: async () => true,
    reconciliationSnapshot: async () => providerGapSnapshot,
    completeReconciliation: async (
      _id: string,
      _snapshot: ReconciliationSnapshot,
      inventory: Readonly<Record<string, unknown>>,
    ) => {
      inventories.push(inventory);
      return true;
    },
    markDone: async () => undefined,
    markFailed: async () => undefined,
  } as unknown as DurableStore;
  const remote = {
    discoverArchiveObjects: async () => [],
    verifyArchiveObject: async () => true,
    putArchiveObject: async () => ({
      versionId: "final-v1",
      size: 100,
      checksum: "final-crc",
    }),
  } as unknown as RemoteEffects;
  const runner = new EffectRunner(
    store,
    remote,
    { now: () => new Date(1_000) },
    {
      owner: "40000000-0000-4000-8000-000000000009",
      leaseMs: 1_000,
      batchSize: 1,
    },
  );

  await runner.tick();

  assert.equal(inventories[0]?.status, "incomplete");
  assert.deepEqual(inventories[0]?.missing, [
    {
      class: "provider_terminal",
      reason: "provider_attempt_failed",
      ...providerGapSnapshot.providerGaps[0],
    },
  ]);
});

test("redundant reconciliation completes after the archive is already terminal", async () => {
  const calls: string[] = [];
  const store = {
    claimEffects: async () => [effect],
    currentGeneration: async () => 3,
    persistCalling: async () => ({
      ...effect,
      state: "calling" as const,
      requestBytes: new Uint8Array(),
      requestSha256: "a".repeat(64),
    }),
    renewEffectLease: async () => true,
    reconciliationSnapshot: async () => null,
    markDone: async () => {
      calls.push("done");
    },
    markFailed: async () => {
      calls.push("failed");
    },
  } as unknown as DurableStore;
  const remote = {
    discoverArchiveObjects: async () => {
      throw new Error("terminal archive must not be rediscovered");
    },
  } as unknown as RemoteEffects;
  const runner = new EffectRunner(
    store,
    remote,
    { now: () => new Date(1_000) },
    {
      owner: "40000000-0000-4000-8000-000000000009",
      leaseMs: 1_000,
      batchSize: 1,
    },
  );

  await runner.tick();

  assert.deepEqual(calls, ["done"]);
});
