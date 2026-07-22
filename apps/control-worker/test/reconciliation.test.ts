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
  directions: [],
  providerAttempts: [],
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
      _effect: Effect,
      _owner: string,
      _now: Date,
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
  assert.deepEqual(calls, ["snapshot:3:2", "verify", "put", "complete:complete"]);
});

test("forced reconciliation finalizes missing evidence before the archive deadline", async () => {
  const calls: string[] = [];
  const expectation = snapshot.expectations[0];
  assert(expectation);
  const forcedSnapshot: ReconciliationSnapshot = {
    ...snapshot,
    expectations: [
      {
        ...expectation,
        fulfilledObjectId: null,
      },
    ],
    objects: [],
  };
  const store = {
    claimEffects: async () => [effect],
    currentGeneration: async () => effect.generation,
    persistCalling: async () => ({
      ...effect,
      state: "calling" as const,
      requestBytes: new Uint8Array(),
      requestSha256: "a".repeat(64),
    }),
    renewEffectLease: async () => true,
    reconciliationSnapshot: async () => forcedSnapshot,
    completeReconciliation: async (
      _effect: Effect,
      _owner: string,
      _now: Date,
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
    verifyArchiveObject: async () => true,
    putArchiveObject: async () => ({
      versionId: "forced-final-v1",
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

  assert.deepEqual(calls, ["complete:incomplete"]);
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
      _effect: Effect,
      _owner: string,
      _now: Date,
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

  assert.deepEqual(calls, ["complete:complete"]);
});

test("reconciliation rejected by the transactional fence cannot mark the effect done", async () => {
  const calls: string[] = [];
  const store = {
    claimEffects: async () => [effect],
    currentGeneration: async () => effect.generation,
    persistCalling: async () => ({ ...effect, state: "calling" as const }),
    renewEffectLease: async () => true,
    reconciliationSnapshot: async () => snapshot,
    completeReconciliation: async () => {
      calls.push("fence-rejected");
      return false;
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
    verifyArchiveObject: async () => true,
    putArchiveObject: async () => ({
      versionId: "uncommitted-final",
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

  assert.deepEqual(calls, ["fence-rejected", "failed"]);
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
      _effect: Effect,
      _owner: string,
      _now: Date,
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
  assert.equal(inventories[0]?.consultationId, consultationId);
  assert.deepEqual(inventories[0]?.missing, [
    {
      class: "provider_terminal",
      reason: "provider_attempt_failed",
      ...providerGapSnapshot.providerGaps[0],
    },
  ]);
});

test("reconciliation retry reproduces the same inventory after discovering its derived VTT", async () => {
  const sourceParticipantId = "40000000-0000-4000-8000-000000000013";
  const destinationParticipantId = "40000000-0000-4000-8000-000000000014";
  const captionBytes = new TextEncoder().encode(
    JSON.stringify({
      schemaVersion: 1,
      consultationId,
      destinationParticipantId,
      sourceParticipantId,
      utteranceId: "40000000-0000-4000-8000-000000000015",
      revision: 1,
      finality: "final",
      sourceLanguage: "en-US",
      targetLanguage: "de-DE",
      sourceText: "hello",
      translatedText: "hallo",
      sourceSampleStart: 0,
      sourceSampleEnd: 480,
      occurredAtMs: 1_000,
    }),
  );
  const captionObject = {
    id: "40000000-0000-4000-8000-000000000016",
    objectClass: "caption_packet",
    key: `v1/meetings/${consultationId}/captions/packet.json`,
    versionId: "caption-v1",
    size: captionBytes.byteLength,
    sha256: "b".repeat(64),
    s3Checksum: "caption-crc",
    contentType: "application/json",
  };
  const retrySnapshot: ReconciliationSnapshot = {
    ...snapshot,
    reconciliationDeadlineAt: new Date(0),
    expectations: [],
    objects: [captionObject],
    egressResults: [],
  };
  const inventories: Readonly<Record<string, unknown>>[] = [];
  const stored = new Map<
    string,
    {
      body: Uint8Array;
      versionId: string;
      size: number;
      checksum: string;
      contentType: string;
      sha256: string;
    }
  >();
  const errors: string[] = [];
  const store = {
    claimEffects: async () => [effect],
    currentGeneration: async () => effect.generation,
    persistCalling: async () => ({
      ...effect,
      state: "calling" as const,
      requestBytes: new Uint8Array(),
      requestSha256: "a".repeat(64),
    }),
    renewEffectLease: async () => true,
    reconciliationSnapshot: async () => retrySnapshot,
    completeReconciliation: async (
      _effect: Effect,
      _owner: string,
      _now: Date,
      _snapshot: ReconciliationSnapshot,
      inventory: Readonly<Record<string, unknown>>,
    ) => {
      inventories.push(inventory);
      return inventories.length > 1;
    },
    markDone: async () => undefined,
    markFailed: async (_effectId: string, _owner: string, error: string) => {
      errors.push(error);
    },
  } as unknown as DurableStore;
  const remote = {
    discoverArchiveObjects: async () =>
      [...stored.entries()].map(([key, object]) => ({
        key,
        versionId: object.versionId,
        size: object.size,
        checksum: object.checksum,
        contentType: object.contentType,
        sha256: object.sha256,
      })),
    verifyArchiveObject: async () => true,
    readArchiveObject: async () => captionBytes,
    putArchiveObject: async (input: {
      key: string;
      body: Uint8Array;
      contentType: string;
      sha256: string;
    }) => {
      const existing = stored.get(input.key);
      if (existing !== undefined) {
        assert.equal(input.sha256, existing.sha256);
        assert.deepEqual(input.body, existing.body);
        return existing;
      }
      const uploaded = {
        body: input.body,
        versionId: input.key.endsWith("final.json") ? "inventory-v1" : "vtt-v1",
        size: input.body.byteLength,
        checksum: "stored-crc",
        contentType: input.contentType,
        sha256: input.sha256,
      };
      stored.set(input.key, uploaded);
      return uploaded;
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
  await runner.tick();

  assert.deepEqual(errors, ["final inventory create-once fence rejected"]);
  assert.equal(inventories.length, 2);
  assert.deepEqual(inventories[1], inventories[0]);
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

test("reconciliation waits for translated interpretation PCM evidence", async () => {
  const calls: string[] = [];
  const translatedSnapshot: ReconciliationSnapshot = {
    ...snapshot,
    directions: [
      {
        mode: "translated",
        destinationParticipantId: "40000000-0000-4000-8000-000000000006",
        emittedOutput: 48_000,
      },
    ],
  };
  const store = {
    claimEffects: async () => [{ ...effect, plan: { resourceGeneration: 2 } }],
    currentGeneration: async () => 3,
    persistCalling: async () => ({
      ...effect,
      plan: { resourceGeneration: 2 },
      state: "calling" as const,
      requestBytes: new Uint8Array(),
      requestSha256: "a".repeat(64),
    }),
    renewEffectLease: async () => true,
    reconciliationSnapshot: async () => translatedSnapshot,
    completeReconciliation: async () => {
      calls.push("complete");
      return true;
    },
    markDone: async () => calls.push("done"),
    markFailed: async (_id: string, _owner: string, message: string) => calls.push(message),
  } as unknown as DurableStore;
  const remote = {
    discoverArchiveObjects: async () => [],
    verifyArchiveObject: async () => true,
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

  assert.equal(calls.includes("complete"), false);
  assert.match(calls[0] ?? "", /tts_output_pcm/);
  assert.match(calls[0] ?? "", /livekit_output_pcm/);
});

test("reconciliation does not require interpretation PCM for same-language bypass", async () => {
  const calls: string[] = [];
  const sameLanguageSnapshot: ReconciliationSnapshot = {
    ...snapshot,
    directions: [
      {
        mode: "same_language",
        destinationParticipantId: "40000000-0000-4000-8000-000000000006",
        emittedOutput: 0,
      },
    ],
  };
  const store = {
    claimEffects: async () => [effect],
    currentGeneration: async () => effect.generation,
    persistCalling: async () => ({
      ...effect,
      state: "calling" as const,
      requestBytes: new Uint8Array(),
      requestSha256: "a".repeat(64),
    }),
    renewEffectLease: async () => true,
    reconciliationSnapshot: async () => sameLanguageSnapshot,
    completeReconciliation: async () => {
      calls.push("complete");
      return true;
    },
    markDone: async () => calls.push("done"),
    markFailed: async () => calls.push("failed"),
  } as unknown as DurableStore;
  const remote = {
    discoverArchiveObjects: async () => [],
    verifyArchiveObject: async () => true,
    putArchiveObject: async () => ({ versionId: "v1", size: 1, checksum: "crc" }),
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

  assert.deepEqual(calls, ["complete"]);
});
