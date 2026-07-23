import { test } from "bun:test";
import assert from "node:assert/strict";
import { EffectRunner } from "../src/orchestration/effect-runner";
import type {
  ArchivedObject,
  DerivedArchiveObject,
  DurableStore,
  Effect,
  FinalInventoryObject,
  ReconciliationSnapshot,
} from "../src/orchestration/model";
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

test("reconciliation bounds archive-object verification with the shared I/O pool", async () => {
  const verificationSaturated = Promise.withResolvers<void>();
  const releaseVerification = Promise.withResolvers<void>();
  let activeVerifications = 0;
  let maximumVerifications = 0;
  const verificationObjects: ArchivedObject[] = Array.from({ length: 9 }, (_, index) => ({
    id: `40000000-0000-4000-8004-${String(index).padStart(12, "0")}`,
    objectClass: "audio",
    key: `v1/meetings/${consultationId}/audio/${String(index)}.pcm`,
    versionId: `verification-v${String(index)}`,
    size: 960,
    sha256: "a".repeat(64),
    s3Checksum: `verification-crc-${String(index)}`,
    contentType: "audio/L16",
  }));
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
    reconciliationSnapshot: async () => ({
      ...snapshot,
      reconciliationDeadlineAt: new Date(0),
      expectations: [],
      objects: verificationObjects,
      egressResults: [],
    }),
    completeReconciliation: async () => true,
    markFailed: async () => undefined,
  } as unknown as DurableStore;
  const remote = {
    discoverArchiveObjects: async () => [],
    verifyArchiveObject: async () => {
      activeVerifications += 1;
      maximumVerifications = Math.max(maximumVerifications, activeVerifications);
      if (activeVerifications === 4) {
        verificationSaturated.resolve();
      }
      await releaseVerification.promise;
      activeVerifications -= 1;
      return true;
    },
    putArchiveObject: async (input: { body: Uint8Array }) => ({
      versionId: "inventory-v1",
      size: input.body.byteLength,
      checksum: "inventory-crc",
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

  const tick = runner.tick();
  await verificationSaturated.promise;
  assert.equal(activeVerifications, 4);
  assert.equal(maximumVerifications, 4);
  releaseVerification.resolve();
  await tick;

  assert.equal(maximumVerifications, 4);
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

test("reconciliation preserves indexed evidence winners and exact missing output", async () => {
  const outputPrefix = `v1/meetings/${consultationId}/media/composite/3`;
  const attemptId = "40000000-0000-4000-8000-000000000050";
  const destinationParticipantId = "40000000-0000-4000-8000-000000000051";
  const expectationId = "40000000-0000-4000-8000-000000000052";
  const lexicographicWinnerId = "40000000-0000-4000-8000-000000000053";
  const indexedObjects: ArchivedObject[] = [
    {
      id: "40000000-0000-4000-8000-000000000054",
      objectClass: "room_composite",
      key: `${outputPrefix}/z-playlist.m3u8`,
    },
    {
      id: lexicographicWinnerId,
      objectClass: "room_composite",
      key: `${outputPrefix}/a-playlist.m3u8`,
    },
    {
      id: "40000000-0000-4000-8000-000000000055",
      objectClass: "pipeline_exchange",
      key: `v1/meetings/${consultationId}/pipeline/terminal/raw/${attemptId}/terminal.json`,
    },
    {
      id: "40000000-0000-4000-8000-000000000056",
      objectClass: "pipeline_exchange",
      key: `v1/meetings/${consultationId}/pipeline/tts/raw/${attemptId}/response.json`,
    },
    {
      id: "40000000-0000-4000-8000-000000000057",
      objectClass: "tts_output_pcm",
      key: `v1/meetings/${consultationId}/audio/tts-output/${destinationParticipantId}/000000.pcm`,
    },
    {
      id: "40000000-0000-4000-8000-000000000058",
      objectClass: "livekit_output_pcm",
      key: `v1/meetings/${consultationId}/audio/livekit-output/${destinationParticipantId}/000000.pcm`,
    },
  ].map((object, index) => ({
    ...object,
    versionId: `indexed-v${String(index)}`,
    size: 100 + index,
    sha256: "f".repeat(64),
    s3Checksum: `indexed-crc-${String(index)}`,
    contentType: "application/octet-stream",
  }));
  const indexedSnapshot: ReconciliationSnapshot = {
    ...snapshot,
    reconciliationDeadlineAt: new Date(0),
    egressResults: [{ terminal: true, outputPrefix }],
    providerAttempts: [{ attemptId, stage: "tts" }],
    directions: [{ mode: "translated", destinationParticipantId, emittedOutput: 48_000 }],
    expectations: [
      {
        id: expectationId,
        objectClass: "room_composite",
        causalKey: outputPrefix,
        sampleStart: null,
        sampleEnd: null,
        fulfilledObjectId: null,
      },
    ],
    objects: indexedObjects,
  };
  const inventories: Readonly<Record<string, unknown>>[] = [];
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
    reconciliationSnapshot: async () => indexedSnapshot,
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
    markFailed: async () => undefined,
  } as unknown as DurableStore;
  const remote = {
    discoverArchiveObjects: async () => [],
    verifyArchiveObject: async (object: { key: string }) =>
      object.key !== `${outputPrefix}/a-playlist.m3u8`,
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
      expectationId,
      class: "room_composite",
      causalKey: outputPrefix,
      sampleStart: null,
      sampleEnd: null,
      reason: "object_verification_failed",
    },
  ]);
  assert.deepEqual(inventories[0]?.errors, [
    {
      code: "OBJECT_VERIFICATION_FAILED",
      objectIds: [lexicographicWinnerId],
    },
  ]);
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

test("caption reconciliation bounds reads and VTT uploads while preserving stable derived order", async () => {
  const captionObjects: ArchivedObject[] = [];
  const captionBodies = new Map<string, Uint8Array>();
  for (let index = 0; index < 9; index += 1) {
    const suffix = String(index + 20).padStart(12, "0");
    const destinationParticipantId = `40000000-0000-4000-8000-${suffix}`;
    const key = `v1/meetings/${consultationId}/captions/packet-${String(8 - index)}.json`;
    const body = new TextEncoder().encode(
      JSON.stringify({
        schemaVersion: 1,
        consultationId,
        destinationParticipantId,
        sourceParticipantId: "40000000-0000-4000-8000-000000000010",
        utteranceId: `40000000-0000-4000-8001-${suffix}`,
        revision: 1,
        finality: "final",
        sourceLanguage: "en-US",
        targetLanguage: "de-DE",
        sourceText: `source ${String(index)}`,
        translatedText: `translated ${String(index)}`,
        sourceSampleStart: index * 480,
        sourceSampleEnd: (index + 1) * 480,
        occurredAtMs: 1_000 + index,
      }),
    );
    captionBodies.set(key, body);
    captionObjects.push({
      id: `40000000-0000-4000-8002-${suffix}`,
      objectClass: "caption_packet",
      key,
      versionId: `caption-v${String(index)}`,
      size: body.byteLength,
      sha256: "c".repeat(64),
      s3Checksum: `caption-crc-${String(index)}`,
      contentType: "application/json",
    });
  }
  const concurrentSnapshot: ReconciliationSnapshot = {
    ...snapshot,
    reconciliationDeadlineAt: new Date(0),
    expectations: [],
    objects: captionObjects,
    egressResults: [],
  };
  const readsSaturated = Promise.withResolvers<void>();
  const releaseReads = Promise.withResolvers<void>();
  const uploadsSaturated = Promise.withResolvers<void>();
  const releaseUploads = Promise.withResolvers<void>();
  let activeReads = 0;
  let maximumReads = 0;
  let activeUploads = 0;
  let maximumUploads = 0;
  let derivedObjects: readonly DerivedArchiveObject[] = [];
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
    reconciliationSnapshot: async () => concurrentSnapshot,
    completeReconciliation: async (
      _effect: Effect,
      _owner: string,
      _now: Date,
      _snapshot: ReconciliationSnapshot,
      _inventory: Readonly<Record<string, unknown>>,
      _sha256: string,
      _finalObject: FinalInventoryObject,
      completedDerivedObjects: readonly DerivedArchiveObject[],
    ) => {
      derivedObjects = completedDerivedObjects;
      return true;
    },
    markFailed: async () => undefined,
  } as unknown as DurableStore;
  const remote = {
    discoverArchiveObjects: async () => [],
    verifyArchiveObject: async () => true,
    readArchiveObject: async (input: { key: string }) => {
      activeReads += 1;
      maximumReads = Math.max(maximumReads, activeReads);
      if (activeReads === 4) {
        readsSaturated.resolve();
      }
      await releaseReads.promise;
      activeReads -= 1;
      const body = captionBodies.get(input.key);
      assert(body);
      return body;
    },
    putArchiveObject: async (input: { key: string; body: Uint8Array }) => {
      if (input.key.endsWith("/final.vtt")) {
        activeUploads += 1;
        maximumUploads = Math.max(maximumUploads, activeUploads);
        if (activeUploads === 4) {
          uploadsSaturated.resolve();
        }
        await releaseUploads.promise;
        activeUploads -= 1;
      }
      return {
        versionId: input.key.endsWith("/final.vtt") ? "vtt-v1" : "inventory-v1",
        size: input.body.byteLength,
        checksum: "stored-crc",
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

  const tick = runner.tick();
  await readsSaturated.promise;
  assert.equal(activeReads, 4);
  assert.equal(maximumReads, 4);
  releaseReads.resolve();
  await uploadsSaturated.promise;
  assert.equal(activeUploads, 4);
  assert.equal(maximumUploads, 4);
  releaseUploads.resolve();
  await tick;

  assert.equal(derivedObjects.length, 9);
  assert.deepEqual(
    derivedObjects.map((object) => object.key),
    [...derivedObjects.map((object) => object.key)].sort(),
  );
});

test("caption reconciliation deterministically selects highest revisions after unordered reads", async () => {
  const destinationA = "40000000-0000-4000-8000-000000000021";
  const destinationB = "40000000-0000-4000-8000-000000000022";
  const utteranceA = "40000000-0000-4000-8000-000000000031";
  const utteranceB = "40000000-0000-4000-8000-000000000032";
  const packet = (
    destinationParticipantId: string,
    utteranceId: string,
    revision: number,
    translatedText: string,
    sourceSampleStart: number,
  ) => ({
    schemaVersion: 1,
    consultationId,
    destinationParticipantId,
    sourceParticipantId: "40000000-0000-4000-8000-000000000010",
    utteranceId,
    revision,
    finality: "final",
    sourceLanguage: "en-US",
    targetLanguage: "de-DE",
    sourceText: translatedText,
    translatedText,
    sourceSampleStart,
    sourceSampleEnd: sourceSampleStart + 480,
    occurredAtMs: 1_000 + revision,
  });
  const bodies = new Map<string, Uint8Array>([
    [
      "z-low.json",
      new TextEncoder().encode(JSON.stringify(packet(destinationA, utteranceA, 1, "stale", 960))),
    ],
    [
      "a-high.json",
      new TextEncoder().encode(JSON.stringify(packet(destinationA, utteranceA, 3, "newest", 960))),
    ],
    [
      "m-first.json",
      new TextEncoder().encode(JSON.stringify(packet(destinationA, utteranceB, 1, "first", 0))),
    ],
    [
      "b-other.json",
      new TextEncoder().encode(JSON.stringify(packet(destinationB, utteranceA, 2, "other", 480))),
    ],
  ]);
  const readReleases = new Map<
    string,
    { promise: Promise<void>; resolve: (value?: void | PromiseLike<void>) => void }
  >();
  const readsStarted = Promise.withResolvers<void>();
  const highReadCompleted = Promise.withResolvers<void>();
  const captionObjects = [...bodies.entries()].map(([name, body], index): ArchivedObject => {
    const key = `v1/meetings/${consultationId}/captions/${name}`;
    readReleases.set(key, Promise.withResolvers<void>());
    return {
      id: `40000000-0000-4000-8003-${String(index).padStart(12, "0")}`,
      objectClass: "caption_packet",
      key,
      versionId: `caption-v${String(index)}`,
      size: body.byteLength,
      sha256: "d".repeat(64),
      s3Checksum: `caption-crc-${String(index)}`,
      contentType: "application/json",
    };
  });
  const rendered = new Map<string, string>();
  let activeReads = 0;
  let derivedObjects: readonly DerivedArchiveObject[] = [];
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
    reconciliationSnapshot: async () => ({
      ...snapshot,
      reconciliationDeadlineAt: new Date(0),
      expectations: [],
      objects: captionObjects,
      egressResults: [],
    }),
    completeReconciliation: async (
      _effect: Effect,
      _owner: string,
      _now: Date,
      _snapshot: ReconciliationSnapshot,
      _inventory: Readonly<Record<string, unknown>>,
      _sha256: string,
      _finalObject: FinalInventoryObject,
      completedDerivedObjects: readonly DerivedArchiveObject[],
    ) => {
      derivedObjects = completedDerivedObjects;
      return true;
    },
    markFailed: async () => undefined,
  } as unknown as DurableStore;
  const remote = {
    discoverArchiveObjects: async () => [],
    verifyArchiveObject: async () => true,
    readArchiveObject: async (input: { key: string }) => {
      activeReads += 1;
      if (activeReads === 4) {
        readsStarted.resolve();
      }
      const release = readReleases.get(input.key);
      assert(release);
      await release.promise;
      const name = input.key.slice(input.key.lastIndexOf("/") + 1);
      if (name === "z-low.json") {
        await highReadCompleted.promise;
      }
      const body = bodies.get(name);
      assert(body);
      if (name === "a-high.json") {
        highReadCompleted.resolve();
      }
      return body;
    },
    putArchiveObject: async (input: { key: string; body: Uint8Array }) => {
      if (input.key.endsWith("/final.vtt")) {
        rendered.set(input.key, new TextDecoder().decode(input.body));
      }
      return {
        versionId: input.key.endsWith("/final.vtt") ? "vtt-v1" : "inventory-v1",
        size: input.body.byteLength,
        checksum: "stored-crc",
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

  const tick = runner.tick();
  await readsStarted.promise;
  for (const key of [...readReleases.keys()].reverse()) {
    const release = readReleases.get(key);
    assert(release);
    release.resolve();
  }
  await tick;

  assert.equal(
    rendered.get(`v1/meetings/${consultationId}/captions/${destinationA}/final.vtt`),
    "WEBVTT\n\n1\n00:00:00.000 --> 00:00:00.030\nfirst\n\n2\n00:00:00.060 --> 00:00:00.090\nnewest\n",
  );
  assert.equal(
    rendered.get(`v1/meetings/${consultationId}/captions/${destinationB}/final.vtt`),
    "WEBVTT\n\n1\n00:00:00.030 --> 00:00:00.060\nother\n",
  );
  assert.deepEqual(
    derivedObjects.map((object) => object.key),
    [
      `v1/meetings/${consultationId}/captions/${destinationA}/final.vtt`,
      `v1/meetings/${consultationId}/captions/${destinationB}/final.vtt`,
    ],
  );
});

test("destination VTT upload failure prevents inventory completion", async () => {
  const destinationParticipantId = "40000000-0000-4000-8000-000000000041";
  const captionBytes = new TextEncoder().encode(
    JSON.stringify({
      schemaVersion: 1,
      consultationId,
      destinationParticipantId,
      sourceParticipantId: "40000000-0000-4000-8000-000000000010",
      utteranceId: "40000000-0000-4000-8000-000000000042",
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
  const uploadSnapshot: ReconciliationSnapshot = {
    ...snapshot,
    reconciliationDeadlineAt: new Date(0),
    expectations: [],
    egressResults: [],
    objects: [
      {
        id: "40000000-0000-4000-8000-000000000043",
        objectClass: "caption_packet",
        key: `v1/meetings/${consultationId}/captions/upload-failure.json`,
        versionId: "caption-v1",
        size: captionBytes.byteLength,
        sha256: "e".repeat(64),
        s3Checksum: "caption-crc",
        contentType: "application/json",
      },
    ],
  };
  const errors: string[] = [];
  let completions = 0;
  let inventoryUploads = 0;
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
    reconciliationSnapshot: async () => uploadSnapshot,
    completeReconciliation: async () => {
      completions += 1;
      return true;
    },
    markFailed: async (_id: string, _owner: string, message: string) => {
      errors.push(message);
    },
  } as unknown as DurableStore;
  const remote = {
    discoverArchiveObjects: async () => [],
    verifyArchiveObject: async () => true,
    readArchiveObject: async () => captionBytes,
    putArchiveObject: async (input: { key: string }) => {
      if (input.key.endsWith("/final.vtt")) {
        throw new Error("destination VTT upload failed");
      }
      inventoryUploads += 1;
      return { versionId: "unexpected", size: 0, checksum: "unexpected" };
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

  assert.deepEqual(errors, ["destination VTT upload failed"]);
  assert.equal(inventoryUploads, 0);
  assert.equal(completions, 0);
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
