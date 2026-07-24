import { test, vi } from "bun:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { EgressStatus } from "livekit-server-sdk";
import { isViableEgressAdoption } from "../src/adapters/livekit-effects/egress";
import { EffectRunner } from "../src/orchestration/effect-runner";
import type {
  ArchivedObject,
  DurableStore,
  Effect,
  ReconciliationSnapshot,
} from "../src/orchestration/model";
import type { RemoteEffects } from "../src/orchestration/remote";
import { type EffectFaultControl, FileEffectFaultControl } from "../src/runtime/fault-control";

const effect: Effect = {
  id: "10000000-0000-4000-8000-000000000001",
  consultationId: "10000000-0000-4000-8000-000000000002",
  generation: 4,
  kind: "ROOM_CREATE",
  subjectId: "10000000-0000-4000-8000-000000000003",
  occurrenceKey: "ROOM_CREATE:test",
  plan: {
    roomName: "10000000-0000-4000-8000-000000000004",
    emptyTimeout: 300,
  },
  state: "planned",
  requestBytes: null,
  requestSha256: null,
  remoteId: null,
  attempt: 0,
  leaseOwner: null,
  leaseExpiresAt: null,
};
type Adoption = {
  remoteId: string;
  matchesRequest: boolean;
  terminal: boolean;
};

function harness(
  generation: number,
  adoption: Adoption | null,
  state: Effect["state"] = "planned",
  faults?: EffectFaultControl,
) {
  const calls: string[] = [];
  const claimed = {
    ...effect,
    state,
  };
  const store = {
    claimEffects: async () => [claimed],
    currentGeneration: async () => generation,
    persistCalling: async (_id: string, _owner: string, bytes: Uint8Array) => {
      calls.push(`persist:${bytes.length}`);
      return {
        ...effect,
        state: "calling" as const,
        attempt: 1,
      };
    },
    renewEffectLease: async () => true,
    markApplied: async () => {
      calls.push("applied");
      return "applied" as const;
    },
    markDone: async () => {
      calls.push("done");
    },
    markFailed: async () => {
      calls.push("failed");
    },
    markCompensating: async () => {
      calls.push("compensating");
    },
  } as unknown as DurableStore;
  const remote = {
    adopt: async () => {
      calls.push("adopt");
      return adoption;
    },
    execute: async () => {
      calls.push("execute");
      return {
        remoteId: "room-sid",
        result: {},
      };
    },
    compensate: async () => {
      calls.push("compensate");
    },
  } as unknown as RemoteEffects;
  const clock = {
    now: () => new Date(1_000),
  };
  const options = {
    owner: "10000000-0000-4000-8000-000000000009",
    leaseMs: 1_000,
    batchSize: 1,
  };
  const runner = new EffectRunner(store, remote, clock, options, faults);

  return {
    calls,
    runner,
  };
}

async function withFaultFile(
  configuration: unknown,
  action: (faults: FileEffectFaultControl) => Promise<void>,
): Promise<void> {
  const directory = await mkdtemp(join(tmpdir(), "transhooter-fault-control-"));
  const path = join(directory, "faults.json");
  try {
    const serialized = JSON.stringify(configuration);
    if (serialized === undefined) {
      throw new Error("fault configuration must be JSON serializable");
    }
    await writeFile(path, serialized, "utf8");
    await action(new FileEffectFaultControl(path));
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

test("persists canonical request before adoption and remote creation", async () => {
  const { calls, runner } = harness(4, null);
  await runner.tick();
  assert.match(calls[0] ?? "", /^persist:/);
  assert.deepEqual(calls.slice(1), ["adopt", "execute", "applied", "done"]);
});

test("adopts a matching deterministic remote without recreating it", async () => {
  const { calls, runner } = harness(4, {
    remoteId: "existing",
    matchesRequest: true,
    terminal: false,
  });
  await runner.tick();
  assert.deepEqual(calls.slice(1), ["adopt", "applied", "done"]);
});

test("renews the effect lease until markApplied commits for execution and adoption", async () => {
  vi.useFakeTimers();
  try {
    for (const adoption of [
      null,
      { remoteId: "existing", matchesRequest: true, terminal: false },
    ] as const) {
      const applied = Promise.withResolvers<void>();
      const commit = Promise.withResolvers<void>();
      let applying = false;
      let renewalsWhileApplying = 0;
      const calling = { ...effect, state: "calling" as const, attempt: 1 };
      const store = {
        claimEffects: async () => [effect],
        currentGeneration: async () => effect.generation,
        persistCalling: async () => calling,
        renewEffectLease: async () => {
          if (applying) {
            renewalsWhileApplying += 1;
          }
          return true;
        },
        markApplied: async () => {
          applying = true;
          applied.resolve();
          await commit.promise;
          applying = false;
          return "applied" as const;
        },
        markDone: async () => undefined,
        markFailed: async () => undefined,
        markCompensating: async () => undefined,
      } as unknown as DurableStore;
      const remote = {
        adopt: async () => adoption,
        execute: async () => ({ remoteId: "created", result: {} }),
        compensate: async () => undefined,
      } as unknown as RemoteEffects;
      const runner = new EffectRunner(
        store,
        remote,
        { now: () => new Date(1_000) },
        {
          owner: "10000000-0000-4000-8000-000000000009",
          leaseMs: 300,
          batchSize: 1,
        },
      );

      const tick = runner.tick();
      await applied.promise;
      vi.advanceTimersByTime(100);
      commit.resolve();
      await tick;

      assert.ok(renewalsWhileApplying > 0);
    }
  } finally {
    vi.useRealTimers();
  }
});

test("serializes overlapping lease renewals and never requests an earlier expiry", async () => {
  vi.useFakeTimers();
  try {
    const executeStarted = Promise.withResolvers<void>();
    const executeRelease = Promise.withResolvers<void>();
    const blockedRenewalStarted = Promise.withResolvers<void>();
    const blockedRenewalRelease = Promise.withResolvers<void>();
    const queuedRenewalStarted = Promise.withResolvers<void>();
    const queuedRenewalRequested = Promise.withResolvers<void>();
    let observeQueuedRenewalRequest = false;
    const expiries: number[] = [];
    let nowMs = 1_000;
    let activeRenewals = 0;
    let maximumActiveRenewals = 0;
    const store = {
      claimEffects: async () => [effect],
      currentGeneration: async () => effect.generation,
      persistCalling: async () => ({ ...effect, state: "calling" as const, attempt: 1 }),
      renewEffectLease: async (_id: string, _owner: string, expiresAt: Date) => {
        expiries.push(expiresAt.getTime());
        activeRenewals += 1;
        maximumActiveRenewals = Math.max(maximumActiveRenewals, activeRenewals);
        if (expiries.length === 2) {
          blockedRenewalStarted.resolve();
          await blockedRenewalRelease.promise;
        } else if (expiries.length === 3) {
          queuedRenewalStarted.resolve();
        }
        activeRenewals -= 1;
        return true;
      },
      markApplied: async () => "applied" as const,
      markDone: async () => undefined,
      markFailed: async () => undefined,
      markCompensating: async () => undefined,
    } as unknown as DurableStore;
    const remote = {
      adopt: async () => null,
      execute: async () => {
        executeStarted.resolve();
        await executeRelease.promise;
        return { remoteId: "created", result: {} };
      },
      compensate: async () => undefined,
    } as unknown as RemoteEffects;
    const runner = new EffectRunner(
      store,
      remote,
      {
        now: () => {
          if (observeQueuedRenewalRequest) {
            observeQueuedRenewalRequest = false;
            queuedRenewalRequested.resolve();
          }
          return new Date(nowMs);
        },
      },
      {
        owner: "10000000-0000-4000-8000-000000000009",
        leaseMs: 300,
        batchSize: 1,
      },
    );

    const tick = runner.tick();
    await executeStarted.promise;
    nowMs = 2_000;
    vi.advanceTimersByTime(100);
    await blockedRenewalStarted.promise;

    nowMs = 100;
    observeQueuedRenewalRequest = true;
    vi.advanceTimersByTime(100);
    await queuedRenewalRequested.promise;
    assert.equal(expiries.length, 2);
    assert.equal(maximumActiveRenewals, 1);

    blockedRenewalRelease.resolve();
    await queuedRenewalStarted.promise;
    assert.equal(maximumActiveRenewals, 1);
    assert.equal(expiries.length, 3);
    assert.ok(
      expiries.every(
        (expiry, index) =>
          index === 0 || expiry >= (expiries[index - 1] ?? Number.NEGATIVE_INFINITY),
      ),
    );
    executeRelease.resolve();
    await tick;
  } finally {
    vi.useRealTimers();
  }
});

test("waits for every claimed effect before propagating a batch failure", async () => {
  const failed = {
    ...effect,
    id: "10000000-0000-4000-8000-000000000010",
    state: "applied" as const,
  };
  const pending = {
    ...effect,
    id: "10000000-0000-4000-8000-000000000011",
    state: "applied" as const,
  };
  const pendingStarted = Promise.withResolvers<void>();
  const releasePending = Promise.withResolvers<void>();
  let pendingCompleted = false;
  const store = {
    claimEffects: async () => [failed, pending],
    renewEffectLease: async () => true,
    currentGeneration: async () => effect.generation,
    markDone: async (id: string) => {
      if (id === failed.id) {
        throw new Error("first claimed effect failed");
      }
      pendingStarted.resolve();
      await releasePending.promise;
      pendingCompleted = true;
    },
  } as unknown as DurableStore;
  const runner = new EffectRunner(
    store,
    {} as RemoteEffects,
    { now: () => new Date(1_000) },
    {
      owner: "10000000-0000-4000-8000-000000000009",
      leaseMs: 1_000,
      batchSize: 2,
    },
  );

  const tick = runner.tick();
  await pendingStarted.promise;
  let settled = false;
  void tick.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );
  await Promise.resolve();
  assert.equal(settled, false);

  releasePending.resolve();
  await assert.rejects(() => tick, /first claimed effect failed/);
  assert.equal(pendingCompleted, true);
});

test("only starting and active Egress states are viable for adoption", () => {
  for (const status of [EgressStatus.EGRESS_STARTING, EgressStatus.EGRESS_ACTIVE]) {
    assert.equal(isViableEgressAdoption(status), true);
  }
  for (const status of [
    EgressStatus.EGRESS_ENDING,
    EgressStatus.EGRESS_COMPLETE,
    EgressStatus.EGRESS_FAILED,
    EgressStatus.EGRESS_ABORTED,
    EgressStatus.EGRESS_LIMIT_REACHED,
  ]) {
    assert.equal(isViableEgressAdoption(status), false);
  }
});

test("does not compensate a mismatched deterministic remote", async () => {
  const { calls, runner } = harness(4, {
    remoteId: "foreign-room",
    matchesRequest: false,
    terminal: false,
  });
  await runner.tick();
  assert.deepEqual(calls.slice(1), ["adopt", "failed"]);
});

test("fenced generation compensates without a new remote call", async () => {
  const { calls, runner } = harness(5, null);
  await runner.tick();
  assert.deepEqual(calls, ["persist:70", "compensating", "compensate", "done"]);
});

test("resumes a leased compensation without replaying the remote effect", async () => {
  const { calls, runner } = harness(4, null, "compensating");
  await runner.tick();
  assert.deepEqual(calls, ["compensate", "done"]);
});

test("resumes an applied effect from durable state without repeating the remote call", async () => {
  const { calls, runner } = harness(4, null, "applied");

  await runner.tick();

  assert.deepEqual(calls, ["done"]);
});

test("compensates a recovered applied effect when its generation is stale", async () => {
  const calls: string[] = [];
  const applied = {
    ...effect,
    state: "applied" as const,
    remoteId: "persisted-room",
    appliedResult: { status: "created" },
  };
  const store = {
    claimEffects: async () => [applied],
    currentGeneration: async () => applied.generation + 1,
    renewEffectLease: async () => true,
    markCompensating: async () => {
      calls.push("compensating");
    },
    markDone: async () => {
      calls.push("done");
    },
  } as unknown as DurableStore;
  const remote = {
    compensate: async (candidate: Effect) => {
      calls.push(`compensate:${candidate.remoteId}`);
    },
  } as unknown as RemoteEffects;
  const runner = new EffectRunner(
    store,
    remote,
    { now: () => new Date(1_000) },
    {
      owner: "10000000-0000-4000-8000-000000000009",
      leaseMs: 1_000,
      batchSize: 1,
    },
  );

  await runner.tick();

  assert.deepEqual(calls, ["compensating", "compensate:persisted-room", "done"]);
});

test("passes the exact consultation ID to every fault boundary", async () => {
  const decisions: string[] = [];
  const faults: EffectFaultControl = {
    afterPersist: async (kind, consultationId) => {
      decisions.push(`persist:${kind}:${consultationId}`);
    },
    shouldFail: async (kind, consultationId) => {
      decisions.push(`before:${kind}:${consultationId}`);
    },
    afterRemoteSuccess: async (kind, consultationId) => {
      decisions.push(`remote:${kind}:${consultationId}`);
    },
    afterMarkApplied: async (kind, consultationId) => {
      decisions.push(`applied:${kind}:${consultationId}`);
    },
  };
  const { runner } = harness(4, null, "planned", faults);

  await runner.tick();

  assert.deepEqual(decisions, [
    `persist:ROOM_CREATE:${effect.consultationId}`,
    `before:ROOM_CREATE:${effect.consultationId}`,
    `remote:ROOM_CREATE:${effect.consultationId}`,
    `applied:ROOM_CREATE:${effect.consultationId}`,
  ]);
});

test("faults are scoped to the exact consultation and effect kind", async () => {
  const configuredConsultationId = "10000000-0000-4000-8000-000000000012";
  await withFaultFile(
    {
      consultations: {
        [configuredConsultationId]: {
          failEffects: ["ROOM_CREATE"],
          crashAfterPersistCalling: ["ROOM_CREATE"],
        },
      },
    },
    async (faults) => {
      const { calls, runner } = harness(4, null, "planned", faults);
      await runner.tick();
      assert.deepEqual(calls.slice(1), ["adopt", "execute", "applied", "done"]);

      await faults.shouldFail("PARTICIPANT_GRANT", configuredConsultationId);
      await faults.afterPersist("PARTICIPANT_GRANT", configuredConsultationId);
      await faults.afterRemoteSuccess("PARTICIPANT_GRANT", configuredConsultationId);
      await faults.afterMarkApplied("PARTICIPANT_GRANT", configuredConsultationId);
      await assert.rejects(
        () => faults.shouldFail("ROOM_CREATE", configuredConsultationId),
        /test fault denied ROOM_CREATE/,
      );
    },
  );
});

test("consultation fault arrays default to empty", async () => {
  await withFaultFile(
    {
      consultations: {
        [effect.consultationId]: {},
      },
    },
    async (faults) => {
      await faults.shouldFail("ROOM_CREATE", effect.consultationId);
      await faults.afterPersist("ROOM_CREATE", effect.consultationId);
      await faults.afterRemoteSuccess("ROOM_CREATE", effect.consultationId);
      await faults.afterMarkApplied("ROOM_CREATE", effect.consultationId);
    },
  );
});

test("remote-success and applied crash hooks are scoped and use distinct exits", async () => {
  const configuredConsultationId = "10000000-0000-4000-8000-000000000012";
  const exit = vi.spyOn(process, "exit").mockImplementation((code) => {
    throw new Error(`injected-exit:${code}`);
  });
  try {
    await withFaultFile(
      {
        consultations: {
          [configuredConsultationId]: {
            crashAfterRemoteSuccess: ["ROOM_CREATE"],
            crashAfterMarkApplied: ["ROOM_CREATE"],
          },
        },
      },
      async (faults) => {
        await faults.afterRemoteSuccess("ROOM_CREATE", effect.consultationId);
        await faults.afterMarkApplied("ROOM_CREATE", effect.consultationId);
        await faults.afterRemoteSuccess("PARTICIPANT_GRANT", configuredConsultationId);
        await faults.afterMarkApplied("PARTICIPANT_GRANT", configuredConsultationId);
        await assert.rejects(
          () => faults.afterRemoteSuccess("ROOM_CREATE", configuredConsultationId),
          /injected-exit:87/,
        );
        await assert.rejects(
          () => faults.afterMarkApplied("ROOM_CREATE", configuredConsultationId),
          /injected-exit:88/,
        );
      },
    );
  } finally {
    exit.mockRestore();
  }
});

test("rejects global, non-UUID, and unknown-effect fault configurations", async () => {
  const invalidConfigurations = [
    {
      failEffects: ["ROOM_CREATE"],
      crashAfterPersistCalling: [],
    },
    {
      consultations: {
        "not-a-uuid": {
          failEffects: ["ROOM_CREATE"],
        },
      },
    },
    {
      consultations: {
        [effect.consultationId]: {
          failEffects: ["NOT_AN_EFFECT"],
        },
      },
    },
  ];

  for (const configuration of invalidConfigurations) {
    await withFaultFile(configuration, async (faults) => {
      await assert.rejects(() => faults.shouldFail("ROOM_CREATE", effect.consultationId));
    });
  }
});

test("a rejected atomic applied transition compensates stale remote work", async () => {
  const calls: string[] = [];
  let transitionAttempted = false;
  const store = {
    claimEffects: async () => [effect],
    currentGeneration: async () =>
      transitionAttempted ? effect.generation + 1 : effect.generation,
    persistCalling: async () => ({ ...effect, state: "calling" as const, attempt: 1 }),
    renewEffectLease: async () => true,
    markApplied: async () => {
      transitionAttempted = true;
      calls.push("applied-rejected");
      return "rejected" as const;
    },
    markDone: async () => {
      calls.push("done");
    },
    markCompensating: async () => {
      calls.push("compensating");
    },
    markFailed: async () => {
      calls.push("failed");
    },
  } as unknown as DurableStore;
  const remote = {
    adopt: async () => null,
    execute: async () => ({
      remoteId: "RM_created",
      result: {},
    }),
    compensate: async (stale: Effect) => {
      calls.push(`compensate:${stale.remoteId}`);
    },
  } as unknown as RemoteEffects;
  const runner = new EffectRunner(
    store,
    remote,
    { now: () => new Date(1_000) },
    {
      owner: "10000000-0000-4000-8000-000000000009",
      leaseMs: 1_000,
      batchSize: 1,
    },
  );

  await runner.tick();

  assert.deepEqual(calls, ["applied-rejected", "compensating", "compensate:RM_created", "done"]);
});

test("lost compensation lease stops settlement", async () => {
  const calls: string[] = [];
  let renewal = 0;
  const store = {
    claimEffects: async () => [{ ...effect, state: "compensating" as const }],
    currentGeneration: async () => effect.generation + 1,
    renewEffectLease: async () => {
      renewal += 1;
      return renewal === 1;
    },
    markDone: async () => {
      calls.push("done");
    },
  } as unknown as DurableStore;
  const remote = {
    compensate: async () => {
      calls.push("compensate");
    },
  } as unknown as RemoteEffects;
  const runner = new EffectRunner(
    store,
    remote,
    { now: () => new Date(1_000) },
    {
      owner: "10000000-0000-4000-8000-000000000009",
      leaseMs: 1_000,
      batchSize: 1,
    },
  );

  await runner.tick();

  assert.deepEqual(calls, ["compensate"]);
});

test("a stale owner cannot persist a remote success after its lease is stolen", async () => {
  const calls: string[] = [];
  let renewals = 0;
  const store = {
    claimEffects: async () => [effect],
    currentGeneration: async () => effect.generation,
    persistCalling: async () => ({ ...effect, state: "calling" as const, attempt: 1 }),
    renewEffectLease: async () => {
      renewals += 1;
      return renewals === 1;
    },
    markApplied: async () => {
      calls.push("applied");
      return "applied" as const;
    },
    markDone: async () => {
      calls.push("done");
    },
    markFailed: async () => {
      calls.push("failed");
    },
  } as unknown as DurableStore;
  const remote = {
    adopt: async () => null,
    execute: async () => {
      calls.push("remote-success");
      return { remoteId: "RM_stale", result: {} };
    },
  } as unknown as RemoteEffects;
  const runner = new EffectRunner(
    store,
    remote,
    { now: () => new Date(1_000) },
    {
      owner: "10000000-0000-4000-8000-000000000009",
      leaseMs: 1_000,
      batchSize: 1,
    },
  );

  await runner.tick();

  assert.deepEqual(calls, ["remote-success"]);
});

test("caption read failures abort reconciliation and propagate the original failure", async () => {
  const archiveEffect: Effect = {
    ...effect,
    kind: "ARCHIVE_RECONCILE",
    subjectId: effect.consultationId,
    occurrenceKey: "ARCHIVE_RECONCILE:caption-read-failure",
    plan: { forceIncomplete: true, resourceGeneration: effect.generation },
  };
  const captionObjects: ArchivedObject[] = Array.from({ length: 8 }, (_, index) => ({
    id: `10000000-0000-4000-8001-${String(index).padStart(12, "0")}`,
    objectClass: "caption_packet",
    key: `v1/meetings/${effect.consultationId}/captions/${String(index)}.json`,
    versionId: `caption-v${String(index)}`,
    size: 1,
    sha256: "a".repeat(64),
    s3Checksum: `caption-crc-${String(index)}`,
    contentType: "application/json",
  }));
  const snapshot: ReconciliationSnapshot = {
    archiveId: "10000000-0000-4000-8000-000000000011",
    state: "reconciling",
    reconciliationDeadlineAt: new Date(0),
    roomClose: { terminal: true },
    workerTerminal: { terminal: true },
    egressResults: [],
    providerGaps: [],
    directions: [],
    providerAttempts: [],
    expectations: [],
    objects: captionObjects,
  };
  const readsSaturated = Promise.withResolvers<void>();
  const releaseReads = Promise.withResolvers<void>();
  const failingReadReleased = Promise.withResolvers<void>();
  const errors: string[] = [];
  let startedReads = 0;
  let completedReads = 0;
  let inventoryUploads = 0;
  let completions = 0;
  const store = {
    claimEffects: async () => [archiveEffect],
    currentGeneration: async () => archiveEffect.generation,
    persistCalling: async () => ({
      ...archiveEffect,
      state: "calling" as const,
      requestBytes: new Uint8Array(),
      requestSha256: "a".repeat(64),
    }),
    renewEffectLease: async () => true,
    reconciliationSnapshot: async () => snapshot,
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
    readArchiveObject: async (input: { key: string }) => {
      startedReads += 1;
      if (startedReads === 4) {
        readsSaturated.resolve();
      }
      await releaseReads.promise;
      const shouldFail = input.key.endsWith("/0.json");
      if (!shouldFail) {
        await failingReadReleased.promise;
      }
      if (shouldFail) {
        failingReadReleased.resolve();
        throw new Error("caption object read failed");
      }
      completedReads += 1;
      return new TextEncoder().encode("{}");
    },
    putArchiveObject: async () => {
      inventoryUploads += 1;
      return { versionId: "unexpected", size: 0, checksum: "unexpected" };
    },
  } as unknown as RemoteEffects;
  const runner = new EffectRunner(
    store,
    remote,
    { now: () => new Date(1_000) },
    {
      owner: "10000000-0000-4000-8000-000000000009",
      leaseMs: 1_000,
      batchSize: 1,
    },
  );

  const tick = runner.tick();
  await readsSaturated.promise;
  releaseReads.resolve();
  await tick;

  assert.equal(startedReads, 4);
  assert.equal(completedReads, 3);
  assert.deepEqual(errors, ["caption object read failed"]);
  assert.equal(inventoryUploads, 0);
  assert.equal(completions, 0);
});
