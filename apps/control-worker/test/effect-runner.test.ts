import { test, vi } from "bun:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { EffectRunner } from "../src/orchestration/effect-runner";
import type { DurableStore, Effect } from "../src/orchestration/model";
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
  assert.deepEqual(calls, ["compensating", "compensate", "done"]);
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

test("passes the exact consultation ID to both fault decisions", async () => {
  const decisions: string[] = [];
  const faults: EffectFaultControl = {
    afterPersist: async (kind, consultationId) => {
      decisions.push(`after:${kind}:${consultationId}`);
    },
    shouldFail: async (kind, consultationId) => {
      decisions.push(`before:${kind}:${consultationId}`);
    },
  };
  const { runner } = harness(4, null, "planned", faults);

  await runner.tick();

  assert.deepEqual(decisions, [
    `after:ROOM_CREATE:${effect.consultationId}`,
    `before:ROOM_CREATE:${effect.consultationId}`,
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
    },
  );
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

test("generation fenced after a remote create compensates instead of completing", async () => {
  const calls: string[] = [];
  let generationRead = 0;
  const store = {
    claimEffects: async () => [effect],
    currentGeneration: async () => {
      generationRead += 1;
      return generationRead < 3 ? effect.generation : effect.generation + 1;
    },
    persistCalling: async () => ({ ...effect, state: "calling" as const, attempt: 1 }),
    renewEffectLease: async () => true,
    markApplied: async () => {
      calls.push("applied");
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

  assert.deepEqual(calls, ["compensating", "compensate:RM_created", "done"]);
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
