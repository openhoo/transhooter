import { test } from "bun:test";
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
