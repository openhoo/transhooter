import { describe, expect, it, mock } from "bun:test";
import { Prisma, type PrismaClient } from "../src/persistence/database";
import { PrismaEffectRepository } from "../src/persistence/effect-repository";
import { TransactionHandle } from "../src/persistence/transaction";
import type { EffectRepository, ExternalEffect, Transaction } from "../src/ports/index";
import { DurableEffectExecutor } from "../src/rooms/effects";

const NOW = new Date("2026-01-01T00:00:00Z");
const LEASE_OWNER_ID = "00000000-0000-4000-8000-000000000003";
const TRANSACTION = { opaque: Symbol("test") } satisfies Transaction;

const EFFECT: ExternalEffect = {
  id: "00000000-0000-4000-8000-000000000001",
  consultationId: "00000000-0000-4000-8000-000000000002",
  generation: 3,
  kind: "create_room",
  subjectId: "00000000-0000-4000-8000-000000000002",
  state: "planned",
  requestBytes: null,
  requestHash: null,
  leaseOwner: null,
  leaseExpiresAt: null,
  result: null,
  attempts: 0,
};

type EffectRequest = { room: string };
type EffectResult = { generation: number; sid: string };
type CompensationResult = { stopped: boolean };

const RESULT_CODEC = {
  encode: (request: EffectRequest) => new TextEncoder().encode(JSON.stringify(request)),
  hash: () => "hash",
  serializeResult: (value: EffectResult) => value,
  deserializeResult: (value: unknown) => value as EffectResult,
};

function createExecutor(repository: EffectRepository) {
  return new DurableEffectExecutor(repository, { now: () => NOW }, { uuid: () => LEASE_OWNER_ID });
}

function executeEffect(
  executor: DurableEffectExecutor,
  overrides: {
    adopt: () => Promise<EffectResult | null>;
    call: () => Promise<EffectResult>;
    compensate: (request: EffectRequest, result: EffectResult) => Promise<CompensationResult>;
  },
) {
  return executor.execute({
    effectId: EFFECT.id,
    generation: 3,
    request: { room: "opaque" },
    codec: RESULT_CODEC,
    adopt: overrides.adopt,
    call: overrides.call,
    compensate: overrides.compensate,
    resultGeneration: (value) => value.generation,
  });
}

function inspectSql(statement: unknown): string {
  expect(statement).toBeInstanceOf(Prisma.Sql);
  return (statement as Prisma.Sql).strings.join("?").replace(/\s+/g, " ").trim();
}

const EFFECT_PROJECTION =
  "id, consultation_id, generation, effect_kind, subject_id, state, request_bytes, " +
  "request_hash, lease_owner, lease_expires_at, result, attempts";

describe("DurableEffectExecutor", () => {
  it("persists request identity and adopts a matching remote effect before calling", async () => {
    const complete = mock(async () => true);
    const repository = {
      transaction: async <T>(work: (value: Transaction) => Promise<T>) => work(TRANSACTION),
      lock: async () => EFFECT,
      beginCall: async () => true,
      complete,
    } as unknown as EffectRepository;
    const remoteCall = mock(async () => ({ generation: 3, sid: "new" }));
    const executor = createExecutor(repository);

    const result = await executeEffect(executor, {
      adopt: async () => ({ generation: 3, sid: "adopted" }),
      call: remoteCall,
      compensate: async () => ({ stopped: true }),
    });

    expect(result.sid).toBe("adopted");
    expect(remoteCall).not.toHaveBeenCalled();
    expect(complete).toHaveBeenCalledWith(
      EFFECT.id,
      LEASE_OWNER_ID,
      "hash",
      "done",
      result,
      TRANSACTION,
    );
  });

  it("compensates and terminalizes a late remote result", async () => {
    const compensate = mock(async () => ({ stopped: true }));
    const completeCompensation = mock(async () => true);
    const repository = {
      transaction: async <T>(work: (value: Transaction) => Promise<T>) => work(TRANSACTION),
      lock: async () => EFFECT,
      beginCall: async () => true,
      beginCompensation: async () => true,
      complete: async () => true,
      recordCompensationAttempt: async () => undefined,
      completeCompensation,
    } as unknown as EffectRepository;
    const executor = createExecutor(repository);

    await expect(
      executeEffect(executor, {
        adopt: async () => ({ generation: 4, sid: "late" }),
        call: async () => ({ generation: 4, sid: "late" }),
        compensate,
      }),
    ).rejects.toThrowError(/LATE_EFFECT_RESULT/);

    expect(compensate).toHaveBeenCalledTimes(1);
    expect(completeCompensation).toHaveBeenCalledTimes(1);
  });

  it("reclaims an expired compensating effect and resumes idempotent cleanup", async () => {
    const compensatingEffect = {
      ...EFFECT,
      state: "compensating" as const,
      requestHash: "hash",
      result: { generation: 4, sid: "late" },
    };
    const beginCompensation = mock(async () => true);
    const completeCompensation = mock(async () => true);
    const compensate = mock(async () => ({ stopped: true }));
    const repository = {
      transaction: async <T>(work: (value: Transaction) => Promise<T>) => work(TRANSACTION),
      lock: async () => compensatingEffect,
      beginCompensation,
      completeCompensation,
    } as unknown as EffectRepository;
    const executor = createExecutor(repository);

    await expect(
      executeEffect(executor, {
        adopt: async () => null,
        call: async () => ({ generation: 3, sid: "new" }),
        compensate,
      }),
    ).rejects.toThrowError(/LATE_EFFECT_RESULT/);

    expect(beginCompensation).toHaveBeenCalledTimes(1);
    expect(compensate).toHaveBeenCalledWith({ room: "opaque" }, { generation: 4, sid: "late" });
    expect(completeCompensation).toHaveBeenCalledWith(
      EFFECT.id,
      LEASE_OWNER_ID,
      "hash",
      { stopped: true },
      TRANSACTION,
    );
  });

  it("returns a successor's authoritative adoption instead of compensating after lease loss", async () => {
    const compensate = mock(async () => ({ stopped: true }));
    const completedEffect = {
      ...EFFECT,
      state: "done" as const,
      requestHash: "hash",
      result: { generation: 3, sid: "successor" },
    };
    let lockCalls = 0;
    const repository = {
      transaction: async <T>(work: (value: Transaction) => Promise<T>) => work(TRANSACTION),
      lock: async () => {
        lockCalls += 1;
        return lockCalls === 1 ? EFFECT : completedEffect;
      },
      beginCall: async () => true,
      complete: async () => false,
    } as unknown as EffectRepository;
    const executor = createExecutor(repository);

    await expect(
      executeEffect(executor, {
        adopt: async () => ({ generation: 3, sid: "old" }),
        call: async () => ({ generation: 3, sid: "old" }),
        compensate,
      }),
    ).resolves.toEqual({ generation: 3, sid: "successor" });

    expect(compensate).not.toHaveBeenCalled();
  });
});

describe("PrismaEffectRepository projection", () => {
  it("uses the EffectRow projection for inserted and locked effects", async () => {
    const queryRaw = mock(async (_statement: Prisma.Sql) => [
      {
        id: EFFECT.id,
        consultation_id: EFFECT.consultationId,
        generation: EFFECT.generation,
        effect_kind: EFFECT.kind,
        subject_id: EFFECT.subjectId,
        state: EFFECT.state,
        request_bytes: EFFECT.requestBytes,
        request_hash: EFFECT.requestHash,
        lease_owner: EFFECT.leaseOwner,
        lease_expires_at: EFFECT.leaseExpiresAt,
        result: EFFECT.result,
        attempts: EFFECT.attempts,
      },
    ]);
    const transaction = new TransactionHandle({
      $queryRaw: queryRaw,
    } as unknown as Prisma.TransactionClient);
    const repository = new PrismaEffectRepository({} as unknown as PrismaClient);

    await expect(repository.plan(EFFECT, transaction)).resolves.toEqual(EFFECT);
    await expect(repository.lock(EFFECT.id, transaction)).resolves.toEqual(EFFECT);

    const statements = queryRaw.mock.calls.map(([statement]) => inspectSql(statement));
    expect(statements).toHaveLength(2);
    expect(statements[0]).toMatch(new RegExp(`RETURNING ${EFFECT_PROJECTION}$`));
    expect(statements[1]).toContain(`SELECT ${EFFECT_PROJECTION} FROM external_effects`);
    for (const statement of statements) {
      expect(statement).not.toMatch(/(?:SELECT|RETURNING) \*/);
    }
  });

  it("uses the same projection when an identity already exists", async () => {
    const row = {
      id: EFFECT.id,
      consultation_id: EFFECT.consultationId,
      generation: EFFECT.generation,
      effect_kind: EFFECT.kind,
      subject_id: EFFECT.subjectId,
      state: EFFECT.state,
      request_bytes: EFFECT.requestBytes,
      request_hash: EFFECT.requestHash,
      lease_owner: EFFECT.leaseOwner,
      lease_expires_at: EFFECT.leaseExpiresAt,
      result: EFFECT.result,
      attempts: EFFECT.attempts,
    };
    let query = 0;
    const queryRaw = mock(async (_statement: Prisma.Sql) => (query++ === 0 ? [] : [row]));
    const transaction = new TransactionHandle({
      $queryRaw: queryRaw,
    } as unknown as Prisma.TransactionClient);
    const repository = new PrismaEffectRepository({} as unknown as PrismaClient);

    await expect(repository.plan(EFFECT, transaction)).resolves.toEqual(EFFECT);

    const statements = queryRaw.mock.calls.map(([statement]) => inspectSql(statement));
    expect(statements).toHaveLength(2);
    expect(statements[0]).toMatch(new RegExp(`RETURNING ${EFFECT_PROJECTION}$`));
    expect(statements[1]).toContain(`SELECT ${EFFECT_PROJECTION} FROM external_effects`);
  });
});
