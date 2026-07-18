import { describe, expect, it, mock } from "bun:test";
import { ArchiveService } from "../src/archives/service";
import type {
  ArchiveRepository,
  ObjectStoragePort,
  SessionRecord,
  Transaction,
} from "../src/ports/index";

const CONSULTATION = "00000000-0000-4000-8000-000000000001";
const GENERATED_ID = "00000000-0000-4000-8000-000000000002";
const INVENTORY_ID = "00000000-0000-4000-8000-000000000003";
const CHECKPOINT_ID = "00000000-0000-4000-8000-000000000004";
const HOLD_ID = "00000000-0000-4000-8000-000000000010";
const OTHER_HOLD_ID = "00000000-0000-4000-8000-000000000011";
const SESSION_ID = "00000000-0000-4000-8000-000000000020";
const ACTOR_ID = "00000000-0000-4000-8000-000000000021";
const OBJECT_ID = "00000000-0000-4000-8000-000000000099";
const EXPECTATION_ID = "00000000-0000-4000-8000-000000000098";
const NOW = new Date("2026-01-01T00:00:00Z");
const FINAL_HASH = "a".repeat(64);
const CONFLICTING_HASH = "b".repeat(64);

const transaction = { opaque: Symbol("tx") } satisfies Transaction;
const clock = { now: () => new Date("2026-01-01T00:00:00Z") };
const ids = { uuid: () => GENERATED_ID };
const effects = { enqueue: async () => undefined };
const audit = { append: async () => undefined };
const hash = { sha256Canonical: () => FINAL_HASH };

function finalInventory() {
  return {
    schemaVersion: 1 as const,
    inventoryId: INVENTORY_ID,
    consultationId: CONSULTATION,
    status: "complete" as const,
    roomClose: {
      roomId: "room",
      generation: 1,
      closedAtMs: 1,
      reason: "ended",
    },
    workerTerminal: {
      workerEpoch: 1,
      checkpointId: CHECKPOINT_ID,
      outcome: "clean" as const,
      occurredAtMs: 1,
    },
    egressResults: [],
    objects: [],
    missing: [],
    errors: [],
    createdAtMs: 1,
  };
}

function lockedArchive(overrides: Record<string, unknown> = {}) {
  return {
    id: CONSULTATION,
    state: "complete",
    consultationState: "ended",
    writeEpoch: 2,
    completedDeletionEpoch: null,
    finalInventoryHash: FINAL_HASH,
    reconciliationDeadlineAt: null,
    ...overrides,
  };
}

function reauthenticatedSession(): SessionRecord {
  return {
    id: SESSION_ID,
    userId: ACTOR_ID,
    tokenHash: "x",
    csrfHash: "y",
    expiresAt: new Date("2026-01-01T12:00:00Z"),
    reauthenticatedAt: NOW,
    reauthConsultationId: CONSULTATION,
  };
}

function createService(repository: ArchiveRepository, storage: ObjectStoragePort): ArchiveService {
  return new ArchiveService(repository, storage, audit, clock, ids, hash, effects as never);
}

function runTransaction<T>(work: (value: Transaction) => Promise<T>): Promise<T> {
  return work(transaction);
}

describe("ArchiveService invariants", () => {
  it("rejects a different create-once final inventory hash before storage", async () => {
    const storage = {
      putCreateOnce: mock(),
    } as unknown as ObjectStoragePort;
    const repository = {
      transaction: runTransaction,
      lockByConsultation: async () =>
        lockedArchive({
          writeEpoch: 0,
          finalInventoryHash: CONFLICTING_HASH,
        }),
      finalInventoryHash: async () => CONFLICTING_HASH,
    } as unknown as ArchiveRepository;
    const service = createService(repository, storage);

    await expect(service.finalizeInventory(CONSULTATION, finalInventory())).rejects.toThrowError(
      /FINAL_INVENTORY_CONFLICT/,
    );

    expect(storage.putCreateOnce).not.toHaveBeenCalled();
  });

  it("requires two epoch-bound empty deletion scans before deleted", async () => {
    let state = "deleting";
    const scans: number[] = [];
    const repository = {
      transaction: runTransaction,
      lockByConsultation: async () =>
        lockedArchive({
          state,
          writeEpoch: 7,
        }),
      deletionWritersDrained: async () => true,
      recordDeletionScan: async (input: { consecutiveEmpty: number }) => {
        scans.push(input.consecutiveEmpty);
      },
      completeDeletion: async () => {
        state = "deleted";
        return true;
      },
      recordDeletionFailure: async () => undefined,
    } as unknown as ArchiveRepository;
    const storage = {
      listMultipart: async () => [],
      listMeetingVersions: async () => ({ versions: [], cursor: null }),
      abortMultipart: async () => undefined,
      deleteVersions: async () => undefined,
    } as unknown as ObjectStoragePort;
    const service = createService(repository, storage);

    await expect(service.drainDeletion(CONSULTATION, 7)).resolves.toBe(true);

    expect(scans).toEqual([1, 2]);
    expect(state).toBe("deleted");
  });

  it("releases one database hold without disabling storage while another hold remains", async () => {
    const removeHold = mock(async () => undefined);
    const setLegalHold = mock(async () => undefined);
    const repository = {
      transaction: runTransaction,
      lockByConsultation: async () => lockedArchive(),
      activeHolds: async () => [
        {
          id: HOLD_ID,
          reason: "case",
          actorId: ACTOR_ID,
          state: "active" as const,
        },
        {
          id: OTHER_HOLD_ID,
          reason: "other",
          actorId: ACTOR_ID,
          state: "active" as const,
        },
      ],
      claimStaleHoldOperation: async () => null,
      beginHoldOperation: async () => true,
      completeHoldOperation: async () => true,
      removeHold,
    } as unknown as ArchiveRepository;
    const storage = { setLegalHold } as unknown as ObjectStoragePort;
    const service = createService(repository, storage);

    await service.releaseHold(CONSULTATION, HOLD_ID, reauthenticatedSession());

    expect(removeHold).toHaveBeenCalledTimes(1);
    expect(setLegalHold).not.toHaveBeenCalled();
  });

  it("commits releasing before S3 work and re-protects versions after a partial failure", async () => {
    const states: string[] = [];
    const calls: boolean[] = [];
    const transactions: string[] = [];
    const repository = {
      transaction: async <T>(work: (value: Transaction) => Promise<T>) => {
        transactions.push(`tx-${transactions.length + 1}`);
        return work(transaction);
      },
      lockByConsultation: async () => lockedArchive(),
      activeHolds: async () => [
        {
          id: HOLD_ID,
          reason: "case",
          actorId: ACTOR_ID,
          state: "active" as const,
        },
      ],
      claimStaleHoldOperation: async () => null,
      beginHoldOperation: async () => true,
      renewHoldOperation: async () => true,
      completeHoldOperation: async () => true,
      transitionHoldState: async (_id: string, _from: readonly string[], state: string) => {
        states.push(state);
        return true;
      },
      recordHoldResults: async () => undefined,
    } as unknown as ArchiveRepository;
    let releaseCount = 0;
    const storage = {
      listMeetingVersions: async () => ({
        versions: [
          { key: "a", versionId: "1" },
          { key: "b", versionId: "2" },
        ],
        cursor: null,
      }),
      setLegalHold: async (_key: string, _version: string, enabled: boolean) => {
        calls.push(enabled);
        if (!enabled && ++releaseCount === 2) {
          throw new Error("denied");
        }
      },
    } as unknown as ObjectStoragePort;
    const service = createService(repository, storage);

    await expect(
      service.releaseHold(CONSULTATION, HOLD_ID, reauthenticatedSession()),
    ).rejects.toThrowError(/HOLD_RELEASE_FAILED/);

    expect(calls).toEqual([false, false, true]);
    expect(states).toEqual(["releasing", "active"]);
    expect(transactions.length).toBeGreaterThanOrEqual(4);
  });

  it("commits applying before any legal-hold storage call", async () => {
    let inTransaction = false;
    const repository = {
      transaction: async <T>(work: (value: Transaction) => Promise<T>) => {
        inTransaction = true;
        try {
          return await work(transaction);
        } finally {
          inTransaction = false;
        }
      },
      lockByConsultation: async () => lockedArchive(),
      claimStaleHoldOperation: async () => null,
      beginHoldOperation: async () => true,
      renewHoldOperation: async () => true,
      completeHoldOperation: async () => true,
      addHold: async () => undefined,
      transitionHoldState: async () => true,
      recordHoldResults: async () => undefined,
    } as unknown as ArchiveRepository;
    const storage = {
      listMeetingVersions: async () => ({
        versions: [{ key: "a", versionId: "1" }],
        cursor: null,
      }),
      setLegalHold: async () => {
        if (inTransaction) {
          throw new Error("storage called inside transaction");
        }
      },
    } as unknown as ObjectStoragePort;
    const service = createService(repository, storage);

    await expect(service.addHold(CONSULTATION, reauthenticatedSession(), "case")).resolves.toBe(
      GENERATED_ID,
    );
  });

  it("rejects a concurrent hold mutation while an archive operation owner is active", async () => {
    const addHold = mock();
    const storage = {
      listMeetingVersions: mock(),
    } as unknown as ObjectStoragePort;
    const repository = {
      transaction: runTransaction,
      lockByConsultation: async () => lockedArchive(),
      claimStaleHoldOperation: async () => null,
      beginHoldOperation: async () => false,
      addHold,
    } as unknown as ArchiveRepository;
    const service = createService(repository, storage);

    await expect(
      service.addHold(CONSULTATION, reauthenticatedSession(), "case"),
    ).rejects.toThrowError(/HOLD_OPERATION_IN_PROGRESS/);

    expect(addHold).not.toHaveBeenCalled();
    expect(storage.listMeetingVersions).not.toHaveBeenCalled();
  });

  it("reclaims an expired release lease and idempotently completes its storage fan-out", async () => {
    const setLegalHold = mock(async () => undefined);
    const removeHold = mock(async () => undefined);
    const claimStaleHoldOperation = mock(async () => ({
      operationId: HOLD_ID,
      kind: "release" as const,
    }));
    const repository = {
      transaction: runTransaction,
      lockByConsultation: async () => lockedArchive(),
      claimStaleHoldOperation,
      activeHolds: async () => [
        {
          id: HOLD_ID,
          reason: "case",
          actorId: ACTOR_ID,
          state: "releasing" as const,
        },
      ],
      renewHoldOperation: async () => true,
      completeHoldOperation: async () => true,
      recordHoldResults: async () => undefined,
      removeHold,
    } as unknown as ArchiveRepository;
    const storage = {
      listMeetingVersions: async () => ({
        versions: [{ key: "a", versionId: "1" }],
        cursor: null,
      }),
      setLegalHold,
    } as unknown as ObjectStoragePort;
    const service = createService(repository, storage);

    await expect(
      service.releaseHold(CONSULTATION, HOLD_ID, reauthenticatedSession()),
    ).resolves.toBeUndefined();

    expect(claimStaleHoldOperation).toHaveBeenCalledTimes(1);
    expect(setLegalHold).toHaveBeenCalledWith("a", "1", false);
    expect(removeHold).toHaveBeenCalledWith(HOLD_ID, ACTOR_ID, NOW, transaction);
  });

  it("rejects inventory objects that are not the exact persisted archive set before upload", async () => {
    const storage = {
      putCreateOnce: mock(),
    } as unknown as ObjectStoragePort;
    const repository = {
      transaction: runTransaction,
      lockByConsultation: async () =>
        lockedArchive({
          state: "reconciling",
          finalInventoryHash: null,
        }),
      finalInventoryHash: async () => null,
      finalInventory: async () => null,
      unresolvedExpectations: async () => [],
      inventoryObjects: async () => [
        {
          id: OBJECT_ID,
          consultationId: CONSULTATION,
          objectClass: "checkpoint",
          causalKey: "checkpoint:1",
          key: "v1/meetings/x/checkpoint",
          versionId: "v1",
          size: 1,
          sha256: CONFLICTING_HASH,
          s3Checksum: "crc",
          contentType: "application/json",
          sampleStart: null,
          sampleEnd: null,
          attempt: null,
          sequence: null,
          writerEpoch: 2,
        },
      ],
      completePrerequisites: async () => true,
    } as unknown as ArchiveRepository;
    const service = createService(repository, storage);

    await expect(service.finalizeInventory(CONSULTATION, finalInventory())).rejects.toThrowError(
      /INVENTORY_OBJECT_MISMATCH/,
    );

    expect(storage.putCreateOnce).not.toHaveBeenCalled();
  });

  it("applies active legal hold before binding a newly uploaded object", async () => {
    const order: string[] = [];
    const repository = {
      transaction: runTransaction,
      lockByConsultation: async () => lockedArchive(),
      activeHolds: async () => [{ id: HOLD_ID, reason: "case" }],
      recordObject: async () => {
        order.push("record");
      },
      fulfillExpectedArtifact: async () => true,
    } as unknown as ArchiveRepository;
    const storage = {
      setLegalHold: async () => {
        order.push("hold");
      },
    } as unknown as ObjectStoragePort;
    const service = createService(repository, storage);
    const object = {
      id: OBJECT_ID,
      consultationId: CONSULTATION,
      objectClass: "pipeline_exchange",
      causalKey: "exchange:1",
      key: "v1/meetings/x/exchange",
      versionId: "v1",
      size: 1,
      sha256: CONFLICTING_HASH,
      s3Checksum: "crc",
      contentType: "application/octet-stream",
      sampleStart: null,
      sampleEnd: null,
      attempt: 1,
      sequence: 0,
      writerEpoch: 2,
    };

    await service.recordObjectAndFulfill(object, EXPECTATION_ID);

    expect(order).toEqual(["hold", "record"]);
  });

  it("rejects a stale deletion write epoch before listing or deleting storage", async () => {
    const storage = {
      listMultipart: mock(),
    } as unknown as ObjectStoragePort;
    const repository = {
      transaction: runTransaction,
      lockByConsultation: async () =>
        lockedArchive({
          state: "deleting",
          writeEpoch: 8,
        }),
    } as unknown as ArchiveRepository;
    const service = createService(repository, storage);

    await expect(service.drainDeletion(CONSULTATION, 7)).rejects.toThrowError(/ARCHIVE_FENCED/);

    expect(storage.listMultipart).not.toHaveBeenCalled();
  });

  it("accepts exact replay of an already completed deletion epoch", async () => {
    const storage = {
      listMultipart: mock(),
    } as unknown as ObjectStoragePort;
    const repository = {
      transaction: runTransaction,
      lockByConsultation: async () =>
        lockedArchive({
          state: "deleted",
          consultationState: "deleted",
          writeEpoch: 7,
          completedDeletionEpoch: 7,
          finalInventoryHash: null,
        }),
    } as unknown as ArchiveRepository;
    const service = createService(repository, storage);

    await expect(service.drainDeletion(CONSULTATION, 7)).resolves.toBe(true);

    expect(storage.listMultipart).not.toHaveBeenCalled();
  });

  it("does not scan storage until every pre-fence writer acknowledges terminal", async () => {
    const storage = {
      listMultipart: mock(),
    } as unknown as ObjectStoragePort;
    const repository = {
      transaction: runTransaction,
      lockByConsultation: async () =>
        lockedArchive({
          state: "deleting",
          writeEpoch: 7,
        }),
      deletionWritersDrained: async () => false,
    } as unknown as ArchiveRepository;
    const service = createService(repository, storage);

    await expect(service.drainDeletion(CONSULTATION, 7)).resolves.toBe(false);

    expect(storage.listMultipart).not.toHaveBeenCalled();
  });
});
