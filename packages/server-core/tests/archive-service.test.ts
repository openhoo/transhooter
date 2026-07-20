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
const OTHER_OBJECT_ID = "00000000-0000-4000-8000-000000000097";
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

function finalizingRepository(): ArchiveRepository {
  return {
    transaction: runTransaction,
    lockByConsultation: async () =>
      lockedArchive({
        state: "reconciling",
        writeEpoch: 0,
        finalInventoryHash: null,
      }),
    finalInventoryHash: async () => null,
    unresolvedExpectations: async () => [],
    inventoryObjects: async () => [],
    completePrerequisites: async () => true,
    recordObject: async () => undefined,
    createFinalInventory: async () => true,
    transition: async () => true,
  } as unknown as ArchiveRepository;
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

  it("rethrows the original PUT error when create-once recovery finds no object", async () => {
    const putError = new Error("temporary object storage failure");
    const storage = {
      putCreateOnce: async () => {
        throw putError;
      },
      head: async () => null,
    } as unknown as ObjectStoragePort;
    const service = createService(finalizingRepository(), storage);

    await expect(service.finalizeInventory(CONSULTATION, finalInventory())).rejects.toBe(putError);
  });

  it("reports an immutable conflict only when recovery finds a different hash", async () => {
    const storage = {
      putCreateOnce: async () => {
        throw new Error("precondition failed");
      },
      head: async () => ({
        versionId: "existing-version",
        size: 1,
        checksum: CONFLICTING_HASH,
        sha256: CONFLICTING_HASH,
      }),
    } as unknown as ObjectStoragePort;
    const service = createService(finalizingRepository(), storage);

    await expect(service.finalizeInventory(CONSULTATION, finalInventory())).rejects.toMatchObject({
      code: "FINAL_INVENTORY_CONFLICT",
    });
  });

  it("accepts an existing create-once object with the expected hash", async () => {
    const verify = mock(async () => true);
    const storage = {
      putCreateOnce: async () => {
        throw new Error("precondition failed");
      },
      head: async () => ({
        versionId: "existing-version",
        size: 1,
        checksum: FINAL_HASH,
        sha256: FINAL_HASH,
      }),
      verify,
    } as unknown as ObjectStoragePort;
    const service = createService(finalizingRepository(), storage);

    await expect(service.finalizeInventory(CONSULTATION, finalInventory())).resolves.toEqual({
      created: true,
      sha256: FINAL_HASH,
    });
    expect(verify).toHaveBeenCalledWith({
      key: `v1/meetings/${CONSULTATION}/inventory/final.json`,
      versionId: "existing-version",
      size: 1,
      checksum: FINAL_HASH,
    });
  });

  it("rejects expired sessions and future reauthentication timestamps before mutation", async () => {
    const transactionCall = mock(runTransaction);
    const service = createService(
      { transaction: transactionCall } as unknown as ArchiveRepository,
      {} as ObjectStoragePort,
    );

    await expect(
      service.addHold(
        CONSULTATION,
        {
          ...reauthenticatedSession(),
          expiresAt: NOW,
        },
        "case",
      ),
    ).rejects.toMatchObject({ code: "REAUTH_REQUIRED" });
    await expect(
      service.addHold(
        CONSULTATION,
        {
          ...reauthenticatedSession(),
          reauthenticatedAt: new Date(NOW.getTime() + 1),
        },
        "case",
      ),
    ).rejects.toMatchObject({ code: "REAUTH_REQUIRED" });

    expect(transactionCall).not.toHaveBeenCalled();
  });

  it("records the deletion actor and required reason in durable admission records", async () => {
    const appended: unknown[] = [];
    const enqueued: unknown[] = [];
    const repository = {
      transaction: runTransaction,
      lockByConsultation: async () => lockedArchive(),
      claimStaleHoldOperation: async () => null,
      activeHolds: async () => [],
      incrementWriteEpoch: async () => 3,
      fenceWritersForDeletion: async () => undefined,
      transition: async () => true,
    } as unknown as ArchiveRepository;
    const service = new ArchiveService(
      repository,
      {} as ObjectStoragePort,
      { append: async (event: unknown) => void appended.push(event) } as never,
      clock,
      ids,
      hash,
      { enqueue: async (effect: unknown) => void enqueued.push(effect) } as never,
    );

    await service.beginDelete(CONSULTATION, reauthenticatedSession(), "  retention elapsed  ");

    expect(appended).toContainEqual(
      expect.objectContaining({
        actorId: ACTOR_ID,
        kind: "archive.deletion_admitted",
        details: { writeEpoch: 3, reason: "retention elapsed" },
      }),
    );
    expect(enqueued).toContainEqual(
      expect.objectContaining({
        payload: expect.objectContaining({
          actorId: ACTOR_ID,
          reason: "retention elapsed",
        }),
      }),
    );
  });

  it("requires two epoch-bound empty deletion scans before deleted", async () => {
    let state = "deleting";
    const scans: number[] = [];
    const results: unknown[] = [];
    let versionListings = 0;
    const repository = {
      transaction: runTransaction,
      lockByConsultation: async () =>
        lockedArchive({
          state,
          writeEpoch: 7,
        }),
      deletionWritersDrained: async () => true,
      recordDeletionScan: async (input: { consecutiveEmpty: number; result: unknown }) => {
        scans.push(input.consecutiveEmpty);
        results.push(input.result);
      },
      completeDeletion: async () => {
        state = "deleted";
        return true;
      },
      recordDeletionFailure: async () => undefined,
    } as unknown as ArchiveRepository;
    const storage = {
      listMultipart: async () => [],
      listMeetingVersions: async () => ({
        versions:
          versionListings++ === 0 ? [{ key: "v1/meetings/x/object", versionId: "version-1" }] : [],
        cursor: null,
      }),
      abortMultipart: async () => undefined,
      deleteVersions: async () => undefined,
    } as unknown as ObjectStoragePort;
    const service = createService(repository, storage);

    await expect(service.drainDeletion(CONSULTATION, 7, "retention expired")).resolves.toBe(true);

    expect(scans).toEqual([1, 2]);
    expect(results[0]).toEqual({
      deletedVersions: 1,
      abortedUploads: 0,
      writersDrained: true,
      reason: "retention expired",
      versions: [
        {
          key: "v1/meetings/x/object",
          versionId: "version-1",
          outcome: "deleted",
          reason: "retention expired",
        },
      ],
    });
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

  it("terminates version pagination when a continuation cursor repeats", async () => {
    const listMeetingVersions = mock(async () => ({
      versions: [],
      cursor: "repeated-cursor",
    }));
    const repository = {
      transaction: runTransaction,
      lockByConsultation: async () => lockedArchive(),
      claimStaleHoldOperation: async () => null,
      beginHoldOperation: async () => true,
      addHold: async () => undefined,
    } as unknown as ArchiveRepository;
    const storage = { listMeetingVersions } as unknown as ObjectStoragePort;
    const service = createService(repository, storage);

    await expect(
      service.addHold(CONSULTATION, reauthenticatedSession(), "case"),
    ).rejects.toMatchObject({
      code: "ARCHIVE_STORAGE_PROTOCOL_ERROR",
      message: "version listing repeated continuation cursor: repeated-cursor",
    });
    expect(listMeetingVersions).toHaveBeenCalledTimes(2);
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
      inventoryObjects: async () => [
        {
          id: OBJECT_ID,
          consultationId: CONSULTATION,
          objectClass: "pipeline_exchange",
          causalKey: "exchange:1",
          key: "a",
          versionId: "1",
          size: 1,
          sha256: FINAL_HASH,
          s3Checksum: "crc",
          contentType: "application/octet-stream",
          sampleStart: null,
          sampleEnd: null,
          attempt: 1,
          sequence: 0,
          writerEpoch: 2,
        },
      ],
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
      verify: async () => true,
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

    await expect(service.drainDeletion(CONSULTATION, 7, "retention expired")).rejects.toThrowError(
      /ARCHIVE_FENCED/,
    );

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

    await expect(service.drainDeletion(CONSULTATION, 7, "retention expired")).resolves.toBe(true);

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

    await expect(service.drainDeletion(CONSULTATION, 7, "retention expired")).resolves.toBe(false);

    expect(storage.listMultipart).not.toHaveBeenCalled();
  });
  it("verifies an exact storage version before recording an ordinary object", async () => {
    const recordObject = mock(async () => undefined);
    const repository = {
      transaction: runTransaction,
      lockByConsultation: async () => lockedArchive({ state: "recording" }),
      activeHolds: async () => [],
      recordObject,
    } as unknown as ArchiveRepository;
    const verify = mock(async () => false);
    const service = createService(repository, { verify } as unknown as ObjectStoragePort);

    await expect(
      service.recordObject(CONSULTATION, 2, "exchange:1", {
        objectId: OBJECT_ID,
        class: "pipeline_exchange",
        key: "v1/meetings/x/exchange",
        versionId: "v1",
        size: 1,
        sha256: CONFLICTING_HASH,
        s3Checksum: "crc",
        contentType: "application/octet-stream",
        sampleRange: null,
        attempt: 1,
        sequence: 0,
      }),
    ).rejects.toThrowError(/ARCHIVE_OBJECT_VERIFICATION_FAILED/);

    expect(verify).toHaveBeenCalledWith({
      key: "v1/meetings/x/exchange",
      versionId: "v1",
      size: 1,
      checksum: "crc",
    });
    expect(recordObject).not.toHaveBeenCalled();
  });

  it("atomically rejects a stale worker reservation before recording", async () => {
    const recordObject = mock(async () => undefined);
    const lockActiveWorkerWriter = mock(async () => false);
    const repository = {
      transaction: runTransaction,
      lockByConsultation: async () => lockedArchive({ state: "recording" }),
      lockActiveWorkerWriter,
      recordObject,
    } as unknown as ArchiveRepository;
    const verify = mock(async () => true);
    const service = createService(repository, { verify } as unknown as ObjectStoragePort);

    await expect(
      service.recordWorkerObject(
        CONSULTATION,
        {
          generation: 3,
          workerId: ACTOR_ID,
          workerEpoch: 4,
        },
        2,
        "exchange:1",
        {
          objectId: OBJECT_ID,
          class: "pipeline_exchange",
          key: "v1/meetings/x/exchange",
          versionId: "v1",
          size: 1,
          sha256: CONFLICTING_HASH,
          s3Checksum: "crc",
          contentType: "application/octet-stream",
          sampleRange: null,
          attempt: 1,
          sequence: 0,
        },
      ),
    ).rejects.toThrowError(/ARCHIVE_WRITER_FENCED/);

    expect(lockActiveWorkerWriter).toHaveBeenCalledWith(
      {
        consultationId: CONSULTATION,
        generation: 3,
        workerId: ACTOR_ID,
        workerEpoch: 4,
        writerEpoch: 2,
      },
      transaction,
    );
    expect(verify).not.toHaveBeenCalled();
    expect(recordObject).not.toHaveBeenCalled();
  });

  it("stably rescans and releases a version created during final-hold release", async () => {
    let scans = 0;
    const removeHold = mock(async () => undefined);
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
      ],
      claimStaleHoldOperation: async () => null,
      beginHoldOperation: async () => true,
      renewHoldOperation: async () => true,
      completeHoldOperation: async () => true,
      transitionHoldState: async () => true,
      recordHoldResults: async () => undefined,
      inventoryObjects: async () => [
        {
          id: OBJECT_ID,
          consultationId: CONSULTATION,
          objectClass: "pipeline_exchange",
          causalKey: "exchange:1",
          key: "a",
          versionId: "1",
          size: 1,
          sha256: FINAL_HASH,
          s3Checksum: "crc",
          contentType: "application/octet-stream",
          sampleStart: null,
          sampleEnd: null,
          attempt: 1,
          sequence: 0,
          writerEpoch: 2,
        },
        {
          id: OTHER_OBJECT_ID,
          consultationId: CONSULTATION,
          objectClass: "pipeline_exchange",
          causalKey: "exchange:2",
          key: "b",
          versionId: "2",
          size: 1,
          sha256: FINAL_HASH,
          s3Checksum: "crc",
          contentType: "application/octet-stream",
          sampleStart: null,
          sampleEnd: null,
          attempt: 1,
          sequence: 1,
          writerEpoch: 2,
        },
      ],
      removeHold,
    } as unknown as ArchiveRepository;
    const setLegalHold = mock(async () => undefined);
    const storage = {
      listMeetingVersions: async () => {
        scans += 1;
        return {
          versions:
            scans === 1
              ? [{ key: "a", versionId: "1" }]
              : [
                  { key: "a", versionId: "1" },
                  { key: "b", versionId: "2" },
                ],
          cursor: null,
        };
      },
      setLegalHold,
    } as unknown as ObjectStoragePort;
    const service = createService(repository, storage);

    await service.releaseHold(CONSULTATION, HOLD_ID, reauthenticatedSession());

    expect(scans).toBe(3);
    expect(setLegalHold).toHaveBeenCalledWith("a", "1", false);
    expect(setLegalHold).toHaveBeenCalledWith("b", "2", false);
    expect(removeHold).toHaveBeenCalledTimes(1);
  });

  it("revalidates recorded exact versions under the archive lock before dropping the final hold", async () => {
    const order: string[] = [];
    let inventoryReads = 0;
    let lateObjectCommitted = false;
    const removeHold = mock(async () => {
      order.push("hold-removed");
    });
    const completeHoldOperation = mock(async () => true);
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
      ],
      claimStaleHoldOperation: async () => null,
      beginHoldOperation: async () => true,
      renewHoldOperation: async () => true,
      completeHoldOperation,
      transitionHoldState: async () => true,
      recordHoldResults: async () => undefined,
      inventoryObjects: async () => {
        inventoryReads += 1;
        order.push("recorded-set-revalidated");
        const objects = [
          {
            id: OBJECT_ID,
            consultationId: CONSULTATION,
            objectClass: "pipeline_exchange",
            causalKey: "exchange:1",
            key: "a",
            versionId: "1",
            size: 1,
            sha256: FINAL_HASH,
            s3Checksum: "crc",
            contentType: "application/octet-stream",
            sampleStart: null,
            sampleEnd: null,
            attempt: 1,
            sequence: 0,
            writerEpoch: 2,
          },
          {
            id: OTHER_OBJECT_ID,
            consultationId: CONSULTATION,
            objectClass: "inventory_supplement",
            causalKey: "inventory:supplement:late",
            key: "late-supplement",
            versionId: "late-version",
            size: 1,
            sha256: FINAL_HASH,
            s3Checksum: "crc",
            contentType: "application/json",
            sampleStart: null,
            sampleEnd: null,
            attempt: null,
            sequence: null,
            writerEpoch: 2,
          },
        ];
        return lateObjectCommitted ? objects : objects.slice(0, 1);
      },
      removeHold,
    } as unknown as ArchiveRepository;
    let scans = 0;
    const storage = {
      listMeetingVersions: async () => {
        scans += 1;
        if (scans === 2) {
          lateObjectCommitted = true;
          order.push("late-object-committed");
        }
        return {
          versions: [{ key: "a", versionId: "1" }],
          cursor: null,
        };
      },
      setLegalHold: async (key: string, versionId: string, enabled: boolean) => {
        order.push(`${enabled ? "protect" : "release"}:${key}:${versionId}`);
      },
    } as unknown as ObjectStoragePort;
    const service = createService(repository, storage);

    await service.releaseHold(CONSULTATION, HOLD_ID, reauthenticatedSession());

    expect(scans).toBe(2);
    expect(inventoryReads).toBe(2);
    expect(completeHoldOperation).toHaveBeenCalledTimes(1);
    expect(order).toEqual([
      "release:a:1",
      "late-object-committed",
      "recorded-set-revalidated",
      "release:late-supplement:late-version",
      "recorded-set-revalidated",
      "hold-removed",
    ]);
  });

  it("treats an identical concurrent final inventory commit as a hash replay", async () => {
    let locks = 0;
    let hashReads = 0;
    const recordObject = mock(async () => undefined);
    const repository = {
      transaction: runTransaction,
      lockByConsultation: async () => {
        locks += 1;
        return lockedArchive({
          state: locks === 1 ? "reconciling" : "complete",
          finalInventoryHash: locks === 1 ? null : FINAL_HASH,
          writeEpoch: 0,
        });
      },
      finalInventoryHash: async () => {
        hashReads += 1;
        return hashReads === 1 ? null : FINAL_HASH;
      },
      unresolvedExpectations: async () => [],
      inventoryObjects: async () => [],
      completePrerequisites: async () => true,
      recordObject,
    } as unknown as ArchiveRepository;
    const storage = {
      putCreateOnce: async () => ({ versionId: "v1", size: 1, checksum: FINAL_HASH }),
      verify: async () => true,
    } as unknown as ObjectStoragePort;
    const service = createService(repository, storage);

    await expect(service.finalizeInventory(CONSULTATION, finalInventory())).resolves.toEqual({
      created: false,
      sha256: FINAL_HASH,
    });
    expect(recordObject).not.toHaveBeenCalled();
  });
  it("rejects duplicate claimed IDs and key-version pairs instead of length-only matching", async () => {
    const claimed = {
      objectId: OBJECT_ID,
      class: "checkpoint" as const,
      key: `v1/meetings/${CONSULTATION}/inventory/checkpoint`,
      versionId: "v1",
      size: 1,
      sha256: CONFLICTING_HASH,
      s3Checksum: "crc",
      contentType: "application/json",
      sampleRange: null,
      attempt: null,
      sequence: null,
    };
    const persisted = [
      {
        id: OBJECT_ID,
        consultationId: CONSULTATION,
        objectClass: "checkpoint",
        causalKey: "checkpoint:1",
        key: claimed.key,
        versionId: claimed.versionId,
        size: claimed.size,
        sha256: claimed.sha256,
        s3Checksum: claimed.s3Checksum,
        contentType: claimed.contentType,
        sampleStart: null,
        sampleEnd: null,
        attempt: null,
        sequence: null,
        writerEpoch: 2,
      },
      {
        id: OTHER_OBJECT_ID,
        consultationId: CONSULTATION,
        objectClass: "checkpoint",
        causalKey: "checkpoint:2",
        key: `v1/meetings/${CONSULTATION}/inventory/checkpoint-2`,
        versionId: "v2",
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
    ];
    const putCreateOnce = mock();
    const repository = {
      transaction: runTransaction,
      lockByConsultation: async () =>
        lockedArchive({ state: "reconciling", finalInventoryHash: null }),
      finalInventoryHash: async () => null,
      unresolvedExpectations: async () => [],
      inventoryObjects: async () => persisted,
      completePrerequisites: async () => true,
    } as unknown as ArchiveRepository;
    const service = createService(repository, { putCreateOnce } as unknown as ObjectStoragePort);
    for (const objects of [
      [
        claimed,
        {
          ...claimed,
          key: `v1/meetings/${CONSULTATION}/inventory/checkpoint-2`,
          versionId: "v2",
        },
      ],
      [claimed, { ...claimed, objectId: OTHER_OBJECT_ID }],
    ]) {
      await expect(
        service.finalizeInventory(CONSULTATION, {
          ...finalInventory(),
          objects,
        }),
      ).rejects.toThrowError(/INVENTORY_OBJECT_MISMATCH/);
    }
    expect(putCreateOnce).not.toHaveBeenCalled();
  });
  it("rejects a false content-type claim even when every other object field matches", async () => {
    const claimed = {
      objectId: OBJECT_ID,
      class: "checkpoint" as const,
      key: `v1/meetings/${CONSULTATION}/inventory/checkpoint`,
      versionId: "v1",
      size: 1,
      sha256: CONFLICTING_HASH,
      s3Checksum: "crc",
      contentType: "application/json",
      sampleRange: null,
      attempt: null,
      sequence: null,
    };
    const putCreateOnce = mock();
    const repository = {
      transaction: runTransaction,
      lockByConsultation: async () =>
        lockedArchive({ state: "reconciling", finalInventoryHash: null }),
      finalInventoryHash: async () => null,
      unresolvedExpectations: async () => [],
      inventoryObjects: async () => [
        {
          id: OBJECT_ID,
          consultationId: CONSULTATION,
          objectClass: claimed.class,
          causalKey: "checkpoint:1",
          key: claimed.key,
          versionId: claimed.versionId,
          size: claimed.size,
          sha256: claimed.sha256,
          s3Checksum: claimed.s3Checksum,
          contentType: "application/octet-stream",
          sampleStart: null,
          sampleEnd: null,
          attempt: null,
          sequence: null,
          writerEpoch: 2,
        },
      ],
      completePrerequisites: async () => true,
    } as unknown as ArchiveRepository;
    const service = createService(repository, { putCreateOnce } as unknown as ObjectStoragePort);

    await expect(
      service.finalizeInventory(CONSULTATION, {
        ...finalInventory(),
        objects: [claimed],
      }),
    ).rejects.toMatchObject({ code: "INVENTORY_OBJECT_MISMATCH" });
    expect(putCreateOnce).not.toHaveBeenCalled();
  });

  it("rejects spool-drainer writes after archive finalization", async () => {
    const verify = mock(async () => true);
    const recordObject = mock(async () => undefined);
    const service = createService(
      {
        transaction: runTransaction,
        lockByConsultation: async () => lockedArchive({ state: "complete" }),
        recordObject,
      } as unknown as ArchiveRepository,
      { verify } as unknown as ObjectStoragePort,
    );

    await expect(
      service.recordDrainerObject(CONSULTATION, "exchange:late", {
        objectId: OBJECT_ID,
        class: "pipeline_exchange",
        key: "v1/meetings/x/late",
        versionId: "v1",
        size: 1,
        sha256: CONFLICTING_HASH,
        s3Checksum: "crc",
        contentType: "application/octet-stream",
        sampleRange: null,
        attempt: 1,
        sequence: 0,
      }),
    ).rejects.toThrowError(/ARCHIVE_WRITER_FENCED/);
    expect(verify).not.toHaveBeenCalled();
    expect(recordObject).not.toHaveBeenCalled();
  });
});
