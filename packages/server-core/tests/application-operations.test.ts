import { describe, expect, it, mock } from "bun:test";
import {
  checkpointPersistenceValues,
  PrismaApplicationOperations,
} from "../src/application-operations";
import { PrismaLanguageRepository } from "../src/persistence/application-repositories";
import { Prisma, type PrismaClient } from "../src/persistence/database";
import { TransactionHandle } from "../src/persistence/repositories";

const ADMIN = {
  userId: "00000000-0000-4000-8000-000000000001",
  role: "admin" as const,
};
const PROFILE_FRESH_UNTIL = new Date("2026-02-01T00:00:00Z");
function inspectSql(statement: unknown): { text: string; values: readonly unknown[] } {
  expect(statement).toBeInstanceOf(Prisma.Sql);
  const sql = statement as Prisma.Sql;
  return {
    text: sql.strings.join("?").replace(/\s+/g, " ").trim(),
    values: sql.values,
  };
}

const ENABLED_DIRECTION_ROW = {
  id: "00000000-0000-4000-8000-000000000002",
  profile_id: "00000000-0000-4000-8000-000000000010",
  source_locale: "en-US",
  target_locale: "de-DE",
  mode: "translated",
  snapshot: { stt: {} },
  profile_name: "google-eu",
  revision: 4,
  fresh_until: PROFILE_FRESH_UNTIL,
  enabled: true,
};

const DISABLED_DIRECTION_ROW = {
  id: "00000000-0000-4000-8000-000000000003",
  profile_id: "00000000-0000-4000-8000-000000000010",
  source_locale: "de-DE",
  target_locale: "en-US",
  mode: "translated",
  snapshot: { stt: {} },
  profile_name: "google-eu",
  revision: 4,
  fresh_until: PROFILE_FRESH_UNTIL,
  enabled: false,
};

const PROVIDER_SELECTION = {
  profileId: "google-eu",
  profileRevision: 4,
  capabilityHash: "a".repeat(64),
  participantIds: ["00000000-0000-4000-8000-000000000011", "00000000-0000-4000-8000-000000000012"],
  directions: [
    {
      mode: "translated",
      sourceParticipantId: "00000000-0000-4000-8000-000000000011",
      destinationParticipantId: "00000000-0000-4000-8000-000000000012",
      capabilityRowId: "00000000-0000-4000-8000-000000000013",
      stt: {
        provider: "google",
        endpoint: "https://eu-speech.googleapis.com",
        region: "eu",
        model: "chirp",
        adapterBuild: "adapter-1",
        policy: "policy-1",
        credential: { reference: "google-adc", version: "7" },
        limits: {},
        locale: "en-US",
        encoding: "LINEAR16",
      },
      targetCode: "DE",
      translation: {
        provider: "google",
        endpoint: "https://translate-eu.googleapis.com",
        region: "eu",
        model: "general/nmt",
        adapterBuild: "adapter-1",
        policy: "policy-1",
        credential: { reference: "google-adc", version: "7" },
        limits: {},
        sourceCode: "EN",
        targetCode: "DE",
      },
      tts: {
        provider: "google",
        endpoint: "https://eu-texttospeech.googleapis.com",
        region: "eu",
        model: "chirp-3",
        adapterBuild: "adapter-1",
        policy: "policy-1",
        credential: { reference: "google-adc", version: "7" },
        limits: {},
        locale: "de-DE",
        voice: "de-DE-Chirp3-HD-Aoede",
        encoding: "LINEAR16",
        sampleRate: 48_000,
      },
    },
    {
      mode: "same_language",
      sourceParticipantId: "00000000-0000-4000-8000-000000000012",
      destinationParticipantId: "00000000-0000-4000-8000-000000000011",
      capabilityRowId: "00000000-0000-4000-8000-000000000014",
      stt: {
        provider: "google",
        endpoint: "https://eu-speech.googleapis.com",
        region: "eu",
        model: "chirp",
        adapterBuild: "adapter-1",
        policy: "policy-1",
        credential: { reference: "google-adc", version: "7" },
        limits: {},
        locale: "de-DE",
        encoding: "LINEAR16",
      },
      bypass: true,
    },
  ],
};

const PROVIDER_REPORT = {
  directionId: "00000000-0000-4000-8000-000000000013",
  stage: "translation" as const,
  terminalId: "00000000-0000-4000-8000-000000000015",
  operationId: "00000000-0000-4000-8000-000000000016",
  attemptId: "00000000-0000-4000-8000-000000000017",
  attemptNumber: 1,
  retryOfAttemptId: null,
  outcome: "succeeded" as const,
  error: null,
  retryDecision: {
    action: "do_not_retry" as const,
    reason: "accepted",
    retryAtMs: null,
    previousAttemptId: null,
  },
  watermarks: {
    acceptedInputSequence: 1,
    acceptedInputSampleEnd: 4_000,
    receivedOutputSequence: 1,
    receivedOutputSampleEnd: null,
    emittedOutputSequence: null,
    emittedOutputSampleEnd: null,
  },
  credentialVersion: "7",
  credentialFingerprint: "opaque-fingerprint",
  transport: "http" as const,
  rawReferences: [
    {
      objectId: "00000000-0000-4000-8000-000000000018",
      ordinal: 0,
      sha256: "b".repeat(64),
      size: 120,
      mediaType: "application/json",
    },
  ],
  terminalHash: "c".repeat(64),
  startedAtMs: 1_700_000_000_000,
  occurredAtMs: 1_700_000_000_100,
};

const PROVIDER_CONTEXT_ROW = {
  selection: PROVIDER_SELECTION,
  profile_id: "00000000-0000-4000-8000-000000000019",
  profile_revision: 4,
  profile_name: "google-eu",
  archive_id: "00000000-0000-4000-8000-000000000020",
  capability_version: "translation-v3",
};

const ARCHIVE_DETAIL_ROW = {
  id: "00000000-0000-4000-8000-000000000004",
  consultation_id: "00000000-0000-4000-8000-000000000005",
  state: "complete",
  inventory: { status: "complete" },
  sha256: "inventory-hash",
  inventory_version_id: "version-1",
  active_holds: [
    {
      id: "00000000-0000-4000-8000-000000000006",
      reason: "litigation",
    },
  ],
  egress_ids: ["EG_room_g1", "EG_participant_g1", "EG_room_g2"],
  provider_attempt_ids: ["00000000-0000-4000-8000-000000000007"],
  provider_attempt_groups: [
    {
      stage: "stt",
      provider: "google",
      direction: "00000000-0000-4000-8000-000000000008",
      attemptIds: ["00000000-0000-4000-8000-000000000007"],
    },
  ],
};

function prismaRawQueryError(originalCode: string): Prisma.PrismaClientKnownRequestError {
  return new Prisma.PrismaClientKnownRequestError(`Raw query failed. Code: \`${originalCode}\`.`, {
    code: "P2010",
    clientVersion: "7.6.0",
    meta: {
      driverAdapterError: {
        name: "DriverAdapterError",
        cause: { originalCode },
      },
    },
  });
}

function createOperations(rows: unknown[]) {
  const queryRaw = mock(async (_statement: Prisma.Sql) => rows);
  const operations = new PrismaApplicationOperations(
    { $queryRaw: queryRaw } as unknown as PrismaClient,
    {} as never,
    { now: () => new Date() },
  );
  return { queryRaw, operations };
}

function createSequentialOperations(results: Array<Record<string, unknown>[]>) {
  const queryRaw = mock(async (statement: Prisma.Sql) => {
    const query = inspectSql(statement).text;
    if (query.startsWith("SELECT id FROM consultations") && query.endsWith("FOR UPDATE")) {
      return [{ id: "00000000-0000-4000-8000-000000000021" }];
    }
    if (
      query.startsWith("SELECT consultation_id FROM worker_job_epochs") &&
      query.endsWith("FOR UPDATE")
    ) {
      return [{ consultation_id: "00000000-0000-4000-8000-000000000021" }];
    }
    return results.shift() ?? [];
  });
  const executeRaw = mock(async (_statement: Prisma.Sql) => 0);
  const transaction = async <T>(callback: (transaction: Prisma.TransactionClient) => Promise<T>) =>
    callback({
      $queryRaw: queryRaw,
      $executeRaw: executeRaw,
    } as unknown as Prisma.TransactionClient);
  const operations = new PrismaApplicationOperations(
    {
      $queryRaw: queryRaw,
      $executeRaw: executeRaw,
      $transaction: transaction,
    } as unknown as PrismaClient,
    {} as never,
    { now: () => new Date() },
  );
  return { queryRaw, executeRaw, operations };
}

describe("ApplicationOperations typed views", () => {
  it("returns enabled and disabled current profile directions", async () => {
    const { operations } = createOperations([ENABLED_DIRECTION_ROW, DISABLED_DIRECTION_ROW]);

    const rows = await operations.adminLanguages(ADMIN, "google-eu");

    expect(rows.map((row) => row.enabled)).toEqual([true, false]);
    expect(rows.every((row) => row.revision === 4)).toBe(true);
    expect(rows.every((row) => row.profileName === "google-eu")).toBe(true);
    expect(rows.every((row) => row.profileId === "00000000-0000-4000-8000-000000000010")).toBe(
      true,
    );
  });

  it("accepts heartbeats only for an active unfenced reservation and job epoch", async () => {
    const queryRaw = mock(async (_statement: Prisma.Sql) => []);
    const operations = new PrismaApplicationOperations(
      { $queryRaw: queryRaw } as unknown as PrismaClient,
      {} as never,
      { now: () => new Date("2026-01-01T00:00:00Z") },
    );

    await expect(
      operations.heartbeat(
        "00000000-0000-4000-8000-000000000021",
        3,
        "00000000-0000-4000-8000-000000000022",
        2,
      ),
    ).resolves.toBe(false);

    const heartbeatSql = inspectSql(queryRaw.mock.calls[0]?.[0]).text;
    expect(heartbeatSql).toContain("consultation.state IN ('ready','active')");
    expect(heartbeatSql).toContain("reservation.fenced_at IS NULL");
    expect(heartbeatSql).toContain("reservation.released_at IS NULL");
    expect(heartbeatSql).toContain("job.fenced_at IS NULL");
    expect(heartbeatSql).toContain("job.terminal_at IS NULL");
    expect(heartbeatSql).toContain("FOR UPDATE OF consultation,reservation,job");
  });

  it("returns non-null proof arrays and scoped active hold details", async () => {
    const { operations } = createOperations([ARCHIVE_DETAIL_ROW]);

    const detail = await operations.archiveGet(ADMIN, "00000000-0000-4000-8000-000000000004");

    expect(detail.activeHolds).toEqual([
      {
        id: "00000000-0000-4000-8000-000000000006",
        reason: "litigation",
      },
    ]);
    expect(detail.inventoryVersionId).toBe("version-1");
    expect(detail.egressIds).toEqual(["EG_room_g1", "EG_participant_g1", "EG_room_g2"]);
    expect(detail.providerAttemptGroups[0]?.attemptIds).toEqual(detail.providerAttemptIds);
  });

  it("lists archive objects only for finalized archives", async () => {
    const { queryRaw, operations } = createOperations([] as unknown[]);

    await operations.archiveObjects(ADMIN, "00000000-0000-4000-8000-000000000004", null, 50);

    const query = inspectSql(queryRaw.mock.calls[0]?.[0]).text;
    expect(query).toContain("a.state IN ('complete','incomplete')");
    expect(query).toContain("JOIN final_inventories f ON f.archive_id=a.id");
  });

  it("persists a fenced immutable provider terminal and accepts only an exact replay", async () => {
    const input = {
      consultationId: "00000000-0000-4000-8000-000000000021",
      generation: 3,
      workerId: "00000000-0000-4000-8000-000000000022",
      epoch: 2,
      eventId: "00000000-0000-4000-8000-000000000023",
      report: PROVIDER_REPORT,
    };
    const inserted = createSequentialOperations([
      [PROVIDER_CONTEXT_ROW],
      [{ id: PROVIDER_REPORT.attemptId }],
    ]);
    await expect(inserted.operations.providerAttempt(input)).resolves.toBe(true);
    expect(inserted.queryRaw).toHaveBeenCalledTimes(2);

    const replayed = createSequentialOperations([[PROVIDER_CONTEXT_ROW], [], [{ "?column?": 1 }]]);
    await expect(replayed.operations.providerAttempt(input)).resolves.toBe(true);

    const conflict = createSequentialOperations([[PROVIDER_CONTEXT_ROW], [], []]);
    await expect(conflict.operations.providerAttempt(input)).rejects.toThrow(
      "PROVIDER_ATTEMPT_CONFLICT",
    );
  });
  it("retries a provider terminal after a Prisma P2010 PostgreSQL deadlock", async () => {
    let calls = 0;
    const queryRaw = mock(async () => {
      calls += 1;
      if (calls === 1) {
        return [PROVIDER_CONTEXT_ROW];
      }
      if (calls === 2) {
        throw prismaRawQueryError("40P01");
      }
      return [{ id: PROVIDER_REPORT.attemptId }];
    });
    const operations = new PrismaApplicationOperations(
      { $queryRaw: queryRaw } as unknown as PrismaClient,
      {} as never,
      { now: () => new Date() },
    );

    await expect(
      operations.providerAttempt({
        consultationId: "00000000-0000-4000-8000-000000000021",
        generation: 3,
        workerId: "00000000-0000-4000-8000-000000000022",
        epoch: 2,
        eventId: "00000000-0000-4000-8000-000000000023",
        report: PROVIDER_REPORT,
      }),
    ).resolves.toBe(true);
    expect(queryRaw).toHaveBeenCalledTimes(3);
  });

  it("retries a provider terminal after a Prisma P2010 serialization failure", async () => {
    let calls = 0;
    const queryRaw = mock(async () => {
      calls += 1;
      if (calls === 1) {
        return [PROVIDER_CONTEXT_ROW];
      }
      if (calls === 2) {
        throw prismaRawQueryError("40001");
      }
      return [{ id: PROVIDER_REPORT.attemptId }];
    });
    const operations = new PrismaApplicationOperations(
      { $queryRaw: queryRaw } as unknown as PrismaClient,
      {} as never,
      { now: () => new Date() },
    );

    await expect(
      operations.providerAttempt({
        consultationId: "00000000-0000-4000-8000-000000000021",
        generation: 3,
        workerId: "00000000-0000-4000-8000-000000000022",
        epoch: 2,
        eventId: "00000000-0000-4000-8000-000000000023",
        report: PROVIDER_REPORT,
      }),
    ).resolves.toBe(true);
    expect(queryRaw).toHaveBeenCalledTimes(3);
  });

  it("retains direct PostgreSQL driver-code retry compatibility", async () => {
    let calls = 0;
    const queryRaw = mock(async () => {
      calls += 1;
      if (calls === 1) {
        return [PROVIDER_CONTEXT_ROW];
      }
      if (calls === 2) {
        throw Object.assign(new Error("driver serialization failure"), { code: "40001" });
      }
      return [{ id: PROVIDER_REPORT.attemptId }];
    });
    const operations = new PrismaApplicationOperations(
      { $queryRaw: queryRaw } as unknown as PrismaClient,
      {} as never,
      { now: () => new Date() },
    );

    await expect(
      operations.providerAttempt({
        consultationId: "00000000-0000-4000-8000-000000000021",
        generation: 3,
        workerId: "00000000-0000-4000-8000-000000000022",
        epoch: 2,
        eventId: "00000000-0000-4000-8000-000000000023",
        report: PROVIDER_REPORT,
      }),
    ).resolves.toBe(true);
    expect(queryRaw).toHaveBeenCalledTimes(3);
  });
  it("does not retry a Prisma P2010 raw-query error with another SQLSTATE", async () => {
    const error = prismaRawQueryError("23505");
    let calls = 0;
    const queryRaw = mock(async () => {
      calls += 1;
      if (calls === 1) {
        return [PROVIDER_CONTEXT_ROW];
      }
      throw error;
    });
    const operations = new PrismaApplicationOperations(
      { $queryRaw: queryRaw } as unknown as PrismaClient,
      {} as never,
      { now: () => new Date() },
    );

    await expect(
      operations.providerAttempt({
        consultationId: "00000000-0000-4000-8000-000000000021",
        generation: 3,
        workerId: "00000000-0000-4000-8000-000000000022",
        epoch: 2,
        eventId: "00000000-0000-4000-8000-000000000023",
        report: PROVIDER_REPORT,
      }),
    ).rejects.toBe(error);
    expect(queryRaw).toHaveBeenCalledTimes(2);
  });

  it("rejects provider evidence from a fenced worker or an unselected stage", async () => {
    const fenced = createSequentialOperations([[]]);
    await expect(
      fenced.operations.providerAttempt({
        consultationId: "00000000-0000-4000-8000-000000000021",
        generation: 3,
        workerId: "00000000-0000-4000-8000-000000000022",
        epoch: 2,
        eventId: "00000000-0000-4000-8000-000000000023",
        report: PROVIDER_REPORT,
      }),
    ).rejects.toThrow("PROVIDER_ATTEMPT_FENCED");

    const bypass = createSequentialOperations([[PROVIDER_CONTEXT_ROW]]);
    await expect(
      bypass.operations.providerAttempt({
        consultationId: "00000000-0000-4000-8000-000000000021",
        generation: 3,
        workerId: "00000000-0000-4000-8000-000000000022",
        epoch: 2,
        eventId: "00000000-0000-4000-8000-000000000023",
        report: {
          ...PROVIDER_REPORT,
          directionId: "00000000-0000-4000-8000-000000000014",
          stage: "tts",
        },
      }),
    ).rejects.toThrow("PROVIDER_STAGE_MISMATCH");
  });
  it("persists checkpoint sample watermark and direction independently of wall time", () => {
    const persisted = checkpointPersistenceValues({
      checkpointId: "00000000-0000-4000-8000-000000000031",
      workerEpoch: 2,
      sourceParticipantId: "00000000-0000-4000-8000-000000000011",
      destinationParticipantId: "00000000-0000-4000-8000-000000000012",
      acceptedInputSequence: 8,
      acceptedInput: 32_000,
      receivedOutput: 24_000,
      emittedOutput: 20_000,
      previousCheckpointSha256: null,
      highWatermarkSha256: "d".repeat(64),
      expectedObjectIds: [],
      observedObjectIds: [],
      gaps: [],
      terminal: false,
      occurredAtMs: 1_700_000_000_000,
    });

    expect(persisted).toEqual({
      acceptedInputSequence: 8,
      acceptedInput: 32_000,
      receivedOutput: 24_000,
      emittedOutput: 20_000,
      sourceParticipantId: "00000000-0000-4000-8000-000000000011",
      destinationParticipantId: "00000000-0000-4000-8000-000000000012",
      createdAt: new Date(1_700_000_000_000),
    });
  });

  it("continues an output-only chain from its unique structural head when input watermarks tie", async () => {
    const outputOnly = createSequentialOperations([
      [{ id: "00000000-0000-4000-8000-000000000032" }],
    ]);

    await expect(
      outputOnly.operations.checkpoint({
        workerId: "00000000-0000-4000-8000-000000000022",
        consultationId: "00000000-0000-4000-8000-000000000021",
        generation: 3,
        writeEpoch: 1,
        objectKey: "v1/output-only-checkpoint.json",
        checkpoint: {
          checkpointId: "00000000-0000-4000-8000-000000000032",
          workerEpoch: 2,
          sourceParticipantId: "00000000-0000-4000-8000-000000000011",
          destinationParticipantId: "00000000-0000-4000-8000-000000000012",
          acceptedInputSequence: 8,
          acceptedInput: 32_000,
          receivedOutput: 28_000,
          emittedOutput: 24_000,
          previousCheckpointSha256: "d".repeat(64),
          highWatermarkSha256: "e".repeat(64),
          expectedObjectIds: [],
          observedObjectIds: [],
          gaps: [],
          terminal: false,
          occurredAtMs: 1_700_000_000_000,
        },
      }),
    ).resolves.toBe(true);

    const insertStatement = outputOnly.queryRaw.mock.calls.find(([statement]) =>
      inspectSql(statement).text.includes("INSERT INTO worker_checkpoints"),
    )?.[0];
    const insertSql = inspectSql(insertStatement).text;
    expect(insertSql).toContain("WITH chain_heads AS");
    expect(insertSql).toContain("child.previous_hash=head.checkpoint_hash");
    expect(insertSql).toContain("(SELECT count(*) FROM chain_heads) <= 1");
    expect(insertSql).toContain("SELECT max(chain_heads.checkpoint_hash) FROM chain_heads");
    expect(insertSql).not.toContain("ORDER BY head.accepted_input_sequence");
  });

  it("rejects a second child that no longer names the unique chain head", async () => {
    const fork = createSequentialOperations([[], []]);
    await expect(
      fork.operations.checkpoint({
        workerId: "00000000-0000-4000-8000-000000000022",
        consultationId: "00000000-0000-4000-8000-000000000021",
        generation: 3,
        writeEpoch: 1,
        objectKey: "v1/forked-checkpoint.json",
        checkpoint: {
          checkpointId: "00000000-0000-4000-8000-000000000033",
          workerEpoch: 2,
          sourceParticipantId: "00000000-0000-4000-8000-000000000011",
          destinationParticipantId: "00000000-0000-4000-8000-000000000012",
          acceptedInputSequence: 8,
          acceptedInput: 32_000,
          receivedOutput: 28_000,
          emittedOutput: 24_000,
          previousCheckpointSha256: "d".repeat(64),
          highWatermarkSha256: "f".repeat(64),
          expectedObjectIds: [],
          observedObjectIds: [],
          gaps: [],
          terminal: false,
          occurredAtMs: 1_700_000_000_000,
        },
      }),
    ).rejects.toThrow("CHECKPOINT_CONFLICT");
  });

  it("accepts terminal checkpoints in either direction and settles only after both exist", async () => {
    const terminalCheckpoint = {
      checkpointId: "00000000-0000-4000-8000-000000000031",
      workerEpoch: 2,
      sourceParticipantId: "00000000-0000-4000-8000-000000000011",
      destinationParticipantId: "00000000-0000-4000-8000-000000000012",
      acceptedInputSequence: 8,
      acceptedInput: 32_000,
      receivedOutput: 24_000,
      emittedOutput: 20_000,
      previousCheckpointSha256: null,
      highWatermarkSha256: "d".repeat(64),
      expectedObjectIds: [],
      observedObjectIds: [],
      gaps: [],
      terminal: true,
      occurredAtMs: 1_700_000_000_000,
    };
    const input = {
      workerId: "00000000-0000-4000-8000-000000000022",
      consultationId: "00000000-0000-4000-8000-000000000021",
      generation: 3,
      writeEpoch: 1,
      objectKey: "v1/checkpoint.json",
      checkpoint: terminalCheckpoint,
    };

    const firstDirection = createSequentialOperations([[{ id: terminalCheckpoint.checkpointId }]]);
    await expect(firstDirection.operations.checkpoint(input)).resolves.toBe(true);
    expect(firstDirection.queryRaw).toHaveBeenCalledTimes(3);
    expect(firstDirection.executeRaw).toHaveBeenCalledTimes(1);

    const reverseDirection = createSequentialOperations([
      [{ id: "00000000-0000-4000-8000-000000000032" }],
    ]);
    await expect(
      reverseDirection.operations.checkpoint({
        ...input,
        checkpoint: {
          ...terminalCheckpoint,
          checkpointId: "00000000-0000-4000-8000-000000000032",
          sourceParticipantId: terminalCheckpoint.destinationParticipantId,
          destinationParticipantId: terminalCheckpoint.sourceParticipantId,
          highWatermarkSha256: "e".repeat(64),
        },
      }),
    ).resolves.toBe(true);
    expect(reverseDirection.queryRaw).toHaveBeenCalledTimes(3);
    expect(reverseDirection.executeRaw).toHaveBeenCalledTimes(1);
  });
  it("serializes concurrent directional appends and rejects a stale committed head", async () => {
    let tail = Promise.resolve();
    let insertAttempt = 0;
    let activeLocks = 0;
    let maximumActiveLocks = 0;
    const database = {
      $transaction: async <T>(callback: (transaction: Prisma.TransactionClient) => Promise<T>) => {
        let release: (() => void) | undefined;
        let transactionLocked = false;
        const queryRaw = async (statement: Prisma.Sql): Promise<Record<string, unknown>[]> => {
          const query = inspectSql(statement).text;
          if (query.startsWith("SELECT id FROM consultations") && query.endsWith("FOR UPDATE")) {
            if (!transactionLocked) {
              const predecessor = tail;
              tail = new Promise<void>((resolve) => {
                release = resolve;
              });
              await predecessor;
              activeLocks += 1;
              maximumActiveLocks = Math.max(maximumActiveLocks, activeLocks);
              transactionLocked = true;
            }
            return [{ id: "00000000-0000-4000-8000-000000000021" }];
          }
          if (query.startsWith("SELECT consultation_id FROM worker_job_epochs")) {
            return [{ consultation_id: "00000000-0000-4000-8000-000000000021" }];
          }
          if (query.includes("INSERT INTO worker_checkpoints")) {
            insertAttempt += 1;
            if (insertAttempt === 1) {
              await Promise.resolve();
              return [{ id: "00000000-0000-4000-8000-000000000031" }];
            }
          }
          return [];
        };
        try {
          return await callback({ $queryRaw: queryRaw } as unknown as Prisma.TransactionClient);
        } finally {
          if (release !== undefined) {
            activeLocks -= 1;
            release();
          }
        }
      },
    };
    const operations = new PrismaApplicationOperations(
      database as unknown as PrismaClient,
      {} as never,
      { now: () => new Date() },
    );
    const base = {
      workerId: "00000000-0000-4000-8000-000000000022",
      consultationId: "00000000-0000-4000-8000-000000000021",
      generation: 3,
      writeEpoch: 1,
      objectKey: "v1/checkpoint.json",
      checkpoint: {
        checkpointId: "00000000-0000-4000-8000-000000000031",
        workerEpoch: 2,
        sourceParticipantId: "00000000-0000-4000-8000-000000000011",
        destinationParticipantId: "00000000-0000-4000-8000-000000000012",
        acceptedInputSequence: 8,
        acceptedInput: 32_000,
        receivedOutput: 24_000,
        emittedOutput: 20_000,
        previousCheckpointSha256: null,
        highWatermarkSha256: "d".repeat(64),
        expectedObjectIds: [],
        observedObjectIds: [],
        gaps: [],
        terminal: false,
        occurredAtMs: 1_700_000_000_000,
      },
    };

    const settled = await Promise.allSettled([
      operations.checkpoint(base),
      operations.checkpoint({
        ...base,
        checkpoint: {
          ...base.checkpoint,
          checkpointId: "00000000-0000-4000-8000-000000000032",
          highWatermarkSha256: "e".repeat(64),
        },
      }),
    ]);

    expect(maximumActiveLocks).toBe(1);
    expect(settled.filter((result) => result.status === "fulfilled")).toHaveLength(1);
    expect(settled.filter((result) => result.status === "rejected")).toHaveLength(1);
    const rejected = settled.find(
      (result): result is PromiseRejectedResult => result.status === "rejected",
    );
    expect(rejected?.reason).toBeInstanceOf(Error);
    expect(String(rejected?.reason)).toContain("CHECKPOINT_CONFLICT");
  });

  it("accepts an exact terminal replay after clean two-direction settlement", async () => {
    const replayed = createSequentialOperations([[], [{ "?column?": 1 }]]);

    await expect(
      replayed.operations.checkpoint({
        workerId: "00000000-0000-4000-8000-000000000022",
        consultationId: "00000000-0000-4000-8000-000000000021",
        generation: 3,
        writeEpoch: 1,
        objectKey: "v1/checkpoint.json",
        checkpoint: {
          checkpointId: "00000000-0000-4000-8000-000000000032",
          workerEpoch: 2,
          sourceParticipantId: "00000000-0000-4000-8000-000000000012",
          destinationParticipantId: "00000000-0000-4000-8000-000000000011",
          acceptedInputSequence: 8,
          acceptedInput: 32_000,
          receivedOutput: 24_000,
          emittedOutput: 20_000,
          previousCheckpointSha256: null,
          highWatermarkSha256: "e".repeat(64),
          expectedObjectIds: [],
          observedObjectIds: [],
          gaps: [],
          terminal: true,
          occurredAtMs: 1_700_000_000_000,
        },
      }),
    ).resolves.toBe(true);
    expect(replayed.queryRaw).toHaveBeenCalledTimes(4);
    expect(replayed.executeRaw).toHaveBeenCalledTimes(1);

    const replayStatement = replayed.queryRaw.mock.calls.find(([statement]) =>
      inspectSql(statement).text.startsWith("SELECT 1 FROM worker_checkpoints checkpoint"),
    )?.[0];
    const replaySql = inspectSql(replayStatement).text;
    expect(replaySql).toContain("job.terminal_outcome='clean'");
    expect(replaySql).toContain("consultation.generation=checkpoint.generation");
    expect(replaySql).toContain("reservation.fenced_at IS NULL");
  });
  it("rejects an old-generation or inactive-job checkpoint replay", async () => {
    const operations = createSequentialOperations([[], []]);

    await expect(
      operations.operations.checkpoint({
        workerId: "00000000-0000-4000-8000-000000000022",
        consultationId: "00000000-0000-4000-8000-000000000021",
        generation: 2,
        writeEpoch: 1,
        objectKey: "v1/stale-checkpoint.json",
        checkpoint: {
          checkpointId: "00000000-0000-4000-8000-000000000033",
          workerEpoch: 2,
          sourceParticipantId: "00000000-0000-4000-8000-000000000011",
          destinationParticipantId: "00000000-0000-4000-8000-000000000012",
          acceptedInputSequence: 1,
          acceptedInput: 4_000,
          receivedOutput: 0,
          emittedOutput: 0,
          previousCheckpointSha256: null,
          highWatermarkSha256: "f".repeat(64),
          expectedObjectIds: [],
          observedObjectIds: [],
          gaps: [],
          terminal: false,
          occurredAtMs: 1_700_000_000_000,
        },
      }),
    ).rejects.toThrow("CHECKPOINT_CONFLICT");
  });

  it("leaves explicit worker failure eligible for supervisor checkpoint settlement", async () => {
    const queryRaw = mock(async (statement: Prisma.Sql) => {
      const query = inspectSql(statement).text;
      if (query.startsWith("SELECT 1 FROM outbox")) {
        return [];
      }
      if (query.startsWith("WITH locked AS")) {
        return [{ generation: 4 }];
      }
      return [];
    });
    const executeRaw = mock(async (_statement: Prisma.Sql) => 1);
    const database = {
      $transaction: async <T>(callback: (transaction: Prisma.TransactionClient) => Promise<T>) =>
        callback({
          $queryRaw: queryRaw,
          $executeRaw: executeRaw,
        } as unknown as Prisma.TransactionClient),
    };
    const operations = new PrismaApplicationOperations(
      database as unknown as PrismaClient,
      {} as never,
      { now: () => new Date("2026-01-01T00:00:00Z") },
    );

    await expect(
      operations.workerFailure({
        consultationId: "00000000-0000-4000-8000-000000000021",
        generation: 3,
        workerId: "00000000-0000-4000-8000-000000000022",
        epoch: 2,
        eventId: "00000000-0000-4000-8000-000000000023",
        kindName: "spool_unwritable",
        message: "fsync failed",
        lastCheckpointHashes: {},
      }),
    ).resolves.toBe(true);

    const admissionStatement = queryRaw.mock.calls.find(([statement]) =>
      inspectSql(statement).text.startsWith("WITH locked AS"),
    )?.[0];
    const admissionSql = inspectSql(admissionStatement).text;
    expect(admissionSql).toContain("lease_expires_at=?");
    expect(admissionSql).toContain("accepting_load=false");
    expect(admissionSql).not.toContain("released_at=");
    expect(admissionSql).not.toContain("archive_state");
    expect(admissionSql).not.toContain("terminal_at=");
  });
});

describe("Language capability revision fencing", () => {
  const CAPABILITY_ID = "00000000-0000-4000-8000-000000000041";
  const PROFILE_ID = "00000000-0000-4000-8000-000000000042";

  function repositoryFixture(updated: readonly { id: string }[]) {
    const queryRaw = mock(async (_statement: Prisma.Sql) => [...updated]);
    const repository = new PrismaLanguageRepository({} as unknown as PrismaClient);
    const transaction = new TransactionHandle({
      $queryRaw: queryRaw,
    } as unknown as Prisma.TransactionClient);
    return { repository, transaction, queryRaw };
  }

  it("updates an exact current profile revision once", async () => {
    const fixture = repositoryFixture([{ id: CAPABILITY_ID }]);

    await fixture.repository.setEnabled(CAPABILITY_ID, PROFILE_ID, 7, true, fixture.transaction);

    expect(fixture.queryRaw).toHaveBeenCalledTimes(1);
    const statement = fixture.queryRaw.mock.calls[0]?.[0];
    const inspected = inspectSql(statement);
    expect(inspected.text).toContain("UPDATE language_capabilities");
    expect(inspected.text).toContain("profile_id = ?::uuid");
    expect(inspected.text).toContain("revision = ?");
    expect(inspected.text).toContain("FROM provider_profiles");
    expect(inspected.text).toContain("current_revision = ?");
    expect(inspected.values).toEqual([true, CAPABILITY_ID, PROFILE_ID, 7, PROFILE_ID, 7]);
  });

  it("returns a conflict when the capability revision is stale or absent", async () => {
    const fixture = repositoryFixture([]);

    await expect(
      fixture.repository.setEnabled(CAPABILITY_ID, PROFILE_ID, 6, false, fixture.transaction),
    ).rejects.toMatchObject({ code: "CAPABILITY_REVISION_CONFLICT" });
  });
});
