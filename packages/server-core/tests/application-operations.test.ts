import { describe, expect, it, mock } from "bun:test";
import { PgDialect } from "drizzle-orm/pg-core";
import {
  checkpointPersistenceValues,
  DrizzleApplicationOperations,
} from "../src/application-operations";

const ADMIN = {
  userId: "00000000-0000-4000-8000-000000000001",
  role: "admin" as const,
};
const PROFILE_FRESH_UNTIL = new Date("2026-02-01T00:00:00Z");

const ENABLED_DIRECTION_ROW = {
  id: "00000000-0000-4000-8000-000000000002",
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
  egress_ids: ["EG_room"],
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

function createOperations(rows: unknown[]) {
  const execute = mock(async () => ({ rows }));
  const operations = new DrizzleApplicationOperations({ execute } as never, {} as never, {
    now: () => new Date(),
  });
  return { execute, operations };
}

function createSequentialOperations(
  results: Array<{ rows: Record<string, unknown>[]; rowCount?: number }>,
) {
  const execute = mock(async (_statement: unknown) => results.shift() ?? { rows: [], rowCount: 0 });
  const operations = new DrizzleApplicationOperations({ execute } as never, {} as never, {
    now: () => new Date(),
  });
  return { execute, operations };
}

describe("ApplicationOperations typed views", () => {
  it("returns enabled and disabled current profile directions", async () => {
    const { operations } = createOperations([ENABLED_DIRECTION_ROW, DISABLED_DIRECTION_ROW]);

    const rows = await operations.adminLanguages(ADMIN, "google-eu");

    expect(rows.map((row) => row.enabled)).toEqual([true, false]);
    expect(rows.every((row) => row.revision === 4)).toBe(true);
    expect(rows.every((row) => row.profileName === "google-eu")).toBe(true);
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
    expect(detail.egressIds).toEqual(["EG_room"]);
    expect(detail.providerAttemptGroups[0]?.attemptIds).toEqual(detail.providerAttemptIds);
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
      { rows: [PROVIDER_CONTEXT_ROW], rowCount: 1 },
      { rows: [{ id: PROVIDER_REPORT.attemptId }], rowCount: 1 },
    ]);
    await expect(inserted.operations.providerAttempt(input)).resolves.toBe(true);
    expect(inserted.execute).toHaveBeenCalledTimes(2);

    const replayed = createSequentialOperations([
      { rows: [PROVIDER_CONTEXT_ROW], rowCount: 1 },
      { rows: [], rowCount: 0 },
      { rows: [{ "?column?": 1 }], rowCount: 1 },
    ]);
    await expect(replayed.operations.providerAttempt(input)).resolves.toBe(true);

    const conflict = createSequentialOperations([
      { rows: [PROVIDER_CONTEXT_ROW], rowCount: 1 },
      { rows: [], rowCount: 0 },
      { rows: [], rowCount: 0 },
    ]);
    await expect(conflict.operations.providerAttempt(input)).rejects.toThrow(
      "PROVIDER_ATTEMPT_CONFLICT",
    );
  });
  it("retries a provider terminal after a PostgreSQL deadlock", async () => {
    let calls = 0;
    const execute = mock(async () => {
      calls += 1;
      if (calls === 1) {
        return { rows: [PROVIDER_CONTEXT_ROW], rowCount: 1 };
      }
      if (calls === 2) {
        throw new Error("provider attempt deadlocked", { cause: { code: "40P01" } });
      }
      return { rows: [{ id: PROVIDER_REPORT.attemptId }], rowCount: 1 };
    });
    const operations = new DrizzleApplicationOperations({ execute } as never, {} as never, {
      now: () => new Date(),
    });

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
    expect(execute).toHaveBeenCalledTimes(3);
  });

  it("rejects provider evidence from a fenced worker or an unselected stage", async () => {
    const fenced = createSequentialOperations([{ rows: [], rowCount: 0 }]);
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

    const bypass = createSequentialOperations([{ rows: [PROVIDER_CONTEXT_ROW], rowCount: 1 }]);
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

    const firstDirection = createSequentialOperations([
      { rows: [{ id: terminalCheckpoint.checkpointId }], rowCount: 1 },
      { rows: [], rowCount: 0 },
    ]);
    await expect(firstDirection.operations.checkpoint(input)).resolves.toBe(true);
    expect(firstDirection.execute).toHaveBeenCalledTimes(2);

    const reverseDirection = createSequentialOperations([
      { rows: [{ id: "00000000-0000-4000-8000-000000000032" }], rowCount: 1 },
      { rows: [{ worker_id: input.workerId }], rowCount: 1 },
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
    expect(reverseDirection.execute).toHaveBeenCalledTimes(2);
  });

  it("accepts an exact terminal replay after clean two-direction settlement", async () => {
    const replayed = createSequentialOperations([
      { rows: [], rowCount: 0 },
      { rows: [{ "?column?": 1 }], rowCount: 1 },
      { rows: [], rowCount: 0 },
    ]);

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
    expect(replayed.execute).toHaveBeenCalledTimes(3);

    const replayStatement = replayed.execute.mock.calls[1]?.[0];
    const replaySql = new PgDialect().sqlToQuery(replayStatement as never).sql;
    expect(replaySql).toContain("job.terminal_outcome='clean'");
    expect(replaySql).toContain("consultation.generation=checkpoint.generation");
    expect(replaySql).toContain("reservation.fenced_at IS NULL");
  });
  it("rejects an old-generation or inactive-job checkpoint replay", async () => {
    const operations = createSequentialOperations([
      { rows: [], rowCount: 0 },
      { rows: [], rowCount: 0 },
    ]);

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
    const transactionResults = [
      { rows: [], rowCount: 0 },
      { rows: [{ generation: 4 }], rowCount: 1 },
      { rows: [], rowCount: 1 },
      { rows: [], rowCount: 1 },
      { rows: [], rowCount: 1 },
    ];
    const execute = mock(
      async (_statement: unknown) => transactionResults.shift() ?? { rows: [], rowCount: 0 },
    );
    const database = {
      execute,
      transaction: async <T>(callback: (transaction: { execute: typeof execute }) => Promise<T>) =>
        callback({ execute }),
    };
    const operations = new DrizzleApplicationOperations(database as never, {} as never, {
      now: () => new Date("2026-01-01T00:00:00Z"),
    });

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

    const admissionStatement = execute.mock.calls[1]?.[0];
    const admissionSql = new PgDialect().sqlToQuery(admissionStatement as never).sql;
    expect(admissionSql).toContain("lease_expires_at=");
    expect(admissionSql).toContain("accepting_load=false");
    expect(admissionSql).not.toContain("released_at=");
    expect(admissionSql).not.toContain("archive_state");
    expect(admissionSql).not.toContain("terminal_at=");
  });
});
