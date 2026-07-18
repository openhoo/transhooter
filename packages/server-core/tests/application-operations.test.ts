import { describe, expect, it, mock } from "bun:test";
import { DrizzleApplicationOperations } from "../src/application-operations";

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
  const execute = mock(async () => results.shift() ?? { rows: [], rowCount: 0 });
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
});
