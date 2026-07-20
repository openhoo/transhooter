import { test } from "bun:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import {
  ArchiveGapSchema,
  ArchiveObjectRecordSchema,
  ArchiveRecordSchema,
  ArchiveStateSchema,
  CAPTION_TOPIC,
  CaptionPacketSchema,
  CONTRACT_SCHEMAS,
  ConsultationStateSchema,
  ExternalEffectStateSchema,
  FinalInventorySchema,
  HttpRawReferenceSchema,
  InterpretationTrackNameSchema,
  InventorySupplementSchema,
  MagicLinkPurposeSchema,
  NullableStaffRoleSchema,
  OrderedHeaderSchema,
  ParticipantAttributesSchema,
  ParticipantRoleSchema,
  PcmSidecarSchema,
  ProviderAttemptReportSchema,
  ProviderAttemptTerminalSchema,
  RetryDecisionSchema,
  RoomProviderSelectionSchema,
  SampleRangeSchema,
  STATUS_TOPIC,
  StaffRoleSchema,
  StatusPacketSchema,
  WebSocketRawReferenceSchema,
  type WorkerCheckpoint,
  WorkerCheckpointSchema,
  WorkerJobMetadataSchema,
} from "../src/index";

const consultationId = "018f1f3c-0f63-7d65-8eb1-1f250f9f9891";
const employeeId = "018f1f3c-0f63-7d65-8eb1-1f250f9f9892";
const customerId = "018f1f3c-0f63-7d65-8eb1-1f250f9f9893";
const attemptId = "018f1f3c-0f63-7d65-8eb1-1f250f9f9894";
const objectId = "018f1f3c-0f63-7d65-8eb1-1f250f9f9895";
const sha256 = "a".repeat(64);

type GeneratedBundle = {
  schemas: Record<string, Record<string, unknown>>;
};

const generatedBundle = JSON.parse(
  readFileSync(new URL("../generated/contracts.schema.json", import.meta.url), "utf8"),
) as GeneratedBundle;

function assertGeneratedRejects(fixtures: ReadonlyArray<readonly [string, unknown]>): void {
  const validation = spawnSync(
    "python",
    [
      "-c",
      [
        "import json, sys",
        "from jsonschema import Draft202012Validator",
        "payload = json.load(sys.stdin)",
        "for name, instance in payload['fixtures']:",
        "    if Draft202012Validator(payload['schemas'][name]).is_valid(instance):",
        "        print(name)",
        "        sys.exit(1)",
      ].join("\n"),
    ],
    {
      input: JSON.stringify({ schemas: generatedBundle.schemas, fixtures }),
      encoding: "utf8",
    },
  );
  assert.equal(
    validation.status,
    0,
    validation.stderr || `generated schema accepted ${validation.stdout}`,
  );
}

function assertGeneratedAccepts(fixtures: ReadonlyArray<readonly [string, unknown]>): void {
  const validation = spawnSync(
    "python",
    [
      "-c",
      [
        "import json, sys",
        "from jsonschema import Draft202012Validator",
        "payload = json.load(sys.stdin)",
        "for name, instance in payload['fixtures']:",
        "    errors = list(Draft202012Validator(payload['schemas'][name]).iter_errors(instance))",
        "    if errors:",
        "        print(f'{name}: {errors[0].message}')",
        "        sys.exit(1)",
      ].join("\n"),
    ],
    {
      input: JSON.stringify({ schemas: generatedBundle.schemas, fixtures }),
      encoding: "utf8",
    },
  );
  assert.equal(
    validation.status,
    0,
    validation.stderr || `generated schema rejected ${validation.stdout}`,
  );
}

function runtimeRefinements(name: string): string[] {
  const refinements = generatedBundle.schemas[name]?.["x-transhooter-runtime-refinements"];
  assert.ok(Array.isArray(refinements), `${name} must disclose runtime-only refinements`);
  return refinements as string[];
}
const pipelineObjectKey = `v1/meetings/${consultationId}/pipeline/translation/fixture/${attemptId}/000001.json`;

const providerCredential = {
  reference: "secret/provider",
  version: "7",
};
const commonStage = {
  provider: "fixture",
  endpoint: "https://fixture.invalid",
  region: "eu",
  model: "fixture-v1",
  adapterBuild: "fixture-1.0.0",
  policy: "policy-v1",
  credential: providerCredential,
  limits: { requestsPerMinute: 100 },
};
const sttSelection = {
  ...commonStage,
  locale: "en-US",
  encoding: "LINEAR16",
};
const translationSelection = {
  ...commonStage,
  sourceCode: "EN",
  targetCode: "DE",
};
const ttsSelection = {
  ...commonStage,
  locale: "de-DE",
  voice: "fixture-de",
  encoding: "LINEAR16",
  sampleRate: 48_000,
};

function makeProviderSelection() {
  return {
    profileId: "google-eu",
    profileRevision: 3,
    capabilityHash: sha256,
    participantIds: [employeeId, customerId],
    directions: [
      {
        mode: "translated",
        sourceParticipantId: employeeId,
        destinationParticipantId: customerId,
        capabilityRowId: "018f1f3c-0f63-7d65-8eb1-1f250f9f9811",
        stt: sttSelection,
        targetCode: "DE",
        translation: translationSelection,
        tts: ttsSelection,
      },
      {
        mode: "same_language",
        sourceParticipantId: customerId,
        destinationParticipantId: employeeId,
        capabilityRowId: "018f1f3c-0f63-7d65-8eb1-1f250f9f9812",
        stt: {
          ...sttSelection,
          locale: "de-DE",
        },
        bypass: true,
      },
    ],
  } as const;
}

const archiveObject = {
  objectId,
  class: "pipeline_exchange",
  key: pipelineObjectKey,
  versionId: "opaque-version",
  size: 123,
  sha256,
  s3Checksum: "crc64nvme-value",
  contentType: "application/json",
  sampleRange: { start: 0, end: 4000 },
  attempt: 1,
  sequence: 0,
} as const;

const rawArtifact = {
  objectId,
  key: pipelineObjectKey,
  sha256,
  size: 123,
};
const httpRawReference = {
  transport: "http",
  method: "POST",
  url: "https://fixture.invalid/translate",
  status: 200,
  requestHeaders: [
    {
      name: "content-type",
      value: "application/json",
      redacted: false,
      secretReference: null,
    },
  ],
  requestBody: rawArtifact,
  responseHeaders: [],
  responseBody: rawArtifact,
} as const;

const providerTerminal = {
  terminalId: "018f1f3c-0f63-7d65-8eb1-1f250f9f9896",
  operationId: "018f1f3c-0f63-7d65-8eb1-1f250f9f9897",
  attemptId,
  stage: "translation",
  outcome: "succeeded",
  error: null,
  retryDecision: {
    action: "do_not_retry",
    reason: "accepted result",
    retryAtMs: null,
    previousAttemptId: null,
  },
  retryOfAttemptId: null,
  watermarks: {
    acceptedInputSequence: 0,
    acceptedInputSampleEnd: 4000,
    receivedOutputSequence: 0,
    receivedOutputSampleEnd: null,
    emittedOutputSequence: 0,
    emittedOutputSampleEnd: null,
  },
  credentialVersion: "7",
  credentialFingerprint: "sha256:opaque",
  occurredAtMs: 1_700_000_000_000,
  transport: "http",
  rawReference: httpRawReference,
} as const;

test("participant and infrastructure identities are strict UUID contracts", () => {
  const interpretationTrackName = `interpretation:${customerId}`;
  const participantAttributes = {
    "consultation.id": consultationId,
    "consultation.role": "employee",
    "consultation.language": "en-US",
  };
  const invalidParticipantAttributes = {
    "consultation.id": "not-a-uuid",
    "consultation.role": "employee",
    "consultation.language": "en-US",
  };

  const parsedInterpretationTrackName =
    InterpretationTrackNameSchema.parse(interpretationTrackName);
  const parsedParticipantAttributes = ParticipantAttributesSchema.parse(participantAttributes);

  assert.equal(CAPTION_TOPIC, "consultation.translation.v1");
  assert.equal(STATUS_TOPIC, "consultation.status.v1");
  assert.equal(parsedInterpretationTrackName, interpretationTrackName);
  assert.throws(() => InterpretationTrackNameSchema.parse("interpretation:customer@example.com"));
  assert.deepEqual(parsedParticipantAttributes, participantAttributes);
  assert.throws(() => ParticipantAttributesSchema.parse(invalidParticipantAttributes));
});

test("caption packet enforces exact wire fields, UUIDs, revisions, finality, and inclusive-exclusive samples", () => {
  const packet = {
    schemaVersion: 1,
    consultationId,
    destinationParticipantId: customerId,
    sourceParticipantId: employeeId,
    utteranceId: attemptId,
    revision: 1,
    finality: "provisional",
    sourceLanguage: "en-US",
    targetLanguage: "de-DE",
    sourceText: "hello",
    translatedText: "hallo",
    sourceSampleStart: 100,
    sourceSampleEnd: 200,
    occurredAtMs: 1_700_000_000_000,
  } as const;

  const parsedPacket = CaptionPacketSchema.parse(packet);

  assert.deepEqual(parsedPacket, packet);
  assert.throws(() => CaptionPacketSchema.parse({ ...packet, revision: 0 }));
  assert.throws(() => CaptionPacketSchema.parse({ ...packet, finality: "span_final" }));
  assert.throws(() => CaptionPacketSchema.parse({ ...packet, sourceSampleEnd: 100 }));
  assert.throws(() => CaptionPacketSchema.parse({ ...packet, unknown: true }));
});

test("sample and PCM contracts preserve exact inclusive-exclusive ranges", () => {
  const sampleRange = { start: 0, end: 1 };
  const pcmSidecar = {
    encoding: "PCM_S16LE",
    rate: 48_000,
    channels: 1,
    format: "raw",
    sampleRange: { start: 0, end: 960 },
  };

  const parsedSampleRange = SampleRangeSchema.parse(sampleRange);
  const parsedPcmSidecar = PcmSidecarSchema.parse(pcmSidecar);

  assert.deepEqual(parsedSampleRange, sampleRange);
  assert.throws(() => SampleRangeSchema.parse({ start: 1, end: 1 }));
  assert.deepEqual(parsedPcmSidecar, pcmSidecar);
});

test("room provider selection freezes exactly two inverse directions", () => {
  const selection = makeProviderSelection();
  const duplicateDirectionSelection = {
    ...selection,
    directions: [selection.directions[0], selection.directions[0]],
  };
  const duplicateParticipantSelection = {
    ...selection,
    participantIds: [employeeId, employeeId],
  };

  const parsedSelection = RoomProviderSelectionSchema.parse(selection);

  assert.deepEqual(parsedSelection, selection);
  assert.throws(() => RoomProviderSelectionSchema.parse(duplicateDirectionSelection));
  assert.throws(() => RoomProviderSelectionSchema.parse(duplicateParticipantSelection));
});

test("worker job metadata binds epochs, exact humans, and the complete frozen provider selection", () => {
  const providerSelection = makeProviderSelection();
  const metadata = {
    schemaVersion: 1,
    consultationId,
    generation: 2,
    roomName: "018f1f3c-0f63-7d65-8eb1-1f250f9f9830",
    workerIdentity: "018f1f3c-0f63-7d65-8eb1-1f250f9f9831",
    workerEpoch: 4,
    writeEpoch: 0,
    expectedParticipantIds: [employeeId, customerId],
    expectedLivekitIdentities: [
      "018f1f3c-0f63-7d65-8eb1-1f250f9f9832",
      "018f1f3c-0f63-7d65-8eb1-1f250f9f9833",
    ],
    providerSelection,
    snapshotHash: "b".repeat(64),
  } as const;

  const parsedMetadata = WorkerJobMetadataSchema.parse(metadata);

  assert.deepEqual(parsedMetadata, metadata);
  assert.throws(() =>
    WorkerJobMetadataSchema.parse({
      ...metadata,
      snapshotHash: "not-a-hash",
    }),
  );
  assert.throws(() =>
    WorkerJobMetadataSchema.parse({
      ...metadata,
      writeEpoch: -1,
    }),
  );
  assert.throws(() =>
    WorkerJobMetadataSchema.parse({
      ...metadata,
      expectedParticipantIds: [employeeId, attemptId],
    }),
  );
  assert.throws(() =>
    WorkerJobMetadataSchema.parse({
      ...metadata,
      workerIdentity: metadata.expectedLivekitIdentities[0],
    }),
  );
  assert.throws(() =>
    WorkerJobMetadataSchema.parse({
      ...metadata,
      expectedLivekitIdentities: [
        metadata.expectedLivekitIdentities[0],
        metadata.expectedLivekitIdentities[0],
      ],
    }),
  );
});

test("status discriminant implements all four exact wire variants", () => {
  const common = {
    schemaVersion: 1,
    consultationId,
    generation: 1,
    occurredAtMs: 1000,
  } as const;
  const variants = [
    {
      ...common,
      state: "ready",
      reasonCode: "CAPTURE_READY",
      subjectParticipantId: employeeId,
      participantEgressId: "egress-opaque",
      shutdownAtMs: null,
    },
    {
      ...common,
      state: "active",
      reasonCode: "SAME_LANGUAGE_BYPASS",
      sourceParticipantId: employeeId,
      destinationParticipantId: customerId,
      shutdownAtMs: null,
    },
    {
      ...common,
      state: "active",
      reasonCode: "ARCHIVE_FAILED",
      shutdownAtMs: 5000,
    },
    {
      ...common,
      state: "finalizing",
      reasonCode: "SHUTDOWN",
      shutdownAtMs: 5000,
    },
  ] as const;

  for (const variant of variants) {
    const parsedVariant = StatusPacketSchema.parse(variant);
    assert.deepEqual(parsedVariant, variant);
  }
  assert.throws(() => StatusPacketSchema.parse({ ...variants[0], shutdownAtMs: 10 }));
  assert.throws(() => StatusPacketSchema.parse({ ...variants[3], state: "ready" }));
  assert.throws(() =>
    StatusPacketSchema.parse({
      ...variants[3],
      subjectParticipantId: employeeId,
    }),
  );
});

test("provider terminal permits exactly one matching raw transport reference", () => {
  const parsedProviderTerminal = ProviderAttemptTerminalSchema.parse(providerTerminal);

  assert.deepEqual(parsedProviderTerminal, providerTerminal);
  const retryableError = {
    kind: "rate_limit" as const,
    scope: "operation" as const,
    providerRetryAdvice: "retry_after" as const,
    providerCode: "429",
    providerRequestId: null,
    retryDelayMs: 250,
    attemptId,
    rawObjectIds: [objectId],
  };
  const retryDecision = {
    action: "retry" as const,
    reason: "safe uncommitted replay",
    retryAtMs: providerTerminal.occurredAtMs + 250,
    previousAttemptId: attemptId,
  };
  assert.deepEqual(
    ProviderAttemptTerminalSchema.parse({
      ...providerTerminal,
      outcome: "failed",
      error: retryableError,
      retryDecision,
    }),
    {
      ...providerTerminal,
      outcome: "failed",
      error: retryableError,
      retryDecision,
    },
  );
  assert.throws(() =>
    ProviderAttemptTerminalSchema.parse({
      ...providerTerminal,
      outcome: "failed",
      error: retryableError,
      retryDecision: {
        ...retryDecision,
        previousAttemptId: null,
      },
    }),
  );
  assert.deepEqual(
    ProviderAttemptTerminalSchema.parse({
      ...providerTerminal,
      outcome: "cancelled",
    }),
    { ...providerTerminal, outcome: "cancelled" },
  );
  const cancelledTerminal = {
    ...providerTerminal,
    outcome: "cancelled",
    error: {
      ...retryableError,
      kind: "cancelled",
      providerRetryAdvice: "never",
      retryDelayMs: null,
    },
  } as const;
  assert.equal(ProviderAttemptTerminalSchema.parse(cancelledTerminal).outcome, "cancelled");
  assertGeneratedAccepts([
    ["HttpProviderAttemptTerminal", cancelledTerminal],
    ["ProviderAttemptTerminal", cancelledTerminal],
  ]);
  assert.throws(() =>
    ProviderAttemptTerminalSchema.parse({
      ...providerTerminal,
      retryDecision,
    }),
  );
  assert.throws(() =>
    ProviderAttemptTerminalSchema.parse({
      ...providerTerminal,
      outcome: "cancelled",
      retryDecision: {
        ...providerTerminal.retryDecision,
        action: "degrade",
      },
    }),
  );
  assert.throws(() =>
    ProviderAttemptTerminalSchema.parse({
      ...providerTerminal,
      outcome: "failed",
      error: {
        ...retryableError,
        kind: "authentication",
        providerRetryAdvice: "unspecified",
      },
      retryDecision,
    }),
  );
  assert.throws(() =>
    ProviderAttemptTerminalSchema.parse({
      ...providerTerminal,
      outcome: "failed",
      error: { ...retryableError, kind: "cancelled" },
    }),
  );
  assert.throws(() =>
    ProviderAttemptTerminalSchema.parse({
      ...providerTerminal,
      transport: "grpc",
    }),
  );
  assert.throws(() =>
    ProviderAttemptTerminalSchema.parse({
      ...providerTerminal,
      grpcRawReference: {},
    }),
  );
  assert.throws(() =>
    ProviderAttemptTerminalSchema.parse({
      ...providerTerminal,
      outcome: "failed",
    }),
  );
  assert.throws(() =>
    ProviderAttemptTerminalSchema.parse({
      ...providerTerminal,
      retryOfAttemptId: attemptId,
      retryDecision: {
        ...providerTerminal.retryDecision,
        previousAttemptId: null,
      },
    }),
  );
  assert.throws(() =>
    ProviderAttemptTerminalSchema.parse({
      ...providerTerminal,
      outcome: "failed",
      error: {
        kind: "provider",
        scope: "operation",
        providerRetryAdvice: "never",
        providerCode: "failure",
        providerRequestId: null,
        retryDelayMs: null,
        attemptId: employeeId,
        rawObjectIds: [objectId],
      },
    }),
  );
  assert.deepEqual(
    ProviderAttemptTerminalSchema.parse({
      ...providerTerminal,
      retryOfAttemptId: employeeId,
    }).retryOfAttemptId,
    employeeId,
  );
});

test("provider attempt reports reject malformed and terminally inconsistent evidence", () => {
  const report = {
    directionId: makeProviderSelection().directions[0].capabilityRowId,
    stage: "translation",
    terminalId: providerTerminal.terminalId,
    operationId: providerTerminal.operationId,
    attemptId,
    attemptNumber: 1,
    retryOfAttemptId: null,
    outcome: "succeeded",
    error: null,
    retryDecision: providerTerminal.retryDecision,
    watermarks: providerTerminal.watermarks,
    credentialVersion: "7",
    credentialFingerprint: "opaque-fingerprint",
    transport: "http",
    rawReferences: [
      {
        objectId,
        ordinal: 0,
        sha256,
        size: 123,
        mediaType: "application/json",
      },
    ],
    terminalHash: sha256,
    startedAtMs: 1_700_000_000_000,
    occurredAtMs: 1_700_000_000_100,
  } as const;

  assert.deepEqual(ProviderAttemptReportSchema.parse(report), report);
  const retryingReport = {
    ...report,
    outcome: "failed" as const,
    error: {
      kind: "rate_limit" as const,
      scope: "operation" as const,
      providerRetryAdvice: "retry_after" as const,
      providerCode: "429",
      providerRequestId: null,
      retryDelayMs: 250,
      attemptId,
      rawObjectIds: [objectId],
    },
    retryDecision: {
      action: "retry" as const,
      reason: "safe uncommitted replay",
      retryAtMs: report.occurredAtMs + 250,
      previousAttemptId: attemptId,
    },
  };
  assert.deepEqual(ProviderAttemptReportSchema.parse(retryingReport), retryingReport);
  assert.throws(() =>
    ProviderAttemptReportSchema.parse({
      ...report,
      retryDecision: retryingReport.retryDecision,
    }),
  );
  assert.throws(() =>
    ProviderAttemptReportSchema.parse({
      ...retryingReport,
      error: {
        ...retryingReport.error,
        kind: "invalid_request",
        providerRetryAdvice: "unspecified",
      },
    }),
  );
  assert.throws(() =>
    ProviderAttemptReportSchema.parse({
      ...report,
      outcome: "cancelled",
      retryDecision: {
        ...report.retryDecision,
        action: "degrade",
      },
    }),
  );
  assert.throws(() =>
    ProviderAttemptReportSchema.parse({ ...report, outcome: "failed", error: null }),
  );
  assert.throws(() =>
    ProviderAttemptReportSchema.parse({
      ...report,
      startedAtMs: report.occurredAtMs + 1,
    }),
  );
  assert.throws(() =>
    ProviderAttemptReportSchema.parse({
      ...report,
      attemptNumber: 2,
      retryOfAttemptId: null,
    }),
  );
  assert.throws(() =>
    ProviderAttemptReportSchema.parse({
      ...report,
      attemptNumber: 2,
      retryOfAttemptId: attemptId,
    }),
  );
  const successfulRetryReport = {
    ...report,
    attemptNumber: 2,
    retryOfAttemptId: employeeId,
  } as const;
  assert.deepEqual(ProviderAttemptReportSchema.parse(successfulRetryReport), successfulRetryReport);
  assert.throws(() =>
    ProviderAttemptReportSchema.parse({
      ...report,
      rawReferences: [...report.rawReferences, report.rawReferences[0]],
    }),
  );
  assert.throws(() =>
    ProviderAttemptReportSchema.parse({
      ...report,
      unexpectedProvider: "untrusted",
    }),
  );
  assert.throws(() =>
    ProviderAttemptReportSchema.parse({
      ...report,
      attemptNumber: 2,
      retryOfAttemptId: employeeId,
      retryDecision: {
        ...report.retryDecision,
        previousAttemptId: customerId,
      },
    }),
  );
});

test("worker checkpoints require canonical direction and sample watermarks", () => {
  const checkpoint: WorkerCheckpoint = {
    checkpointId: "018f1f3c-0f63-7d65-8eb1-1f250f9f9820",
    workerEpoch: 1,
    sourceParticipantId: employeeId,
    destinationParticipantId: customerId,
    acceptedInputSequence: 15,
    acceptedInput: 4000,
    receivedOutput: 3000,
    emittedOutput: 1920,
    previousCheckpointSha256: null,
    highWatermarkSha256: sha256,
    expectedObjectIds: [objectId],
    observedObjectIds: [objectId],
    gaps: [],
    terminal: false,
    occurredAtMs: 1000,
  };

  assert.deepEqual(WorkerCheckpointSchema.parse(checkpoint), checkpoint);

  for (const field of [
    "sourceParticipantId",
    "destinationParticipantId",
    "acceptedInputSequence",
    "acceptedInput",
    "receivedOutput",
    "emittedOutput",
  ] as const) {
    const incompleteCheckpoint: Record<string, unknown> = { ...checkpoint };
    delete incompleteCheckpoint[field];
    assert.throws(() => WorkerCheckpointSchema.parse(incompleteCheckpoint));
  }

  for (const malformedFields of [
    { sourceParticipantId: "not-a-uuid" },
    { destinationParticipantId: "not-a-uuid" },
    { acceptedInputSequence: -1 },
    { acceptedInput: -1 },
    { receivedOutput: -1 },
    { emittedOutput: -1 },
    { acceptedInputSequence: 1.5 },
    { receivedOutput: "3000" },
    { acceptedInput: 1.5 },
    { emittedOutput: "1920" },
  ]) {
    assert.throws(() =>
      WorkerCheckpointSchema.parse({
        ...checkpoint,
        ...malformedFields,
      }),
    );
  }

  assert.throws(() =>
    WorkerCheckpointSchema.parse({
      ...checkpoint,
      unknownWatermark: 4000,
    }),
  );
  assert.throws(() =>
    WorkerCheckpointSchema.parse({
      ...checkpoint,
      destinationParticipantId: checkpoint.sourceParticipantId,
    }),
  );
});

test("archive records are opaque, strict, and inventories preserve create-once relationships", () => {
  const archiveRecord = {
    recordKind: "object",
    value: archiveObject,
  } as const;
  const inventory = {
    schemaVersion: 1,
    inventoryId: "018f1f3c-0f63-7d65-8eb1-1f250f9f9821",
    consultationId,
    status: "complete",
    roomClose: {
      roomId: "room-opaque",
      generation: 1,
      closedAtMs: 1000,
      reason: "ended",
    },
    workerTerminal: {
      workerEpoch: 1,
      checkpointId: "018f1f3c-0f63-7d65-8eb1-1f250f9f9822",
      outcome: "clean",
      occurredAtMs: 999,
    },
    egressResults: [],
    objects: [archiveObject],
    missing: [],
    errors: [],
    createdAtMs: 1001,
  } as const;
  const missingInventory = {
    ...inventory,
    missing: [
      {
        owner: "worker",
        objectClass: "stt_input_pcm",
        sampleRange: { start: 0, end: 1 },
        segmentStart: null,
        segmentEnd: null,
        reason: "missing",
      },
    ],
  };
  const segmentGap = {
    owner: "participant-egress",
    objectClass: "participant_media",
    sampleRange: null,
    segmentStart: 9,
    segmentEnd: 3,
    reason: "unordered",
  } as const;
  const failedWorkerInventory = {
    ...inventory,
    workerTerminal: { ...inventory.workerTerminal, outcome: "failed" },
  } as const;
  const fencedWorkerInventory = {
    ...inventory,
    workerTerminal: { ...inventory.workerTerminal, outcome: "fenced" },
  } as const;
  const failedEgressInventory = {
    ...inventory,
    egressResults: [
      {
        egressId: "egress-1",
        kind: "participant",
        subjectParticipantId: employeeId,
        outcome: "failed",
        objectIds: [],
        gaps: [],
      },
    ],
  } as const;
  const gappedEgressInventory = {
    ...inventory,
    egressResults: [
      {
        egressId: "egress-2",
        kind: "participant",
        subjectParticipantId: employeeId,
        outcome: "complete",
        objectIds: [],
        gaps: [{ ...segmentGap, segmentStart: 3, segmentEnd: 9 }],
      },
    ],
  } as const;
  const supplement = {
    schemaVersion: 1,
    supplementId: "018f1f3c-0f63-7d65-8eb1-1f250f9f9823",
    consultationId,
    finalInventorySha256: sha256,
    addedObjects: [archiveObject],
    closedGapIndexes: [0],
    errors: [],
    createdAtMs: 2000,
  } as const;

  const parsedArchiveObject = ArchiveObjectRecordSchema.parse(archiveObject);
  const parsedArchiveRecord = ArchiveRecordSchema.parse(archiveRecord);
  const parsedInventory = FinalInventorySchema.parse(inventory);
  const parsedSupplement = InventorySupplementSchema.parse(supplement);

  assert.deepEqual(parsedArchiveObject, archiveObject);
  assert.throws(() =>
    ArchiveObjectRecordSchema.parse({
      ...archiveObject,
      key: "v1/meetings/customer@example.com/audio.pcm",
    }),
  );
  assert.throws(() =>
    ArchiveObjectRecordSchema.parse({
      ...archiveObject,
      versionId: null,
    }),
  );
  assert.deepEqual(parsedArchiveRecord, archiveRecord);
  assert.deepEqual(parsedInventory, inventory);
  assert.throws(() => FinalInventorySchema.parse(missingInventory));
  assert.throws(() => ArchiveGapSchema.parse(segmentGap));
  assert.throws(() => FinalInventorySchema.parse(failedWorkerInventory));
  assert.throws(() => FinalInventorySchema.parse(fencedWorkerInventory));
  assert.throws(() => FinalInventorySchema.parse(failedEgressInventory));
  assert.throws(() => FinalInventorySchema.parse(gappedEgressInventory));
  assertGeneratedRejects([
    ["FinalInventory", missingInventory],
    ["FinalInventory", failedWorkerInventory],
    ["FinalInventory", fencedWorkerInventory],
    ["FinalInventory", failedEgressInventory],
    ["FinalInventory", gappedEgressInventory],
  ]);
  assert.deepEqual(parsedSupplement, supplement);
});

test("transport references enforce network URL schemes with runtime and generated parity", () => {
  const acceptedHttpReferences = [
    "HTTP://fixture.invalid/translate",
    "hTtPs://fixture.invalid/translate",
  ].map((url) => ({ ...httpRawReference, url }));
  const websocketReference = {
    transport: "websocket",
    url: "wss://fixture.invalid/listen",
    upgradeRequestHeaders: [],
    upgradeResponseHeaders: [],
    frames: [],
    closeCode: 1000,
    closeReason: "complete",
  } as const;
  const acceptedWebSocketReferences = [
    "WS://fixture.invalid/listen",
    "WsS://fixture.invalid/listen",
  ].map((url) => ({ ...websocketReference, url }));
  const invalidHttp = { ...httpRawReference, url: "ftp://fixture.invalid/translate" };
  const invalidWebSocket = { ...websocketReference, url: "https://fixture.invalid/listen" };

  for (const reference of acceptedHttpReferences) {
    assert.deepEqual(HttpRawReferenceSchema.parse(reference), reference);
  }
  for (const reference of acceptedWebSocketReferences) {
    assert.deepEqual(WebSocketRawReferenceSchema.parse(reference), reference);
  }
  assert.throws(() => HttpRawReferenceSchema.parse(invalidHttp));
  assert.deepEqual(WebSocketRawReferenceSchema.parse(websocketReference), websocketReference);
  assert.throws(() => WebSocketRawReferenceSchema.parse(invalidWebSocket));
  assertGeneratedAccepts([
    ...acceptedHttpReferences.map((reference) => ["HttpRawReference", reference] as const),
    ...acceptedWebSocketReferences.map(
      (reference) => ["WebSocketRawReference", reference] as const,
    ),
  ]);
  assertGeneratedRejects([
    ["HttpRawReference", invalidHttp],
    ["WebSocketRawReference", invalidWebSocket],
  ]);
});

test("generated schemas preserve representable refinements and disclose value comparisons", () => {
  const gapWithoutRange = {
    owner: "worker",
    objectClass: "checkpoint",
    sampleRange: null,
    segmentStart: null,
    segmentEnd: null,
    reason: "unknown",
  } as const;
  const retryWithoutLink = {
    action: "retry",
    reason: "safe replay",
    retryAtMs: 250,
    previousAttemptId: null,
  } as const;
  const stopWithoutLink = {
    action: "do_not_retry",
    reason: "cancelled",
    retryAtMs: null,
    previousAttemptId: null,
  } as const;
  const malformedHeader = {
    name: "authorization",
    value: "[redacted]",
    redacted: true,
    secretReference: null,
  } as const;
  const failedTerminalWithoutError = {
    ...providerTerminal,
    outcome: "failed",
    error: null,
  } as const;

  assert.throws(() => ArchiveGapSchema.parse(gapWithoutRange));
  assert.throws(() => RetryDecisionSchema.parse(retryWithoutLink));
  assert.deepEqual(RetryDecisionSchema.parse(stopWithoutLink), stopWithoutLink);
  assert.throws(() => OrderedHeaderSchema.parse(malformedHeader));
  assert.throws(() => ProviderAttemptTerminalSchema.parse(failedTerminalWithoutError));
  assertGeneratedRejects([
    ["ArchiveGap", gapWithoutRange],
    ["RetryDecision", retryWithoutLink],
    ["OrderedHeader", malformedHeader],
    ["ProviderAttemptTerminal", failedTerminalWithoutError],
    ["HttpProviderAttemptTerminal", failedTerminalWithoutError],
  ]);
  assertGeneratedAccepts([["RetryDecision", stopWithoutLink]]);

  const retryLinkRefinements = [
    "successful and cancelled terminals cannot carry retryDecision actions other than do_not_retry",
    "successful and cancelled terminals cannot link retryDecision.previousAttemptId",
    "retryDecision.previousAttemptId must equal attemptId for retry and when otherwise present",
    "retries require a retryable failed provider error",
    "retryOfAttemptId must differ from attemptId",
  ];
  for (const name of [
    "GrpcProviderAttemptTerminal",
    "HttpProviderAttemptTerminal",
    "ProviderAttemptTerminal",
    "WebSocketProviderAttemptTerminal",
  ]) {
    assert.deepEqual(runtimeRefinements(name), [
      "error.attemptId must equal attemptId",
      ...retryLinkRefinements,
    ]);
  }
  assert.deepEqual(runtimeRefinements("ProviderAttemptReport"), [
    "error.attemptId must equal attemptId",
    ...retryLinkRefinements,
    "occurredAtMs must not precede startedAtMs",
    "rawReferences ordinals must be unique",
  ]);

  const runtimeOnlySchemas = [
    "ArchiveGap",
    "CaptionPacket",
    "GrpcProviderAttemptTerminal",
    "HttpProviderAttemptTerminal",
    "ProviderAttemptReport",
    "ProviderAttemptTerminal",
    "RoomProviderSelection",
    "SameLanguageBypassStatus",
    "SampleRange",
    "WebSocketProviderAttemptTerminal",
    "WorkerCheckpoint",
    "WorkerJobMetadata",
  ];
  for (const name of runtimeOnlySchemas) assert.ok(runtimeRefinements(name).length > 0);
});

test("generated artifact registers every public transport and raw evidence schema", () => {
  assert.deepEqual(
    Object.keys(generatedBundle.schemas).sort(),
    Object.keys(CONTRACT_SCHEMAS).sort(),
  );
  for (const name of [
    "OrderedHeader",
    "WebSocketFrameReference",
    "GrpcMessageReference",
    "HttpRawReference",
    "WebSocketRawReference",
    "GrpcRawReference",
    "RawTransportReference",
    "HttpProviderAttemptTerminal",
    "WebSocketProviderAttemptTerminal",
    "GrpcProviderAttemptTerminal",
    "ProviderAttemptRawReference",
  ]) {
    assert.ok(generatedBundle.schemas[name], `${name} missing from generated artifact`);
  }
});

test("database literal schemas reject unapproved states and roles", () => {
  assert.deepEqual(StaffRoleSchema.options, ["employee", "admin"]);
  assert.equal(NullableStaffRoleSchema.parse(null), null);
  assert.deepEqual(MagicLinkPurposeSchema.options, [
    "sign_in",
    "consultation_invite",
    "archive_delete_reauth",
  ]);
  assert.deepEqual(ParticipantRoleSchema.options, ["employee", "customer"]);
  assert.deepEqual(ConsultationStateSchema.options, [
    "invited",
    "ready",
    "active",
    "finalizing",
    "ended",
    "cancelled",
    "deleted",
  ]);
  assert.deepEqual(ArchiveStateSchema.options, [
    "pending",
    "recording",
    "reconciling",
    "complete",
    "incomplete",
    "deleting",
    "deleted",
  ]);
  assert.deepEqual(ExternalEffectStateSchema.options, [
    "planned",
    "calling",
    "applied",
    "compensating",
    "done",
    "failed",
  ]);
  assert.throws(() => ConsultationStateSchema.parse("completed"));
});
