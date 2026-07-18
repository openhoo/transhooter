import { test } from "bun:test";
import assert from "node:assert/strict";
import {
  ArchiveObjectRecordSchema,
  ArchiveRecordSchema,
  ArchiveStateSchema,
  CAPTION_TOPIC,
  CaptionPacketSchema,
  ConsultationStateSchema,
  ExternalEffectStateSchema,
  FinalInventorySchema,
  InterpretationTrackNameSchema,
  InventorySupplementSchema,
  MagicLinkPurposeSchema,
  NullableStaffRoleSchema,
  ParticipantAttributesSchema,
  ParticipantRoleSchema,
  PcmSidecarSchema,
  ProviderAttemptReportSchema,
  ProviderAttemptTerminalSchema,
  RoomProviderSelectionSchema,
  SampleRangeSchema,
  STATUS_TOPIC,
  StaffRoleSchema,
  StatusPacketSchema,
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
  assert.throws(() => StatusPacketSchema.parse({ ...variants[2], state: "ready" }));
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
      rawReferences: [...report.rawReferences, report.rawReferences[0]],
    }),
  );
  assert.throws(() =>
    ProviderAttemptReportSchema.parse({
      ...report,
      unexpectedProvider: "untrusted",
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
      compatibilityWatermark: 4000,
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
  assert.deepEqual(parsedSupplement, supplement);
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
