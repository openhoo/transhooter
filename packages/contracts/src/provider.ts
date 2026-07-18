import { z } from "zod";

import {
  NonNegativeIntegerSchema,
  NullableUuidSchema,
  OpaqueKeySchema,
  PositiveIntegerSchema,
  Sha256Schema,
  UuidSchema,
} from "./primitives";

export const CredentialReferenceSchema = z
  .object({
    reference: z.string().min(1),
    version: z.string().min(1),
  })
  .strict();
export type CredentialReference = z.infer<typeof CredentialReferenceSchema>;

const FrozenStageSchema = z
  .object({
    provider: z.string().min(1),
    endpoint: z.url(),
    region: z.string().min(1),
    model: z.string().min(1),
    adapterBuild: z.string().min(1),
    policy: z.string().min(1),
    credential: CredentialReferenceSchema,
    limits: z.record(z.string(), z.number().nonnegative()),
  })
  .strict();

export const SttSelectionSchema = FrozenStageSchema.extend({
  locale: z.string().min(1),
  encoding: z.string().min(1),
}).strict();
export type SttSelection = z.infer<typeof SttSelectionSchema>;

export const TranslationSelectionSchema = FrozenStageSchema.extend({
  sourceCode: z.string().min(1),
  targetCode: z.string().min(1),
}).strict();
export type TranslationSelection = z.infer<typeof TranslationSelectionSchema>;

export const TtsSelectionSchema = FrozenStageSchema.extend({
  locale: z.string().min(1),
  voice: z.string().min(1),
  encoding: z.string().min(1),
  sampleRate: PositiveIntegerSchema,
}).strict();
export type TtsSelection = z.infer<typeof TtsSelectionSchema>;

export const SameLanguageDirectionSelectionSchema = z
  .object({
    mode: z.literal("same_language"),
    sourceParticipantId: UuidSchema,
    destinationParticipantId: UuidSchema,
    capabilityRowId: UuidSchema,
    stt: SttSelectionSchema,
    bypass: z.literal(true),
  })
  .strict();
export type SameLanguageDirectionSelection = z.infer<typeof SameLanguageDirectionSelectionSchema>;

export const TranslatedDirectionSelectionSchema = z
  .object({
    mode: z.literal("translated"),
    sourceParticipantId: UuidSchema,
    destinationParticipantId: UuidSchema,
    capabilityRowId: UuidSchema,
    stt: SttSelectionSchema,
    targetCode: z.string().min(1),
    translation: TranslationSelectionSchema,
    tts: TtsSelectionSchema,
  })
  .strict();
export type TranslatedDirectionSelection = z.infer<typeof TranslatedDirectionSelectionSchema>;

export const DirectionSelectionSchema = z.discriminatedUnion("mode", [
  SameLanguageDirectionSelectionSchema,
  TranslatedDirectionSelectionSchema,
]);
export type DirectionSelection = z.infer<typeof DirectionSelectionSchema>;

export const RoomProviderSelectionSchema = z
  .object({
    profileId: z.string().regex(/^[a-z0-9]+(?:-[a-z0-9]+)*$/u),
    profileRevision: PositiveIntegerSchema,
    capabilityHash: Sha256Schema,
    participantIds: z.tuple([UuidSchema, UuidSchema]),
    directions: z.tuple([DirectionSelectionSchema, DirectionSelectionSchema]),
  })
  .strict()
  .superRefine(({ participantIds, directions }, context) => {
    if (participantIds[0] === participantIds[1]) {
      context.addIssue({
        code: "custom",
        message: "participants must be distinct",
        path: ["participantIds", 1],
      });
    }
    const expected = new Set(participantIds);
    const sources = new Set(directions.map(({ sourceParticipantId }) => sourceParticipantId));
    for (const [index, direction] of directions.entries()) {
      if (
        !expected.has(direction.sourceParticipantId) ||
        !expected.has(direction.destinationParticipantId)
      ) {
        context.addIssue({
          code: "custom",
          message: "direction participants must belong to the room",
          path: ["directions", index],
        });
      }
      if (direction.sourceParticipantId === direction.destinationParticipantId) {
        context.addIssue({
          code: "custom",
          message: "direction endpoints must differ",
          path: ["directions", index],
        });
      }
    }
    if (sources.size !== 2 || !participantIds.every((id) => sources.has(id))) {
      context.addIssue({
        code: "custom",
        message: "exactly one direction per source participant",
        path: ["directions"],
      });
    }
  });
export type RoomProviderSelection = z.infer<typeof RoomProviderSelectionSchema>;

export const WorkerJobMetadataSchema = z
  .object({
    schemaVersion: z.literal(1),
    consultationId: UuidSchema,
    generation: PositiveIntegerSchema,
    roomName: UuidSchema,
    workerIdentity: UuidSchema,
    workerEpoch: PositiveIntegerSchema,
    writeEpoch: NonNegativeIntegerSchema,
    expectedParticipantIds: z.tuple([UuidSchema, UuidSchema]),
    expectedLivekitIdentities: z.tuple([UuidSchema, UuidSchema]),
    providerSelection: RoomProviderSelectionSchema,
    snapshotHash: Sha256Schema,
    adoptionId: UuidSchema.optional(),
  })
  .strict()
  .superRefine((metadata, context) => {
    if (metadata.expectedParticipantIds[0] === metadata.expectedParticipantIds[1]) {
      context.addIssue({
        code: "custom",
        message: "expected participant IDs must be distinct",
        path: ["expectedParticipantIds", 1],
      });
    }
    if (metadata.expectedLivekitIdentities[0] === metadata.expectedLivekitIdentities[1]) {
      context.addIssue({
        code: "custom",
        message: "expected LiveKit identities must be distinct",
        path: ["expectedLivekitIdentities", 1],
      });
    }
    if (metadata.expectedLivekitIdentities.includes(metadata.workerIdentity)) {
      context.addIssue({
        code: "custom",
        message: "worker identity must be absent from expected human identities",
        path: ["workerIdentity"],
      });
    }
    const expectedIds = new Set(metadata.expectedParticipantIds);
    if (!metadata.providerSelection.participantIds.every((id) => expectedIds.has(id))) {
      context.addIssue({
        code: "custom",
        message: "expected participant IDs must match the frozen provider selection",
        path: ["expectedParticipantIds"],
      });
    }
  });
export type WorkerJobMetadata = z.infer<typeof WorkerJobMetadataSchema>;

export const OrderedHeaderSchema = z
  .object({
    name: z.string().min(1),
    value: z.string(),
    redacted: z.boolean(),
    secretReference: z.string().min(1).nullable(),
  })
  .strict()
  .refine((header) => header.redacted === (header.secretReference !== null), {
    message: "redacted headers require a secret reference and unredacted headers forbid one",
  });
export type OrderedHeader = z.infer<typeof OrderedHeaderSchema>;

const RawArtifactSchema = z
  .object({
    objectId: UuidSchema,
    key: OpaqueKeySchema,
    sha256: Sha256Schema,
    size: NonNegativeIntegerSchema,
  })
  .strict();

export const HttpRawReferenceSchema = z
  .object({
    transport: z.literal("http"),
    method: z.string().min(1),
    url: z.url(),
    status: z.number().int().min(100).max(599).nullable(),
    requestHeaders: z.array(OrderedHeaderSchema),
    requestBody: RawArtifactSchema,
    responseHeaders: z.array(OrderedHeaderSchema),
    responseBody: RawArtifactSchema.nullable(),
  })
  .strict();
export type HttpRawReference = z.infer<typeof HttpRawReferenceSchema>;

export const WebSocketFrameReferenceSchema = z
  .object({
    ordinal: NonNegativeIntegerSchema,
    direction: z.enum(["sent", "received"]),
    opcode: z.enum(["continuation", "text", "binary", "close", "ping", "pong"]),
    fin: z.boolean(),
    payload: RawArtifactSchema,
  })
  .strict();
export type WebSocketFrameReference = z.infer<typeof WebSocketFrameReferenceSchema>;

export const WebSocketRawReferenceSchema = z
  .object({
    transport: z.literal("websocket"),
    url: z.url(),
    upgradeRequestHeaders: z.array(OrderedHeaderSchema),
    upgradeResponseHeaders: z.array(OrderedHeaderSchema),
    frames: z.array(WebSocketFrameReferenceSchema),
    closeCode: z.number().int().min(0).max(4999).nullable(),
    closeReason: z.string().nullable(),
  })
  .strict();
export type WebSocketRawReference = z.infer<typeof WebSocketRawReferenceSchema>;

export const GrpcMessageReferenceSchema = z
  .object({
    ordinal: NonNegativeIntegerSchema,
    direction: z.enum(["sent", "received"]),
    payload: RawArtifactSchema,
  })
  .strict();
export type GrpcMessageReference = z.infer<typeof GrpcMessageReferenceSchema>;

export const GrpcRawReferenceSchema = z
  .object({
    transport: z.literal("grpc"),
    method: z.string().min(1),
    messages: z.array(GrpcMessageReferenceSchema),
    initialMetadata: z.array(OrderedHeaderSchema),
    statusCode: z.string().min(1).nullable(),
    statusDetails: z.string().nullable(),
    trailingMetadata: z.array(OrderedHeaderSchema),
  })
  .strict();
export type GrpcRawReference = z.infer<typeof GrpcRawReferenceSchema>;

export const RawTransportReferenceSchema = z.discriminatedUnion("transport", [
  HttpRawReferenceSchema,
  WebSocketRawReferenceSchema,
  GrpcRawReferenceSchema,
]);
export type RawTransportReference = z.infer<typeof RawTransportReferenceSchema>;

export const ProviderErrorSchema = z
  .object({
    kind: z.enum([
      "authentication",
      "quota",
      "rate_limit",
      "transport",
      "provider",
      "invalid_request",
      "invalid_response",
      "internal",
      "cancelled",
    ]),
    scope: z.enum(["operation", "session"]),
    providerRetryAdvice: z.enum(["never", "retry_after", "unspecified"]),
    providerCode: z.string().nullable(),
    providerRequestId: z.string().nullable(),
    retryDelayMs: NonNegativeIntegerSchema.nullable(),
    attemptId: UuidSchema,
    rawObjectIds: z.array(UuidSchema),
  })
  .strict();
export type ProviderError = z.infer<typeof ProviderErrorSchema>;

export const RetryDecisionSchema = z
  .object({
    action: z.enum(["retry", "do_not_retry", "degrade"]),
    reason: z.string().min(1),
    retryAtMs: NonNegativeIntegerSchema.nullable(),
    previousAttemptId: NullableUuidSchema,
  })
  .strict()
  .superRefine((decision, context) => {
    const isRetry = decision.action === "retry";
    const hasRetryTime = decision.retryAtMs !== null;
    if (isRetry !== hasRetryTime) {
      context.addIssue({
        code: "custom",
        message: "only retries have retryAtMs",
        path: ["retryAtMs"],
      });
    }
  });
export type RetryDecision = z.infer<typeof RetryDecisionSchema>;

export const ProviderWatermarksSchema = z
  .object({
    acceptedInputSequence: NonNegativeIntegerSchema.nullable(),
    acceptedInputSampleEnd: NonNegativeIntegerSchema.nullable(),
    receivedOutputSequence: NonNegativeIntegerSchema.nullable(),
    receivedOutputSampleEnd: NonNegativeIntegerSchema.nullable(),
    emittedOutputSequence: NonNegativeIntegerSchema.nullable(),
    emittedOutputSampleEnd: NonNegativeIntegerSchema.nullable(),
  })
  .strict();
export type ProviderWatermarks = z.infer<typeof ProviderWatermarksSchema>;

const ProviderAttemptTerminalCommonShape = {
  terminalId: UuidSchema,
  operationId: UuidSchema,
  attemptId: UuidSchema,
  stage: z.enum(["stt", "translation", "tts"]),
  outcome: z.enum(["succeeded", "failed", "cancelled"]),
  error: ProviderErrorSchema.nullable(),
  retryDecision: RetryDecisionSchema,
  retryOfAttemptId: NullableUuidSchema,
  watermarks: ProviderWatermarksSchema,
  credentialVersion: z.string().min(1),
  credentialFingerprint: z.string().min(1),
  occurredAtMs: NonNegativeIntegerSchema,
};

export const HttpProviderAttemptTerminalSchema = z
  .object({
    ...ProviderAttemptTerminalCommonShape,
    transport: z.literal("http"),
    rawReference: HttpRawReferenceSchema,
  })
  .strict();
export const WebSocketProviderAttemptTerminalSchema = z
  .object({
    ...ProviderAttemptTerminalCommonShape,
    transport: z.literal("websocket"),
    rawReference: WebSocketRawReferenceSchema,
  })
  .strict();
export const GrpcProviderAttemptTerminalSchema = z
  .object({
    ...ProviderAttemptTerminalCommonShape,
    transport: z.literal("grpc"),
    rawReference: GrpcRawReferenceSchema,
  })
  .strict();

export const ProviderAttemptTerminalSchema = z
  .discriminatedUnion("transport", [
    HttpProviderAttemptTerminalSchema,
    WebSocketProviderAttemptTerminalSchema,
    GrpcProviderAttemptTerminalSchema,
  ])
  .superRefine((terminal, context) => {
    const didFail = terminal.outcome === "failed";
    const hasError = terminal.error !== null;
    if (didFail !== hasError) {
      context.addIssue({
        code: "custom",
        message: "only failed terminals carry an error",
        path: ["error"],
      });
    }
    if (terminal.retryDecision.previousAttemptId !== terminal.retryOfAttemptId) {
      context.addIssue({
        code: "custom",
        message: "retry links must agree",
        path: ["retryOfAttemptId"],
      });
    }
  });
export type ProviderAttemptTerminal = z.infer<typeof ProviderAttemptTerminalSchema>;

export const ProviderAttemptRawReferenceSchema = z
  .object({
    objectId: UuidSchema,
    ordinal: NonNegativeIntegerSchema,
    sha256: Sha256Schema,
    size: NonNegativeIntegerSchema,
    mediaType: z.string().min(1),
  })
  .strict();
export type ProviderAttemptRawReference = z.infer<typeof ProviderAttemptRawReferenceSchema>;

export const ProviderAttemptReportSchema = z
  .object({
    directionId: UuidSchema,
    stage: z.enum(["stt", "translation", "tts"]),
    terminalId: UuidSchema,
    operationId: UuidSchema,
    attemptId: UuidSchema,
    attemptNumber: PositiveIntegerSchema,
    retryOfAttemptId: NullableUuidSchema,
    outcome: z.enum(["succeeded", "failed", "cancelled"]),
    error: ProviderErrorSchema.nullable(),
    retryDecision: RetryDecisionSchema,
    watermarks: ProviderWatermarksSchema,
    credentialVersion: z.string().min(1),
    credentialFingerprint: z.string().min(1),
    transport: z.enum(["http", "websocket", "grpc"]),
    rawReferences: z.array(ProviderAttemptRawReferenceSchema),
    terminalHash: Sha256Schema,
    startedAtMs: NonNegativeIntegerSchema,
    occurredAtMs: NonNegativeIntegerSchema,
  })
  .strict()
  .superRefine((report, context) => {
    if ((report.outcome === "failed") !== (report.error !== null)) {
      context.addIssue({
        code: "custom",
        message: "only failed attempts carry an error",
        path: ["error"],
      });
    }
    if (report.error !== null && report.error.attemptId !== report.attemptId) {
      context.addIssue({
        code: "custom",
        message: "error attemptId must match the reported attempt",
        path: ["error", "attemptId"],
      });
    }
    if (report.occurredAtMs < report.startedAtMs) {
      context.addIssue({
        code: "custom",
        message: "occurredAtMs must not precede startedAtMs",
        path: ["occurredAtMs"],
      });
    }
    const firstAttempt = report.attemptNumber === 1;
    if (firstAttempt !== (report.retryOfAttemptId === null)) {
      context.addIssue({
        code: "custom",
        message: "only the first attempt omits retryOfAttemptId",
        path: ["retryOfAttemptId"],
      });
    }
    const ordinals = new Set<number>();
    for (const [index, reference] of report.rawReferences.entries()) {
      if (ordinals.has(reference.ordinal)) {
        context.addIssue({
          code: "custom",
          message: "raw reference ordinals must be unique",
          path: ["rawReferences", index, "ordinal"],
        });
      }
      ordinals.add(reference.ordinal);
    }
  });
export type ProviderAttemptReport = z.infer<typeof ProviderAttemptReportSchema>;
