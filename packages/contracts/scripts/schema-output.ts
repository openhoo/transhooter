import { z } from "zod";
import {
  ArchiveGapSchema,
  CaptionPacketSchema,
  CONTRACT_SCHEMAS,
  FinalInventorySchema,
  GrpcProviderAttemptTerminalSchema,
  HttpProviderAttemptTerminalSchema,
  HttpRawReferenceSchema,
  OrderedHeaderSchema,
  ProviderAttemptReportSchema,
  ProviderAttemptTerminalSchema,
  RetryDecisionSchema,
  RoomProviderSelectionSchema,
  SameLanguageBypassStatusSchema,
  SampleRangeSchema,
  WebSocketProviderAttemptTerminalSchema,
  WebSocketRawReferenceSchema,
  WorkerCheckpointSchema,
  WorkerJobMetadataSchema,
} from "../src/index";

type JsonObject = Record<string, unknown>;

type RefinementOverride = {
  readonly schema: z.ZodType;
  readonly constraints?: JsonObject;
  readonly runtimeOnly?: readonly string[];
};

const nonNull = { not: { type: "null" } };
const terminalErrorConstraints: JsonObject = {
  if: { properties: { outcome: { const: "failed" } }, required: ["outcome"] },
  // biome-ignore lint/suspicious/noThenProperty: Draft 2020-12 requires the `then` keyword.
  then: { properties: { error: nonNull }, required: ["error"] },
  else: { properties: { error: { type: "null" } }, required: ["error"] },
};
const terminalRuntimeRefinements = [
  "error.attemptId must equal attemptId",
  "retryDecision.previousAttemptId must be null or equal attemptId",
  "retry action requires retryDecision.previousAttemptId to equal attemptId",
  "retryOfAttemptId must differ from attemptId",
] as const;

/**
 * Zod intentionally omits refinements from JSON Schema. Every public refined
 * contract is therefore listed here: representable rules become Draft 2020-12
 * constraints and value-to-value comparisons are disclosed explicitly rather
 * than being silently weakened.
 */
export const REFINEMENT_OVERRIDES: Readonly<Record<string, RefinementOverride>> = {
  ArchiveGap: {
    schema: ArchiveGapSchema,
    constraints: {
      anyOf: [
        { properties: { sampleRange: nonNull }, required: ["sampleRange"] },
        {
          properties: { segmentStart: nonNull, segmentEnd: nonNull },
          required: ["segmentStart", "segmentEnd"],
        },
      ],
    },
    runtimeOnly: ["segmentEnd must not precede segmentStart"],
  },
  CaptionPacket: {
    schema: CaptionPacketSchema,
    runtimeOnly: [
      "sourceParticipantId must differ from destinationParticipantId",
      "sourceSampleEnd must exceed sourceSampleStart",
    ],
  },
  FinalInventory: {
    schema: FinalInventorySchema,
    constraints: {
      if: { properties: { status: { const: "complete" } }, required: ["status"] },
      // biome-ignore lint/suspicious/noThenProperty: Draft 2020-12 requires the `then` keyword.
      then: {
        properties: {
          workerTerminal: {
            properties: { outcome: { const: "clean" } },
            required: ["outcome"],
          },
          egressResults: {
            items: {
              properties: {
                outcome: { const: "complete" },
                gaps: { maxItems: 0 },
              },
              required: ["outcome", "gaps"],
            },
          },
          missing: { maxItems: 0 },
          errors: { maxItems: 0 },
        },
        required: ["workerTerminal", "egressResults", "missing", "errors"],
      },
    },
  },
  GrpcProviderAttemptTerminal: {
    schema: GrpcProviderAttemptTerminalSchema,
    constraints: terminalErrorConstraints,
    runtimeOnly: terminalRuntimeRefinements,
  },
  HttpProviderAttemptTerminal: {
    schema: HttpProviderAttemptTerminalSchema,
    constraints: terminalErrorConstraints,
    runtimeOnly: terminalRuntimeRefinements,
  },
  OrderedHeader: {
    schema: OrderedHeaderSchema,
    constraints: {
      oneOf: [
        {
          properties: { redacted: { const: true }, secretReference: nonNull },
          required: ["redacted", "secretReference"],
        },
        {
          properties: { redacted: { const: false }, secretReference: { type: "null" } },
          required: ["redacted", "secretReference"],
        },
      ],
    },
  },
  ProviderAttemptReport: {
    schema: ProviderAttemptReportSchema,
    constraints: {
      allOf: [
        terminalErrorConstraints,
        {
          if: { properties: { attemptNumber: { const: 1 } }, required: ["attemptNumber"] },
          // biome-ignore lint/suspicious/noThenProperty: Draft 2020-12 requires the `then` keyword.
          then: {
            properties: { retryOfAttemptId: { type: "null" } },
            required: ["retryOfAttemptId"],
          },
          else: {
            properties: { retryOfAttemptId: nonNull },
            required: ["retryOfAttemptId"],
          },
        },
      ],
    },
    runtimeOnly: [
      "error.attemptId must equal attemptId",
      "retryDecision.previousAttemptId must be null or equal attemptId",
      "retry action requires retryDecision.previousAttemptId to equal attemptId",
      "retryOfAttemptId must differ from attemptId",
      "occurredAtMs must not precede startedAtMs",
      "rawReferences ordinals must be unique",
    ],
  },
  ProviderAttemptTerminal: {
    schema: ProviderAttemptTerminalSchema,
    constraints: terminalErrorConstraints,
    runtimeOnly: terminalRuntimeRefinements,
  },
  RetryDecision: {
    schema: RetryDecisionSchema,
    constraints: {
      if: { properties: { action: { const: "retry" } }, required: ["action"] },
      // biome-ignore lint/suspicious/noThenProperty: Draft 2020-12 requires the `then` keyword.
      then: { properties: { retryAtMs: nonNull }, required: ["retryAtMs"] },
      else: { properties: { retryAtMs: { type: "null" } }, required: ["retryAtMs"] },
    },
  },
  RoomProviderSelection: {
    schema: RoomProviderSelectionSchema,
    runtimeOnly: [
      "participantIds must be distinct",
      "direction endpoints must be distinct room participants",
      "there must be exactly one direction per source participant",
    ],
  },
  SameLanguageBypassStatus: {
    schema: SameLanguageBypassStatusSchema,
    runtimeOnly: ["sourceParticipantId must differ from destinationParticipantId"],
  },
  SampleRange: {
    schema: SampleRangeSchema,
    runtimeOnly: ["end must exceed start"],
  },
  WebSocketProviderAttemptTerminal: {
    schema: WebSocketProviderAttemptTerminalSchema,
    constraints: terminalErrorConstraints,
    runtimeOnly: terminalRuntimeRefinements,
  },
  WorkerCheckpoint: {
    schema: WorkerCheckpointSchema,
    runtimeOnly: ["sourceParticipantId must differ from destinationParticipantId"],
  },
  WorkerJobMetadata: {
    schema: WorkerJobMetadataSchema,
    runtimeOnly: [
      "expected participant IDs and LiveKit identities must each be distinct",
      "worker identity must differ from human identities",
      "expected participant IDs must match the provider selection",
    ],
  },
};

const TRANSPORT_OVERRIDES: Readonly<Record<string, RefinementOverride>> = {
  HttpRawReference: {
    schema: HttpRawReferenceSchema,
    constraints: {
      allOf: [
        {
          properties: {
            url: { type: "string", format: "uri", pattern: "^[Hh][Tt][Tt][Pp][Ss]?://" },
          },
        },
      ],
    },
  },
  WebSocketRawReference: {
    schema: WebSocketRawReferenceSchema,
    constraints: {
      allOf: [
        {
          properties: {
            url: { type: "string", format: "uri", pattern: "^[Ww][Ss][Ss]?://" },
          },
        },
      ],
    },
  },
};

const ALL_OVERRIDES = [
  ...Object.values(REFINEMENT_OVERRIDES),
  ...Object.values(TRANSPORT_OVERRIDES),
];

function sortJson(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortJson);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
        .map(([key, nested]) => [key, sortJson(nested)]),
    );
  }
  return value;
}

function mergeOverride(target: JsonObject, override: RefinementOverride): void {
  if (override.constraints !== undefined) {
    for (const [key, value] of Object.entries(override.constraints)) {
      if (key === "allOf" && Array.isArray(target.allOf) && Array.isArray(value)) {
        target.allOf = [...target.allOf, ...value];
      } else {
        target[key] = value;
      }
    }
  }
  if (override.runtimeOnly !== undefined) {
    target["x-transhooter-runtime-refinements"] = [...override.runtimeOnly];
  }
}

function validateOverrideInventory(): void {
  for (const [name, override] of Object.entries({
    ...REFINEMENT_OVERRIDES,
    ...TRANSPORT_OVERRIDES,
  })) {
    if (CONTRACT_SCHEMAS[name as keyof typeof CONTRACT_SCHEMAS] !== override.schema) {
      throw new Error(`JSON Schema override ${name} does not match a registered public contract`);
    }
    if (override.constraints === undefined && override.runtimeOnly === undefined) {
      throw new Error(`JSON Schema override ${name} handles no refinement`);
    }
  }
}

export function generatedSchemaText(): string {
  validateOverrideInventory();
  const schemas = Object.fromEntries(
    Object.entries(CONTRACT_SCHEMAS).map(([name, schema]) => [
      name,
      z.toJSONSchema(schema, {
        target: "draft-2020-12",
        unrepresentable: "throw",
        override: ({ zodSchema, jsonSchema }) => {
          for (const override of ALL_OVERRIDES) {
            if (zodSchema === override.schema) mergeOverride(jsonSchema as JsonObject, override);
          }
        },
      }),
    ]),
  );
  return `${JSON.stringify(
    sortJson({
      $schema: "https://json-schema.org/draft/2020-12/schema",
      package: "@transhooter/contracts",
      schemas,
    }),
    null,
    2,
  )}\n`;
}
