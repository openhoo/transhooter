import { z } from "zod";

import {
  NonNegativeIntegerSchema,
  NullableUuidSchema,
  OpaqueKeySchema,
  PositiveIntegerSchema,
  SampleRangeSchema,
  Sha256Schema,
  UuidSchema,
} from "./primitives";

export const ArchiveObjectClassSchema = z.enum([
  "pipeline_exchange",
  "provider_terminal",
  "stt_input_pcm",
  "tts_output_pcm",
  "livekit_output_pcm",
  "pcm_sidecar",
  "caption_ledger",
  "caption_vtt",
  "composite_media",
  "participant_media",
  "track_media",
  "egress_manifest",
  "checkpoint",
  "final_inventory",
  "inventory_supplement",
]);
export type ArchiveObjectClass = z.infer<typeof ArchiveObjectClassSchema>;

export const ArchiveObjectRecordSchema = z
  .object({
    objectId: UuidSchema,
    class: ArchiveObjectClassSchema,
    key: OpaqueKeySchema,
    versionId: z.string().min(1),
    size: NonNegativeIntegerSchema,
    sha256: Sha256Schema,
    s3Checksum: z.string().min(1),
    contentType: z.string().min(1),
    sampleRange: SampleRangeSchema.nullable(),
    attempt: PositiveIntegerSchema.nullable(),
    sequence: NonNegativeIntegerSchema.nullable(),
  })
  .strict();
export type ArchiveObjectRecord = z.infer<typeof ArchiveObjectRecordSchema>;

export const PcmSidecarSchema = z
  .object({
    encoding: z.enum(["LINEAR16", "PCM_S16LE", "PCM_F32LE"]),
    rate: PositiveIntegerSchema,
    channels: PositiveIntegerSchema,
    format: z.enum(["raw", "wav"]),
    sampleRange: SampleRangeSchema,
  })
  .strict();
export type PcmSidecar = z.infer<typeof PcmSidecarSchema>;

export const ArchiveGapSchema = z
  .object({
    owner: z.string().min(1),
    objectClass: ArchiveObjectClassSchema,
    sampleRange: SampleRangeSchema.nullable(),
    segmentStart: NonNegativeIntegerSchema.nullable(),
    segmentEnd: NonNegativeIntegerSchema.nullable(),
    reason: z.string().min(1),
  })
  .strict()
  .superRefine((gap, context) => {
    if (gap.sampleRange === null && (gap.segmentStart === null || gap.segmentEnd === null)) {
      context.addIssue({
        code: "custom",
        message: "gap requires a sample or segment range",
      });
    }
    if (gap.segmentStart !== null && gap.segmentEnd !== null && gap.segmentEnd < gap.segmentStart) {
      context.addIssue({
        code: "custom",
        message: "segmentEnd must not precede segmentStart",
        path: ["segmentEnd"],
      });
    }
  });
export type ArchiveGap = z.infer<typeof ArchiveGapSchema>;

export const WorkerCheckpointSchema = z
  .object({
    checkpointId: UuidSchema,
    workerEpoch: PositiveIntegerSchema,
    sourceParticipantId: UuidSchema,
    destinationParticipantId: UuidSchema,
    acceptedInputSequence: NonNegativeIntegerSchema,
    acceptedInput: NonNegativeIntegerSchema,
    receivedOutput: NonNegativeIntegerSchema,
    emittedOutput: NonNegativeIntegerSchema,
    previousCheckpointSha256: Sha256Schema.nullable(),
    highWatermarkSha256: Sha256Schema,
    expectedObjectIds: z.array(UuidSchema),
    observedObjectIds: z.array(UuidSchema),
    gaps: z.array(ArchiveGapSchema),
    terminal: z.boolean(),
    occurredAtMs: NonNegativeIntegerSchema,
  })
  .strict()
  .refine((checkpoint) => checkpoint.sourceParticipantId !== checkpoint.destinationParticipantId, {
    message: "checkpoint source and destination must differ",
    path: ["destinationParticipantId"],
  });
export type WorkerCheckpoint = z.infer<typeof WorkerCheckpointSchema>;

export const RoomCloseRecordSchema = z
  .object({
    roomId: z.string().min(1),
    generation: PositiveIntegerSchema,
    closedAtMs: NonNegativeIntegerSchema,
    reason: z.string().min(1),
  })
  .strict();
export type RoomCloseRecord = z.infer<typeof RoomCloseRecordSchema>;

export const WorkerTerminalRecordSchema = z
  .object({
    workerEpoch: PositiveIntegerSchema,
    checkpointId: UuidSchema,
    outcome: z.enum(["clean", "fenced", "failed"]),
    occurredAtMs: NonNegativeIntegerSchema,
  })
  .strict();
export type WorkerTerminalRecord = z.infer<typeof WorkerTerminalRecordSchema>;

export const EgressResultSchema = z
  .object({
    egressId: z.string().min(1),
    kind: z.enum(["room_composite", "participant", "track"]),
    subjectParticipantId: NullableUuidSchema,
    outcome: z.enum(["complete", "failed", "aborted", "limit_reached"]),
    objectIds: z.array(UuidSchema),
    gaps: z.array(ArchiveGapSchema),
  })
  .strict();
export type EgressResult = z.infer<typeof EgressResultSchema>;

export const InventoryErrorSchema = z
  .object({
    code: z.string().min(1),
    message: z.string().min(1),
    owner: z.string().min(1).nullable(),
  })
  .strict();
export type InventoryError = z.infer<typeof InventoryErrorSchema>;

export const FinalInventorySchema = z
  .object({
    schemaVersion: z.literal(1),
    inventoryId: UuidSchema,
    consultationId: UuidSchema,
    status: z.enum(["complete", "incomplete"]),
    roomClose: RoomCloseRecordSchema,
    workerTerminal: WorkerTerminalRecordSchema,
    egressResults: z.array(EgressResultSchema),
    objects: z.array(ArchiveObjectRecordSchema),
    missing: z.array(ArchiveGapSchema),
    errors: z.array(InventoryErrorSchema),
    createdAtMs: NonNegativeIntegerSchema,
  })
  .strict()
  .superRefine((inventory, context) => {
    const isComplete = inventory.status === "complete";
    const hasIncompleteWorker = inventory.workerTerminal.outcome !== "clean";
    const hasIncompleteEgress = inventory.egressResults.some(
      (result) => result.outcome !== "complete" || result.gaps.length > 0,
    );
    if (
      isComplete &&
      (inventory.missing.length > 0 ||
        inventory.errors.length > 0 ||
        hasIncompleteWorker ||
        hasIncompleteEgress)
    ) {
      context.addIssue({
        code: "custom",
        message: "complete inventory requires clean, gap-free worker and Egress results",
        path: ["status"],
      });
    }
  });
export type FinalInventory = z.infer<typeof FinalInventorySchema>;

export const InventorySupplementSchema = z
  .object({
    schemaVersion: z.literal(1),
    supplementId: UuidSchema,
    consultationId: UuidSchema,
    finalInventorySha256: Sha256Schema,
    addedObjects: z.array(ArchiveObjectRecordSchema),
    closedGapIndexes: z.array(NonNegativeIntegerSchema),
    errors: z.array(InventoryErrorSchema),
    createdAtMs: NonNegativeIntegerSchema,
  })
  .strict();
export type InventorySupplement = z.infer<typeof InventorySupplementSchema>;

export const ArchiveRecordSchema = z.discriminatedUnion("recordKind", [
  z
    .object({
      recordKind: z.literal("object"),
      value: ArchiveObjectRecordSchema,
    })
    .strict(),
  z
    .object({
      recordKind: z.literal("checkpoint"),
      value: WorkerCheckpointSchema,
    })
    .strict(),
  z
    .object({
      recordKind: z.literal("final_inventory"),
      value: FinalInventorySchema,
    })
    .strict(),
  z
    .object({
      recordKind: z.literal("inventory_supplement"),
      value: InventorySupplementSchema,
    })
    .strict(),
]);
export type ArchiveRecord = z.infer<typeof ArchiveRecordSchema>;
