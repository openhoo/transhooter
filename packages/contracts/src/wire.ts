import { z } from "zod";

import { NonNegativeIntegerSchema, PositiveIntegerSchema, UuidSchema } from "./primitives";

export const CAPTION_TOPIC = "consultation.translation.v1" as const;
export const STATUS_TOPIC = "consultation.status.v1" as const;
export const InterpretationTrackNameSchema = z
  .string()
  .regex(
    /^interpretation:[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u,
  );
export type InterpretationTrackName = z.infer<typeof InterpretationTrackNameSchema>;

export const CaptionFinalitySchema = z.enum(["provisional", "final"]);
export type CaptionFinality = z.infer<typeof CaptionFinalitySchema>;

export const CaptionPacketSchema = z
  .object({
    schemaVersion: z.literal(1),
    consultationId: UuidSchema,
    destinationParticipantId: UuidSchema,
    sourceParticipantId: UuidSchema,
    utteranceId: UuidSchema,
    revision: PositiveIntegerSchema,
    finality: CaptionFinalitySchema,
    sourceLanguage: z.string().min(1),
    targetLanguage: z.string().min(1),
    sourceText: z.string(),
    translatedText: z.string(),
    sourceSampleStart: NonNegativeIntegerSchema,
    sourceSampleEnd: PositiveIntegerSchema,
    occurredAtMs: NonNegativeIntegerSchema,
  })
  .strict()
  .superRefine((packet, context) => {
    if (packet.sourceParticipantId === packet.destinationParticipantId) {
      context.addIssue({
        code: "custom",
        message: "caption source and destination must differ",
        path: ["destinationParticipantId"],
      });
    }
    if (packet.sourceSampleEnd <= packet.sourceSampleStart) {
      context.addIssue({
        code: "custom",
        message: "source sample bounds are inclusive-exclusive",
        path: ["sourceSampleEnd"],
      });
    }
  });
export type CaptionPacket = z.infer<typeof CaptionPacketSchema>;

const StatusCommonShape = {
  schemaVersion: z.literal(1),
  consultationId: UuidSchema,
  generation: PositiveIntegerSchema,
  occurredAtMs: NonNegativeIntegerSchema,
};

export const CaptureReadyStatusSchema = z
  .object({
    ...StatusCommonShape,
    state: z.enum(["ready", "active"]),
    reasonCode: z.literal("CAPTURE_READY"),
    subjectParticipantId: UuidSchema,
    participantEgressId: z.string().min(1),
    shutdownAtMs: z.null(),
  })
  .strict();
export type CaptureReadyStatus = z.infer<typeof CaptureReadyStatusSchema>;

export const SameLanguageBypassStatusSchema = z
  .object({
    ...StatusCommonShape,
    state: z.enum(["ready", "active"]),
    reasonCode: z.literal("SAME_LANGUAGE_BYPASS"),
    sourceParticipantId: UuidSchema,
    destinationParticipantId: UuidSchema,
    shutdownAtMs: z.null(),
  })
  .strict()
  .refine((status) => status.sourceParticipantId !== status.destinationParticipantId, {
    message: "status source and destination must differ",
    path: ["destinationParticipantId"],
  });
export type SameLanguageBypassStatus = z.infer<typeof SameLanguageBypassStatusSchema>;

export const ArchiveFailedStatusSchema = z
  .object({
    ...StatusCommonShape,
    state: z.enum(["active", "finalizing"]),
    reasonCode: z.literal("ARCHIVE_FAILED"),
    shutdownAtMs: NonNegativeIntegerSchema,
  })
  .strict();
export type ArchiveFailedStatus = z.infer<typeof ArchiveFailedStatusSchema>;

export const ShutdownStatusSchema = z
  .object({
    ...StatusCommonShape,
    state: z.literal("finalizing"),
    reasonCode: z.literal("SHUTDOWN"),
    shutdownAtMs: NonNegativeIntegerSchema,
  })
  .strict();
export type ShutdownStatus = z.infer<typeof ShutdownStatusSchema>;

export const StatusPacketSchema = z.discriminatedUnion("reasonCode", [
  CaptureReadyStatusSchema,
  SameLanguageBypassStatusSchema,
  ArchiveFailedStatusSchema,
  ShutdownStatusSchema,
]);
export type StatusPacket = z.infer<typeof StatusPacketSchema>;
