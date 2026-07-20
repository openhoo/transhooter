import { z } from "zod";

export const UuidSchema = z.uuid();
export const NonNegativeIntegerSchema = z.number().int().nonnegative();
export const PositiveIntegerSchema = z.number().int().positive();
export const Sha256Schema = z.string().regex(/^[a-f0-9]{64}$/u, "expected lowercase SHA-256 hex");
export const OpaqueKeySchema = z
  .string()
  .min(1)
  .max(1024)
  .regex(
    /^v1\/meetings\/[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\/[a-z0-9][a-z0-9._/-]*$/u,
  );
export const NullableUuidSchema = UuidSchema.nullable();

export const STAFF_ROLE_VALUES = ["employee", "admin"] as const;
export const MAGIC_LINK_PURPOSE_VALUES = [
  "sign_in",
  "consultation_invite",
  "archive_delete_reauth",
] as const;
export const PARTICIPANT_ROLE_VALUES = ["employee", "customer"] as const;
export const CONSULTATION_STATE_VALUES = [
  "invited",
  "ready",
  "active",
  "finalizing",
  "ended",
  "cancelled",
  "deleted",
] as const;
export const ARCHIVE_STATE_VALUES = [
  "pending",
  "recording",
  "reconciling",
  "complete",
  "incomplete",
  "deleting",
  "deleted",
] as const;
export const EXTERNAL_EFFECT_STATE_VALUES = [
  "planned",
  "calling",
  "applied",
  "compensating",
  "done",
  "failed",
] as const;

export const StaffRoleSchema = z.enum(STAFF_ROLE_VALUES);
export type StaffRole = (typeof STAFF_ROLE_VALUES)[number];

export const NullableStaffRoleSchema = StaffRoleSchema.nullable();
export type NullableStaffRole = z.infer<typeof NullableStaffRoleSchema>;

export const MagicLinkPurposeSchema = z.enum(MAGIC_LINK_PURPOSE_VALUES);
export type MagicLinkPurpose = (typeof MAGIC_LINK_PURPOSE_VALUES)[number];

export const ParticipantRoleSchema = z.enum(PARTICIPANT_ROLE_VALUES);
export type ParticipantRole = (typeof PARTICIPANT_ROLE_VALUES)[number];

export const ConsultationStateSchema = z.enum(CONSULTATION_STATE_VALUES);
export type ConsultationState = (typeof CONSULTATION_STATE_VALUES)[number];

export const ArchiveStateSchema = z.enum(ARCHIVE_STATE_VALUES);
export type ArchiveState = (typeof ARCHIVE_STATE_VALUES)[number];

export const ExternalEffectStateSchema = z.enum(EXTERNAL_EFFECT_STATE_VALUES);
export type ExternalEffectState = (typeof EXTERNAL_EFFECT_STATE_VALUES)[number];

export const ParticipantAttributesSchema = z
  .object({
    "consultation.id": UuidSchema,
    "consultation.role": ParticipantRoleSchema,
    "consultation.language": z.string().min(1),
  })
  .strict();
export type ParticipantAttributes = z.infer<typeof ParticipantAttributesSchema>;

export const ParticipantSchema = z
  .object({
    participantId: UuidSchema,
    consultationId: UuidSchema,
    role: ParticipantRoleSchema,
    displayName: z.string().min(1),
    language: z.string().min(1).nullable(),
    livekitIdentity: UuidSchema,
  })
  .strict();
export type Participant = z.infer<typeof ParticipantSchema>;

export const SampleRangeSchema = z
  .object({
    start: NonNegativeIntegerSchema,
    end: PositiveIntegerSchema,
  })
  .strict()
  .refine(({ start, end }) => end > start, {
    message: "sample range is inclusive-exclusive and end must exceed start",
    path: ["end"],
  });
export type SampleRange = z.infer<typeof SampleRangeSchema>;
