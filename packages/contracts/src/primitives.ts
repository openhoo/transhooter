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

export const StaffRoleSchema = z.enum(["employee", "admin"]);
export type StaffRole = z.infer<typeof StaffRoleSchema>;

export const NullableStaffRoleSchema = StaffRoleSchema.nullable();
export type NullableStaffRole = z.infer<typeof NullableStaffRoleSchema>;

export const MagicLinkPurposeSchema = z.enum([
  "sign_in",
  "consultation_invite",
  "archive_delete_reauth",
]);
export type MagicLinkPurpose = z.infer<typeof MagicLinkPurposeSchema>;

export const ParticipantRoleSchema = z.enum(["employee", "customer"]);
export type ParticipantRole = z.infer<typeof ParticipantRoleSchema>;

export const ConsultationStateSchema = z.enum([
  "invited",
  "ready",
  "active",
  "finalizing",
  "ended",
  "cancelled",
  "deleted",
]);
export type ConsultationState = z.infer<typeof ConsultationStateSchema>;

export const ArchiveStateSchema = z.enum([
  "pending",
  "recording",
  "reconciling",
  "complete",
  "incomplete",
  "deleting",
  "deleted",
]);
export type ArchiveState = z.infer<typeof ArchiveStateSchema>;

export const ExternalEffectStateSchema = z.enum([
  "planned",
  "calling",
  "applied",
  "compensating",
  "done",
  "failed",
]);
export type ExternalEffectState = z.infer<typeof ExternalEffectStateSchema>;

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
