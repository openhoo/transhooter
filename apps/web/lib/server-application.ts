import "server-only";
import { createHmac, timingSafeEqual } from "node:crypto";
import {
  ArchiveObjectRecordSchema,
  ArchiveStateSchema,
  ConsultationStateSchema,
  FinalInventorySchema,
  ParticipantRoleSchema,
  ProviderAttemptReportSchema,
  RoomProviderSelectionSchema,
  StaffRoleSchema,
  WorkerCheckpointSchema,
} from "@transhooter/contracts";
import { DomainError, type UUID, type WebCommand } from "@transhooter/server-core";
import { cookies, headers } from "next/headers";
import { notFound, redirect } from "next/navigation";
import { z } from "zod";
import { configuredApplication } from "./server/composition";
import { webConfig } from "./server/config";
import { withWebOperation } from "./server/telemetry";
import { durableConsultationDestination } from "./shared/consultation-routing";

const UuidSchema = z.uuid();
const ObjectSchema = z.record(z.string(), z.unknown());
const CapabilityRefreshSchema = z.object({
  profileId: UuidSchema,
  revision: z.number().int().positive(),
  profileName: z.string().regex(/^[a-z0-9]+(?:-[a-z0-9]+)*$/u),
  capabilityHash: z.string().regex(/^[0-9a-f]{64}$/u),
  adapterBuilds: z.unknown(),
  policy: z.unknown(),
  credentialReferences: z.unknown(),
  complete: z.boolean(),
  rows: z.array(
    z.object({
      sourceLocale: z.string().min(1),
      targetLocale: z.string().min(1),
      mode: z.enum(["translated", "same_language"]),
      enabled: z.boolean(),
      snapshot: z.unknown(),
      capabilityHash: z.string().min(1),
      freshUntil: z.coerce.date(),
    }),
  ),
});
const ProviderAttemptEnvelopeSchema = z
  .object({
    consultationId: UuidSchema,
    generation: z.number().int().nonnegative(),
    workerId: UuidSchema,
    epoch: z.number().int().nonnegative(),
    eventId: UuidSchema,
    report: ProviderAttemptReportSchema,
  })
  .strict();
const WorkerFailureEnvelopeSchema = z
  .object({
    consultationId: UuidSchema,
    generation: z.number().int().nonnegative(),
    workerId: UuidSchema,
    epoch: z.number().int().nonnegative(),
    eventId: UuidSchema,
    kind: z.string().min(1).max(120),
    message: z.string().min(1).max(2_000),
    phase: z.string().min(1).max(120).optional(),
    snapshotHash: z
      .string()
      .regex(/^[0-9a-f]{64}$/u)
      .optional(),
    lastCheckpointHashes: z.record(UuidSchema, z.string().regex(/^[0-9a-f]{64}$/u)),
  })
  .strict();
const JSON_BODY_LIMIT_BYTES = 1_048_576;
const WEBHOOK_BODY_LIMIT_BYTES = 1_048_576;
const AuthResultSchema = z.object({
  session: z.object({
    id: UuidSchema,
    userId: UuidSchema,
    tokenHash: z.string(),
    csrfHash: z.string(),
    expiresAt: z.coerce.date(),
    reauthenticatedAt: z.coerce.date().nullable(),
    reauthConsultationId: UuidSchema.nullable(),
  }),
  user: z.object({
    id: UuidSchema,
    email: z.email(),
    displayName: z.string(),
    staffRole: StaffRoleSchema.nullable(),
  }),
});
const ConsultationSchema = z
  .object({
    id: UuidSchema,
    state: ConsultationStateSchema,
    archiveState: ArchiveStateSchema,
    providerProfileId: z.string(),
    providerProfileRevision: z.number().int(),
    participants: z
      .array(
        z
          .object({
            id: UuidSchema,
            role: ParticipantRoleSchema,
            userId: UuidSchema,
            livekitIdentity: UuidSchema,
            displayName: z.string().nullable(),
            language: z.string().nullable(),
            consent: z.object({ snapshotHash: z.string() }).loose().nullable(),
          })
          .loose(),
      )
      .length(2),
    providerSelection: RoomProviderSelectionSchema.nullable(),
    snapshotHash: z.string().nullable(),
    generation: z.number().int(),
    roomName: z.string().nullable(),
    roomSid: z.string().nullable(),
    dispatchId: z.string().nullable(),
    compositeEgressId: z.string().nullable(),
    createdAt: z.coerce.date(),
    updatedAt: z.coerce.date(),
  })
  .loose();

type RequestContext = {
  request: Request;
  params: Record<string, string>;
  query: Record<string, string>;
  body: unknown;
  rawBody: Uint8Array | null;
  sessionToken: string | null;
  csrfToken: string | null;
  exchangeNonce: string | null;
};

type Authenticated = z.infer<typeof AuthResultSchema>;

function displayString(value: unknown, fallback = ""): string {
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  ) {
    return String(value);
  }
  return fallback;
}
function requiredUuid(value: string | undefined, field: string): UUID {
  const parsed = UuidSchema.safeParse(value);
  if (!parsed.success) {
    throw new DomainError("INVALID_UUID", `${field} must be a UUID`);
  }
  return parsed.data;
}

async function authenticate(sessionToken: string | null): Promise<Authenticated> {
  if (!sessionToken) {
    throw new DomainError("UNAUTHENTICATED");
  }
  const result = await configuredApplication().execute(
    { kind: "auth.authenticate" },
    { sessionToken },
  );
  return AuthResultSchema.parse(result);
}

export type PageViewer = {
  staffRole: "employee" | "admin" | null;
};

export async function optionalPageViewer(): Promise<PageViewer | null> {
  const sessionToken = (await cookies()).get("session")?.value ?? null;
  if (!sessionToken) {
    return null;
  }
  try {
    const authenticated = await authenticate(sessionToken);
    return { staffRole: authenticated.user.staffRole };
  } catch (error) {
    if (error instanceof DomainError && error.code === "UNAUTHENTICATED") {
      return null;
    }
    throw error;
  }
}

export async function requirePageViewer(): Promise<PageViewer> {
  const viewer = await optionalPageViewer();
  if (!viewer) {
    redirect("/sign-in");
  }
  return viewer;
}

export function providerAttemptCommand(
  body: unknown,
): Extract<WebCommand, { kind: "internal.providerAttempt" }> {
  const envelope = ProviderAttemptEnvelopeSchema.parse(body);
  return {
    kind: "internal.providerAttempt",
    ...envelope,
  };
}

export function workerFailureCommand(
  body: unknown,
): Extract<WebCommand, { kind: "internal.failure" }> {
  const { kind, phase, snapshotHash, ...envelope } = WorkerFailureEnvelopeSchema.parse(body);
  return {
    kind: "internal.failure",
    kindName: kind,
    ...envelope,
    ...(phase === undefined ? {} : { phase }),
    ...(snapshotHash === undefined ? {} : { snapshotHash }),
  };
}

export function archiveDeleteCommand(
  body: unknown,
): Extract<WebCommand, { kind: "archive.delete" }> {
  const parsed = z
    .object({
      consultationId: UuidSchema,
      reason: z.string().trim().min(1).max(2_000),
    })
    .parse(body);
  return {
    kind: "archive.delete",
    consultationId: parsed.consultationId,
    reason: parsed.reason,
  };
}

export function consultationCreateCommand(
  body: unknown,
): Extract<WebCommand, { kind: "consultation.createInvitation" }> {
  const parsed = ObjectSchema.parse(body);
  return {
    kind: "consultation.createInvitation",
    customerEmail: z.email().parse(parsed.customerEmail),
    customerName: z.string().min(1).max(120).parse(parsed.customerName),
    providerProfileId: requiredUuid(
      z.string().parse(parsed.providerProfileId),
      "providerProfileId",
    ),
    creationIdempotencyKey: requiredUuid(
      z.string().parse(parsed.creationIdempotencyKey),
      "creationIdempotencyKey",
    ),
  };
}

export function adminLanguageUpdateCommand(
  body: unknown,
): Extract<WebCommand, { kind: "language.enable" }> {
  const parsed = ObjectSchema.parse(body);
  return {
    kind: "language.enable",
    capabilityId: requiredUuid(z.string().parse(parsed.directionId), "directionId"),
    profileId: requiredUuid(z.string().parse(parsed.profileId), "profileId"),
    profileRevision: z.number().int().positive().parse(parsed.profileRevision),
    enabled: z.boolean().parse(parsed.enabled),
  };
}

function commandFor(operation: string, context: RequestContext): WebCommand {
  const body = ObjectSchema.parse(context.body ?? {});
  switch (operation) {
    case "auth.magicLink.request":
      return { kind: "auth.requestMagicLink", email: displayString(body.email) };
    case "auth.exchange.prepare":
      return {
        kind: "auth.beginExchange",
        token: z.string().parse(context.query.token),
      };
    case "auth.exchange.verify":
      if (!context.exchangeNonce) {
        throw new DomainError("INVALID_EXCHANGE");
      }
      return { kind: "auth.verifyExchange", nonce: context.exchangeNonce };
    case "auth.logout":
      return { kind: "auth.logout" };
    case "auth.archiveDeleteReauth.request":
      return {
        kind: "auth.reauthenticate",
        consultationId: requiredUuid(z.string().parse(body.consultationId), "consultationId"),
      };
    case "consultations.list":
      return { kind: "consultation.list" };
    case "consultations.create.options":
      return {
        kind: "consultation.options",
        providerProfileId: z.string().parse(body.providerProfileId ?? webConfig().providerProfile),
      };
    case "consultations.create":
      return consultationCreateCommand(body);
    case "consultations.get":
      return {
        kind: "consultation.get",
        consultationId: requiredUuid(context.params.id, "id"),
      };
    case "consultations.preferences.update":
      return {
        kind: "consultation.preferences",
        consultationId: requiredUuid(context.params.id, "id"),
        displayName: z.string().min(1).max(120).parse(body.displayName),
        language: z.string().min(1).parse(body.language),
      };
    case "consultations.consent.record":
      return {
        kind: "consultation.consent",
        consultationId: requiredUuid(context.params.id, "id"),
        snapshotHash: z.string().min(1).parse(body.snapshotHash),
      };
    case "consultations.join":
      return {
        kind: "consultation.join",
        consultationId: requiredUuid(context.params.id, "id"),
      };
    case "consultations.livekitToken":
      return {
        kind: "consultation.token",
        consultationId: requiredUuid(context.params.id, "id"),
      };
    case "consultations.room":
      return {
        kind: "consultation.room",
        consultationId: requiredUuid(context.params.id, "id"),
      };
    case "consultations.end":
      return {
        kind: "consultation.end",
        consultationId: requiredUuid(context.params.id, "id"),
      };
    case "consultations.cancel":
      return {
        kind: "consultation.cancel",
        consultationId: requiredUuid(context.params.id, "id"),
      };
    case "consultations.invitation.resend":
      return {
        kind: "consultation.resend",
        consultationId: requiredUuid(context.params.id, "id"),
      };
    case "archives.list":
      return { kind: "archive.list" };
    case "archives.get":
      return {
        kind: "archive.get",
        archiveId: requiredUuid(context.params.id, "id"),
      };
    case "archives.objects.list":
      return {
        kind: "archive.objects",
        archiveId: requiredUuid(context.params.id, "id"),
        cursor: context.query.cursor ?? null,
        limit: 100,
      };
    case "archives.object.download":
      return {
        kind: "archive.download",
        archiveId: requiredUuid(context.params.id, "id"),
        objectId: requiredUuid(z.string().parse(body.objectId), "objectId"),
      };
    case "archives.hold.update": {
      const archiveConsultationId = requiredUuid(
        z.string().parse(body.consultationId),
        "consultationId",
      );
      if (body.enabled === false) {
        return {
          kind: "archive.releaseHold",
          consultationId: archiveConsultationId,
          holdId: requiredUuid(z.string().parse(body.holdId), "holdId"),
        };
      }
      return {
        kind: "archive.hold",
        consultationId: archiveConsultationId,
        reason: z.string().min(1).parse(body.reason),
      };
    }
    case "archives.delete":
      return archiveDeleteCommand(body);
    case "languages.catalog":
      return {
        kind: "consultation.options",
        providerProfileId: context.query.providerProfileId ?? webConfig().providerProfile,
      };
    case "admin.failures.list":
      return { kind: "admin.failures" };
    case "admin.languages.list":
      return {
        kind: "admin.languages",
        providerProfileId: context.query.providerProfileId ?? webConfig().providerProfile,
      };

    case "admin.languages.update":
      return adminLanguageUpdateCommand(body);
    case "internal.capabilities.update":
      return {
        kind: "internal.capability",
        refresh: CapabilityRefreshSchema.parse(body),
      };
    case "internal.worker.heartbeat":
      return {
        kind: "internal.heartbeat",
        consultationId: requiredUuid(z.string().parse(body.consultationId), "consultationId"),
        generation: z.number().int().nonnegative().parse(body.generation),
        workerId: requiredUuid(z.string().parse(body.workerId), "workerId"),
        epoch: z.number().int().nonnegative().parse(body.epoch),
      };
    case "internal.archive.checkpoint":
      return {
        kind: "internal.checkpoint",
        workerId: requiredUuid(z.string().parse(body.workerId), "workerId"),
        consultationId: requiredUuid(z.string().parse(body.consultationId), "consultationId"),
        generation: z.number().int().nonnegative().parse(body.generation),
        writeEpoch: z.number().int().nonnegative().parse(body.writeEpoch),
        objectKey: z.string().min(1).parse(body.objectKey),
        checkpoint: WorkerCheckpointSchema.parse(body.checkpoint),
      };
    case "internal.providerAttempt":
      return providerAttemptCommand(body);
    case "internal.failure":
      return workerFailureCommand(body);
    case "internal.archiveObject": {
      const generation = z.number().int().nonnegative().optional().parse(body.generation);
      const workerId = UuidSchema.optional().parse(body.workerId);
      const workerEpoch = z.number().int().nonnegative().optional().parse(body.epoch);
      const writerEpoch = z.number().int().nonnegative().optional().parse(body.writerEpoch);
      return {
        kind: "internal.archiveObject",
        consultationId: requiredUuid(z.string().parse(body.consultationId), "consultationId"),
        ...(generation === undefined ? {} : { generation }),
        ...(workerId === undefined ? {} : { workerId }),
        ...(workerEpoch === undefined ? {} : { workerEpoch }),
        ...(writerEpoch === undefined ? {} : { writerEpoch }),
        causalKey: z.string().min(1).parse(body.causalKey),
        object: ArchiveObjectRecordSchema.parse(body.object),
      };
    }
    case "internal.archive.finalize":
      return {
        kind: "internal.finalize",
        consultationId: requiredUuid(z.string().parse(body.consultationId), "consultationId"),
        inventory: FinalInventorySchema.parse(body.inventory),
      };
    case "internal.egressLayout.authorize":
      return {
        kind: "internal.egressLayout",
        consultationId: requiredUuid(context.query.consultationId, "consultationId"),
        generation: z.coerce.number().int().nonnegative().parse(context.query.generation),
      };
    case "internal.archiveRecording":
      return {
        kind: "internal.archiveRecording",
        consultationId: requiredUuid(z.string().parse(body.consultationId), "consultationId"),
      };
    case "internal.deleteDrain":
      return {
        kind: "internal.deleteDrain",
        consultationId: requiredUuid(z.string().parse(body.consultationId), "consultationId"),
        writeEpoch: z.number().int().nonnegative().parse(body.writeEpoch),
        reason: z.string().trim().min(1).max(2_000).parse(body.reason),
      };
    default:
      throw new DomainError("NOT_FOUND");
  }
}

function csrfAccepted(request: Request, cookieToken: string | undefined): boolean {
  const url = new URL(request.url);
  const bypassesCsrf =
    request.method === "GET" ||
    request.method === "HEAD" ||
    url.pathname.startsWith("/api/internal/") ||
    url.pathname === "/api/webhooks/livekit";
  if (bypassesCsrf) {
    return true;
  }

  const origin = request.headers.get("origin");
  if ((origin && origin !== new URL(webConfig().publicUrl).origin) || !cookieToken) {
    return false;
  }
  const headerToken = request.headers.get("x-csrf-token");
  if (!headerToken) {
    return false;
  }
  const cookieBytes = Buffer.from(cookieToken);
  const headerBytes = Buffer.from(headerToken);
  return cookieBytes.length === headerBytes.length && timingSafeEqual(cookieBytes, headerBytes);
}

export function presentConsultationList(result: unknown) {
  const page = z
    .object({
      consultations: z.array(ConsultationSchema),
      viewer: z.object({ staffRole: StaffRoleSchema.nullable() }),
    })
    .parse(result);
  return {
    viewer: page.viewer,
    consultations: page.consultations.map((consultation) => {
      const customer = consultation.participants.find((slot) => slot.role === "customer");
      const href =
        durableConsultationDestination(consultation) ?? `/consultations/${consultation.id}/lobby`;
      return {
        id: consultation.id,
        customerName: customer?.displayName ?? "Invited customer",
        status: consultation.state,
        startsAt: consultation.createdAt.toISOString(),
        href,
        canCancel: consultation.state === "invited" || consultation.state === "ready",
        canResend: consultation.state === "invited",
      };
    }),
  };
}

function presentProfileOptions(result: unknown) {
  const rows = z.array(ObjectSchema).parse(result);
  const unique = new Map<string, { id: string; name: string; revision: number }>();
  for (const row of rows) {
    const id = UuidSchema.parse(row.profile_id);
    unique.set(id, {
      id,
      name: displayString(row.profile_name),
      revision: z.coerce.number().int().positive().catch(1).parse(row.revision),
    });
  }
  return { profiles: [...unique.values()] };
}

function consultationPhase(
  consultation: z.infer<typeof ConsultationSchema>,
  own: z.infer<typeof ConsultationSchema>["participants"][number] | undefined,
  redirectTo: string | null,
): string {
  if (consultation.state === "cancelled" || consultation.state === "deleted") {
    return "terminal";
  }
  if ((consultation.state === "ready" || consultation.state === "active") && !redirectTo) {
    return "ready";
  }
  if (!own?.language) {
    return "preferences";
  }
  if (consultation.participants.some((slot) => !slot.language)) {
    return "waiting";
  }
  const ownConsentRecorded =
    consultation.snapshotHash !== null && own.consent?.snapshotHash === consultation.snapshotHash;
  if (consultation.snapshotHash && ownConsentRecorded && consultation.state === "invited") {
    return "consent-waiting";
  }
  return consultation.snapshotHash ? "consent" : "waiting";
}

async function presentConsultationLobby(
  result: unknown,
  context: RequestContext,
): Promise<unknown> {
  const consultation = ConsultationSchema.parse(result);
  const auth = await authenticate(context.sessionToken);
  const own = consultation.participants.find((slot) => slot.userId === auth.user.id);
  const directions =
    consultation.providerSelection?.directions.map((direction) => {
      const source = consultation.participants.find(
        (slot) => slot.id === direction.sourceParticipantId,
      );
      const destination = consultation.participants.find(
        (slot) => slot.id === direction.destinationParticipantId,
      );
      return {
        sourceLabel: source?.language ?? source?.displayName ?? "Source",
        destinationLabel: destination?.language ?? destination?.displayName ?? "Destination",
        speech: `${direction.stt.provider} · ${direction.stt.model}`,
        translation:
          direction.mode === "translated"
            ? `${direction.translation.provider} · ${direction.translation.model}`
            : "Same-language bypass",
        voice:
          direction.mode === "translated"
            ? `${direction.tts.provider} · ${direction.tts.voice}`
            : "Original audio",
        region: direction.stt.region,
      };
    }) ?? [];
  const optionRows = z.array(ObjectSchema).parse(
    await configuredApplication().execute(
      {
        kind: "consultation.options",
        providerProfileId: consultation.providerProfileId,
      },
      context.sessionToken ? { sessionToken: context.sessionToken } : {},
    ),
  );
  const languageCodes = new Set(
    optionRows
      .flatMap((row) => [displayString(row.source_locale), displayString(row.target_locale)])
      .filter(Boolean),
  );
  const redirectTo = durableConsultationDestination(consultation);
  return {
    state: consultation.state,
    phase: consultationPhase(consultation, own, redirectTo),
    snapshotHash: consultation.snapshotHash,
    profileName: displayString(
      optionRows[0]?.profile_name,
      consultation.providerSelection?.profileId ?? consultation.providerProfileId,
    ),
    profileRevision: consultation.providerProfileRevision,
    directions,
    languages: [...languageCodes].sort().map((code) => ({ code, label: code })),
    consented: own?.consent?.snapshotHash === consultation.snapshotHash,
    ...(redirectTo ? { redirectTo } : {}),
  };
}

function archiveObjectGroup(objectClass: string): string {
  if (objectClass.includes("composite")) {
    return "composite";
  }
  if (
    objectClass.includes("participant") ||
    objectClass.includes("original") ||
    objectClass.includes("stt_input")
  ) {
    return "original";
  }
  if (
    objectClass.includes("interpret") ||
    objectClass.includes("tts") ||
    objectClass.includes("livekit_output")
  ) {
    return "interpretation";
  }
  if (objectClass.includes("caption") || objectClass.includes("vtt")) {
    return "captions";
  }
  if (objectClass.includes("inventory") || objectClass.includes("checkpoint")) {
    return "inventory";
  }
  return "pipeline";
}

export function presentArchiveObjects(result: unknown) {
  const page = z
    .object({
      objects: z.array(
        z.object({
          id: UuidSchema,
          object_class: z.string(),
          key: z.string().min(1),
          content_type: z.string(),
          size: z.coerce.number().nonnegative(),
          sha256: z.string(),
          s3_checksum: z.string().min(1),
          version_id: z.string(),
        }),
      ),
      cursor: z.string().nullable(),
    })
    .parse(result);
  return {
    objects: page.objects.map((object) => ({
      id: object.id,
      group: archiveObjectGroup(object.object_class),
      label: object.object_class,
      key: object.key,
      contentType: object.content_type,
      size: object.size,
      sha256: object.sha256,
      s3Checksum: object.s3_checksum,
      versionId: object.version_id,
    })),
    nextCursor: page.cursor,
  };
}

async function presentArchive(result: unknown, context: RequestContext): Promise<unknown> {
  const archive = ObjectSchema.parse(result);
  const auth = await authenticate(context.sessionToken);
  const archiveId = requiredUuid(z.string().parse(archive.id), "archiveId");
  const page = presentArchiveObjects(
    await configuredApplication().execute(
      {
        kind: "archive.objects",
        archiveId,
        cursor: context.query.cursor ?? null,
        limit: 100,
      },
      context.sessionToken ? { sessionToken: context.sessionToken } : {},
    ),
  );
  const inventory =
    archive.inventory && typeof archive.inventory === "object"
      ? ObjectSchema.parse(archive.inventory)
      : {};
  const missing = Array.isArray(inventory.missing) ? inventory.missing : [];
  const errors = Array.isArray(inventory.errors) ? inventory.errors : [];
  const objects = page.objects;
  const activeHolds = z
    .array(
      z.object({
        id: UuidSchema,
        reason: z.string(),
      }),
    )
    .catch([])
    .parse(archive.activeHolds);
  const egressIds = z.array(z.string()).catch([]).parse(archive.egressIds);
  const providerAttemptIds = z.array(UuidSchema).catch([]).parse(archive.providerAttemptIds);
  const providerAttemptGroups = z
    .array(
      z.object({
        stage: z.enum(["stt", "translation", "tts"]),
        provider: z.string(),
        direction: z.string(),
        attemptIds: z.array(UuidSchema),
      }),
    )
    .catch([])
    .parse(archive.providerAttemptGroups);
  return {
    id: archiveId,
    consultationId: requiredUuid(z.string().parse(archive.consultationId), "consultationId"),
    status: z.string().parse(archive.state),
    objects,
    gaps: [
      ...missing.map((detail) => ({
        class: "missing",
        detail: displayString(detail, JSON.stringify(detail)),
      })),
      ...errors.map((detail) => ({
        class: "error",
        detail: displayString(detail, JSON.stringify(detail)),
      })),
    ],
    nextCursor: page.nextCursor,
    canAdminister: auth.user.staffRole === "admin",
    activeHolds,
    inventoryVersion: z.string().nullable().catch(null).parse(archive.inventoryVersionId),
    inventorySha256: z.string().nullable().catch(null).parse(archive.inventorySha256),
    egressIds,
    providerAttemptIds,
    providerAttemptGroups,
  };
}

function presentFailures(result: unknown) {
  const rows = z.array(ObjectSchema).parse(result);
  return {
    failures: rows.map((row) => ({
      id: z.string().parse(row.id),
      occurredAt: z.coerce
        .date()
        .parse(row.updated_at ?? Date.now())
        .toISOString(),
      consultationId: z.string().parse(row.consultation_id),
      stage: z.string().parse(row.state),
      code: "EXTERNAL_EFFECT_FAILED",
      summary: JSON.stringify(row.result ?? {}),
      archiveId: z.string().parse(row.consultation_id),
    })),
  };
}

export function presentLanguages(result: unknown) {
  const rows = z.array(ObjectSchema).parse(result);
  return {
    directions: rows.map((row) => {
      const snapshot = ObjectSchema.safeParse(row.snapshot);
      return {
        id: String(row.id),
        profileId: String(row.profileId ?? row.profile_id),
        profile: String(row.profileName ?? row.profile_name),
        revision: Number(row.revision),
        source: String(row.sourceLocale ?? row.source_locale),
        target: String(row.targetLocale ?? row.target_locale),
        providers: JSON.stringify(row.snapshot ?? {}),
        region: snapshot.success
          ? displayString(snapshot.data.region, "Configured region")
          : "Configured region",
        enabled: Boolean(row.enabled),
        freshAt: new Date(String(row.freshUntil ?? row.fresh_until)).toISOString(),
      };
    }),
  };
}

export function presentConsultationEnd(result: unknown): {
  generation: number;
  shutdownAtMs: number;
} {
  const ended = z
    .object({
      consultation: ConsultationSchema,
      generation: z.number().int().nonnegative(),
      shutdownAtMs: z.number().int().nonnegative(),
    })
    .parse(result);
  return {
    generation: ended.generation,
    shutdownAtMs: ended.shutdownAtMs,
  };
}

async function present(
  operation: string,
  result: unknown,
  context: RequestContext,
): Promise<unknown> {
  if (operation === "consultations.list") {
    return presentConsultationList(result);
  }
  if (operation === "consultations.create.options") {
    return presentProfileOptions(result);
  }
  if (operation === "consultations.get" || operation === "consultations.preferences.update") {
    return presentConsultationLobby(result, context);
  }
  if (operation === "consultations.join") {
    const joined = z
      .object({
        status: z.enum(["ready", "PROVISIONING"]),
        consultation: ConsultationSchema,
      })
      .parse(result);
    const redirectTo = durableConsultationDestination(joined.consultation);
    return {
      status: redirectTo ? "ready" : "provisioning",
      ...(redirectTo ? { redirectTo } : {}),
    };
  }
  if (operation === "consultations.livekitToken") {
    return { token: z.string().parse(result) };
  }
  if (operation === "consultations.room") {
    const room = z
      .object({
        consultation_id: UuidSchema,
        state: z.enum(["ready", "active"]),
        generation: z.coerce.number().int(),
        worker_identity: UuidSchema,
        participant_id: UuidSchema,
        participant_identity: UuidSchema,
        role: ParticipantRoleSchema,
        display_name: z.string().nullable(),
        other_participant_id: UuidSchema,
        other_identity: UuidSchema,
        other_display_name: z.string().nullable(),
      })
      .parse(result);
    return {
      consultationId: room.consultation_id,
      participantId: room.participant_id,
      participantIdentity: room.participant_identity,
      otherParticipantId: room.other_participant_id,
      otherIdentity: room.other_identity,
      generation: room.generation,
      liveKitUrl: webConfig().liveKitBrowserUrl,
      displayName: room.display_name ?? "Participant",
      otherDisplayName: room.other_display_name ?? "Other participant",
      role: room.role,
      state: room.state,
    };
  }
  if (operation === "archives.get") {
    return presentArchive(result, context);
  }
  if (operation === "consultations.end") {
    return presentConsultationEnd(result);
  }
  if (operation === "archives.object.download") {
    return { url: z.url().parse(result) };
  }
  if (operation === "admin.failures.list") {
    return presentFailures(result);
  }
  if (operation === "admin.languages.list") {
    return presentLanguages(result);
  }
  if (operation === "archives.list") {
    return { archives: result };
  }
  if (operation === "internal.egressLayout.authorize") {
    const row = ObjectSchema.parse(result);
    const participants = z
      .array(
        z
          .object({
            identity: UuidSchema,
            displayName: z.string().nullable(),
          })
          .loose(),
      )
      .parse(row.participants);
    return {
      consultationId: String(row.id),
      participants: participants.map((participant) => ({
        identity: participant.identity,
        displayName: participant.displayName ?? "Participant",
        videoTrackSid: null,
        audioTrackSid: null,
      })),
    };
  }
  return result;
}

export function statusFor(error: DomainError): number {
  if (
    error.code === "UNAUTHENTICATED" ||
    error.code === "UNAUTHENTICATED_INTERNAL" ||
    error.code === "UNAUTHORIZED_INTERNAL"
  ) {
    return 401;
  }
  if (
    error.code === "FORBIDDEN" ||
    error.code === "FORBIDDEN_INTERNAL" ||
    error.code === "CSRF_REJECTED"
  ) {
    return 403;
  }
  if (error.code === "NOT_FOUND") {
    return 404;
  }
  if (
    [
      "WAITING_FOR_PREFERENCES",
      "CONSENT_REQUIRED",
      "SNAPSHOT_CHANGED",
      "INVALID_STATE",
      "PREFERENCES_LOCKED",
      "CONSULTATION_CREATION_CONFLICT",
      "PROVIDER_ATTEMPT_FENCED",
      "PROVIDER_ATTEMPT_CONFLICT",
      "PROVIDER_SELECTION_MISMATCH",
      "PROVIDER_DIRECTION_MISMATCH",
      "PROVIDER_STAGE_MISMATCH",
      "PROVIDER_CREDENTIAL_MISMATCH",
      "CAPABILITY_REVISION_CONFLICT",
    ].includes(error.code)
  ) {
    return 409;
  }
  if (error.code === "PAYLOAD_TOO_LARGE") {
    return 413;
  }
  return 400;
}

async function dispatch(operation: string, context: RequestContext): Promise<unknown> {
  if (operation === "webhooks.livekit.receive") {
    if (context.rawBody === null) {
      throw new DomainError("INVALID_WEBHOOK");
    }
    return configuredApplication().roomService.acceptWebhook(
      context.rawBody,
      Object.fromEntries(context.request.headers.entries()),
    );
  }
  return configuredApplication().execute(commandFor(operation, context), {
    request: context.request,
    internalHeaders: Object.fromEntries(context.request.headers.entries()),
    ...(context.sessionToken ? { sessionToken: context.sessionToken } : {}),
    ...(context.csrfToken ? { csrfToken: context.csrfToken } : {}),
  });
}

async function readLimitedBody(request: Request, limit: number): Promise<Uint8Array> {
  const declaredLength = request.headers.get("content-length");
  if (declaredLength !== null) {
    const parsedLength = Number(declaredLength);
    if (Number.isFinite(parsedLength) && parsedLength > limit) {
      throw new DomainError("PAYLOAD_TOO_LARGE");
    }
  }
  const reader = request.body?.getReader();
  if (!reader) {
    return new Uint8Array();
  }
  const chunks: Uint8Array[] = [];
  let length = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    length += value.byteLength;
    if (length > limit) {
      await reader.cancel();
      throw new DomainError("PAYLOAD_TOO_LARGE");
    }
    chunks.push(value);
  }
  const body = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

export async function parseRequestPayload(
  operation: string,
  request: Request,
): Promise<{ body: unknown; rawBody: Uint8Array | null }> {
  const hasBody =
    request.method !== "GET" &&
    request.method !== "HEAD" &&
    request.headers.get("content-length") !== "0";
  if (!hasBody) {
    return { body: undefined, rawBody: null };
  }
  const rawBody = await readLimitedBody(
    request,
    operation === "webhooks.livekit.receive" ? WEBHOOK_BODY_LIMIT_BYTES : JSON_BODY_LIMIT_BYTES,
  );
  if (operation === "webhooks.livekit.receive") {
    return { body: undefined, rawBody };
  }
  if (rawBody.byteLength === 0) {
    return { body: undefined, rawBody: null };
  }
  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(rawBody);
  } catch (error) {
    if (error instanceof TypeError) {
      throw new SyntaxError("Request body is not valid UTF-8", { cause: error });
    }
    throw error;
  }
  return { body: JSON.parse(text) as unknown, rawBody: null };
}

function applicationResponse(payload: unknown): Response {
  return Response.json(payload, {
    headers: {
      "cache-control": "no-store, max-age=0",
      "referrer-policy": "no-referrer",
    },
  });
}

export function normalizeAuthFlowError(operation: string, error: DomainError): DomainError {
  const isExchangeOperation =
    operation === "auth.exchange.prepare" || operation === "auth.exchange.verify";
  if (
    isExchangeOperation &&
    (error.code === "INVALID_OR_EXPIRED_LINK" ||
      error.code === "INVALID_EXCHANGE" ||
      error.code === "INVALID_EXCHANGE_CONTEXT")
  ) {
    return new DomainError("INVALID_OR_EXPIRED_LINK", "This sign-in flow is unavailable");
  }
  return error;
}

function errorResponse(error: DomainError | z.ZodError): Response {
  if (error instanceof DomainError) {
    return Response.json(
      { code: error.code, message: error.message },
      {
        status: statusFor(error),
        headers: { "cache-control": "no-store" },
      },
    );
  }
  return Response.json(
    {
      code: "INVALID_REQUEST",
      message: "Request validation failed",
      issues: error.issues,
    },
    {
      status: 400,
      headers: { "cache-control": "no-store" },
    },
  );
}

function applicationFailureResponse(operation: string, error: unknown): Response {
  const summary: { operation: string; name: string; code?: string; status?: number } = {
    operation,
    name: error instanceof Error ? error.name : typeof error,
  };
  let current = error;
  for (let depth = 0; depth < 4 && current !== null && typeof current === "object"; depth += 1) {
    const record = current as Record<string, unknown>;
    if (summary.code === undefined && ["string", "number"].includes(typeof record.code)) {
      summary.code = String(record.code);
    }
    const status = record.statusCode ?? record.status;
    if (summary.status === undefined && typeof status === "number") {
      summary.status = status;
    }
    current = record.cause;
  }
  console.error("Unhandled API request failure", summary);

  const invalidJson = error instanceof SyntaxError;
  return Response.json(
    {
      code: "REQUEST_FAILED",
      message: invalidJson
        ? "Request body is not valid JSON"
        : "The request could not be completed",
    },
    {
      status: invalidJson ? 400 : 503,
      headers: { "cache-control": "no-store" },
    },
  );
}

async function executeApplication(
  operation: string,
  request: Request,
  params: Record<string, string> = {},
): Promise<Response> {
  const cookieStore = await cookies();
  const csrfToken = cookieStore.get("csrf")?.value;
  if (!csrfAccepted(request, csrfToken)) {
    return Response.json(
      { code: "CSRF_REJECTED", message: "Security validation failed" },
      {
        status: 403,
        headers: { "cache-control": "no-store" },
      },
    );
  }

  try {
    const url = new URL(request.url);
    const payload = await parseRequestPayload(operation, request);
    const context: RequestContext = {
      request,
      params,
      query: Object.fromEntries(url.searchParams.entries()),
      body: payload.body,
      rawBody: payload.rawBody,
      sessionToken: cookieStore.get("session")?.value ?? null,
      csrfToken: csrfToken ?? null,
      exchangeNonce: cookieStore.get("exchange")?.value ?? null,
    };
    const result = await dispatch(operation, context);
    if (
      operation === "auth.magicLink.request" ||
      operation === "auth.archiveDeleteReauth.request"
    ) {
      return Response.json(
        {},
        {
          status: 202,
          headers: { "cache-control": "no-store" },
        },
      );
    }
    if (operation === "auth.exchange.prepare") {
      const parsed = z
        .object({
          exchangeNonce: z.string(),
          verificationCsrfToken: z.string(),
        })
        .parse(result);
      cookieStore.set("exchange", parsed.exchangeNonce, {
        httpOnly: true,
        secure: webConfig().appEnv === "production",
        sameSite: "strict",
        path: "/",
        maxAge: 300,
      });
      cookieStore.set("csrf", parsed.verificationCsrfToken, {
        httpOnly: false,
        secure: webConfig().appEnv === "production",
        sameSite: "strict",
        path: "/",
        maxAge: 300,
      });
    }
    if (operation === "auth.exchange.verify") {
      const parsed = z
        .object({
          sessionToken: z.string(),
          csrfToken: z.string(),
          purpose: z.string(),
        })
        .parse(result);
      cookieStore.set("session", parsed.sessionToken, {
        httpOnly: true,
        secure: webConfig().appEnv === "production",
        sameSite: "lax",
        path: "/",
        maxAge: 43_200,
      });
      cookieStore.set("csrf", parsed.csrfToken, {
        httpOnly: false,
        secure: webConfig().appEnv === "production",
        sameSite: "strict",
        path: "/",
        maxAge: 43_200,
      });
      cookieStore.delete("exchange");
      return Response.json(
        { redirectTo: "/consultations" },
        { headers: { "cache-control": "no-store" } },
      );
    }
    if (operation === "auth.logout") {
      cookieStore.delete("session");
      return new Response(null, {
        status: 204,
        headers: { "cache-control": "no-store" },
      });
    }

    const presented = (await present(operation, result, context)) ?? null;
    const isProvisioningJoin =
      operation === "consultations.join" &&
      z.object({ status: z.literal("provisioning") }).safeParse(presented).success;
    if (isProvisioningJoin) {
      return Response.json(presented, {
        status: 202,
        headers: {
          "cache-control": "no-store, max-age=0",
          "referrer-policy": "no-referrer",
          "retry-after": "2",
        },
      });
    }
    return applicationResponse(presented);
  } catch (error) {
    if (error instanceof DomainError) {
      return errorResponse(normalizeAuthFlowError(operation, error));
    }
    if (error instanceof z.ZodError) {
      return errorResponse(error);
    }
    throw error;
  }
}

export async function execute(
  operation: string,
  request: Request,
  params: Record<string, string> = {},
): Promise<Response> {
  const startedAt = Date.now();
  try {
    return await withWebOperation(operation, "api", async () => {
      try {
        return await executeApplication(operation, request, params);
      } catch (error) {
        return applicationFailureResponse(operation, error);
      }
    });
  } finally {
    if (operation.startsWith("auth.")) {
      const remaining = 250 - (Date.now() - startedAt);
      if (remaining > 0) {
        const { promise, resolve } = Promise.withResolvers<void>();
        setTimeout(resolve, remaining);
        await promise;
      }
    }
  }
}

async function requirePageDataApplication<T>(
  operation: string,
  params: Record<string, string> = {},
  query: Record<string, string> = {},
): Promise<T> {
  const cookieStore = await cookies();
  const requestHeaders = await headers();
  const request = new Request(`${webConfig().publicUrl}/internal-page`, {
    headers: Object.fromEntries(requestHeaders.entries()),
  });
  const context: RequestContext = {
    request,
    params,
    query,
    body: {},
    rawBody: null,
    sessionToken: cookieStore.get("session")?.value ?? null,
    csrfToken: cookieStore.get("csrf")?.value ?? null,
    exchangeNonce: cookieStore.get("exchange")?.value ?? null,
  };
  try {
    const result = await dispatch(operation, context);
    if (operation === "consultations.get") {
      const consultation = ConsultationSchema.parse(result);
      const destination = durableConsultationDestination(consultation);
      if (destination) {
        redirect(destination);
      }
    }
    return (await present(operation, result, context)) as T;
  } catch (error) {
    if (error instanceof DomainError && error.code === "UNAUTHENTICATED") {
      redirect("/sign-in");
    }
    if (error instanceof DomainError && error.code === "NOT_FOUND") {
      notFound();
    }
    throw error;
  }
}

export function requirePageData<T>(
  operation: string,
  params: Record<string, string> = {},
  query: Record<string, string> = {},
): Promise<T> {
  return withWebOperation(operation, "page", () =>
    requirePageDataApplication<T>(operation, params, query),
  );
}

export async function requireSignedEgressLayout(input: {
  consultationId: string;
  generation: string;
  expires: string;
  signature: string;
}): Promise<{
  consultationId: string;
  participants: readonly [
    { identity: string; displayName: string },
    { identity: string; displayName: string },
  ];
}> {
  const consultationId = requiredUuid(input.consultationId, "consultationId");
  const generation = z.coerce.number().int().nonnegative().parse(input.generation);
  const expires = z.coerce.number().int().parse(input.expires);
  const now = Date.now();
  if (expires < now || expires > now + 600_000) {
    throw new DomainError("UNAUTHENTICATED");
  }

  const secret = webConfig().egressLayoutSigningKey;
  if (!/^[0-9a-f]{64}$/i.test(input.signature)) {
    throw new DomainError("UNAUTHENTICATED");
  }
  const message = `${consultationId}\n${String(generation)}\n${String(expires)}`;
  const expected = createHmac("sha256", secret).update(message).digest();
  const supplied = Buffer.from(input.signature, "hex");
  if (supplied.length !== expected.length || !timingSafeEqual(supplied, expected)) {
    throw new DomainError("UNAUTHENTICATED");
  }

  const result = await configuredApplication().execute(
    { kind: "internal.egressLayout", consultationId, generation },
    { internalHeaders: { authorization: `Bearer ${secret}` } },
  );
  const row = ObjectSchema.parse(result);
  const participants = z
    .array(
      z
        .object({
          identity: UuidSchema,
          displayName: z.string().nullable(),
        })
        .loose(),
    )
    .length(2)
    .parse(row.participants);
  const first = participants[0];
  const second = participants[1];
  if (!first || !second) {
    throw new DomainError("NOT_FOUND");
  }
  return {
    consultationId,
    participants: [
      {
        identity: first.identity,
        displayName: first.displayName ?? "Participant",
      },
      {
        identity: second.identity,
        displayName: second.displayName ?? "Participant",
      },
    ],
  };
}

export async function ready(): Promise<boolean> {
  return configuredApplication().ready();
}
