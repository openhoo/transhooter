import {
  ARCHIVE_STATE_VALUES,
  CONSULTATION_STATE_VALUES,
  EXTERNAL_EFFECT_STATE_VALUES,
  MAGIC_LINK_PURPOSE_VALUES,
  PARTICIPANT_ROLE_VALUES,
  STAFF_ROLE_VALUES,
  TRANSPORT_KIND_VALUES,
} from "@transhooter/contracts";
import { pgTable, unique, check, uuid, text, boolean, integer, timestamp, foreignKey, jsonb, bigint, index, bigserial, numeric, uniqueIndex, primaryKey, pgEnum, customType } from "drizzle-orm/pg-core"
import { sql } from "drizzle-orm"

const bytea = customType<{ data: Uint8Array; driverData: Buffer }>({ dataType: () => "bytea" })

export const archiveState = pgEnum("archive_state", ARCHIVE_STATE_VALUES)
export const consultationState = pgEnum("consultation_state", CONSULTATION_STATE_VALUES)
export const externalEffectState = pgEnum("external_effect_state", EXTERNAL_EFFECT_STATE_VALUES)
export const magicLinkPurpose = pgEnum("magic_link_purpose", MAGIC_LINK_PURPOSE_VALUES)
export const participantRole = pgEnum("participant_role", PARTICIPANT_ROLE_VALUES)
export const staffRole = pgEnum("staff_role", STAFF_ROLE_VALUES)
export const terminalOutcome = pgEnum("terminal_outcome", ['succeeded', 'failed', 'cancelled', 'degraded', 'unknown'])
export const transportKind = pgEnum("transport_kind", TRANSPORT_KIND_VALUES)


export const providerProfiles = pgTable("provider_profiles", {
	id: uuid().primaryKey().notNull(),
	name: text().notNull(),
	enabled: boolean().default(false).notNull(),
	currentRevision: integer("current_revision").notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	unique("provider_profiles_name_key").on(table.name),
	check("provider_profiles_current_revision_check", sql`current_revision > 0`),
]);

export const languageCapabilities = pgTable("language_capabilities", {
	id: uuid().primaryKey().notNull(),
	profileId: uuid("profile_id").notNull(),
	revision: integer().notNull(),
	sourceLocale: text("source_locale").notNull(),
	targetLocale: text("target_locale").notNull(),
	mode: text().notNull(),
	sttProvider: text("stt_provider").notNull(),
	sttEndpoint: text("stt_endpoint").notNull(),
	sttModel: text("stt_model").notNull(),
	sttEncoding: text("stt_encoding").notNull(),
	sttLimits: jsonb("stt_limits").notNull(),
	translationProvider: text("translation_provider"),
	translationEndpoint: text("translation_endpoint"),
	translationModel: text("translation_model"),
	translationCode: text("translation_code"),
	ttsProvider: text("tts_provider"),
	ttsEndpoint: text("tts_endpoint"),
	ttsModel: text("tts_model"),
	ttsVoice: text("tts_voice"),
	ttsFormat: text("tts_format"),
	ttsLimits: jsonb("tts_limits"),
	region: text().notNull(),
	adapterVersion: text("adapter_version").notNull(),
	capabilityVersion: text("capability_version").notNull(),
	capabilityHash: text("capability_hash").notNull(),
	enabled: boolean().notNull(),
	freshUntil: timestamp("fresh_until", { withTimezone: true, mode: 'date' }).notNull(),
	snapshot: jsonb().notNull(),
}, (table) => [
	foreignKey({
			columns: [table.profileId, table.revision],
			foreignColumns: [providerProfileRevisions.profileId, providerProfileRevisions.revision],
			name: "language_capabilities_profile_id_revision_fkey"
		}),
	unique("language_capabilities_profile_id_revision_source_locale_tar_key").on(table.profileId, table.revision, table.sourceLocale, table.targetLocale, table.mode),
	check("language_capabilities_mode_check", sql`mode = ANY (ARRAY['translated'::text, 'same_language'::text])`),
	check("language_capabilities_check", sql`((mode = 'same_language'::text) AND (translation_provider IS NULL) AND (tts_provider IS NULL)) OR ((mode = 'translated'::text) AND (translation_provider IS NOT NULL) AND (translation_endpoint IS NOT NULL) AND (translation_model IS NOT NULL) AND (translation_code IS NOT NULL) AND (tts_provider IS NOT NULL) AND (tts_endpoint IS NOT NULL) AND (tts_model IS NOT NULL) AND (tts_voice IS NOT NULL) AND (tts_format IS NOT NULL) AND (tts_limits IS NOT NULL))`),
]);

export const consultations = pgTable("consultations", {
	id: uuid().primaryKey().notNull(),
	state: consultationState().default('invited').notNull(),
	providerProfileId: uuid("provider_profile_id").notNull(),
	providerProfileRevision: integer("provider_profile_revision").notNull(),
	providerSelection: jsonb("provider_selection"),
	snapshotHash: text("snapshot_hash"),
	generation: integer().default(0).notNull(),
	roomName: text("room_name"),
	roomSid: text("room_sid"),
	workerIdentity: uuid("worker_identity"),
	dispatchId: text("dispatch_id"),
	compositeEgressId: text("composite_egress_id"),
	readyDeadlineAt: timestamp("ready_deadline_at", { withTimezone: true, mode: 'date' }),
	finalizeDeadlineAt: timestamp("finalize_deadline_at", { withTimezone: true, mode: 'date' }),
	bothAbsentSince: timestamp("both_absent_since", { withTimezone: true, mode: 'date' }),
	admissionFencedAt: timestamp("admission_fenced_at", { withTimezone: true, mode: 'date' }),
	effectGeneration: integer("effect_generation").default(0).notNull(),
	employeeUserId: uuid("employee_user_id"),
	creationIdempotencyKey: text("creation_idempotency_key"),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	presenceEpoch: bigint("presence_epoch", { mode: "number" }).default(0).notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'date' }).notNull(),
	deletedAt: timestamp("deleted_at", { withTimezone: true, mode: 'date' }),
}, (table) => [
	foreignKey({
			columns: [table.providerProfileId, table.providerProfileRevision],
			foreignColumns: [providerProfileRevisions.profileId, providerProfileRevisions.revision],
			name: "consultations_provider_profile_id_provider_profile_revisio_fkey"
		}),
	foreignKey({
			columns: [table.employeeUserId],
			foreignColumns: [users.id],
			name: "consultations_employee_user_id_fkey"
		}),
	unique("consultations_room_name_key").on(table.roomName),
	unique("consultations_worker_identity_key").on(table.workerIdentity),
	unique("consultations_employee_user_id_creation_idempotency_key_key").on(table.employeeUserId, table.creationIdempotencyKey),
	check("consultations_generation_check", sql`generation >= 0`),
	check("consultations_effect_generation_check", sql`effect_generation >= 0`),
	check("consultations_presence_epoch_check", sql`presence_epoch >= 0`),
	check("consultations_creation_idempotency_key_scope_check", sql`(employee_user_id IS NULL) = (creation_idempotency_key IS NULL)`),
	check("consultations_check", sql`(provider_selection IS NULL) = (snapshot_hash IS NULL)`),
	check("consultations_check1", sql`(state = 'invited'::consultation_state) OR (room_name IS NOT NULL) OR (state = ANY (ARRAY['cancelled'::consultation_state, 'deleted'::consultation_state]))`),
]);

export const consultationParticipants = pgTable("consultation_participants", {
	id: uuid().primaryKey().notNull(),
	consultationId: uuid("consultation_id").notNull(),
	userId: uuid("user_id").notNull(),
	role: participantRole().notNull(),
	livekitIdentity: uuid("livekit_identity").notNull(),
	displayName: text("display_name"),
	language: text(),
	capabilityRowId: uuid("capability_row_id"),
	consentVersion: integer("consent_version"),
	consentCopyHash: text("consent_copy_hash"),
	consentSnapshotHash: text("consent_snapshot_hash"),
	consentedAt: timestamp("consented_at", { withTimezone: true, mode: 'date' }),
	present: boolean().default(false).notNull(),
	presenceEventId: text("presence_event_id"),
	presenceEventTime: timestamp("presence_event_time", { withTimezone: true, mode: 'date' }),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	absenceOrder: bigint("absence_order", { mode: "number" }).default(0).notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	readyOrder: bigint("ready_order", { mode: "number" }).default(0).notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	finalizeOrder: bigint("finalize_order", { mode: "number" }).default(0).notNull(),
	publicationGranted: boolean("publication_granted").default(false).notNull(),
	participantEgressId: text("participant_egress_id"),
	joinedAt: timestamp("joined_at", { withTimezone: true, mode: 'date' }),
	disconnectedAt: timestamp("disconnected_at", { withTimezone: true, mode: 'date' }),
}, (table) => [
	foreignKey({
			columns: [table.consultationId],
			foreignColumns: [consultations.id],
			name: "consultation_participants_consultation_id_fkey"
		}),
	foreignKey({
			columns: [table.userId],
			foreignColumns: [users.id],
			name: "consultation_participants_user_id_fkey"
		}),
	foreignKey({
			columns: [table.capabilityRowId],
			foreignColumns: [languageCapabilities.id],
			name: "consultation_participants_capability_row_id_fkey"
		}),
	unique("consultation_participants_consultation_id_id_key").on(table.consultationId, table.id),
	unique("consultation_participants_consultation_id_role_key").on(table.consultationId, table.role),
	unique("consultation_participants_consultation_id_user_id_key").on(table.consultationId, table.userId),
	unique("consultation_participants_livekit_identity_key").on(table.livekitIdentity),
	check("consultation_participants_absence_order_check", sql`absence_order >= 0`),
	check("consultation_participants_ready_order_check", sql`ready_order >= 0`),
	check("consultation_participants_finalize_order_check", sql`finalize_order >= 0`),
	check("consultation_participants_check", sql`id = livekit_identity`),
	check("consultation_participants_check1", sql`((consent_version IS NULL) = (consent_copy_hash IS NULL)) AND ((consent_version IS NULL) = (consent_snapshot_hash IS NULL)) AND ((consent_version IS NULL) = (consented_at IS NULL))`),
	check("consultation_participants_consent_version_check", sql`(consent_version IS NULL) OR (consent_version = 1)`),
]);

export const users = pgTable("users", {
	id: uuid().primaryKey().notNull(),
	email: text().notNull(),
	displayName: text("display_name").notNull(),
	staffRole: staffRole("staff_role"),
	piiErasedAt: timestamp("pii_erased_at", { withTimezone: true, mode: 'date' }),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	unique("users_email_key").on(table.email),
]);

export const roomProviderSelections = pgTable("room_provider_selections", {
	consultationId: uuid("consultation_id").primaryKey().notNull(),
	profileId: uuid("profile_id").notNull(),
	profileRevision: integer("profile_revision").notNull(),
	capabilityHash: text("capability_hash").notNull(),
	selectionHash: text("selection_hash").notNull(),
	selection: jsonb().notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.consultationId],
			foreignColumns: [consultations.id],
			name: "room_provider_selections_consultation_id_fkey"
		}),
	foreignKey({
			columns: [table.profileId, table.profileRevision],
			foreignColumns: [providerProfileRevisions.profileId, providerProfileRevisions.revision],
			name: "room_provider_selections_profile_id_profile_revision_fkey"
		}),
	unique("room_provider_selections_selection_hash_key").on(table.selectionHash),
]);

export const magicLinks = pgTable("magic_links", {
	id: uuid().primaryKey().notNull(),
	userId: uuid("user_id"),
	consultationId: uuid("consultation_id"),
	sessionId: uuid("session_id"),
	purpose: magicLinkPurpose().notNull(),
	tokenHash: text("token_hash").notNull(),
	expiresAt: timestamp("expires_at", { withTimezone: true, mode: 'date' }).notNull(),
	consumedAt: timestamp("consumed_at", { withTimezone: true, mode: 'date' }),
	revokedAt: timestamp("revoked_at", { withTimezone: true, mode: 'date' }),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
	sealedRawToken: text("sealed_raw_token").notNull(),
	sealedTokenKeyId: text("sealed_token_key_id").notNull(),
}, (table) => [
	foreignKey({
			columns: [table.userId],
			foreignColumns: [users.id],
			name: "magic_links_user_id_fkey"
		}),
	foreignKey({
			columns: [table.consultationId],
			foreignColumns: [consultations.id],
			name: "magic_links_consultation_id_fkey"
		}),
	foreignKey({
			columns: [table.sessionId],
			foreignColumns: [sessions.id],
			name: "magic_links_session_fk"
		}),
	unique("magic_links_token_hash_key").on(table.tokenHash),
	uniqueIndex("magic_links_active_identity_unique").on(
		sql`COALESCE(${table.userId}::text, '')`,
		table.purpose,
		sql`COALESCE(${table.consultationId}::text, '')`,
		sql`COALESCE(${table.sessionId}::text, '')`,
	).where(sql`${table.consumedAt} IS NULL AND ${table.revokedAt} IS NULL`),
	check("magic_links_check", sql`(purpose <> 'archive_delete_reauth'::magic_link_purpose) OR ((user_id IS NOT NULL) AND (consultation_id IS NOT NULL) AND (session_id IS NOT NULL))`),
	check("magic_links_sealed_token_nonempty_check", sql`length(sealed_raw_token) > 0 AND length(sealed_token_key_id) > 0`),
]);

export const pendingExchanges = pgTable("pending_exchanges", {
	id: uuid().primaryKey().notNull(),
	magicLinkId: uuid("magic_link_id").notNull(),
	nonceHash: text("nonce_hash").notNull(),
	csrfHash: text("csrf_hash").notNull(),
	expiresAt: timestamp("expires_at", { withTimezone: true, mode: 'date' }).notNull(),
	consumedAt: timestamp("consumed_at", { withTimezone: true, mode: 'date' }),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.magicLinkId],
			foreignColumns: [magicLinks.id],
			name: "pending_exchanges_magic_link_id_fkey"
		}),
	unique("pending_exchanges_nonce_hash_key").on(table.nonceHash),
	uniqueIndex("pending_exchanges_live_magic_link_unique").on(table.magicLinkId).where(sql`${table.consumedAt} IS NULL`),
]);

export const sessions = pgTable("sessions", {
	id: uuid().primaryKey().notNull(),
	userId: uuid("user_id").notNull(),
	tokenHash: text("token_hash").notNull(),
	csrfHash: text("csrf_hash").notNull(),
	expiresAt: timestamp("expires_at", { withTimezone: true, mode: 'date' }).notNull(),
	reauthenticatedAt: timestamp("reauthenticated_at", { withTimezone: true, mode: 'date' }),
	reauthConsultationId: uuid("reauth_consultation_id"),
	revokedAt: timestamp("revoked_at", { withTimezone: true, mode: 'date' }),
	replacedBy: uuid("replaced_by"),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.userId],
			foreignColumns: [users.id],
			name: "sessions_user_id_fkey"
		}),
	foreignKey({
			columns: [table.reauthConsultationId],
			foreignColumns: [consultations.id],
			name: "sessions_reauth_consultation_id_fkey"
		}),
	foreignKey({
			columns: [table.replacedBy],
			foreignColumns: [table.id],
			name: "sessions_replaced_by_fkey"
		}),
	unique("sessions_token_hash_key").on(table.tokenHash),
	check("sessions_check", sql`(reauthenticated_at IS NULL) = (reauth_consultation_id IS NULL)`),
]);

export const magicLinkRequests = pgTable("magic_link_requests", {
	id: bigserial({ mode: "bigint" }).primaryKey().notNull(),
	emailHash: text("email_hash"),
	ipHash: text("ip_hash").notNull(),
	requestedAt: timestamp("requested_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	index("magic_link_email_rate_idx").using("btree", table.emailHash.asc().nullsLast().op("text_ops"), table.requestedAt.asc().nullsLast().op("timestamptz_ops")),
	index("magic_link_ip_rate_idx").using("btree", table.ipHash.asc().nullsLast().op("text_ops"), table.requestedAt.asc().nullsLast().op("timestamptz_ops")),
	index("magic_link_requested_at_idx").on(table.requestedAt),
]);

export const workerLeases = pgTable("worker_leases", {
	workerId: uuid("worker_id").primaryKey().notNull(),
	acceptingLoad: boolean("accepting_load").notNull(),
	capacity: integer().notNull(),
	reserved: integer().notNull(),
	encryptedSpoolPercent: numeric("encrypted_spool_percent", { precision: 5, scale:  2 }).notNull(),
	providersOk: boolean("providers_ok").notNull(),
	archiveOk: boolean("archive_ok").notNull(),
	heartbeatAt: timestamp("heartbeat_at", { withTimezone: true, mode: 'date' }).notNull(),
	expiresAt: timestamp("expires_at", { withTimezone: true, mode: 'date' }).notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	epoch: bigint({ mode: "number" }).notNull(),
	status: jsonb().notNull(),
}, (table) => [
	check("worker_leases_capacity_check", sql`capacity >= 0`),
	check("worker_leases_check", sql`(reserved >= 0) AND (reserved <= capacity)`),
	check("worker_leases_encrypted_spool_percent_check", sql`(encrypted_spool_percent >= (0)::numeric) AND (encrypted_spool_percent <= (100)::numeric)`),
	check("worker_leases_epoch_check", sql`epoch >= 0`),
]);

export const workerCheckpoints = pgTable("worker_checkpoints", {
	id: uuid().primaryKey().notNull(),
	consultationId: uuid("consultation_id").notNull(),
	generation: integer().notNull(),
	workerId: uuid("worker_id").notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	workerEpoch: bigint("worker_epoch", { mode: "number" }).notNull(),
	writeEpoch: integer("write_epoch").notNull(),
	sourceParticipantId: uuid("source_participant_id").notNull(),
	destinationParticipantId: uuid("destination_participant_id").notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	acceptedInputSequence: bigint("accepted_input_sequence", { mode: "number" }).notNull(),
	acceptedInput: bigint("accepted_input", { mode: "number" }).notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	receivedOutput: bigint("received_output", { mode: "number" }).notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	emittedOutput: bigint("emitted_output", { mode: "number" }).notNull(),
	previousHash: text("previous_hash"),
	checkpointHash: text("checkpoint_hash").notNull(),
	expectedIds: jsonb("expected_ids").notNull(),
	observedIds: jsonb("observed_ids").notNull(),
	gaps: jsonb().notNull(),
	terminal: boolean().notNull(),
	objectKey: text("object_key").notNull(),
	objectVersionId: text("object_version_id"),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.consultationId],
			foreignColumns: [consultations.id],
			name: "worker_checkpoints_consultation_id_fkey"
		}),
	foreignKey({
			columns: [table.consultationId, table.generation, table.workerId, table.workerEpoch],
			foreignColumns: [workerReservations.consultationId, workerReservations.generation, workerReservations.workerId, workerReservations.epoch],
			name: "worker_checkpoints_consultation_id_generation_worker_id_wo_fkey"
		}),
	foreignKey({
			columns: [table.consultationId, table.sourceParticipantId],
			foreignColumns: [consultationParticipants.consultationId, consultationParticipants.id],
			name: "worker_checkpoints_source_participant_fkey"
		}),
	foreignKey({
			columns: [table.consultationId, table.destinationParticipantId],
			foreignColumns: [consultationParticipants.consultationId, consultationParticipants.id],
			name: "worker_checkpoints_destination_participant_fkey"
		}),
	unique("worker_checkpoints_direction_watermarks_key").on(table.consultationId, table.workerEpoch, table.sourceParticipantId, table.destinationParticipantId, table.acceptedInputSequence, table.acceptedInput, table.receivedOutput, table.emittedOutput),
	unique("worker_checkpoints_checkpoint_hash_key").on(table.checkpointHash),
	uniqueIndex("worker_checkpoints_terminal_direction_unique").using("btree", table.consultationId.asc().nullsLast().op("uuid_ops"), table.generation.asc().nullsLast().op("int4_ops"), table.workerId.asc().nullsLast().op("uuid_ops"), table.workerEpoch.asc().nullsLast().op("int8_ops"), table.sourceParticipantId.asc().nullsLast().op("uuid_ops"), table.destinationParticipantId.asc().nullsLast().op("uuid_ops")).where(sql`terminal`),
	check("worker_checkpoints_accepted_input_sequence_check", sql`accepted_input_sequence >= 0`),
	check("worker_checkpoints_accepted_input_check", sql`accepted_input >= 0`),
	check("worker_checkpoints_received_output_check", sql`received_output >= 0`),
	check("worker_checkpoints_emitted_output_check", sql`emitted_output >= 0`),
]);

export const externalEffects = pgTable("external_effects", {
	id: uuid().primaryKey().notNull(),
	consultationId: uuid("consultation_id").notNull(),
	generation: integer().notNull(),
	effectKind: text("effect_kind").notNull(),
	subjectId: uuid("subject_id").notNull(),
	state: externalEffectState().notNull(),
	requestBytes: bytea("request_bytes"),
	requestHash: text("request_hash"),
	leaseOwner: uuid("lease_owner"),
	leaseExpiresAt: timestamp("lease_expires_at", { withTimezone: true, mode: 'date' }),
	result: jsonb(),
	attempts: integer().default(0).notNull(),
	compensationResult: jsonb("compensation_result"),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'date' }).notNull(),
	occurrenceKey: text("occurrence_key").default("").notNull(),
}, (table) => [
	foreignKey({
			columns: [table.consultationId],
			foreignColumns: [consultations.id],
			name: "external_effects_consultation_id_fkey"
		}),
	unique("external_effect_occurrence_unique").on(table.consultationId, table.generation, table.effectKind, table.subjectId, table.occurrenceKey),
	check("external_effects_attempts_check", sql`attempts >= 0`),
	check("external_effects_check", sql`(request_bytes IS NULL) = (request_hash IS NULL)`),
	check("external_effects_check1", sql`(state = 'planned'::external_effect_state) OR (request_hash IS NOT NULL)`),
	check("external_effects_egress_intent_redacted_check", sql`effect_kind NOT IN ('ROOM_COMPOSITE_EGRESS', 'PARTICIPANT_EGRESS') OR request_bytes IS NULL OR lower(convert_from(request_bytes, 'UTF8')) !~ '"(accesskey|secret|custombaseurl|signature|authorization|token|filenameprefix|playlistname|liveplaylistname)"'`),
]);

export const effectCompensationAttempts = pgTable("effect_compensation_attempts", {
	id: bigserial({ mode: "bigint" }).primaryKey().notNull(),
	effectId: uuid("effect_id").notNull(),
	owner: uuid().notNull(),
	requestHash: text("request_hash").notNull(),
	result: jsonb().notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.effectId],
			foreignColumns: [externalEffects.id],
			name: "effect_compensation_attempts_effect_id_fkey"
		}),
	unique("effect_compensation_attempts_effect_id_owner_request_hash_key").on(table.effectId, table.owner, table.requestHash),
]);

export const outbox = pgTable("outbox", {
	id: uuid().primaryKey().notNull(),
	topic: text().notNull(),
	aggregateId: uuid("aggregate_id").notNull(),
	generation: integer().notNull(),
	payload: jsonb().notNull(),
	availableAt: timestamp("available_at", { withTimezone: true, mode: 'date' }).notNull(),
	attempts: integer().default(0).notNull(),
	leaseOwner: uuid("lease_owner"),
	leaseExpiresAt: timestamp("lease_expires_at", { withTimezone: true, mode: 'date' }),
	deliveredAt: timestamp("delivered_at", { withTimezone: true, mode: 'date' }),
}, (table) => [
	index("outbox_claim_idx").using("btree", table.availableAt.asc().nullsLast().op("timestamptz_ops")).where(sql`(delivered_at IS NULL)`),
	check("outbox_attempts_check", sql`attempts >= 0`),
]);

export const egressJobs = pgTable("egress_jobs", {
	id: uuid().primaryKey().notNull(),
	consultationId: uuid("consultation_id").notNull(),
	generation: integer().notNull(),
	kind: text().notNull(),
	subjectId: uuid("subject_id").notNull(),
	egressId: text("egress_id"),
	requestHash: text("request_hash").notNull(),
	state: text().notNull(),
	outputPrefix: text("output_prefix").notNull(),
	startedAt: timestamp("started_at", { withTimezone: true, mode: 'date' }),
	terminalAt: timestamp("terminal_at", { withTimezone: true, mode: 'date' }),
	terminalResult: jsonb("terminal_result"),
	expectedArtifactId: uuid("expected_artifact_id"),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.consultationId],
			foreignColumns: [consultations.id],
			name: "egress_jobs_consultation_id_fkey"
		}),
	foreignKey({
			columns: [table.expectedArtifactId],
			foreignColumns: [expectedArchiveArtifacts.id],
			name: "egress_expected_artifact_fk"
		}),
	unique("egress_jobs_consultation_id_generation_kind_subject_id_key").on(table.consultationId, table.generation, table.kind, table.subjectId),
	unique("egress_jobs_egress_id_key").on(table.egressId),
	check("egress_jobs_kind_check", sql`kind = ANY (ARRAY['room_composite'::text, 'participant'::text, 'track'::text])`),
	check("egress_jobs_state_check", sql`state = ANY (ARRAY['requested'::text, 'EGRESS_STARTING'::text, 'EGRESS_ACTIVE'::text, 'EGRESS_ENDING'::text, 'EGRESS_COMPLETE'::text, 'EGRESS_FAILED'::text, 'EGRESS_ABORTED'::text, 'EGRESS_LIMIT_REACHED'::text])`),
]);

export const archives = pgTable("archives", {
	id: uuid().primaryKey().notNull(),
	consultationId: uuid("consultation_id").notNull(),
	state: archiveState().default('pending').notNull(),
	writeEpoch: integer("write_epoch").default(0).notNull(),
	finalInventoryHash: text("final_inventory_hash"),
	reconciliationDeadlineAt: timestamp("reconciliation_deadline_at", { withTimezone: true, mode: 'date' }),
	deletionFailure: jsonb("deletion_failure"),
	completedDeletionEpoch: integer("completed_deletion_epoch"),
	deletedAt: timestamp("deleted_at", { withTimezone: true, mode: 'date' }),
	holdOperationId: uuid("hold_operation_id"),
	holdOperationOwner: uuid("hold_operation_owner"),
	holdOperationKind: text("hold_operation_kind"),
	holdOperationStartedAt: timestamp("hold_operation_started_at", { withTimezone: true, mode: 'date' }),
	holdOperationLeaseExpiresAt: timestamp("hold_operation_lease_expires_at", { withTimezone: true, mode: 'date' }),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.consultationId],
			foreignColumns: [consultations.id],
			name: "archives_consultation_id_fkey"
		}),
	unique("archives_consultation_id_key").on(table.consultationId),
	unique("archives_id_consultation_id_key").on(table.id, table.consultationId),
	check("archives_write_epoch_check", sql`write_epoch >= 0`),
	check("archives_hold_operation_kind_check", sql`hold_operation_kind = ANY (ARRAY['add'::text, 'release'::text])`),
	check("archives_check", sql`((hold_operation_id IS NULL) = (hold_operation_owner IS NULL)) AND ((hold_operation_id IS NULL) = (hold_operation_kind IS NULL)) AND ((hold_operation_id IS NULL) = (hold_operation_started_at IS NULL)) AND ((hold_operation_id IS NULL) = (hold_operation_lease_expires_at IS NULL))`),
]);

export const expectedArchiveArtifacts = pgTable("expected_archive_artifacts", {
	id: uuid().primaryKey().notNull(),
	archiveId: uuid("archive_id").notNull(),
	effectId: uuid("effect_id"),
	profileId: uuid("profile_id").notNull(),
	profileRevision: integer("profile_revision").notNull(),
	objectClass: text("object_class").notNull(),
	causalKey: text("causal_key").notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	sampleStart: bigint("sample_start", { mode: "number" }),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	sampleEnd: bigint("sample_end", { mode: "number" }),
	segmentStart: integer("segment_start"),
	segmentEnd: integer("segment_end"),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	ownerEpoch: bigint("owner_epoch", { mode: "number" }).notNull(),
	disposition: text().default('expected').notNull(),
	fulfilledObjectId: uuid("fulfilled_object_id"),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.archiveId],
			foreignColumns: [archives.id],
			name: "expected_archive_artifacts_archive_id_fkey"
		}),
	foreignKey({
			columns: [table.effectId],
			foreignColumns: [externalEffects.id],
			name: "expected_archive_artifacts_effect_id_fkey"
		}),
	foreignKey({
			columns: [table.profileId, table.profileRevision],
			foreignColumns: [providerProfileRevisions.profileId, providerProfileRevisions.revision],
			name: "expected_archive_artifacts_profile_id_profile_revision_fkey"
		}),
	foreignKey({
			columns: [table.fulfilledObjectId],
			foreignColumns: [archiveObjects.id],
			name: "expected_fulfilled_object_fk"
		}),
	unique("expected_archive_artifacts_archive_id_object_class_causal_k_key").on(table.archiveId, table.objectClass, table.causalKey),
	unique("expected_archive_artifacts_effect_id_key").on(table.effectId),
	check("expected_archive_artifacts_check", sql`((sample_start IS NULL) AND (sample_end IS NULL)) OR ((sample_start >= 0) AND (sample_end > sample_start))`),
	check("expected_archive_artifacts_check1", sql`((segment_start IS NULL) AND (segment_end IS NULL)) OR ((segment_start >= 0) AND (segment_end > segment_start))`),
]);

export const archiveObjects = pgTable("archive_objects", {
	id: uuid().primaryKey().notNull(),
	archiveId: uuid("archive_id").notNull(),
	objectClass: text("object_class").notNull(),
	causalKey: text("causal_key").notNull(),
	key: text().notNull(),
	versionId: text("version_id").notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	size: bigint({ mode: "number" }).notNull(),
	sha256: text().notNull(),
	s3Checksum: text("s3_checksum").notNull(),
	contentType: text("content_type").notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	sampleStart: bigint("sample_start", { mode: "number" }),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	sampleEnd: bigint("sample_end", { mode: "number" }),
	attempt: integer(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	sequence: bigint({ mode: "number" }),
	writerEpoch: integer("writer_epoch").notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.archiveId],
			foreignColumns: [archives.id],
			name: "archive_objects_archive_id_fkey"
		}),
	unique("archive_objects_archive_id_object_class_attempt_sequence_key").on(table.archiveId, table.objectClass, table.attempt, table.sequence),
	unique("archive_objects_key_version_id_key").on(table.key, table.versionId),
	check("archive_objects_size_check", sql`size >= 0`),
	check("archive_objects_attempt_check", sql`(attempt IS NULL) OR (attempt > 0)`),
	check("archive_objects_check", sql`((sample_start IS NULL) AND (sample_end IS NULL)) OR ((sample_start >= 0) AND (sample_end > sample_start))`),
	check("archive_objects_sequence_check", sql`(sequence IS NULL) OR (sequence >= 0)`),
]);

export const multipartUploads = pgTable("multipart_uploads", {
	id: uuid().primaryKey().notNull(),
	archiveId: uuid("archive_id").notNull(),
	key: text().notNull(),
	uploadId: text("upload_id").notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	size: bigint({ mode: "number" }).notNull(),
	state: text().notNull(),
	checksum: text().notNull(),
	writerEpoch: integer("writer_epoch").notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
	updatedAt: timestamp("updated_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.archiveId],
			foreignColumns: [archives.id],
			name: "multipart_uploads_archive_id_fkey"
		}),
	unique("multipart_uploads_upload_id_key").on(table.uploadId),
	check("multipart_uploads_state_check", sql`state = ANY (ARRAY['open'::text, 'completing'::text, 'complete'::text, 'aborted'::text])`),
	check("multipart_uploads_size_check", sql`size >= 104857600`),
]);

export const providerAttempts = pgTable("provider_attempts", {
	id: uuid().primaryKey().notNull(),
	archiveId: uuid("archive_id").notNull(),
	consultationId: uuid("consultation_id").notNull(),
	profileId: uuid("profile_id").notNull(),
	profileRevision: integer("profile_revision").notNull(),
	stage: text().notNull(),
	provider: text().notNull(),
	directionId: uuid("direction_id").notNull(),
	operationId: uuid("operation_id").notNull(),
	attemptNumber: integer("attempt_number").notNull(),
	retryOf: uuid("retry_of"),
	credentialReference: text("credential_reference").notNull(),
	credentialVersion: text("credential_version").notNull(),
	credentialFingerprint: text("credential_fingerprint").notNull(),
	endpoint: text().notNull(),
	apiVersion: text("api_version").notNull(),
	model: text().notNull(),
	voice: text(),
	outcome: terminalOutcome(),
	errorKind: text("error_kind"),
	errorScope: text("error_scope"),
	providerRetryAdvice: text("provider_retry_advice"),
	providerCode: text("provider_code"),
	providerRequestId: text("provider_request_id"),
	retryDelayMs: integer("retry_delay_ms"),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	acceptedInputWatermark: bigint("accepted_input_watermark", { mode: "number" }),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	receivedOutputWatermark: bigint("received_output_watermark", { mode: "number" }),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	emittedOutputWatermark: bigint("emitted_output_watermark", { mode: "number" }),
	retryDecision: jsonb("retry_decision"),
	transport: transportKind(),
	rawHttp: jsonb("raw_http"),
	rawWebsocket: jsonb("raw_websocket"),
	rawGrpc: jsonb("raw_grpc"),
	terminalHash: text("terminal_hash"),
	startedAt: timestamp("started_at", { withTimezone: true, mode: 'date' }).notNull(),
	terminalAt: timestamp("terminal_at", { withTimezone: true, mode: 'date' }),
}, (table) => [
	foreignKey({
			columns: [table.archiveId, table.consultationId],
			foreignColumns: [archives.id, archives.consultationId],
			name: "provider_attempts_archive_consultation_fkey"
		}),
	foreignKey({
			columns: [table.retryOf],
			foreignColumns: [table.id],
			name: "provider_attempts_retry_of_fkey"
		}),
	foreignKey({
			columns: [table.profileId, table.profileRevision],
			foreignColumns: [providerProfileRevisions.profileId, providerProfileRevisions.revision],
			name: "provider_attempts_profile_id_profile_revision_fkey"
		}),
	unique("provider_attempts_consultation_id_direction_id_operation_id_key").on(table.consultationId, table.directionId, table.operationId, table.attemptNumber),
	unique("provider_attempts_terminal_hash_key").on(table.terminalHash),
	check("provider_attempts_stage_check", sql`stage = ANY (ARRAY['stt'::text, 'translation'::text, 'tts'::text])`),
	check("provider_attempts_attempt_number_check", sql`attempt_number > 0`),
	check("provider_attempts_error_scope_check", sql`(error_scope IS NULL) OR (error_scope = ANY (ARRAY['operation'::text, 'session'::text]))`),
	check("provider_attempts_provider_retry_advice_check", sql`(provider_retry_advice IS NULL) OR (provider_retry_advice = ANY (ARRAY['never'::text, 'retry_after'::text, 'unspecified'::text]))`),
	check("provider_attempts_check", sql`(terminal_at IS NULL) = (outcome IS NULL)`),
	check("provider_attempts_check1", sql`((transport IS NULL) AND (raw_http IS NULL) AND (raw_websocket IS NULL) AND (raw_grpc IS NULL)) OR ((transport = 'http'::transport_kind) AND (raw_http IS NOT NULL) AND (raw_websocket IS NULL) AND (raw_grpc IS NULL)) OR ((transport = 'websocket'::transport_kind) AND (raw_http IS NULL) AND (raw_websocket IS NOT NULL) AND (raw_grpc IS NULL)) OR ((transport = 'grpc'::transport_kind) AND (raw_http IS NULL) AND (raw_websocket IS NULL) AND (raw_grpc IS NOT NULL))`),
	check("provider_attempts_check2", sql`(terminal_at IS NULL) OR ((transport IS NOT NULL) AND (retry_decision IS NOT NULL) AND (terminal_hash IS NOT NULL))`),
]);

export const finalInventories = pgTable("final_inventories", {
	archiveId: uuid("archive_id").primaryKey().notNull(),
	status: text().notNull(),
	inventory: jsonb().notNull(),
	sha256: text().notNull(),
	objectId: uuid("object_id"),
	roomClose: jsonb("room_close").notNull(),
	workerTerminal: jsonb("worker_terminal").notNull(),
	egressResults: jsonb("egress_results").notNull(),
	missing: jsonb().notNull(),
	errors: jsonb().notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.archiveId],
			foreignColumns: [archives.id],
			name: "final_inventories_archive_id_fkey"
		}),
	foreignKey({
			columns: [table.objectId],
			foreignColumns: [archiveObjects.id],
			name: "final_inventories_object_id_fkey"
		}),
	unique("final_inventories_sha256_key").on(table.sha256),
	check("final_inventories_status_check", sql`status = ANY (ARRAY['complete'::text, 'incomplete'::text])`),
]);

export const inventorySupplements = pgTable("inventory_supplements", {
	id: uuid().primaryKey().notNull(),
	archiveId: uuid("archive_id").notNull(),
	finalInventorySha256: text("final_inventory_sha256").notNull(),
	supplement: jsonb().notNull(),
	sha256: text().notNull(),
	objectId: uuid("object_id"),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.archiveId],
			foreignColumns: [archives.id],
			name: "inventory_supplements_archive_id_fkey"
		}),
	foreignKey({
			columns: [table.objectId],
			foreignColumns: [archiveObjects.id],
			name: "inventory_supplements_object_id_fkey"
		}),
	foreignKey({
			columns: [table.archiveId],
			foreignColumns: [finalInventories.archiveId],
			name: "inventory_supplements_archive_id_fkey1"
		}),
	unique("inventory_supplements_sha256_key").on(table.sha256),
]);

export const legalHolds = pgTable("legal_holds", {
	id: uuid().primaryKey().notNull(),
	archiveId: uuid("archive_id").notNull(),
	reason: text().notNull(),
	actorId: uuid("actor_id").notNull(),
	sessionId: uuid("session_id").notNull(),
	reauthenticatedAt: timestamp("reauthenticated_at", { withTimezone: true, mode: 'date' }).notNull(),
	state: text().default('applying').notNull(),
	placedAt: timestamp("placed_at", { withTimezone: true, mode: 'date' }).notNull(),
	releasedAt: timestamp("released_at", { withTimezone: true, mode: 'date' }),
	releasedBy: uuid("released_by"),
	aggregateResult: jsonb("aggregate_result"),
	perVersionResults: jsonb("per_version_results"),
}, (table) => [
	uniqueIndex("legal_holds_active_unique").using("btree", table.archiveId.asc().nullsLast().op("uuid_ops"), table.id.asc().nullsLast().op("uuid_ops")).where(sql`(released_at IS NULL)`),
	foreignKey({
			columns: [table.archiveId],
			foreignColumns: [archives.id],
			name: "legal_holds_archive_id_fkey"
		}),
	foreignKey({
			columns: [table.actorId],
			foreignColumns: [users.id],
			name: "legal_holds_actor_id_fkey"
		}),
	foreignKey({
			columns: [table.sessionId],
			foreignColumns: [sessions.id],
			name: "legal_holds_session_id_fkey"
		}),
	foreignKey({
			columns: [table.releasedBy],
			foreignColumns: [users.id],
			name: "legal_holds_released_by_fkey"
		}),
	check("legal_holds_state_check", sql`state = ANY (ARRAY['applying'::text, 'active'::text, 'releasing'::text, 'failed'::text, 'released'::text])`),
	check("legal_holds_check", sql`reauthenticated_at >= (placed_at - '00:05:00'::interval)`),
	check("legal_holds_check1", sql`(state = 'released'::text) = (released_at IS NOT NULL)`),
	check("legal_holds_check2", sql`(released_at IS NULL) = (released_by IS NULL)`),
]);

export const deletionScans = pgTable("deletion_scans", {
	id: uuid().primaryKey().notNull(),
	archiveId: uuid("archive_id").notNull(),
	writeEpoch: integer("write_epoch").notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	versionCount: bigint("version_count", { mode: "number" }).notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	multipartCount: bigint("multipart_count", { mode: "number" }).notNull(),
	consecutiveEmpty: integer("consecutive_empty").notNull(),
	result: jsonb().notNull(),
	scannedAt: timestamp("scanned_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.archiveId],
			foreignColumns: [archives.id],
			name: "deletion_scans_archive_id_fkey"
		}),
	check("deletion_scans_consecutive_empty_check", sql`(consecutive_empty >= 0) AND (consecutive_empty <= 2)`),
]);

export const auditEvents = pgTable("audit_events", {
	sequence: bigserial({ mode: "bigint" }).primaryKey().notNull(),
	id: uuid().notNull(),
	aggregateId: uuid("aggregate_id").notNull(),
	actorId: uuid("actor_id"),
	kind: text().notNull(),
	occurredAt: timestamp("occurred_at", { withTimezone: true, mode: 'date' }).notNull(),
	details: jsonb().notNull(),
}, (table) => [
	index("audit_aggregate_sequence_idx").using("btree", table.aggregateId.asc().nullsLast().op("uuid_ops"), table.sequence.asc().nullsLast().op("int8_ops")),
	foreignKey({
			columns: [table.actorId],
			foreignColumns: [users.id],
			name: "audit_events_actor_id_fkey"
		}),
	unique("audit_events_id_key").on(table.id),
]);

export const inbox = pgTable("inbox", {
	source: text().notNull(),
	eventId: text("event_id").notNull(),
	occurredAt: timestamp("occurred_at", { withTimezone: true, mode: 'date' }).notNull(),
	payloadHash: text("payload_hash").notNull(),
	payload: jsonb().notNull(),
	receivedAt: timestamp("received_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	primaryKey({ columns: [table.source, table.eventId], name: "inbox_pkey"}),
]);

export const multipartParts = pgTable("multipart_parts", {
	uploadId: uuid("upload_id").notNull(),
	partNumber: integer("part_number").notNull(),
	etag: text().notNull(),
	checksum: text().notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	size: bigint({ mode: "number" }).notNull(),
	uploadedAt: timestamp("uploaded_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.uploadId],
			foreignColumns: [multipartUploads.id],
			name: "multipart_parts_upload_id_fkey"
		}),
	primaryKey({ columns: [table.uploadId, table.partNumber], name: "multipart_parts_pkey"}),
	check("multipart_parts_part_number_check", sql`(part_number >= 1) AND (part_number <= 10000)`),
	check("multipart_parts_size_check", sql`size > 0`),
]);

export const providerProfileRevisions = pgTable("provider_profile_revisions", {
	profileId: uuid("profile_id").notNull(),
	revision: integer().notNull(),
	capabilityHash: text("capability_hash").notNull(),
	adapterBuilds: jsonb("adapter_builds").notNull(),
	policy: jsonb().notNull(),
	credentialReferences: jsonb("credential_references").notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.profileId],
			foreignColumns: [providerProfiles.id],
			name: "provider_profile_revisions_profile_id_fkey"
		}),
	primaryKey({ columns: [table.profileId, table.revision], name: "provider_profile_revisions_pkey"}),
	unique("provider_profile_revisions_profile_id_capability_hash_key").on(table.profileId, table.capabilityHash),
	check("provider_profile_revisions_revision_check", sql`revision > 0`),
]);

export const orchestrationDeadlines = pgTable("orchestration_deadlines", {
	consultationId: uuid("consultation_id").notNull(),
	generation: integer().notNull(),
	kind: text().notNull(),
	dueAt: timestamp("due_at", { withTimezone: true, mode: 'date' }).notNull(),
	leaseOwner: uuid("lease_owner"),
	leaseExpiresAt: timestamp("lease_expires_at", { withTimezone: true, mode: 'date' }),
	completedAt: timestamp("completed_at", { withTimezone: true, mode: 'date' }),
}, (table) => [
	foreignKey({
			columns: [table.consultationId],
			foreignColumns: [consultations.id],
			name: "orchestration_deadlines_consultation_id_fkey"
		}),
	primaryKey({ columns: [table.consultationId, table.generation, table.kind], name: "orchestration_deadlines_pkey"}),
	check("orchestration_deadlines_kind_check", sql`kind = ANY (ARRAY['ready'::text, 'absence'::text, 'finalize'::text, 'archive-reconcile'::text])`),
]);

export const workerJobEpochs = pgTable("worker_job_epochs", {
	consultationId: uuid("consultation_id").notNull(),
	generation: integer().notNull(),
	workerId: uuid("worker_id").notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	epoch: bigint({ mode: "number" }).notNull(),
	writeEpoch: integer("write_epoch").default(0).notNull(),
	heartbeatAt: timestamp("heartbeat_at", { withTimezone: true, mode: 'date' }).notNull(),
	fencedAt: timestamp("fenced_at", { withTimezone: true, mode: 'date' }),
	terminalCheckpointId: uuid("terminal_checkpoint_id"),
	terminalOutcome: text("terminal_outcome"),
	terminalAt: timestamp("terminal_at", { withTimezone: true, mode: 'date' }),
}, (table) => [
	foreignKey({
			columns: [table.consultationId],
			foreignColumns: [consultations.id],
			name: "worker_job_epochs_consultation_id_fkey"
		}),
	foreignKey({
			columns: [table.workerId],
			foreignColumns: [workerLeases.workerId],
			name: "worker_job_epochs_worker_id_fkey"
		}),
	foreignKey({
			columns: [table.terminalCheckpointId],
			foreignColumns: [workerCheckpoints.id],
			name: "worker_job_terminal_checkpoint_fk"
		}),
	primaryKey({ columns: [table.consultationId, table.generation, table.epoch], name: "worker_job_epochs_pkey"}),
	unique("worker_job_epochs_worker_id_epoch_consultation_id_key").on(table.consultationId, table.workerId, table.epoch),
	check("worker_job_epochs_epoch_check", sql`epoch >= 0`),
	check("worker_job_epochs_write_epoch_check", sql`write_epoch >= 0`),
	check("worker_job_epochs_terminal_outcome_check", sql`terminal_outcome = ANY (ARRAY['clean'::text, 'fenced'::text, 'failed'::text])`),
]);

export const workerReservations = pgTable("worker_reservations", {
	consultationId: uuid("consultation_id").notNull(),
	generation: integer().notNull(),
	workerId: uuid("worker_id").notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	epoch: bigint({ mode: "number" }).notNull(),
	selectionHash: text("selection_hash").notNull(),
	reservedAt: timestamp("reserved_at", { withTimezone: true, mode: 'date' }).notNull(),
	heartbeatAt: timestamp("heartbeat_at", { withTimezone: true, mode: 'date' }).notNull(),
	leaseExpiresAt: timestamp("lease_expires_at", { withTimezone: true, mode: 'date' }).notNull(),
	acceptingLoad: boolean("accepting_load").notNull(),
	fencedAt: timestamp("fenced_at", { withTimezone: true, mode: 'date' }),
	fenceReason: text("fence_reason"),
	supervisorOwner: uuid("supervisor_owner"),
	supervisorLeaseExpiresAt: timestamp("supervisor_lease_expires_at", { withTimezone: true, mode: 'date' }),
	releasedAt: timestamp("released_at", { withTimezone: true, mode: 'date' }),
}, (table) => [
	foreignKey({
			columns: [table.consultationId],
			foreignColumns: [consultations.id],
			name: "worker_reservations_consultation_id_fkey"
		}),
	foreignKey({
			columns: [table.workerId],
			foreignColumns: [workerLeases.workerId],
			name: "worker_reservations_worker_id_fkey"
		}),
	primaryKey({ columns: [table.consultationId, table.generation], name: "worker_reservations_pkey"}),
	unique("worker_reservations_consultation_id_generation_worker_id_ep_key").on(table.consultationId, table.generation, table.workerId, table.epoch),
	check("worker_reservations_epoch_check", sql`epoch >= 0`),
]);

export const captionLedger = pgTable("caption_ledger", {
	consultationId: uuid("consultation_id").notNull(),
	destinationParticipantId: uuid("destination_participant_id").notNull(),
	utteranceId: uuid("utterance_id").notNull(),
	revision: integer().notNull(),
	finality: text().notNull(),
	sourceParticipantId: uuid("source_participant_id").notNull(),
	sourceLanguage: text("source_language").notNull(),
	targetLanguage: text("target_language").notNull(),
	sourceText: text("source_text").notNull(),
	translatedText: text("translated_text").notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	sourceSampleStart: bigint("source_sample_start", { mode: "number" }).notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	sourceSampleEnd: bigint("source_sample_end", { mode: "number" }).notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	occurredAtMs: bigint("occurred_at_ms", { mode: "number" }).notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	writerEpoch: bigint("writer_epoch", { mode: "number" }).notNull(),
	archivedObjectId: uuid("archived_object_id"),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'date' }).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.consultationId],
			foreignColumns: [consultations.id],
			name: "caption_ledger_consultation_id_fkey"
		}),
	foreignKey({
			columns: [table.archivedObjectId],
			foreignColumns: [archiveObjects.id],
			name: "caption_ledger_archived_object_id_fkey"
		}),
	primaryKey({ columns: [table.consultationId, table.destinationParticipantId, table.utteranceId, table.revision], name: "caption_ledger_pkey"}),
	check("caption_ledger_revision_check", sql`revision > 0`),
	check("caption_ledger_finality_check", sql`finality = ANY (ARRAY['provisional'::text, 'final'::text])`),
	check("caption_ledger_source_sample_start_check", sql`source_sample_start >= 0`),
	check("caption_ledger_check", sql`source_sample_end > source_sample_start`),
]);
