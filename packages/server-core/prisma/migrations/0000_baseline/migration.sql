CREATE TYPE "public"."archive_state" AS ENUM('pending', 'recording', 'reconciling', 'complete', 'incomplete', 'deleting', 'deleted');--> statement-breakpoint
CREATE TYPE "public"."consultation_state" AS ENUM('invited', 'ready', 'active', 'finalizing', 'ended', 'cancelled', 'deleted');--> statement-breakpoint
CREATE TYPE "public"."external_effect_state" AS ENUM('planned', 'calling', 'applied', 'compensating', 'done', 'failed');--> statement-breakpoint
CREATE TYPE "public"."magic_link_purpose" AS ENUM('sign_in', 'consultation_invite', 'archive_delete_reauth');--> statement-breakpoint
CREATE TYPE "public"."participant_role" AS ENUM('employee', 'customer');--> statement-breakpoint
CREATE TYPE "public"."staff_role" AS ENUM('employee', 'admin');--> statement-breakpoint
CREATE TYPE "public"."terminal_outcome" AS ENUM('succeeded', 'failed', 'cancelled', 'degraded', 'unknown');--> statement-breakpoint
CREATE TYPE "public"."transport_kind" AS ENUM('http', 'websocket', 'grpc');--> statement-breakpoint
CREATE TABLE "archive_objects" (
	"id" uuid PRIMARY KEY NOT NULL,
	"archive_id" uuid NOT NULL,
	"object_class" text NOT NULL,
	"causal_key" text NOT NULL,
	"key" text NOT NULL,
	"version_id" text NOT NULL,
	"size" bigint NOT NULL,
	"sha256" text NOT NULL,
	"s3_checksum" text NOT NULL,
	"content_type" text NOT NULL,
	"sample_start" bigint,
	"sample_end" bigint,
	"attempt" integer,
	"sequence" bigint,
	"writer_epoch" integer NOT NULL,
	"created_at" timestamp with time zone NOT NULL,
	CONSTRAINT "archive_objects_archive_id_object_class_attempt_sequence_key" UNIQUE("archive_id","object_class","attempt","sequence"),
	CONSTRAINT "archive_objects_key_version_id_key" UNIQUE("key","version_id"),
	CONSTRAINT "archive_objects_size_check" CHECK (size >= 0),
	CONSTRAINT "archive_objects_attempt_check" CHECK ((attempt IS NULL) OR (attempt > 0)),
	CONSTRAINT "archive_objects_check" CHECK (((sample_start IS NULL) AND (sample_end IS NULL)) OR ((sample_start >= 0) AND (sample_end > sample_start))),
	CONSTRAINT "archive_objects_sequence_check" CHECK ((sequence IS NULL) OR (sequence >= 0))
);
--> statement-breakpoint
CREATE TABLE "archives" (
	"id" uuid PRIMARY KEY NOT NULL,
	"consultation_id" uuid NOT NULL,
	"state" "archive_state" DEFAULT 'pending' NOT NULL,
	"write_epoch" integer DEFAULT 0 NOT NULL,
	"final_inventory_hash" text,
	"reconciliation_deadline_at" timestamp with time zone,
	"deletion_failure" jsonb,
	"completed_deletion_epoch" integer,
	"deleted_at" timestamp with time zone,
	"hold_operation_id" uuid,
	"hold_operation_owner" uuid,
	"hold_operation_kind" text,
	"hold_operation_started_at" timestamp with time zone,
	"hold_operation_lease_expires_at" timestamp with time zone,
	"created_at" timestamp with time zone NOT NULL,
	"updated_at" timestamp with time zone NOT NULL,
	CONSTRAINT "archives_consultation_id_key" UNIQUE("consultation_id"),
	CONSTRAINT "archives_id_consultation_id_key" UNIQUE("id","consultation_id"),
	CONSTRAINT "archives_write_epoch_check" CHECK (write_epoch >= 0),
	CONSTRAINT "archives_hold_operation_kind_check" CHECK (hold_operation_kind = ANY (ARRAY['add'::text, 'release'::text])),
	CONSTRAINT "archives_check" CHECK (((hold_operation_id IS NULL) = (hold_operation_owner IS NULL)) AND ((hold_operation_id IS NULL) = (hold_operation_kind IS NULL)) AND ((hold_operation_id IS NULL) = (hold_operation_started_at IS NULL)) AND ((hold_operation_id IS NULL) = (hold_operation_lease_expires_at IS NULL)))
);
--> statement-breakpoint
CREATE TABLE "audit_events" (
	"sequence" bigserial PRIMARY KEY NOT NULL,
	"id" uuid NOT NULL,
	"aggregate_id" uuid NOT NULL,
	"actor_id" uuid,
	"kind" text NOT NULL,
	"occurred_at" timestamp with time zone NOT NULL,
	"details" jsonb NOT NULL,
	CONSTRAINT "audit_events_id_key" UNIQUE("id")
);
--> statement-breakpoint
CREATE TABLE "caption_ledger" (
	"consultation_id" uuid NOT NULL,
	"destination_participant_id" uuid NOT NULL,
	"utterance_id" uuid NOT NULL,
	"revision" integer NOT NULL,
	"finality" text NOT NULL,
	"source_participant_id" uuid NOT NULL,
	"source_language" text NOT NULL,
	"target_language" text NOT NULL,
	"source_text" text NOT NULL,
	"translated_text" text NOT NULL,
	"source_sample_start" bigint NOT NULL,
	"source_sample_end" bigint NOT NULL,
	"occurred_at_ms" bigint NOT NULL,
	"writer_epoch" bigint NOT NULL,
	"archived_object_id" uuid,
	"created_at" timestamp with time zone NOT NULL,
	CONSTRAINT "caption_ledger_pkey" PRIMARY KEY("consultation_id","destination_participant_id","utterance_id","revision"),
	CONSTRAINT "caption_ledger_revision_check" CHECK (revision > 0),
	CONSTRAINT "caption_ledger_finality_check" CHECK (finality = ANY (ARRAY['provisional'::text, 'final'::text])),
	CONSTRAINT "caption_ledger_source_sample_start_check" CHECK (source_sample_start >= 0),
	CONSTRAINT "caption_ledger_check" CHECK (source_sample_end > source_sample_start)
);
--> statement-breakpoint
CREATE TABLE "consultation_participants" (
	"id" uuid PRIMARY KEY NOT NULL,
	"consultation_id" uuid NOT NULL,
	"user_id" uuid NOT NULL,
	"role" "participant_role" NOT NULL,
	"livekit_identity" uuid NOT NULL,
	"display_name" text,
	"language" text,
	"capability_row_id" uuid,
	"consent_version" integer,
	"consent_copy_hash" text,
	"consent_snapshot_hash" text,
	"consented_at" timestamp with time zone,
	"present" boolean DEFAULT false NOT NULL,
	"presence_event_id" text,
	"presence_event_time" timestamp with time zone,
	"absence_order" bigint DEFAULT 0 NOT NULL,
	"ready_order" bigint DEFAULT 0 NOT NULL,
	"finalize_order" bigint DEFAULT 0 NOT NULL,
	"publication_granted" boolean DEFAULT false NOT NULL,
	"participant_egress_id" text,
	"joined_at" timestamp with time zone,
	"disconnected_at" timestamp with time zone,
	CONSTRAINT "consultation_participants_consultation_id_id_key" UNIQUE("consultation_id","id"),
	CONSTRAINT "consultation_participants_consultation_id_role_key" UNIQUE("consultation_id","role"),
	CONSTRAINT "consultation_participants_consultation_id_user_id_key" UNIQUE("consultation_id","user_id"),
	CONSTRAINT "consultation_participants_livekit_identity_key" UNIQUE("livekit_identity"),
	CONSTRAINT "consultation_participants_absence_order_check" CHECK (absence_order >= 0),
	CONSTRAINT "consultation_participants_ready_order_check" CHECK (ready_order >= 0),
	CONSTRAINT "consultation_participants_finalize_order_check" CHECK (finalize_order >= 0),
	CONSTRAINT "consultation_participants_check" CHECK (id = livekit_identity),
	CONSTRAINT "consultation_participants_check1" CHECK (((consent_version IS NULL) = (consent_copy_hash IS NULL)) AND ((consent_version IS NULL) = (consent_snapshot_hash IS NULL)) AND ((consent_version IS NULL) = (consented_at IS NULL))),
	CONSTRAINT "consultation_participants_consent_version_check" CHECK ((consent_version IS NULL) OR (consent_version = 1))
);
--> statement-breakpoint
CREATE TABLE "consultations" (
	"id" uuid PRIMARY KEY NOT NULL,
	"state" "consultation_state" DEFAULT 'invited' NOT NULL,
	"provider_profile_id" uuid NOT NULL,
	"provider_profile_revision" integer NOT NULL,
	"provider_selection" jsonb,
	"snapshot_hash" text,
	"generation" integer DEFAULT 0 NOT NULL,
	"room_name" text,
	"room_sid" text,
	"worker_identity" uuid,
	"dispatch_id" text,
	"composite_egress_id" text,
	"ready_deadline_at" timestamp with time zone,
	"finalize_deadline_at" timestamp with time zone,
	"both_absent_since" timestamp with time zone,
	"admission_fenced_at" timestamp with time zone,
	"effect_generation" integer DEFAULT 0 NOT NULL,
	"employee_user_id" uuid,
	"creation_idempotency_key" text,
	"presence_epoch" bigint DEFAULT 0 NOT NULL,
	"created_at" timestamp with time zone NOT NULL,
	"updated_at" timestamp with time zone NOT NULL,
	"deleted_at" timestamp with time zone,
	CONSTRAINT "consultations_room_name_key" UNIQUE("room_name"),
	CONSTRAINT "consultations_worker_identity_key" UNIQUE("worker_identity"),
	CONSTRAINT "consultations_employee_user_id_creation_idempotency_key_key" UNIQUE("employee_user_id","creation_idempotency_key"),
	CONSTRAINT "consultations_generation_check" CHECK (generation >= 0),
	CONSTRAINT "consultations_effect_generation_check" CHECK (effect_generation >= 0),
	CONSTRAINT "consultations_presence_epoch_check" CHECK (presence_epoch >= 0),
	CONSTRAINT "consultations_creation_idempotency_key_scope_check" CHECK ((employee_user_id IS NULL) = (creation_idempotency_key IS NULL)),
	CONSTRAINT "consultations_check" CHECK ((provider_selection IS NULL) = (snapshot_hash IS NULL)),
	CONSTRAINT "consultations_check1" CHECK ((state = 'invited'::consultation_state) OR (room_name IS NOT NULL) OR (state = ANY (ARRAY['cancelled'::consultation_state, 'deleted'::consultation_state])))
);
--> statement-breakpoint
CREATE TABLE "deletion_scans" (
	"id" uuid PRIMARY KEY NOT NULL,
	"archive_id" uuid NOT NULL,
	"write_epoch" integer NOT NULL,
	"version_count" bigint NOT NULL,
	"multipart_count" bigint NOT NULL,
	"consecutive_empty" integer NOT NULL,
	"result" jsonb NOT NULL,
	"scanned_at" timestamp with time zone NOT NULL,
	CONSTRAINT "deletion_scans_consecutive_empty_check" CHECK ((consecutive_empty >= 0) AND (consecutive_empty <= 2))
);
--> statement-breakpoint
CREATE TABLE "effect_compensation_attempts" (
	"id" bigserial PRIMARY KEY NOT NULL,
	"effect_id" uuid NOT NULL,
	"owner" uuid NOT NULL,
	"request_hash" text NOT NULL,
	"result" jsonb NOT NULL,
	"created_at" timestamp with time zone NOT NULL,
	CONSTRAINT "effect_compensation_attempts_effect_id_owner_request_hash_key" UNIQUE("effect_id","owner","request_hash")
);
--> statement-breakpoint
CREATE TABLE "egress_jobs" (
	"id" uuid PRIMARY KEY NOT NULL,
	"consultation_id" uuid NOT NULL,
	"generation" integer NOT NULL,
	"kind" text NOT NULL,
	"subject_id" uuid NOT NULL,
	"egress_id" text,
	"request_hash" text NOT NULL,
	"state" text NOT NULL,
	"output_prefix" text NOT NULL,
	"started_at" timestamp with time zone,
	"terminal_at" timestamp with time zone,
	"terminal_result" jsonb,
	"expected_artifact_id" uuid,
	"created_at" timestamp with time zone NOT NULL,
	CONSTRAINT "egress_jobs_consultation_id_generation_kind_subject_id_key" UNIQUE("consultation_id","generation","kind","subject_id"),
	CONSTRAINT "egress_jobs_egress_id_key" UNIQUE("egress_id"),
	CONSTRAINT "egress_jobs_kind_check" CHECK (kind = ANY (ARRAY['room_composite'::text, 'participant'::text, 'track'::text])),
	CONSTRAINT "egress_jobs_state_check" CHECK (state = ANY (ARRAY['requested'::text, 'EGRESS_STARTING'::text, 'EGRESS_ACTIVE'::text, 'EGRESS_ENDING'::text, 'EGRESS_COMPLETE'::text, 'EGRESS_FAILED'::text, 'EGRESS_ABORTED'::text, 'EGRESS_LIMIT_REACHED'::text]))
);
--> statement-breakpoint
CREATE TABLE "expected_archive_artifacts" (
	"id" uuid PRIMARY KEY NOT NULL,
	"archive_id" uuid NOT NULL,
	"effect_id" uuid,
	"profile_id" uuid NOT NULL,
	"profile_revision" integer NOT NULL,
	"object_class" text NOT NULL,
	"causal_key" text NOT NULL,
	"sample_start" bigint,
	"sample_end" bigint,
	"segment_start" integer,
	"segment_end" integer,
	"owner_epoch" bigint NOT NULL,
	"disposition" text DEFAULT 'expected' NOT NULL,
	"fulfilled_object_id" uuid,
	"created_at" timestamp with time zone NOT NULL,
	CONSTRAINT "expected_archive_artifacts_archive_id_object_class_causal_k_key" UNIQUE("archive_id","object_class","causal_key"),
	CONSTRAINT "expected_archive_artifacts_effect_id_key" UNIQUE("effect_id"),
	CONSTRAINT "expected_archive_artifacts_check" CHECK (((sample_start IS NULL) AND (sample_end IS NULL)) OR ((sample_start >= 0) AND (sample_end > sample_start))),
	CONSTRAINT "expected_archive_artifacts_check1" CHECK (((segment_start IS NULL) AND (segment_end IS NULL)) OR ((segment_start >= 0) AND (segment_end > segment_start)))
);
--> statement-breakpoint
CREATE TABLE "external_effects" (
	"id" uuid PRIMARY KEY NOT NULL,
	"consultation_id" uuid NOT NULL,
	"generation" integer NOT NULL,
	"effect_kind" text NOT NULL,
	"subject_id" uuid NOT NULL,
	"state" "external_effect_state" NOT NULL,
	"request_bytes" "bytea",
	"request_hash" text,
	"lease_owner" uuid,
	"lease_expires_at" timestamp with time zone,
	"result" jsonb,
	"attempts" integer DEFAULT 0 NOT NULL,
	"compensation_result" jsonb,
	"created_at" timestamp with time zone NOT NULL,
	"updated_at" timestamp with time zone NOT NULL,
	"occurrence_key" text DEFAULT '' NOT NULL,
	CONSTRAINT "external_effect_occurrence_unique" UNIQUE("consultation_id","generation","effect_kind","subject_id","occurrence_key"),
	CONSTRAINT "external_effects_attempts_check" CHECK (attempts >= 0),
	CONSTRAINT "external_effects_check" CHECK ((request_bytes IS NULL) = (request_hash IS NULL)),
	CONSTRAINT "external_effects_check1" CHECK ((state = 'planned'::external_effect_state) OR (request_hash IS NOT NULL)),
	CONSTRAINT "external_effects_egress_intent_redacted_check" CHECK (effect_kind NOT IN ('ROOM_COMPOSITE_EGRESS', 'PARTICIPANT_EGRESS') OR request_bytes IS NULL OR lower(convert_from(request_bytes, 'UTF8')) !~ '"(accesskey|secret|custombaseurl|signature|authorization|token|filenameprefix|playlistname|liveplaylistname)"')
);
--> statement-breakpoint
CREATE TABLE "final_inventories" (
	"archive_id" uuid PRIMARY KEY NOT NULL,
	"status" text NOT NULL,
	"inventory" jsonb NOT NULL,
	"sha256" text NOT NULL,
	"object_id" uuid,
	"room_close" jsonb NOT NULL,
	"worker_terminal" jsonb NOT NULL,
	"egress_results" jsonb NOT NULL,
	"missing" jsonb NOT NULL,
	"errors" jsonb NOT NULL,
	"created_at" timestamp with time zone NOT NULL,
	CONSTRAINT "final_inventories_sha256_key" UNIQUE("sha256"),
	CONSTRAINT "final_inventories_status_check" CHECK (status = ANY (ARRAY['complete'::text, 'incomplete'::text]))
);
--> statement-breakpoint
CREATE TABLE "inbox" (
	"source" text NOT NULL,
	"event_id" text NOT NULL,
	"occurred_at" timestamp with time zone NOT NULL,
	"payload_hash" text NOT NULL,
	"payload" jsonb NOT NULL,
	"received_at" timestamp with time zone NOT NULL,
	CONSTRAINT "inbox_pkey" PRIMARY KEY("source","event_id")
);
--> statement-breakpoint
CREATE TABLE "inventory_supplements" (
	"id" uuid PRIMARY KEY NOT NULL,
	"archive_id" uuid NOT NULL,
	"final_inventory_sha256" text NOT NULL,
	"supplement" jsonb NOT NULL,
	"sha256" text NOT NULL,
	"object_id" uuid,
	"created_at" timestamp with time zone NOT NULL,
	CONSTRAINT "inventory_supplements_sha256_key" UNIQUE("sha256")
);
--> statement-breakpoint
CREATE TABLE "language_capabilities" (
	"id" uuid PRIMARY KEY NOT NULL,
	"profile_id" uuid NOT NULL,
	"revision" integer NOT NULL,
	"source_locale" text NOT NULL,
	"target_locale" text NOT NULL,
	"mode" text NOT NULL,
	"stt_provider" text NOT NULL,
	"stt_endpoint" text NOT NULL,
	"stt_model" text NOT NULL,
	"stt_encoding" text NOT NULL,
	"stt_limits" jsonb NOT NULL,
	"translation_provider" text,
	"translation_endpoint" text,
	"translation_model" text,
	"translation_code" text,
	"tts_provider" text,
	"tts_endpoint" text,
	"tts_model" text,
	"tts_voice" text,
	"tts_format" text,
	"tts_limits" jsonb,
	"region" text NOT NULL,
	"adapter_version" text NOT NULL,
	"capability_version" text NOT NULL,
	"capability_hash" text NOT NULL,
	"enabled" boolean NOT NULL,
	"fresh_until" timestamp with time zone NOT NULL,
	"snapshot" jsonb NOT NULL,
	CONSTRAINT "language_capabilities_profile_id_revision_source_locale_tar_key" UNIQUE("profile_id","revision","source_locale","target_locale","mode"),
	CONSTRAINT "language_capabilities_mode_check" CHECK (mode = ANY (ARRAY['translated'::text, 'same_language'::text])),
	CONSTRAINT "language_capabilities_check" CHECK (((mode = 'same_language'::text) AND (translation_provider IS NULL) AND (tts_provider IS NULL)) OR ((mode = 'translated'::text) AND (translation_provider IS NOT NULL) AND (translation_endpoint IS NOT NULL) AND (translation_model IS NOT NULL) AND (translation_code IS NOT NULL) AND (tts_provider IS NOT NULL) AND (tts_endpoint IS NOT NULL) AND (tts_model IS NOT NULL) AND (tts_voice IS NOT NULL) AND (tts_format IS NOT NULL) AND (tts_limits IS NOT NULL)))
);
--> statement-breakpoint
CREATE TABLE "legal_holds" (
	"id" uuid PRIMARY KEY NOT NULL,
	"archive_id" uuid NOT NULL,
	"reason" text NOT NULL,
	"actor_id" uuid NOT NULL,
	"session_id" uuid NOT NULL,
	"reauthenticated_at" timestamp with time zone NOT NULL,
	"state" text DEFAULT 'applying' NOT NULL,
	"placed_at" timestamp with time zone NOT NULL,
	"released_at" timestamp with time zone,
	"released_by" uuid,
	"aggregate_result" jsonb,
	"per_version_results" jsonb,
	CONSTRAINT "legal_holds_state_check" CHECK (state = ANY (ARRAY['applying'::text, 'active'::text, 'releasing'::text, 'failed'::text, 'released'::text])),
	CONSTRAINT "legal_holds_check" CHECK (reauthenticated_at >= (placed_at - '00:05:00'::interval)),
	CONSTRAINT "legal_holds_check1" CHECK ((state = 'released'::text) = (released_at IS NOT NULL)),
	CONSTRAINT "legal_holds_check2" CHECK ((released_at IS NULL) = (released_by IS NULL))
);
--> statement-breakpoint
CREATE TABLE "magic_link_requests" (
	"id" bigserial PRIMARY KEY NOT NULL,
	"email_hash" text,
	"ip_hash" text NOT NULL,
	"requested_at" timestamp with time zone NOT NULL
);
--> statement-breakpoint
CREATE TABLE "magic_links" (
	"id" uuid PRIMARY KEY NOT NULL,
	"user_id" uuid,
	"consultation_id" uuid,
	"session_id" uuid,
	"purpose" "magic_link_purpose" NOT NULL,
	"token_hash" text NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"consumed_at" timestamp with time zone,
	"revoked_at" timestamp with time zone,
	"created_at" timestamp with time zone NOT NULL,
	"sealed_raw_token" text NOT NULL,
	"sealed_token_key_id" text NOT NULL,
	CONSTRAINT "magic_links_token_hash_key" UNIQUE("token_hash"),
	CONSTRAINT "magic_links_check" CHECK ((purpose <> 'archive_delete_reauth'::magic_link_purpose) OR ((user_id IS NOT NULL) AND (consultation_id IS NOT NULL) AND (session_id IS NOT NULL))),
	CONSTRAINT "magic_links_sealed_token_nonempty_check" CHECK (length(sealed_raw_token) > 0 AND length(sealed_token_key_id) > 0)
);
--> statement-breakpoint
CREATE TABLE "multipart_parts" (
	"upload_id" uuid NOT NULL,
	"part_number" integer NOT NULL,
	"etag" text NOT NULL,
	"checksum" text NOT NULL,
	"size" bigint NOT NULL,
	"uploaded_at" timestamp with time zone NOT NULL,
	CONSTRAINT "multipart_parts_pkey" PRIMARY KEY("upload_id","part_number"),
	CONSTRAINT "multipart_parts_part_number_check" CHECK ((part_number >= 1) AND (part_number <= 10000)),
	CONSTRAINT "multipart_parts_size_check" CHECK (size > 0)
);
--> statement-breakpoint
CREATE TABLE "multipart_uploads" (
	"id" uuid PRIMARY KEY NOT NULL,
	"archive_id" uuid NOT NULL,
	"key" text NOT NULL,
	"upload_id" text NOT NULL,
	"size" bigint NOT NULL,
	"state" text NOT NULL,
	"checksum" text NOT NULL,
	"writer_epoch" integer NOT NULL,
	"created_at" timestamp with time zone NOT NULL,
	"updated_at" timestamp with time zone NOT NULL,
	CONSTRAINT "multipart_uploads_upload_id_key" UNIQUE("upload_id"),
	CONSTRAINT "multipart_uploads_state_check" CHECK (state = ANY (ARRAY['open'::text, 'completing'::text, 'complete'::text, 'aborted'::text])),
	CONSTRAINT "multipart_uploads_size_check" CHECK (size >= 104857600)
);
--> statement-breakpoint
CREATE TABLE "orchestration_deadlines" (
	"consultation_id" uuid NOT NULL,
	"generation" integer NOT NULL,
	"kind" text NOT NULL,
	"due_at" timestamp with time zone NOT NULL,
	"lease_owner" uuid,
	"lease_expires_at" timestamp with time zone,
	"completed_at" timestamp with time zone,
	CONSTRAINT "orchestration_deadlines_pkey" PRIMARY KEY("consultation_id","generation","kind"),
	CONSTRAINT "orchestration_deadlines_kind_check" CHECK (kind = ANY (ARRAY['ready'::text, 'absence'::text, 'finalize'::text, 'archive-reconcile'::text]))
);
--> statement-breakpoint
CREATE TABLE "outbox" (
	"id" uuid PRIMARY KEY NOT NULL,
	"topic" text NOT NULL,
	"aggregate_id" uuid NOT NULL,
	"generation" integer NOT NULL,
	"payload" jsonb NOT NULL,
	"available_at" timestamp with time zone NOT NULL,
	"attempts" integer DEFAULT 0 NOT NULL,
	"lease_owner" uuid,
	"lease_expires_at" timestamp with time zone,
	"delivered_at" timestamp with time zone,
	CONSTRAINT "outbox_attempts_check" CHECK (attempts >= 0)
);
--> statement-breakpoint
CREATE TABLE "pending_exchanges" (
	"id" uuid PRIMARY KEY NOT NULL,
	"magic_link_id" uuid NOT NULL,
	"nonce_hash" text NOT NULL,
	"csrf_hash" text NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"consumed_at" timestamp with time zone,
	"created_at" timestamp with time zone NOT NULL,
	CONSTRAINT "pending_exchanges_nonce_hash_key" UNIQUE("nonce_hash")
);
--> statement-breakpoint
CREATE TABLE "provider_attempts" (
	"id" uuid PRIMARY KEY NOT NULL,
	"archive_id" uuid NOT NULL,
	"consultation_id" uuid NOT NULL,
	"profile_id" uuid NOT NULL,
	"profile_revision" integer NOT NULL,
	"stage" text NOT NULL,
	"provider" text NOT NULL,
	"direction_id" uuid NOT NULL,
	"operation_id" uuid NOT NULL,
	"attempt_number" integer NOT NULL,
	"retry_of" uuid,
	"credential_reference" text NOT NULL,
	"credential_version" text NOT NULL,
	"credential_fingerprint" text NOT NULL,
	"endpoint" text NOT NULL,
	"api_version" text NOT NULL,
	"model" text NOT NULL,
	"voice" text,
	"outcome" "terminal_outcome",
	"error_kind" text,
	"error_scope" text,
	"provider_retry_advice" text,
	"provider_code" text,
	"provider_request_id" text,
	"retry_delay_ms" integer,
	"accepted_input_watermark" bigint,
	"received_output_watermark" bigint,
	"emitted_output_watermark" bigint,
	"retry_decision" jsonb,
	"transport" "transport_kind",
	"raw_http" jsonb,
	"raw_websocket" jsonb,
	"raw_grpc" jsonb,
	"terminal_hash" text,
	"started_at" timestamp with time zone NOT NULL,
	"terminal_at" timestamp with time zone,
	CONSTRAINT "provider_attempts_consultation_id_direction_id_operation_id_key" UNIQUE("consultation_id","direction_id","operation_id","attempt_number"),
	CONSTRAINT "provider_attempts_terminal_hash_key" UNIQUE("terminal_hash"),
	CONSTRAINT "provider_attempts_stage_check" CHECK (stage = ANY (ARRAY['stt'::text, 'translation'::text, 'tts'::text])),
	CONSTRAINT "provider_attempts_attempt_number_check" CHECK (attempt_number > 0),
	CONSTRAINT "provider_attempts_error_scope_check" CHECK ((error_scope IS NULL) OR (error_scope = ANY (ARRAY['operation'::text, 'session'::text]))),
	CONSTRAINT "provider_attempts_provider_retry_advice_check" CHECK ((provider_retry_advice IS NULL) OR (provider_retry_advice = ANY (ARRAY['never'::text, 'retry_after'::text, 'unspecified'::text]))),
	CONSTRAINT "provider_attempts_check" CHECK ((terminal_at IS NULL) = (outcome IS NULL)),
	CONSTRAINT "provider_attempts_check1" CHECK (((transport IS NULL) AND (raw_http IS NULL) AND (raw_websocket IS NULL) AND (raw_grpc IS NULL)) OR ((transport = 'http'::transport_kind) AND (raw_http IS NOT NULL) AND (raw_websocket IS NULL) AND (raw_grpc IS NULL)) OR ((transport = 'websocket'::transport_kind) AND (raw_http IS NULL) AND (raw_websocket IS NOT NULL) AND (raw_grpc IS NULL)) OR ((transport = 'grpc'::transport_kind) AND (raw_http IS NULL) AND (raw_websocket IS NULL) AND (raw_grpc IS NOT NULL))),
	CONSTRAINT "provider_attempts_check2" CHECK ((terminal_at IS NULL) OR ((transport IS NOT NULL) AND (retry_decision IS NOT NULL) AND (terminal_hash IS NOT NULL)))
);
--> statement-breakpoint
CREATE TABLE "provider_profile_revisions" (
	"profile_id" uuid NOT NULL,
	"revision" integer NOT NULL,
	"capability_hash" text NOT NULL,
	"adapter_builds" jsonb NOT NULL,
	"policy" jsonb NOT NULL,
	"credential_references" jsonb NOT NULL,
	"created_at" timestamp with time zone NOT NULL,
	CONSTRAINT "provider_profile_revisions_pkey" PRIMARY KEY("profile_id","revision"),
	CONSTRAINT "provider_profile_revisions_profile_id_capability_hash_key" UNIQUE("profile_id","capability_hash"),
	CONSTRAINT "provider_profile_revisions_revision_check" CHECK (revision > 0)
);
--> statement-breakpoint
CREATE TABLE "provider_profiles" (
	"id" uuid PRIMARY KEY NOT NULL,
	"name" text NOT NULL,
	"enabled" boolean DEFAULT false NOT NULL,
	"current_revision" integer NOT NULL,
	"created_at" timestamp with time zone NOT NULL,
	CONSTRAINT "provider_profiles_name_key" UNIQUE("name"),
	CONSTRAINT "provider_profiles_current_revision_check" CHECK (current_revision > 0)
);
--> statement-breakpoint
CREATE TABLE "room_provider_selections" (
	"consultation_id" uuid PRIMARY KEY NOT NULL,
	"profile_id" uuid NOT NULL,
	"profile_revision" integer NOT NULL,
	"capability_hash" text NOT NULL,
	"selection_hash" text NOT NULL,
	"selection" jsonb NOT NULL,
	"created_at" timestamp with time zone NOT NULL,
	CONSTRAINT "room_provider_selections_selection_hash_key" UNIQUE("selection_hash")
);
--> statement-breakpoint
CREATE TABLE "sessions" (
	"id" uuid PRIMARY KEY NOT NULL,
	"user_id" uuid NOT NULL,
	"token_hash" text NOT NULL,
	"csrf_hash" text NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"reauthenticated_at" timestamp with time zone,
	"reauth_consultation_id" uuid,
	"revoked_at" timestamp with time zone,
	"replaced_by" uuid,
	"created_at" timestamp with time zone NOT NULL,
	CONSTRAINT "sessions_token_hash_key" UNIQUE("token_hash"),
	CONSTRAINT "sessions_check" CHECK ((reauthenticated_at IS NULL) = (reauth_consultation_id IS NULL))
);
--> statement-breakpoint
CREATE TABLE "users" (
	"id" uuid PRIMARY KEY NOT NULL,
	"email" text NOT NULL,
	"display_name" text NOT NULL,
	"staff_role" "staff_role",
	"pii_erased_at" timestamp with time zone,
	"created_at" timestamp with time zone NOT NULL,
	CONSTRAINT "users_email_key" UNIQUE("email")
);
--> statement-breakpoint
CREATE TABLE "worker_checkpoints" (
	"id" uuid PRIMARY KEY NOT NULL,
	"consultation_id" uuid NOT NULL,
	"generation" integer NOT NULL,
	"worker_id" uuid NOT NULL,
	"worker_epoch" bigint NOT NULL,
	"write_epoch" integer NOT NULL,
	"source_participant_id" uuid NOT NULL,
	"destination_participant_id" uuid NOT NULL,
	"accepted_input_sequence" bigint NOT NULL,
	"accepted_input" bigint NOT NULL,
	"received_output" bigint NOT NULL,
	"emitted_output" bigint NOT NULL,
	"previous_hash" text,
	"checkpoint_hash" text NOT NULL,
	"expected_ids" jsonb NOT NULL,
	"observed_ids" jsonb NOT NULL,
	"gaps" jsonb NOT NULL,
	"terminal" boolean NOT NULL,
	"object_key" text NOT NULL,
	"object_version_id" text,
	"created_at" timestamp with time zone NOT NULL,
	CONSTRAINT "worker_checkpoints_direction_watermarks_key" UNIQUE("consultation_id","worker_epoch","source_participant_id","destination_participant_id","accepted_input_sequence","accepted_input","received_output","emitted_output"),
	CONSTRAINT "worker_checkpoints_checkpoint_hash_key" UNIQUE("checkpoint_hash"),
	CONSTRAINT "worker_checkpoints_accepted_input_sequence_check" CHECK (accepted_input_sequence >= 0),
	CONSTRAINT "worker_checkpoints_accepted_input_check" CHECK (accepted_input >= 0),
	CONSTRAINT "worker_checkpoints_received_output_check" CHECK (received_output >= 0),
	CONSTRAINT "worker_checkpoints_emitted_output_check" CHECK (emitted_output >= 0)
);
--> statement-breakpoint
CREATE TABLE "worker_job_epochs" (
	"consultation_id" uuid NOT NULL,
	"generation" integer NOT NULL,
	"worker_id" uuid NOT NULL,
	"epoch" bigint NOT NULL,
	"write_epoch" integer DEFAULT 0 NOT NULL,
	"heartbeat_at" timestamp with time zone NOT NULL,
	"fenced_at" timestamp with time zone,
	"terminal_checkpoint_id" uuid,
	"terminal_outcome" text,
	"terminal_at" timestamp with time zone,
	CONSTRAINT "worker_job_epochs_pkey" PRIMARY KEY("consultation_id","generation","epoch"),
	CONSTRAINT "worker_job_epochs_worker_id_epoch_consultation_id_key" UNIQUE("consultation_id","worker_id","epoch"),
	CONSTRAINT "worker_job_epochs_epoch_check" CHECK (epoch >= 0),
	CONSTRAINT "worker_job_epochs_write_epoch_check" CHECK (write_epoch >= 0),
	CONSTRAINT "worker_job_epochs_terminal_outcome_check" CHECK (terminal_outcome = ANY (ARRAY['clean'::text, 'fenced'::text, 'failed'::text]))
);
--> statement-breakpoint
CREATE TABLE "worker_leases" (
	"worker_id" uuid PRIMARY KEY NOT NULL,
	"accepting_load" boolean NOT NULL,
	"capacity" integer NOT NULL,
	"reserved" integer NOT NULL,
	"encrypted_spool_percent" numeric(5, 2) NOT NULL,
	"providers_ok" boolean NOT NULL,
	"archive_ok" boolean NOT NULL,
	"heartbeat_at" timestamp with time zone NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"epoch" bigint NOT NULL,
	"status" jsonb NOT NULL,
	CONSTRAINT "worker_leases_capacity_check" CHECK (capacity >= 0),
	CONSTRAINT "worker_leases_check" CHECK ((reserved >= 0) AND (reserved <= capacity)),
	CONSTRAINT "worker_leases_encrypted_spool_percent_check" CHECK ((encrypted_spool_percent >= (0)::numeric) AND (encrypted_spool_percent <= (100)::numeric)),
	CONSTRAINT "worker_leases_epoch_check" CHECK (epoch >= 0)
);
--> statement-breakpoint
CREATE TABLE "worker_reservations" (
	"consultation_id" uuid NOT NULL,
	"generation" integer NOT NULL,
	"worker_id" uuid NOT NULL,
	"epoch" bigint NOT NULL,
	"selection_hash" text NOT NULL,
	"reserved_at" timestamp with time zone NOT NULL,
	"heartbeat_at" timestamp with time zone NOT NULL,
	"lease_expires_at" timestamp with time zone NOT NULL,
	"accepting_load" boolean NOT NULL,
	"fenced_at" timestamp with time zone,
	"fence_reason" text,
	"supervisor_owner" uuid,
	"supervisor_lease_expires_at" timestamp with time zone,
	"released_at" timestamp with time zone,
	CONSTRAINT "worker_reservations_pkey" PRIMARY KEY("consultation_id","generation"),
	CONSTRAINT "worker_reservations_consultation_id_generation_worker_id_ep_key" UNIQUE("consultation_id","generation","worker_id","epoch"),
	CONSTRAINT "worker_reservations_epoch_check" CHECK (epoch >= 0)
);
--> statement-breakpoint
ALTER TABLE "archive_objects" ADD CONSTRAINT "archive_objects_archive_id_fkey" FOREIGN KEY ("archive_id") REFERENCES "public"."archives"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "archives" ADD CONSTRAINT "archives_consultation_id_fkey" FOREIGN KEY ("consultation_id") REFERENCES "public"."consultations"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "audit_events" ADD CONSTRAINT "audit_events_actor_id_fkey" FOREIGN KEY ("actor_id") REFERENCES "public"."users"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "caption_ledger" ADD CONSTRAINT "caption_ledger_consultation_id_fkey" FOREIGN KEY ("consultation_id") REFERENCES "public"."consultations"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "caption_ledger" ADD CONSTRAINT "caption_ledger_archived_object_id_fkey" FOREIGN KEY ("archived_object_id") REFERENCES "public"."archive_objects"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "consultation_participants" ADD CONSTRAINT "consultation_participants_consultation_id_fkey" FOREIGN KEY ("consultation_id") REFERENCES "public"."consultations"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "consultation_participants" ADD CONSTRAINT "consultation_participants_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "public"."users"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "consultation_participants" ADD CONSTRAINT "consultation_participants_capability_row_id_fkey" FOREIGN KEY ("capability_row_id") REFERENCES "public"."language_capabilities"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "consultations" ADD CONSTRAINT "consultations_provider_profile_id_provider_profile_revisio_fkey" FOREIGN KEY ("provider_profile_id","provider_profile_revision") REFERENCES "public"."provider_profile_revisions"("profile_id","revision") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "consultations" ADD CONSTRAINT "consultations_employee_user_id_fkey" FOREIGN KEY ("employee_user_id") REFERENCES "public"."users"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "deletion_scans" ADD CONSTRAINT "deletion_scans_archive_id_fkey" FOREIGN KEY ("archive_id") REFERENCES "public"."archives"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "effect_compensation_attempts" ADD CONSTRAINT "effect_compensation_attempts_effect_id_fkey" FOREIGN KEY ("effect_id") REFERENCES "public"."external_effects"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "egress_jobs" ADD CONSTRAINT "egress_jobs_consultation_id_fkey" FOREIGN KEY ("consultation_id") REFERENCES "public"."consultations"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "egress_jobs" ADD CONSTRAINT "egress_expected_artifact_fk" FOREIGN KEY ("expected_artifact_id") REFERENCES "public"."expected_archive_artifacts"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "expected_archive_artifacts" ADD CONSTRAINT "expected_archive_artifacts_archive_id_fkey" FOREIGN KEY ("archive_id") REFERENCES "public"."archives"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "expected_archive_artifacts" ADD CONSTRAINT "expected_archive_artifacts_effect_id_fkey" FOREIGN KEY ("effect_id") REFERENCES "public"."external_effects"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "expected_archive_artifacts" ADD CONSTRAINT "expected_archive_artifacts_profile_id_profile_revision_fkey" FOREIGN KEY ("profile_id","profile_revision") REFERENCES "public"."provider_profile_revisions"("profile_id","revision") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "expected_archive_artifacts" ADD CONSTRAINT "expected_fulfilled_object_fk" FOREIGN KEY ("fulfilled_object_id") REFERENCES "public"."archive_objects"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "external_effects" ADD CONSTRAINT "external_effects_consultation_id_fkey" FOREIGN KEY ("consultation_id") REFERENCES "public"."consultations"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "final_inventories" ADD CONSTRAINT "final_inventories_archive_id_fkey" FOREIGN KEY ("archive_id") REFERENCES "public"."archives"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "final_inventories" ADD CONSTRAINT "final_inventories_object_id_fkey" FOREIGN KEY ("object_id") REFERENCES "public"."archive_objects"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "inventory_supplements" ADD CONSTRAINT "inventory_supplements_archive_id_fkey" FOREIGN KEY ("archive_id") REFERENCES "public"."archives"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "inventory_supplements" ADD CONSTRAINT "inventory_supplements_object_id_fkey" FOREIGN KEY ("object_id") REFERENCES "public"."archive_objects"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "inventory_supplements" ADD CONSTRAINT "inventory_supplements_archive_id_fkey1" FOREIGN KEY ("archive_id") REFERENCES "public"."final_inventories"("archive_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "language_capabilities" ADD CONSTRAINT "language_capabilities_profile_id_revision_fkey" FOREIGN KEY ("profile_id","revision") REFERENCES "public"."provider_profile_revisions"("profile_id","revision") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "legal_holds" ADD CONSTRAINT "legal_holds_archive_id_fkey" FOREIGN KEY ("archive_id") REFERENCES "public"."archives"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "legal_holds" ADD CONSTRAINT "legal_holds_actor_id_fkey" FOREIGN KEY ("actor_id") REFERENCES "public"."users"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "legal_holds" ADD CONSTRAINT "legal_holds_session_id_fkey" FOREIGN KEY ("session_id") REFERENCES "public"."sessions"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "legal_holds" ADD CONSTRAINT "legal_holds_released_by_fkey" FOREIGN KEY ("released_by") REFERENCES "public"."users"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "magic_links" ADD CONSTRAINT "magic_links_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "public"."users"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "magic_links" ADD CONSTRAINT "magic_links_consultation_id_fkey" FOREIGN KEY ("consultation_id") REFERENCES "public"."consultations"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "magic_links" ADD CONSTRAINT "magic_links_session_fk" FOREIGN KEY ("session_id") REFERENCES "public"."sessions"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "multipart_parts" ADD CONSTRAINT "multipart_parts_upload_id_fkey" FOREIGN KEY ("upload_id") REFERENCES "public"."multipart_uploads"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "multipart_uploads" ADD CONSTRAINT "multipart_uploads_archive_id_fkey" FOREIGN KEY ("archive_id") REFERENCES "public"."archives"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "orchestration_deadlines" ADD CONSTRAINT "orchestration_deadlines_consultation_id_fkey" FOREIGN KEY ("consultation_id") REFERENCES "public"."consultations"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "pending_exchanges" ADD CONSTRAINT "pending_exchanges_magic_link_id_fkey" FOREIGN KEY ("magic_link_id") REFERENCES "public"."magic_links"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "provider_attempts" ADD CONSTRAINT "provider_attempts_archive_consultation_fkey" FOREIGN KEY ("archive_id","consultation_id") REFERENCES "public"."archives"("id","consultation_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "provider_attempts" ADD CONSTRAINT "provider_attempts_retry_of_fkey" FOREIGN KEY ("retry_of") REFERENCES "public"."provider_attempts"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "provider_attempts" ADD CONSTRAINT "provider_attempts_profile_id_profile_revision_fkey" FOREIGN KEY ("profile_id","profile_revision") REFERENCES "public"."provider_profile_revisions"("profile_id","revision") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "provider_profile_revisions" ADD CONSTRAINT "provider_profile_revisions_profile_id_fkey" FOREIGN KEY ("profile_id") REFERENCES "public"."provider_profiles"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "room_provider_selections" ADD CONSTRAINT "room_provider_selections_consultation_id_fkey" FOREIGN KEY ("consultation_id") REFERENCES "public"."consultations"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "room_provider_selections" ADD CONSTRAINT "room_provider_selections_profile_id_profile_revision_fkey" FOREIGN KEY ("profile_id","profile_revision") REFERENCES "public"."provider_profile_revisions"("profile_id","revision") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "sessions" ADD CONSTRAINT "sessions_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "public"."users"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "sessions" ADD CONSTRAINT "sessions_reauth_consultation_id_fkey" FOREIGN KEY ("reauth_consultation_id") REFERENCES "public"."consultations"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "sessions" ADD CONSTRAINT "sessions_replaced_by_fkey" FOREIGN KEY ("replaced_by") REFERENCES "public"."sessions"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ADD CONSTRAINT "worker_checkpoints_consultation_id_fkey" FOREIGN KEY ("consultation_id") REFERENCES "public"."consultations"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ADD CONSTRAINT "worker_checkpoints_consultation_id_generation_worker_id_wo_fkey" FOREIGN KEY ("consultation_id","generation","worker_id","worker_epoch") REFERENCES "public"."worker_reservations"("consultation_id","generation","worker_id","epoch") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ADD CONSTRAINT "worker_checkpoints_source_participant_fkey" FOREIGN KEY ("consultation_id","source_participant_id") REFERENCES "public"."consultation_participants"("consultation_id","id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ADD CONSTRAINT "worker_checkpoints_destination_participant_fkey" FOREIGN KEY ("consultation_id","destination_participant_id") REFERENCES "public"."consultation_participants"("consultation_id","id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "worker_job_epochs" ADD CONSTRAINT "worker_job_epochs_consultation_id_fkey" FOREIGN KEY ("consultation_id") REFERENCES "public"."consultations"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "worker_job_epochs" ADD CONSTRAINT "worker_job_epochs_worker_id_fkey" FOREIGN KEY ("worker_id") REFERENCES "public"."worker_leases"("worker_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "worker_job_epochs" ADD CONSTRAINT "worker_job_terminal_checkpoint_fk" FOREIGN KEY ("terminal_checkpoint_id") REFERENCES "public"."worker_checkpoints"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "worker_reservations" ADD CONSTRAINT "worker_reservations_consultation_id_fkey" FOREIGN KEY ("consultation_id") REFERENCES "public"."consultations"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "worker_reservations" ADD CONSTRAINT "worker_reservations_worker_id_fkey" FOREIGN KEY ("worker_id") REFERENCES "public"."worker_leases"("worker_id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "audit_aggregate_sequence_idx" ON "audit_events" USING btree ("aggregate_id" uuid_ops,"sequence" int8_ops);--> statement-breakpoint
CREATE UNIQUE INDEX "legal_holds_active_unique" ON "legal_holds" USING btree ("archive_id" uuid_ops,"id" uuid_ops) WHERE (released_at IS NULL);--> statement-breakpoint
CREATE INDEX "magic_link_email_rate_idx" ON "magic_link_requests" USING btree ("email_hash" text_ops,"requested_at" timestamptz_ops);--> statement-breakpoint
CREATE INDEX "magic_link_ip_rate_idx" ON "magic_link_requests" USING btree ("ip_hash" text_ops,"requested_at" timestamptz_ops);--> statement-breakpoint
CREATE INDEX "magic_link_requested_at_idx" ON "magic_link_requests" USING btree ("requested_at");--> statement-breakpoint
CREATE UNIQUE INDEX "magic_links_active_identity_unique" ON "magic_links" USING btree (COALESCE("user_id"::text, ''),"purpose",COALESCE("consultation_id"::text, ''),COALESCE("session_id"::text, '')) WHERE "magic_links"."consumed_at" IS NULL AND "magic_links"."revoked_at" IS NULL;--> statement-breakpoint
CREATE INDEX "outbox_claim_idx" ON "outbox" USING btree ("available_at" timestamptz_ops) WHERE (delivered_at IS NULL);--> statement-breakpoint
CREATE UNIQUE INDEX "pending_exchanges_live_magic_link_unique" ON "pending_exchanges" USING btree ("magic_link_id") WHERE "pending_exchanges"."consumed_at" IS NULL;--> statement-breakpoint
CREATE UNIQUE INDEX "worker_checkpoints_terminal_direction_unique" ON "worker_checkpoints" USING btree ("consultation_id" uuid_ops,"generation" int4_ops,"worker_id" uuid_ops,"worker_epoch" int8_ops,"source_participant_id" uuid_ops,"destination_participant_id" uuid_ops) WHERE terminal;