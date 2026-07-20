DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM "worker_checkpoints"
    WHERE "terminal"
    GROUP BY "consultation_id", "generation", "worker_id", "worker_epoch",
      "source_participant_id", "destination_participant_id"
    HAVING count(*) > 1
  ) THEN
    RAISE EXCEPTION USING
      ERRCODE = 'unique_violation',
      MESSAGE = '0006_orchestration_safety found multiple terminal checkpoints for one worker direction',
      DETAIL = 'A worker epoch may have only one terminal checkpoint for each frozen source/destination direction.',
      HINT = 'Resolve the duplicate terminal checkpoint evidence before applying this migration.';
  END IF;
END
$$;--> statement-breakpoint
ALTER TABLE "consultations" ADD COLUMN "employee_user_id" uuid;--> statement-breakpoint
ALTER TABLE "consultations" ADD COLUMN "creation_idempotency_key" text;--> statement-breakpoint
ALTER TABLE "consultations" ADD COLUMN "presence_epoch" bigint DEFAULT 0 NOT NULL;--> statement-breakpoint
ALTER TABLE "consultations" ADD CONSTRAINT "consultations_employee_user_id_fkey" FOREIGN KEY ("employee_user_id") REFERENCES "public"."users"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "consultations" ADD CONSTRAINT "consultations_employee_user_id_creation_idempotency_key_key" UNIQUE("employee_user_id","creation_idempotency_key");--> statement-breakpoint
ALTER TABLE "consultations" ADD CONSTRAINT "consultations_presence_epoch_check" CHECK ("presence_epoch" >= 0);--> statement-breakpoint
ALTER TABLE "consultations" ADD CONSTRAINT "consultations_creation_idempotency_key_scope_check" CHECK (("employee_user_id" IS NULL) = ("creation_idempotency_key" IS NULL));--> statement-breakpoint
ALTER TABLE "expected_archive_artifacts" ADD COLUMN "effect_id" uuid;--> statement-breakpoint
ALTER TABLE "expected_archive_artifacts" ADD CONSTRAINT "expected_archive_artifacts_effect_id_fkey" FOREIGN KEY ("effect_id") REFERENCES "public"."external_effects"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "expected_archive_artifacts" ADD CONSTRAINT "expected_archive_artifacts_effect_id_key" UNIQUE("effect_id");--> statement-breakpoint
CREATE UNIQUE INDEX "worker_checkpoints_terminal_direction_unique" ON "worker_checkpoints" USING btree ("consultation_id","generation","worker_id","worker_epoch","source_participant_id","destination_participant_id") WHERE "terminal";