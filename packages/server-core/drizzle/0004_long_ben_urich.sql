DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM "worker_checkpoints") THEN
    RAISE EXCEPTION USING
      ERRCODE = 'data_exception',
      MESSAGE = '0004_long_ben_urich cannot losslessly migrate legacy worker_checkpoints',
      DETAIL = 'accepted_input_sequence, received_output, and emitted_output are independent checkpoint dimensions and cannot be derived from the legacy high_watermark scalar.',
      HINT = 'Export and losslessly transform or remove all legacy worker_checkpoints before applying this migration.';
  END IF;
END
$$;--> statement-breakpoint
ALTER TABLE "worker_checkpoints" DROP CONSTRAINT "worker_checkpoints_direction_high_watermark_key";--> statement-breakpoint
ALTER TABLE "worker_checkpoints" DROP CONSTRAINT "worker_checkpoints_high_watermark_check";--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ADD COLUMN "accepted_input_sequence" bigint;--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ADD COLUMN "received_output" bigint;--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ADD COLUMN "emitted_output" bigint;--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ALTER COLUMN "accepted_input_sequence" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ALTER COLUMN "received_output" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ALTER COLUMN "emitted_output" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ADD CONSTRAINT "worker_checkpoints_direction_watermarks_key" UNIQUE("consultation_id","worker_epoch","source_participant_id","destination_participant_id","accepted_input_sequence","high_watermark","received_output","emitted_output");--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ADD CONSTRAINT "worker_checkpoints_accepted_input_sequence_check" CHECK (accepted_input_sequence >= 0);--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ADD CONSTRAINT "worker_checkpoints_accepted_input_check" CHECK (high_watermark >= 0);--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ADD CONSTRAINT "worker_checkpoints_received_output_check" CHECK (received_output >= 0);--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ADD CONSTRAINT "worker_checkpoints_emitted_output_check" CHECK (emitted_output >= 0);