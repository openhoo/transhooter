ALTER TABLE "worker_checkpoints" DROP CONSTRAINT "worker_checkpoints_consultation_id_worker_epoch_high_waterm_key";--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ADD COLUMN "source_participant_id" uuid;--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ADD COLUMN "destination_participant_id" uuid;--> statement-breakpoint
ALTER TABLE "consultation_participants" ADD CONSTRAINT "consultation_participants_consultation_id_id_key" UNIQUE("consultation_id","id");--> statement-breakpoint
CREATE TABLE checkpoint_direction_migration AS
SELECT consultation_id,generation,worker_id,worker_epoch,max(write_epoch) AS write_epoch,
  max(high_watermark) AS high_watermark,bool_or(terminal) AS terminal,
  min(created_at) AS created_at,count(*) AS historical_checkpoint_count
FROM worker_checkpoints
WHERE source_participant_id IS NULL OR destination_participant_id IS NULL
GROUP BY consultation_id,generation,worker_id,worker_epoch;--> statement-breakpoint
UPDATE worker_job_epochs job
SET terminal_checkpoint_id=NULL
WHERE EXISTS (
  SELECT 1 FROM checkpoint_direction_migration historical
  WHERE historical.consultation_id=job.consultation_id
    AND historical.generation=job.generation
    AND historical.worker_id=job.worker_id
    AND historical.worker_epoch=job.epoch
);--> statement-breakpoint
DELETE FROM worker_checkpoints
WHERE source_participant_id IS NULL OR destination_participant_id IS NULL;--> statement-breakpoint
WITH selected_directions AS (
  SELECT selection.consultation_id,
    (direction->>'sourceParticipantId')::uuid AS source_participant_id,
    (direction->>'destinationParticipantId')::uuid AS destination_participant_id
  FROM room_provider_selections selection
  CROSS JOIN LATERAL jsonb_array_elements(selection.selection->'directions') direction
  WHERE jsonb_array_length(selection.selection->'directions')=2
), participant_directions AS (
  SELECT employee.consultation_id,employee.id AS source_participant_id,customer.id AS destination_participant_id
  FROM consultation_participants employee
  JOIN consultation_participants customer ON customer.consultation_id=employee.consultation_id
  WHERE employee.role='employee' AND customer.role='customer'
  UNION ALL
  SELECT employee.consultation_id,customer.id,employee.id
  FROM consultation_participants employee
  JOIN consultation_participants customer ON customer.consultation_id=employee.consultation_id
  WHERE employee.role='employee' AND customer.role='customer'
), directions AS (
  SELECT * FROM selected_directions
  UNION ALL
  SELECT fallback.* FROM participant_directions fallback
  WHERE NOT EXISTS (
    SELECT 1 FROM selected_directions selected
    WHERE selected.consultation_id=fallback.consultation_id
  )
), generated AS MATERIALIZED (
  SELECT gen_random_uuid() AS id,historical.*,direction.source_participant_id,
    direction.destination_participant_id
  FROM checkpoint_direction_migration historical
  JOIN directions direction ON direction.consultation_id=historical.consultation_id
)
INSERT INTO worker_checkpoints(
  id,consultation_id,generation,worker_id,worker_epoch,write_epoch,
  source_participant_id,destination_participant_id,high_watermark,previous_hash,
  checkpoint_hash,expected_ids,observed_ids,gaps,terminal,object_key,
  object_version_id,created_at
)
SELECT generated.id,generated.consultation_id,generated.generation,generated.worker_id,
  generated.worker_epoch,generated.write_epoch,generated.source_participant_id,
  generated.destination_participant_id,generated.high_watermark,NULL,
  encode(sha256(convert_to(concat_ws(':','checkpoint-direction-migration',generated.id::text,
    generated.consultation_id::text,generated.generation::text,generated.worker_epoch::text,
    generated.source_participant_id::text,generated.destination_participant_id::text),'UTF8')),'hex'),
  '[]'::jsonb,'[]'::jsonb,jsonb_build_array(jsonb_build_object(
    'reason','historical_checkpoint_direction_unattributed',
    'historicalCheckpointCount',generated.historical_checkpoint_count,
    'sampleStart',NULL,'sampleEnd',NULL)),generated.terminal,
  'v1/meetings/' || generated.consultation_id::text || '/inventory/checkpoints/supervisor-migration-' || generated.id::text || '.json',
  NULL,generated.created_at
FROM generated;--> statement-breakpoint
UPDATE worker_job_epochs job
SET terminal_checkpoint_id=terminal.id
FROM checkpoint_direction_migration historical
CROSS JOIN LATERAL (
  SELECT checkpoint.id
  FROM worker_checkpoints checkpoint
  WHERE checkpoint.consultation_id=historical.consultation_id
    AND checkpoint.generation=historical.generation
    AND checkpoint.worker_id=historical.worker_id
    AND checkpoint.worker_epoch=historical.worker_epoch
    AND checkpoint.terminal
  ORDER BY checkpoint.source_participant_id,checkpoint.destination_participant_id
  LIMIT 1
) terminal
WHERE historical.terminal
  AND job.consultation_id=historical.consultation_id
  AND job.generation=historical.generation
  AND job.worker_id=historical.worker_id
  AND job.epoch=historical.worker_epoch;--> statement-breakpoint
DROP TABLE checkpoint_direction_migration;--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ALTER COLUMN "source_participant_id" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ALTER COLUMN "destination_participant_id" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ADD CONSTRAINT "worker_checkpoints_source_participant_fkey" FOREIGN KEY ("consultation_id","source_participant_id") REFERENCES "public"."consultation_participants"("consultation_id","id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ADD CONSTRAINT "worker_checkpoints_destination_participant_fkey" FOREIGN KEY ("consultation_id","destination_participant_id") REFERENCES "public"."consultation_participants"("consultation_id","id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "worker_checkpoints" ADD CONSTRAINT "worker_checkpoints_direction_high_watermark_key" UNIQUE("consultation_id","worker_epoch","source_participant_id","destination_participant_id","high_watermark");
