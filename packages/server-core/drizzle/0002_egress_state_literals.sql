UPDATE "egress_jobs"
SET "state" = CASE "state"
  WHEN '0' THEN 'EGRESS_STARTING'
  WHEN '1' THEN 'EGRESS_ACTIVE'
  WHEN '2' THEN 'EGRESS_ENDING'
  WHEN '3' THEN 'EGRESS_COMPLETE'
  WHEN '4' THEN 'EGRESS_FAILED'
  WHEN '5' THEN 'EGRESS_ABORTED'
  WHEN '6' THEN 'EGRESS_LIMIT_REACHED'
  ELSE "state"
END
WHERE "state" IN ('0', '1', '2', '3', '4', '5', '6');--> statement-breakpoint
ALTER TABLE "egress_jobs" ADD CONSTRAINT "egress_jobs_state_check" CHECK (state = ANY (ARRAY['requested'::text, 'EGRESS_STARTING'::text, 'EGRESS_ACTIVE'::text, 'EGRESS_ENDING'::text, 'EGRESS_COMPLETE'::text, 'EGRESS_FAILED'::text, 'EGRESS_ABORTED'::text, 'EGRESS_LIMIT_REACHED'::text]));