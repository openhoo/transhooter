ALTER TABLE "worker_checkpoints"
  DROP CONSTRAINT "worker_checkpoints_direction_watermarks_key";

ALTER TABLE "worker_checkpoints"
  ADD CONSTRAINT "worker_checkpoints_direction_watermarks_key"
  UNIQUE (
    "consultation_id",
    "worker_epoch",
    "source_participant_id",
    "destination_participant_id",
    "accepted_input_sequence",
    "accepted_input",
    "received_output",
    "emitted_output",
    "terminal"
  );
