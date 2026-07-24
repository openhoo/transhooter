SCHEMA_VERSION = 2
UNSUPPORTED_SCHEMA = "unsupported spool schema; drain and recreate the spool before service cutover"

SCHEMA = """
CREATE TABLE spool_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE records (
 ordinal INTEGER PRIMARY KEY AUTOINCREMENT, object_id TEXT NOT NULL UNIQUE,
 attempt_id TEXT NOT NULL, meeting_id TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation >= 0), worker_id TEXT NOT NULL,
 worker_epoch INTEGER NOT NULL CHECK(worker_epoch >= 1), write_epoch INTEGER NOT NULL CHECK(write_epoch >= 0),
 stage TEXT NOT NULL, transport TEXT NOT NULL, direction TEXT NOT NULL, media_type TEXT NOT NULL,
 opaque_path TEXT NOT NULL UNIQUE, key_id TEXT NOT NULL, nonce BLOB NOT NULL,
 plaintext_sha256 TEXT NOT NULL, ciphertext_sha256 TEXT NOT NULL, size INTEGER NOT NULL CHECK(size >= 0),
 sample_start INTEGER, sample_end INTEGER, metadata_json BLOB NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('committed','uploaded','permanent','quarantined')),
 version_id TEXT, s3_checksum TEXT, error_kind TEXT, failed_at TEXT,
 CHECK((sample_start IS NULL AND sample_end IS NULL) OR (sample_start >= 0 AND sample_end > sample_start)),
 CHECK((state='permanent' AND error_kind IS NOT NULL AND failed_at IS NOT NULL) OR
       (state!='permanent' AND error_kind IS NULL AND failed_at IS NULL))
);
CREATE TABLE checkpoint_deliveries (
 checkpoint_id TEXT PRIMARY KEY, record_id TEXT NOT NULL REFERENCES records(object_id),
 meeting_id TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation >= 0), worker_id TEXT NOT NULL,
 worker_epoch INTEGER NOT NULL CHECK(worker_epoch >= 1), write_epoch INTEGER NOT NULL CHECK(write_epoch >= 0),
 source_id TEXT NOT NULL, checkpoint_hash TEXT NOT NULL, previous_hash TEXT,
 control_event_id TEXT NOT NULL UNIQUE, object_key TEXT NOT NULL UNIQUE,
 evidence_ordinal INTEGER CHECK(evidence_ordinal IS NULL OR evidence_ordinal >= 0),
 delivery_state TEXT NOT NULL DEFAULT 'pending' CHECK(delivery_state IN ('pending','acknowledged','permanent')),
 error_kind TEXT, failed_at TEXT,
 CHECK((delivery_state='permanent' AND error_kind IS NOT NULL AND failed_at IS NOT NULL) OR
       (delivery_state!='permanent' AND error_kind IS NULL AND failed_at IS NULL))
);
CREATE INDEX checkpoint_deliveries_pending_scope ON checkpoint_deliveries
 (meeting_id,generation,worker_id,worker_epoch,source_id,delivery_state);
CREATE TABLE consultation_seals (
 seal_id TEXT PRIMARY KEY REFERENCES records(object_id), meeting_id TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation >= 0), worker_id TEXT NOT NULL,
 worker_epoch INTEGER NOT NULL CHECK(worker_epoch >= 1), write_epoch INTEGER NOT NULL CHECK(write_epoch >= 0),
 evidence_ordinal INTEGER NOT NULL CHECK(evidence_ordinal >= 0),
 terminal_outcome TEXT NOT NULL CHECK(length(terminal_outcome) BETWEEN 1 AND 128),
 completion_event_id TEXT NOT NULL UNIQUE, failure_payload BLOB,
 completion_state TEXT NOT NULL DEFAULT 'pending' CHECK(completion_state IN ('pending','acknowledged')),
 checkpoint_id_a TEXT NOT NULL UNIQUE REFERENCES checkpoint_deliveries(checkpoint_id),
 checkpoint_id_b TEXT NOT NULL UNIQUE REFERENCES checkpoint_deliveries(checkpoint_id),
 UNIQUE(meeting_id,generation,worker_id,worker_epoch,write_epoch), CHECK(checkpoint_id_a<>checkpoint_id_b)
);
CREATE TABLE consultation_handoffs (
 meeting_id TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation >= 0), worker_id TEXT NOT NULL,
 worker_epoch INTEGER NOT NULL CHECK(worker_epoch >= 1), write_epoch INTEGER NOT NULL CHECK(write_epoch >= 0),
 state TEXT NOT NULL CHECK(state IN ('active','settling','sealed','relinquished')),
 started_at TEXT NOT NULL, settling_at TEXT, sealed_at TEXT, relinquished_at TEXT,
 reason TEXT CHECK(reason IS NULL OR length(reason) BETWEEN 1 AND 512),
 PRIMARY KEY(meeting_id,generation,worker_id,worker_epoch,write_epoch),
 CHECK((state='active' AND settling_at IS NULL AND sealed_at IS NULL AND relinquished_at IS NULL AND reason IS NULL) OR
       (state='settling' AND settling_at IS NOT NULL AND sealed_at IS NULL AND relinquished_at IS NULL AND reason IS NULL) OR
       (state='sealed' AND settling_at IS NOT NULL AND sealed_at IS NOT NULL AND relinquished_at IS NULL AND reason IS NULL) OR
       (state='relinquished' AND relinquished_at IS NOT NULL AND reason IS NOT NULL))
);
CREATE TABLE compacted_envelopes (
 object_id TEXT PRIMARY KEY REFERENCES records(object_id), compacted_at TEXT NOT NULL
);
"""
