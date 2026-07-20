DO $$
DECLARE
  affected_count bigint;
  affected_ids text;
BEGIN
  WITH affected AS MATERIALIZED (
    SELECT id
    FROM "external_effects"
    WHERE "effect_kind" IN ('ROOM_COMPOSITE_EGRESS', 'PARTICIPANT_EGRESS')
      AND "request_bytes" IS NOT NULL
      AND lower(convert_from("request_bytes", 'UTF8')) ~
        '"(accesskey|secret|custombaseurl|signature|authorization|token|filenameprefix|playlistname|liveplaylistname)"'
  )
  SELECT count(*), (
    SELECT string_agg(id::text, ', ' ORDER BY id::text)
    FROM (SELECT id FROM affected ORDER BY id LIMIT 20) first_affected
  )
  INTO affected_count, affected_ids
  FROM affected;

  IF affected_count > 0 THEN
    RAISE EXCEPTION USING
      ERRCODE = 'data_exception',
      MESSAGE = '0007_redact_egress_intents refused to retain legacy credential-bearing Egress intents',
      DETAIL = format(
        '%s affected external_effects row(s) were found; first IDs: %s.',
        affected_count,
        affected_ids
      ),
      HINT = 'Before retrying, back up the audit evidence and replace each affected request_bytes value with the credential-free canonical Egress intent, recompute request_hash as lowercase SHA-256 hex, and update matching effect_compensation_attempts.request_hash values in the same transaction; otherwise purge the affected consultation only under the configured retention procedure.';
  END IF;
END
$$;--> statement-breakpoint
ALTER TABLE "external_effects" ADD CONSTRAINT "external_effects_egress_intent_redacted_check" CHECK (
  "effect_kind" NOT IN ('ROOM_COMPOSITE_EGRESS', 'PARTICIPANT_EGRESS')
  OR "request_bytes" IS NULL
  OR lower(convert_from("request_bytes", 'UTF8')) !~
    '"(accesskey|secret|custombaseurl|signature|authorization|token|filenameprefix|playlistname|liveplaylistname)"'
);
