DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM "provider_attempts" AS attempt
    JOIN "archives" AS archive ON archive."id" = attempt."archive_id"
    WHERE archive."consultation_id" <> attempt."consultation_id"
  ) THEN
    RAISE EXCEPTION USING
      ERRCODE = 'foreign_key_violation',
      MESSAGE = '0005_cultured_ezekiel found provider_attempts linked to a different consultation than their archive',
      DETAIL = 'The legacy independent archive_id and consultation_id foreign keys permit an invalid composite relationship that cannot be repaired automatically without changing evidence.',
      HINT = 'Repair or remove the mismatched provider_attempts rows before applying this migration.';
  END IF;
END
$$;--> statement-breakpoint
ALTER TABLE "provider_attempts" DROP CONSTRAINT "provider_attempts_archive_id_fkey";
--> statement-breakpoint
ALTER TABLE "provider_attempts" DROP CONSTRAINT "provider_attempts_consultation_id_fkey";
--> statement-breakpoint
ALTER TABLE "archives" ADD CONSTRAINT "archives_id_consultation_id_key" UNIQUE("id","consultation_id");--> statement-breakpoint
ALTER TABLE "provider_attempts" ADD CONSTRAINT "provider_attempts_archive_consultation_fkey" FOREIGN KEY ("archive_id","consultation_id") REFERENCES "public"."archives"("id","consultation_id") ON DELETE no action ON UPDATE no action;