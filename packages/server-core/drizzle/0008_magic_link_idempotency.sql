ALTER TABLE "magic_links" ADD COLUMN "sealed_raw_token" text;--> statement-breakpoint
ALTER TABLE "magic_links" ADD COLUMN "sealed_token_key_id" text;--> statement-breakpoint
CREATE FUNCTION "enforce_magic_link_sealed_token"() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW."sealed_raw_token" IS NULL
    OR length(NEW."sealed_raw_token") = 0
    OR NEW."sealed_token_key_id" IS NULL
    OR length(NEW."sealed_token_key_id") = 0
  THEN
    RAISE EXCEPTION USING
      ERRCODE = 'not_null_violation',
      MESSAGE = 'new magic links require a sealed delivery token and key id';
  END IF;
  RETURN NEW;
END
$$;--> statement-breakpoint
CREATE TRIGGER "magic_links_require_sealed_token"
BEFORE INSERT ON "magic_links"
FOR EACH ROW
EXECUTE FUNCTION "enforce_magic_link_sealed_token"();--> statement-breakpoint
ALTER TABLE "magic_links" ADD CONSTRAINT "magic_links_sealed_token_pair_check" CHECK (
  ("sealed_raw_token" IS NULL) = ("sealed_token_key_id" IS NULL)
);--> statement-breakpoint
WITH ranked_active_links AS (
  SELECT
    "id",
    row_number() OVER (
      PARTITION BY "user_id", "purpose", "consultation_id", "session_id"
      ORDER BY "created_at" DESC, "id" DESC
    ) AS active_rank
  FROM "magic_links"
  WHERE "consumed_at" IS NULL
    AND "revoked_at" IS NULL
), duplicate_active_links AS (
  SELECT "id"
  FROM ranked_active_links
  WHERE active_rank > 1
)
UPDATE "magic_links"
SET "revoked_at" = CURRENT_TIMESTAMP
FROM duplicate_active_links
WHERE "magic_links"."id" = duplicate_active_links."id";--> statement-breakpoint
CREATE UNIQUE INDEX "magic_links_active_identity_unique" ON "magic_links" USING btree (
  "user_id",
  "purpose",
  "consultation_id",
  "session_id"
) NULLS NOT DISTINCT
WHERE "consumed_at" IS NULL AND "revoked_at" IS NULL;
