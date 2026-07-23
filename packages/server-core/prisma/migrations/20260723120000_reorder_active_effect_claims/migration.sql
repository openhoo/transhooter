DROP INDEX "external_effects_active_claim_priority_lease_created_id_idx";

CREATE INDEX "external_effects_active_claim_priority_lease_created_id_idx"
ON "external_effects" (COALESCE("lease_expires_at", "created_at"), (CASE "state" WHEN 'planned'::external_effect_state THEN 1 ELSE 0 END), (CASE "effect_kind" WHEN 'STATUS_PACKET'::text THEN 0 ELSE 1 END), "created_at", "id")
WHERE "state" IN (
  'planned'::external_effect_state,
  'calling'::external_effect_state,
  'applied'::external_effect_state,
  'compensating'::external_effect_state
);
