CREATE INDEX CONCURRENTLY "consultation_participants_user_id_consultation_id_idx"
ON "consultation_participants" ("user_id", "consultation_id");

CREATE INDEX CONCURRENTLY "external_effects_active_claim_priority_lease_created_id_idx"
ON "external_effects" ((CASE "effect_kind" WHEN 'STATUS_PACKET'::text THEN 0 ELSE 1 END), "lease_expires_at" ASC NULLS FIRST, "created_at", "id")
WHERE "state" IN (
  'planned'::external_effect_state,
  'calling'::external_effect_state,
  'applied'::external_effect_state,
  'compensating'::external_effect_state
);
