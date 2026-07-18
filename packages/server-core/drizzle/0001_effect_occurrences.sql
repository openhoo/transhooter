ALTER TABLE "external_effects" DROP CONSTRAINT "external_effect_occurrence_unique";--> statement-breakpoint
ALTER TABLE "external_effects" ADD COLUMN "occurrence_key" text DEFAULT '' NOT NULL;--> statement-breakpoint
ALTER TABLE "external_effects" ADD CONSTRAINT "external_effect_occurrence_unique" UNIQUE("consultation_id","generation","effect_kind","subject_id","occurrence_key");