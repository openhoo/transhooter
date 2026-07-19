ALTER TABLE "provider_attempts" DROP CONSTRAINT "provider_attempts_archive_id_fkey";
--> statement-breakpoint
ALTER TABLE "provider_attempts" DROP CONSTRAINT "provider_attempts_consultation_id_fkey";
--> statement-breakpoint
ALTER TABLE "archives" ADD CONSTRAINT "archives_id_consultation_id_key" UNIQUE("id","consultation_id");--> statement-breakpoint
ALTER TABLE "provider_attempts" ADD CONSTRAINT "provider_attempts_archive_consultation_fkey" FOREIGN KEY ("archive_id","consultation_id") REFERENCES "public"."archives"("id","consultation_id") ON DELETE no action ON UPDATE no action;