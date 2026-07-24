import { describe, expect, it } from "bun:test";
import { readFile } from "node:fs/promises";
import { createPrismaDatabase, type PrismaDatabase } from "@transhooter/server-core/persistence";
import { claimEffects } from "../src/adapters/postgres-store/effects";

async function databaseUrl(): Promise<string> {
  if (process.env.DATABASE_URL) return process.env.DATABASE_URL;
  if (!process.env.DATABASE_URL_FILE) {
    throw new Error("DATABASE_URL or DATABASE_URL_FILE is required");
  }
  return (await readFile(process.env.DATABASE_URL_FILE, "utf8")).trim();
}

const integrationEnabled = process.env.POSTGRES_CONCURRENCY_INTEGRATION === "1";

(integrationEnabled ? describe : describe.skip)("PostgreSQL effect claim fairness", () => {
  it("claims planned work under continuously replenished recovery load", async () => {
    let database: PrismaDatabase | null = null;
    const profileId = crypto.randomUUID();
    const consultationId = crypto.randomUUID();
    const plannedId = crypto.randomUUID();
    const owner = crypto.randomUUID();
    const base = new Date("2000-01-01T00:00:00.000Z");

    database = createPrismaDatabase({ connectionString: await databaseUrl(), pool: { max: 2 } });
    const prisma = database.client;
    try {
      await prisma.$executeRawUnsafe(
        `INSERT INTO provider_profiles(id,name,enabled,current_revision,created_at)
         VALUES ($1,$2,true,1,$3)`,
        profileId,
        `claim-fairness-${profileId}`,
        base,
      );
      await prisma.$executeRawUnsafe(
        `INSERT INTO provider_profile_revisions(
           profile_id,revision,capability_hash,adapter_builds,policy,credential_references,created_at
         ) VALUES ($1,1,$2,'{}'::jsonb,'{}'::jsonb,'[]'::jsonb,$3)`,
        profileId,
        "c".repeat(64),
        base,
      );
      await prisma.$executeRawUnsafe(
        `INSERT INTO consultations(
           id,state,provider_profile_id,provider_profile_revision,created_at,updated_at
         ) VALUES ($1,'invited',$2,1,$3,$3)`,
        consultationId,
        profileId,
        base,
      );
      await prisma.$executeRawUnsafe(
        `INSERT INTO external_effects(
           id,consultation_id,generation,effect_kind,subject_id,state,created_at,updated_at,
           occurrence_key,result
         ) VALUES ($1,$2,0,'ROOM_CREATE',$3,'planned',$4,$4,'planned','{"plan":{}}'::jsonb)`,
        plannedId,
        consultationId,
        crypto.randomUUID(),
        new Date(base.getTime() + 2_000),
      );

      let plannedClaimed = false;
      for (let tick = 0; tick < 4; tick += 1) {
        const recoveryId = crypto.randomUUID();
        const recoveryCreatedAt = new Date(base.getTime() + tick * 1_000);
        await prisma.$executeRawUnsafe(
          `INSERT INTO external_effects(
             id,consultation_id,generation,effect_kind,subject_id,state,created_at,updated_at,
             occurrence_key,lease_expires_at,request_bytes,request_hash,result
           ) VALUES ($1,$2,0,'ROOM_CREATE',$3,'calling',$4,$4,$5,$6,$7,$8,'{"plan":{}}'::jsonb)`,
          recoveryId,
          consultationId,
          crypto.randomUUID(),
          recoveryCreatedAt,
          `recovery-${tick}`,
          recoveryCreatedAt,
          new TextEncoder().encode(`request-${tick}`),
          "d".repeat(64),
        );
        const claimed = await claimEffects(prisma, {
          owner,
          now: new Date(base.getTime() + 10_000 + tick * 1_000),
          leaseMs: 30_000,
          limit: 1,
        });
        if (claimed[0]?.id === plannedId) plannedClaimed = true;
      }

      expect(plannedClaimed).toBe(true);
    } finally {
      await database?.disconnect();
    }
  });

  it("treats a missing worker epoch as terminal absence without bypassing a live epoch", async () => {
    let database: PrismaDatabase | null = null;
    const profileId = crypto.randomUUID();
    const consultationId = crypto.randomUUID();
    const workerId = crypto.randomUUID();
    const noEpochEffectId = crypto.randomUUID();
    const liveEpochEffectId = crypto.randomUUID();
    const owner = crypto.randomUUID();
    const base = new Date("2000-01-01T00:00:00.000Z");

    database = createPrismaDatabase({ connectionString: await databaseUrl(), pool: { max: 2 } });
    const prisma = database.client;
    try {
      await prisma.$executeRawUnsafe(
        `INSERT INTO provider_profiles(id,name,enabled,current_revision,created_at)
         VALUES ($1,$2,true,1,$3)`,
        profileId,
        `claim-worker-terminal-${profileId}`,
        base,
      );
      await prisma.$executeRawUnsafe(
        `INSERT INTO provider_profile_revisions(
           profile_id,revision,capability_hash,adapter_builds,policy,credential_references,created_at
         ) VALUES ($1,1,$2,'{}'::jsonb,'{}'::jsonb,'[]'::jsonb,$3)`,
        profileId,
        "e".repeat(64),
        base,
      );
      await prisma.$executeRawUnsafe(
        `INSERT INTO consultations(
           id,state,provider_profile_id,provider_profile_revision,created_at,updated_at
         ) VALUES ($1,'invited',$2,1,$3,$3)`,
        consultationId,
        profileId,
        base,
      );
      await prisma.$executeRawUnsafe(
        `INSERT INTO worker_leases(
           worker_id,accepting_load,capacity,reserved,encrypted_spool_percent,providers_ok,
           archive_ok,heartbeat_at,expires_at,epoch,status
         ) VALUES ($1,false,1,0,0,true,true,$2,$3,0,'{}'::jsonb)`,
        workerId,
        base,
        new Date(base.getTime() + 60_000),
      );
      await prisma.$executeRawUnsafe(
        `INSERT INTO worker_job_epochs(
           consultation_id,generation,worker_id,epoch,write_epoch,heartbeat_at
         ) VALUES ($1,0,$2,0,0,$3)`,
        consultationId,
        workerId,
        base,
      );
      await prisma.$executeRawUnsafe(
        `INSERT INTO external_effects(
           id,consultation_id,generation,effect_kind,subject_id,state,created_at,updated_at,
           occurrence_key,result
         ) VALUES
           ($1,$2,0,'ROOM_DELETE',$3,'planned',$4,$4,'missing-epoch',
            '{"plan":{"waitForWorkerTerminal":true,"workerTerminalGeneration":1}}'::jsonb),
           ($5,$2,0,'ROOM_DELETE',$6,'planned',$4,$4,'live-epoch',
            '{"plan":{"waitForWorkerTerminal":true,"workerTerminalGeneration":0}}'::jsonb)`,
        noEpochEffectId,
        consultationId,
        crypto.randomUUID(),
        base,
        liveEpochEffectId,
        crypto.randomUUID(),
      );

      const withoutEpoch = await claimEffects(prisma, {
        owner,
        now: new Date(base.getTime() + 10_000),
        leaseMs: 30_000,
        limit: 1,
      });
      expect(withoutEpoch.map(({ id }) => id)).toEqual([noEpochEffectId]);

      await prisma.$executeRawUnsafe(
        `UPDATE worker_job_epochs SET terminal_at=$1,terminal_outcome='failed'
         WHERE consultation_id=$2 AND generation=0`,
        new Date(base.getTime() + 20_000),
        consultationId,
      );
      const afterTerminal = await claimEffects(prisma, {
        owner,
        now: new Date(base.getTime() + 20_000),
        leaseMs: 30_000,
        limit: 1,
      });
      expect(afterTerminal.map(({ id }) => id)).toEqual([liveEpochEffectId]);
    } finally {
      await database?.disconnect();
    }
  });
});
