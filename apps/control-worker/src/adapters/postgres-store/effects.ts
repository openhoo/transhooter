import { Prisma } from "@transhooter/server-core/persistence";
import type {
  AppliedTransition,
  ClaimOptions,
  Effect,
  OutboxItem,
  PlannedEffect,
  Uuid,
} from "../../orchestration/model";
import {
  type AppliedTransitionRow,
  type ExternalEffectRow,
  type IdRow,
  mapEffect,
  mapOutboxItem,
  type OutboxRow,
  type PrismaConnection,
  withTransaction,
} from "./shared";

export async function claimOutbox(
  client: PrismaConnection,
  options: ClaimOptions,
): Promise<readonly OutboxItem[]> {
  const rows = await client.$queryRaw<OutboxRow[]>(Prisma.sql`WITH picked AS (
    SELECT id FROM outbox WHERE delivered_at IS NULL AND available_at <= ${options.now.toISOString()}
      AND (lease_expires_at IS NULL OR lease_expires_at < ${options.now.toISOString()})
    ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT ${options.limit}
  ) UPDATE outbox o SET lease_owner=${options.owner}, lease_expires_at=${new Date(options.now.getTime() + options.leaseMs).toISOString()}, attempts=o.attempts+1
    FROM picked WHERE o.id=picked.id RETURNING o.*`);
  return rows.map(mapOutboxItem);
}

export async function completeOutbox(
  client: PrismaConnection,
  id: Uuid,
  owner: Uuid,
): Promise<void> {
  await client.$executeRaw(
    Prisma.sql`UPDATE outbox SET delivered_at=now(), lease_owner=NULL, lease_expires_at=NULL WHERE id=${id} AND lease_owner=${owner}`,
  );
}

export async function retryOutbox(
  client: PrismaConnection,
  id: Uuid,
  owner: Uuid,
  error: string,
  nextAt: Date,
): Promise<void> {
  await client.$executeRaw(Prisma.sql`UPDATE outbox SET available_at=${nextAt.toISOString()}, lease_owner=NULL, lease_expires_at=NULL,
    payload=jsonb_set(payload,'{lastDispatchError}',to_jsonb(${error}::text),true) WHERE id=${id} AND lease_owner=${owner}`);
}

export async function claimEffects(
  client: PrismaConnection,
  options: ClaimOptions,
): Promise<readonly Effect[]> {
  const rows = await client.$queryRaw<ExternalEffectRow[]>(Prisma.sql`WITH picked AS (
    SELECT candidate.id FROM external_effects candidate WHERE candidate.state IN ('planned','calling','applied','compensating')
      AND (candidate.lease_expires_at IS NULL OR candidate.lease_expires_at < ${options.now.toISOString()})
      AND (
        candidate.result->'plan'->>'dependsOnEffectId' IS NULL
        OR EXISTS (SELECT 1 FROM external_effects dependency WHERE dependency.id=(candidate.result->'plan'->>'dependsOnEffectId')::uuid AND dependency.state='done')
      )
      AND NOT EXISTS (
        SELECT 1 FROM jsonb_array_elements_text(COALESCE(candidate.result->'plan'->'dependsOnEffectIds','[]'::jsonb)) required(id)
        LEFT JOIN external_effects dependency ON dependency.id=required.id::uuid AND dependency.state='done'
        WHERE dependency.id IS NULL
      )
      AND (
        candidate.result->'plan'->>'notBeforeMs' IS NULL
        OR (candidate.result->'plan'->>'notBeforeMs')::bigint <= ${options.now.getTime()}
      )
      AND (
        candidate.result->'plan'->>'waitForWorkerTerminal' IS DISTINCT FROM 'true'
        OR EXISTS (
          SELECT 1 FROM worker_job_epochs epoch
          WHERE epoch.consultation_id=candidate.consultation_id
            AND epoch.generation=COALESCE((candidate.result->'plan'->>'workerTerminalGeneration')::integer,candidate.generation)
            AND epoch.terminal_at IS NOT NULL
        )
      )
    ORDER BY CASE candidate.effect_kind WHEN 'STATUS_PACKET'::text THEN 0 ELSE 1 END,
      candidate.lease_expires_at NULLS FIRST,candidate.created_at,candidate.id
    FOR UPDATE SKIP LOCKED LIMIT ${options.limit}
  ) UPDATE external_effects e SET lease_owner=${options.owner}, lease_expires_at=${new Date(options.now.getTime() + options.leaseMs).toISOString()}
    FROM picked WHERE e.id=picked.id RETURNING e.*`);
  return rows.map(mapEffect);
}

export async function persistCalling(
  client: PrismaConnection,
  effectId: Uuid,
  owner: Uuid,
  requestBytes: Uint8Array,
  requestSha256: string,
): Promise<Effect | null> {
  const rows = await client.$queryRaw<
    ExternalEffectRow[]
  >(Prisma.sql`UPDATE external_effects SET state='calling', request_bytes=COALESCE(request_bytes, ${Buffer.from(requestBytes)}),
    request_hash=COALESCE(request_hash, ${requestSha256}), attempts=attempts+1,updated_at=now()
    WHERE id=${effectId} AND lease_owner=${owner} AND state IN ('planned','calling') AND (request_hash IS NULL OR request_hash=${requestSha256}) RETURNING *`);
  return rows[0] === undefined ? null : mapEffect(rows[0]);
}

export async function markApplied(
  client: PrismaConnection,
  effectId: Uuid,
  owner: Uuid,
  remoteId: string | null,
  result: unknown,
): Promise<AppliedTransition> {
  return withTransaction(client, async (transaction) => {
    await transaction.$queryRaw(Prisma.sql`SELECT consultation.id
      FROM consultations consultation
      JOIN external_effects effect ON effect.consultation_id=consultation.id
      WHERE effect.id=${effectId}
      FOR UPDATE OF consultation`);
    const rows = await transaction.$queryRaw<AppliedTransitionRow[]>(
      markAppliedStatement(effectId, owner, remoteId, result),
    );
    return rows[0]?.transitioned === true ? "applied" : "rejected";
  });
}

export async function markDone(
  client: PrismaConnection,
  effectId: Uuid,
  owner: Uuid,
): Promise<void> {
  await client.$executeRaw(
    Prisma.sql`UPDATE external_effects SET state='done',updated_at=now(),lease_owner=NULL,lease_expires_at=NULL
      WHERE id=${effectId} AND lease_owner=${owner}
        AND lease_expires_at > now()
        AND state IN ('calling','applied','compensating')`,
  );
}

export async function markFailed(
  client: PrismaConnection,
  effectId: Uuid,
  owner: Uuid,
  error: string,
  retryAt: Date | null,
): Promise<void> {
  await client.$executeRaw(Prisma.sql`UPDATE external_effects SET state=${retryAt === null ? "failed" : "calling"},
    result=COALESCE(result,'{}'::jsonb) || ${JSON.stringify({ error })}::jsonb,
    updated_at=now(),lease_owner=NULL,lease_expires_at=${retryAt?.toISOString() ?? null} WHERE id=${effectId} AND lease_owner=${owner}`);
}

export async function renewEffectLease(
  client: PrismaConnection,
  effectId: Uuid,
  owner: Uuid,
  leaseExpiresAt: Date,
): Promise<boolean> {
  const rows = await client.$queryRaw<IdRow[]>(Prisma.sql`UPDATE external_effects
    SET lease_expires_at=GREATEST(lease_expires_at,${leaseExpiresAt.toISOString()}::timestamptz),updated_at=now()
    WHERE id=${effectId} AND lease_owner=${owner} AND lease_expires_at > now()
      AND state IN ('calling','applied','compensating') RETURNING id`);
  return rows.length === 1;
}

export async function markCompensating(
  client: PrismaConnection,
  effectId: Uuid,
  owner: Uuid,
  reason: string,
): Promise<void> {
  await client.$executeRaw(
    Prisma.sql`UPDATE external_effects SET state='compensating',
      result=COALESCE(result,'{}'::jsonb) || ${JSON.stringify({ reason })}::jsonb,updated_at=now()
      WHERE id=${effectId} AND lease_owner=${owner} AND lease_expires_at > now()`,
  );
}

export async function scheduleEffect(
  client: PrismaConnection,
  input: PlannedEffect,
): Promise<void> {
  await withTransaction(client, async (transaction) => {
    await insertPlannedEffects(transaction, [input]);
  });
}

const PLANNED_EFFECT_BATCH_SIZE = 500;

export async function insertPlannedEffects(
  transaction: Prisma.TransactionClient,
  effects: readonly PlannedEffect[],
): Promise<void> {
  for (let offset = 0; offset < effects.length; offset += PLANNED_EFFECT_BATCH_SIZE) {
    await insertPlannedEffectBatch(
      transaction,
      effects.slice(offset, offset + PLANNED_EFFECT_BATCH_SIZE),
    );
  }
}

async function insertPlannedEffectBatch(
  transaction: Prisma.TransactionClient,
  effects: readonly PlannedEffect[],
): Promise<void> {
  if (effects.length === 0) {
    return;
  }

  const plannedRows = effects.map(
    (effect, ordinal) => Prisma.sql`(
      ${ordinal}::integer,${effect.id}::uuid,${effect.consultationId}::uuid,
      ${effect.generation}::integer,${effect.kind}::text,${effect.subjectId}::uuid,
      ${effect.occurrenceKey}::text,${JSON.stringify({ plan: effect.plan })}::jsonb
    )`,
  );
  const authoritative = await transaction.$queryRaw<
    (IdRow & {
      readonly effect_kind: string;
      readonly output_prefix: string | null;
    })[]
  >(Prisma.sql`WITH requested(
      ordinal,id,consultation_id,generation,effect_kind,subject_id,occurrence_key,result
    ) AS (
      VALUES ${Prisma.join(plannedRows)}
    ), unique_requested AS (
      SELECT DISTINCT ON (consultation_id,generation,effect_kind,subject_id,occurrence_key)
        id,consultation_id,generation,effect_kind,subject_id,occurrence_key,result
      FROM requested
      ORDER BY consultation_id,generation,effect_kind,subject_id,occurrence_key,ordinal
    ), inserted AS (
      INSERT INTO external_effects(
        id,consultation_id,generation,effect_kind,subject_id,occurrence_key,
        state,result,attempts,created_at,updated_at
      )
      SELECT id,consultation_id,generation,effect_kind,subject_id,occurrence_key,
        'planned',result,0,now(),now()
      FROM unique_requested
      ON CONFLICT (consultation_id,generation,effect_kind,subject_id,occurrence_key)
      DO UPDATE SET updated_at=external_effects.updated_at
      RETURNING id,consultation_id,generation,effect_kind,subject_id,occurrence_key
    )
    SELECT inserted.id,requested.effect_kind,
      requested.result->'plan'->>'outputPrefix' AS output_prefix
    FROM requested
    JOIN inserted USING (consultation_id,generation,effect_kind,subject_id,occurrence_key)
    ORDER BY requested.ordinal`);
  if (authoritative.length !== effects.length) {
    throw new Error("authoritative effects were not persisted");
  }

  const egressRowsByKey = new Map<string, Prisma.Sql>();
  for (const effect of authoritative) {
    if (
      effect.effect_kind !== "ROOM_COMPOSITE_EGRESS" &&
      effect.effect_kind !== "PARTICIPANT_EGRESS"
    ) {
      continue;
    }
    if (effect.output_prefix === null || effect.output_prefix.length === 0) {
      throw new Error("Egress effect omitted its immutable output prefix");
    }
    const objectClass =
      effect.effect_kind === "ROOM_COMPOSITE_EGRESS" ? "room_composite" : "participant_original";
    const key = JSON.stringify([effect.id, objectClass, effect.output_prefix]);
    if (!egressRowsByKey.has(key)) {
      egressRowsByKey.set(
        key,
        Prisma.sql`(${effect.id}::uuid,${objectClass}::text,${effect.output_prefix}::text)`,
      );
    }
  }
  if (egressRowsByKey.size === 0) {
    return;
  }
  const egressRows = [...egressRowsByKey.values()];
  await transaction.$executeRaw(Prisma.sql`WITH requested(effect_id,object_class,causal_key) AS (
      VALUES ${Prisma.join(egressRows)}
    )
    INSERT INTO expected_archive_artifacts(
      id,archive_id,effect_id,profile_id,profile_revision,object_class,causal_key,
      sample_start,sample_end,owner_epoch,disposition,created_at
    )
    SELECT requested.effect_id,archive.id,requested.effect_id,consultation.provider_profile_id,
      consultation.provider_profile_revision,requested.object_class,requested.causal_key,
      NULL,NULL,archive.write_epoch,'expected',now()
    FROM requested
    JOIN external_effects effect ON effect.id=requested.effect_id
    JOIN consultations consultation ON consultation.id=effect.consultation_id
      AND consultation.generation=effect.generation
    JOIN archives archive ON archive.consultation_id=consultation.id
      AND archive.state NOT IN ('deleting','deleted')
    ON CONFLICT (archive_id,object_class,causal_key) DO UPDATE
    SET effect_id=COALESCE(expected_archive_artifacts.effect_id,EXCLUDED.effect_id)
    WHERE expected_archive_artifacts.effect_id IS NULL
      OR expected_archive_artifacts.effect_id=EXCLUDED.effect_id`);
}

function markAppliedStatement(
  effectId: Uuid,
  owner: Uuid,
  remoteId: string | null,
  result: unknown,
): Prisma.Sql {
  return Prisma.sql`WITH changed AS (
      UPDATE external_effects SET state='applied',
        result=COALESCE(result,'{}'::jsonb) || ${JSON.stringify({ remoteId, value: result })}::jsonb,updated_at=now()
      WHERE id=${effectId} AND lease_owner=${owner} AND lease_expires_at > now()
        AND state='calling'
        AND EXISTS (
          SELECT 1 FROM consultations consultation
          WHERE consultation.id=external_effects.consultation_id
            AND consultation.generation=external_effects.generation
        )
      RETURNING *
    ), egress_inserted AS (
      INSERT INTO egress_jobs(id,consultation_id,generation,kind,subject_id,egress_id,request_hash,state,output_prefix,expected_artifact_id,created_at)
      SELECT id,consultation_id,generation,CASE effect_kind WHEN 'ROOM_COMPOSITE_EGRESS' THEN 'room_composite' ELSE 'participant' END,subject_id,result->>'remoteId',request_hash,
        COALESCE(result->'value'->>'status','requested'),result->'plan'->>'outputPrefix',id,now()
      FROM changed WHERE effect_kind IN ('ROOM_COMPOSITE_EGRESS','PARTICIPANT_EGRESS')
      ON CONFLICT (consultation_id,generation,kind,subject_id) DO UPDATE SET
        egress_id=EXCLUDED.egress_id,state=EXCLUDED.state,expected_artifact_id=EXCLUDED.expected_artifact_id RETURNING id
    ), egress_updated AS (
      UPDATE egress_jobs job SET state=terminal->>'status',terminal_at=now(),terminal_result=terminal
      FROM changed, LATERAL jsonb_array_elements(
        CASE
          WHEN changed.effect_kind='ROOM_DRAIN' THEN COALESCE(changed.result->'value'->'egressTerminals','[]'::jsonb)
          WHEN changed.effect_kind='EGRESS_STOP' THEN jsonb_build_array(changed.result->'value')
          ELSE '[]'::jsonb
        END
      ) terminal WHERE job.egress_id=terminal->>'egressId' RETURNING job.id
    ), consultation_updated AS (
      UPDATE consultations consultation SET
        room_sid=CASE WHEN changed.effect_kind='ROOM_CREATE' THEN changed.result->>'remoteId' ELSE consultation.room_sid END,
        composite_egress_id=CASE WHEN changed.effect_kind='ROOM_COMPOSITE_EGRESS' THEN changed.result->>'remoteId' ELSE consultation.composite_egress_id END,
        dispatch_id=CASE WHEN changed.effect_kind='WORKER_DISPATCH' THEN changed.result->>'remoteId' ELSE consultation.dispatch_id END,
        updated_at=now()
      FROM changed
      WHERE consultation.id=changed.consultation_id AND consultation.generation=changed.generation
        AND changed.effect_kind IN ('ROOM_CREATE','ROOM_COMPOSITE_EGRESS','WORKER_DISPATCH')
      RETURNING consultation.id
    ), outbox_inserted AS (
      INSERT INTO outbox(id,topic,aggregate_id,generation,payload,available_at,attempts)
      SELECT gen_random_uuid(),'orchestration.effect.applied',consultation_id,generation,
        jsonb_build_object('consultationId',consultation_id,'generation',generation,'subjectId',subject_id,'kind',effect_kind,
          'resourceGeneration',COALESCE((result->'plan'->>'resourceGeneration')::integer,generation),
          'participantEgressId',CASE WHEN effect_kind IN ('ROOM_COMPOSITE_EGRESS','PARTICIPANT_EGRESS') THEN result->>'remoteId' ELSE result->'plan'->>'barrierEgressId' END),now(),0 FROM changed
      RETURNING id
    ) SELECT EXISTS(SELECT 1 FROM changed) AS transitioned`;
}
