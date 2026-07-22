import { ArchiveStateSchema } from "@transhooter/contracts";
import type { SQL } from "drizzle-orm";
import { sql } from "drizzle-orm";
import type { PostgresJsDatabase } from "drizzle-orm/postgres-js";
import type { Sql, TransactionSql } from "postgres";
import type {
  ArchivedObject,
  DerivedArchiveObject,
  Effect,
  FinalInventoryObject,
  ReconciliationExpectation,
  ReconciliationSnapshot,
  Uuid,
} from "../../orchestration/model";
import {
  type ArchiveObjectRow,
  type ArchiveStateRow,
  type CheckpointRow,
  type DrainResultRow,
  type EgressResultRow,
  type ExpectationRow,
  type IdRow,
  nullableString,
  type ProviderGapRow,
  type ReconciliationProviderAttemptRow,
  type ReconciliationDirectionRow,
  type ReconciliationArchiveRow,
  type ReconciliationEgressResult,
} from "./shared";

export async function preparePendingArchiveDeletes(
  db: PostgresJsDatabase,
  _client: Sql,
): Promise<void> {
  await db.execute(claimPendingArchiveDeletesStatement());
}

export async function reconciliationSnapshot(
  _db: PostgresJsDatabase,
  client: Sql,
  consultationId: Uuid,
  cleanupGeneration: number,
  resourceGeneration: number,
): Promise<ReconciliationSnapshot | null> {
  return client.begin(async (transaction) => {
    const [archiveStateRow] = await transaction<ArchiveStateRow[]>`SELECT id,state
      FROM archives
      WHERE consultation_id=${consultationId}`;
    if (archiveStateRow === undefined) {
      throw new Error("consultation archive is unavailable");
    }
    const archiveState = ArchiveStateSchema.parse(archiveStateRow.state);
    if (
      archiveState === "complete" ||
      archiveState === "incomplete" ||
      archiveState === "deleting" ||
      archiveState === "deleted"
    ) {
      return null;
    }
    if (archiveState !== "reconciling") {
      throw new Error(`archive is not reconciling: ${archiveState}`);
    }
    const [archiveRow] = await transaction<
      ReconciliationArchiveRow[]
    >`SELECT id,state,reconciliation_deadline_at
      FROM archives
      WHERE consultation_id=${consultationId} AND state='reconciling'
      FOR UPDATE SKIP LOCKED`;
    const archive =
      archiveRow === undefined
        ? undefined
        : {
            id: archiveRow.id,
            state: ArchiveStateSchema.parse(archiveRow.state),
            reconciliationDeadlineAt:
              archiveRow.reconciliation_deadline_at === null
                ? null
                : new Date(String(archiveRow.reconciliation_deadline_at)),
          };
    if (archive === undefined) {
      throw new Error("reconciling archive is unavailable");
    }
    if (archive.reconciliationDeadlineAt === null) {
      throw new Error("reconciling archive has no deadline");
    }
    await transaction`UPDATE worker_reservations reservation SET accepting_load=false,released_at=COALESCE(reservation.released_at,epoch.terminal_at)
      FROM worker_job_epochs epoch
      WHERE reservation.consultation_id=${consultationId} AND reservation.generation=${resourceGeneration}
        AND epoch.consultation_id=reservation.consultation_id AND epoch.generation=reservation.generation
        AND epoch.worker_id=reservation.worker_id AND epoch.epoch=reservation.epoch AND epoch.terminal_at IS NOT NULL
        AND EXISTS (SELECT 1 FROM worker_checkpoints checkpoint WHERE checkpoint.consultation_id=reservation.consultation_id
          AND checkpoint.generation=${resourceGeneration} AND checkpoint.worker_id=reservation.worker_id
          AND checkpoint.worker_epoch=reservation.epoch AND checkpoint.terminal=true)`;
    const archiveId = archive.id;
    const [
      expectations,
      objects,
      checkpoints,
      egress,
      drains,
      providerAttempts,
      providerGaps,
      directions,
    ] = await Promise.all([
      transaction<
        ExpectationRow[]
      >`SELECT id,object_class,causal_key,sample_start,sample_end,fulfilled_object_id
        FROM expected_archive_artifacts WHERE archive_id=${archiveId} ORDER BY object_class,causal_key`,
      transaction<
        ArchiveObjectRow[]
      >`SELECT id,object_class,key,version_id,size,sha256,s3_checksum,content_type
        FROM archive_objects WHERE archive_id=${archiveId} ORDER BY key,version_id`,
      transaction<
        CheckpointRow[]
      >`SELECT to_jsonb(checkpoint) AS checkpoint FROM worker_checkpoints checkpoint
        WHERE consultation_id=${consultationId} AND generation=${resourceGeneration} AND terminal=true ORDER BY created_at DESC LIMIT 1`,
      transaction<
        EgressResultRow[]
      >`SELECT id,egress_id,kind,state,output_prefix,terminal_result FROM egress_jobs
        WHERE consultation_id=${consultationId} AND generation=${resourceGeneration} ORDER BY kind,subject_id`,
      transaction<
        DrainResultRow[]
      >`SELECT result FROM external_effects WHERE consultation_id=${consultationId} AND generation=${cleanupGeneration}
        AND effect_kind='ROOM_DELETE' AND state='done' ORDER BY updated_at DESC LIMIT 1`,
      transaction<ReconciliationProviderAttemptRow[]>`SELECT id AS attempt_id,stage
          FROM provider_attempts
          WHERE consultation_id=${consultationId} AND terminal_at IS NOT NULL AND outcome='succeeded'
          ORDER BY stage,id`,
      transaction<ProviderGapRow[]>`SELECT attempt.id AS attempt_id,attempt.stage,attempt.provider,
        attempt.direction_id,attempt.operation_id,attempt.attempt_number,attempt.outcome,attempt.error_kind,
        attempt.accepted_input_watermark,attempt.received_output_watermark,attempt.emitted_output_watermark,
        attempt.retry_decision
        FROM provider_attempts attempt
        WHERE attempt.consultation_id=${consultationId} AND attempt.terminal_at IS NOT NULL
        AND attempt.outcome <> 'succeeded'
        AND NOT (attempt.outcome='cancelled' AND attempt.id=attempt.operation_id
          AND COALESCE(attempt.accepted_input_watermark,0)=0
          AND COALESCE(attempt.received_output_watermark,0)=0
          AND COALESCE(attempt.emitted_output_watermark,0)=0)
        AND NOT EXISTS (SELECT 1 FROM provider_attempts retry WHERE retry.retry_of=attempt.id)
        ORDER BY attempt.stage,attempt.direction_id,attempt.operation_id,attempt.attempt_number`,
      transaction<ReconciliationDirectionRow[]>`SELECT direction->>'mode' AS mode,
          (direction->>'destinationParticipantId')::uuid AS destination_participant_id,
          COALESCE((SELECT checkpoint.emitted_output FROM worker_checkpoints checkpoint
            WHERE checkpoint.consultation_id=${consultationId} AND checkpoint.generation=${resourceGeneration}
              AND checkpoint.destination_participant_id=(direction->>'destinationParticipantId')::uuid
              AND checkpoint.terminal=true
            ORDER BY checkpoint.accepted_input DESC,checkpoint.created_at DESC LIMIT 1),0) AS emitted_output
          FROM room_provider_selections selection
          CROSS JOIN LATERAL jsonb_array_elements(selection.selection->'directions') direction
          WHERE selection.consultation_id=${consultationId}
          ORDER BY destination_participant_id`,
    ]);

    return mapReconciliationSnapshot(
      archiveId,
      archive.reconciliationDeadlineAt,
      expectations,
      objects,
      checkpoints,
      egress,
      providerAttempts,
      drains,
      providerGaps,
      directions,
    );
  });
}

export async function completeReconciliation(
  _db: PostgresJsDatabase,
  client: Sql,
  effect: Effect,
  owner: Uuid,
  now: Date,
  snapshot: ReconciliationSnapshot,
  inventory: Readonly<Record<string, unknown>>,
  sha256: string,
  finalObject: FinalInventoryObject,
  derivedObjects: readonly DerivedArchiveObject[],
): Promise<boolean> {
  return client.begin(async (transaction) => {
    const fenced = await transaction<IdRow[]>`SELECT effect.id
      FROM external_effects effect
      JOIN consultations consultation ON consultation.id=effect.consultation_id
        AND effect.effect_kind='ARCHIVE_RECONCILE'
      JOIN archives archive ON archive.consultation_id=consultation.id
      WHERE effect.id=${effect.id}
        AND effect.consultation_id=${effect.consultationId}
        AND effect.generation=${effect.generation}
        AND effect.state='calling'
        AND date_trunc('milliseconds',archive.reconciliation_deadline_at)=${snapshot.reconciliationDeadlineAt.toISOString()}
        AND effect.lease_owner=${owner}
        AND effect.lease_expires_at > ${now.toISOString()}
        AND consultation.generation=effect.generation
        AND archive.id=${snapshot.archiveId}
        AND archive.state='reconciling'
      FOR UPDATE OF effect,consultation,archive`;
    if (fenced.length !== 1) {
      return false;
    }
    const status = inventory.status === "complete" ? "complete" : "incomplete";
    await insertReconciliationObjects(
      transaction,
      effect.consultationId,
      snapshot.archiveId,
      sha256,
      finalObject,
      derivedObjects,
    );
    const inserted = await transaction<
      { readonly archive_id: string }[]
    >`INSERT INTO final_inventories(archive_id,status,inventory,sha256,object_id,room_close,worker_terminal,egress_results,missing,errors,created_at)
      VALUES (${snapshot.archiveId},${status},${JSON.stringify(inventory)}::jsonb,${sha256},${finalObject.id},
        ${JSON.stringify(snapshot.roomClose)}::jsonb,${JSON.stringify(snapshot.workerTerminal)}::jsonb,${JSON.stringify(snapshot.egressResults)}::jsonb,
        ${JSON.stringify(inventory.missing ?? [])}::jsonb,${JSON.stringify(inventory.errors ?? [])}::jsonb,now())
      ON CONFLICT (archive_id) DO NOTHING RETURNING archive_id`;
    if (inserted.length !== 1) {
      const existing = await transaction<
        { readonly sha256: string }[]
      >`SELECT sha256 FROM final_inventories WHERE archive_id=${snapshot.archiveId}`;
      if (existing[0]?.sha256 !== sha256) {
        return false;
      }
    }
    const completed = await transaction<IdRow[]>`WITH archive_done AS (
        UPDATE archives SET state=${status},final_inventory_hash=${sha256},updated_at=now()
        WHERE id=${snapshot.archiveId} AND consultation_id=${effect.consultationId}
          AND state='reconciling'
        RETURNING id
      )
      UPDATE external_effects SET state='done',updated_at=now(),
        lease_owner=NULL,lease_expires_at=NULL
      WHERE id=${effect.id} AND lease_owner=${owner} AND state='calling'
        AND EXISTS (SELECT 1 FROM archive_done)
      RETURNING id`;
    if (completed.length !== 1) {
      return false;
    }
    await transaction`UPDATE orchestration_deadlines
      SET completed_at=COALESCE(completed_at,now()),lease_owner=NULL,lease_expires_at=NULL
      WHERE consultation_id=${effect.consultationId} AND generation=${effect.generation}
        AND kind='archive-reconcile'`;
    return true;
  });
}

export async function finishArchiveDeletionIfEmpty(
  db: PostgresJsDatabase,
  _client: Sql,
  consultationId: Uuid,
  generation: number,
  writeEpoch: number,
): Promise<boolean> {
  const rows =
    await db.execute<IdRow>(sql`UPDATE archives a SET state='deleted',updated_at=now() FROM consultations c
    WHERE a.consultation_id=${consultationId} AND a.state='deleting' AND a.write_epoch=${writeEpoch}
      AND c.id=a.consultation_id AND c.generation=${generation} AND c.state='ended'
    RETURNING a.consultation_id AS id`);
  return rows.length === 1;
}

async function insertReconciliationObjects(
  transaction: TransactionSql,
  consultationId: Uuid,
  archiveId: Uuid,
  sha256: string,
  finalObject: FinalInventoryObject,
  derivedObjects: readonly DerivedArchiveObject[],
): Promise<void> {
  const finalKey = `v1/meetings/${consultationId}/inventory/final.json`;
  await transaction`INSERT INTO archive_objects(id,archive_id,object_class,causal_key,key,version_id,size,sha256,s3_checksum,content_type,writer_epoch,created_at)
        SELECT ${finalObject.id}::uuid,id,'inventory','inventory:final',${finalKey}::text,${finalObject.versionId}::text,${finalObject.size}::bigint,${sha256}::text,${finalObject.checksum}::text,'application/json',write_epoch,now()
        FROM archives WHERE id=${archiveId} ON CONFLICT (key,version_id) DO NOTHING`;
  for (const object of derivedObjects) {
    await transaction`INSERT INTO archive_objects(id,archive_id,object_class,causal_key,key,version_id,size,sha256,s3_checksum,content_type,writer_epoch,created_at)
          SELECT ${object.id}::uuid,id,${object.objectClass}::text,${object.key}::text,${object.key}::text,${object.versionId}::text,${object.size}::bigint,${object.sha256}::text,${object.checksum}::text,${object.contentType}::text,write_epoch,now()
          FROM archives WHERE id=${archiveId} ON CONFLICT (key,version_id) DO NOTHING`;
  }
  await transaction`WITH matches AS (
      SELECT expected.id AS expectation_id,(
        SELECT object.id
        FROM archive_objects object
        WHERE object.archive_id=expected.archive_id
          AND object.object_class=expected.object_class
          AND object.key LIKE expected.causal_key || '/%'
        ORDER BY object.key,object.version_id,object.id
        LIMIT 1
      ) AS object_id
      FROM expected_archive_artifacts expected
      WHERE expected.archive_id=${archiveId}
        AND expected.fulfilled_object_id IS NULL
        AND expected.object_class IN ('room_composite','participant_original')
    )
    UPDATE expected_archive_artifacts expected
    SET fulfilled_object_id=matches.object_id,disposition='fulfilled'
    FROM matches
    WHERE expected.id=matches.expectation_id AND matches.object_id IS NOT NULL`;
}

function claimPendingArchiveDeletesStatement(): SQL {
  return sql`WITH fenced AS (
      UPDATE worker_reservations reservation SET fenced_at=COALESCE(fenced_at,now()),fence_reason=COALESCE(fence_reason,'archive deletion admitted')
      FROM archives archive WHERE archive.consultation_id=reservation.consultation_id AND archive.state='deleting' AND reservation.fenced_at IS NULL
      RETURNING reservation.consultation_id
    ), epochs_fenced AS (
      UPDATE worker_job_epochs epoch SET fenced_at=COALESCE(epoch.fenced_at,now())
      FROM archives archive WHERE archive.consultation_id=epoch.consultation_id AND archive.state='deleting' AND epoch.fenced_at IS NULL
      RETURNING epoch.consultation_id
    ), writers_fenced AS (
      UPDATE external_effects effect SET state='failed',lease_owner=NULL,lease_expires_at=NULL,
        result=COALESCE(effect.result,'{}'::jsonb)||jsonb_build_object('error','archive deletion fenced writer'),updated_at=now()
      FROM archives archive WHERE archive.consultation_id=effect.consultation_id AND archive.state='deleting'
        AND effect.effect_kind='ARCHIVE_RECONCILE' AND effect.state IN ('planned','calling')
      RETURNING effect.consultation_id
    ) INSERT INTO external_effects(id,consultation_id,generation,effect_kind,subject_id,occurrence_key,state,result,attempts,created_at,updated_at)
      SELECT gen_random_uuid(),c.id,c.generation,'ARCHIVE_DELETE',a.id,
        'archive-write-epoch:' || a.write_epoch::text,'planned',
        jsonb_build_object('plan',jsonb_build_object('archiveId',a.id,'writeEpoch',a.write_epoch,'reason','retention_delete')),0,now(),now()
      FROM archives a JOIN consultations c ON c.id=a.consultation_id
      WHERE a.state='deleting' AND c.state='ended'
        AND NOT EXISTS (SELECT 1 FROM egress_jobs job WHERE job.consultation_id=c.id AND job.terminal_at IS NULL)
      ON CONFLICT (consultation_id,generation,effect_kind,subject_id,occurrence_key) DO NOTHING`;
}

function mapReconciliationSnapshot(
  archiveId: string,
  reconciliationDeadlineAt: Date,
  expectations: readonly ExpectationRow[],
  objects: readonly ArchiveObjectRow[],
  checkpoints: readonly CheckpointRow[],
  egress: readonly EgressResultRow[],
  providerAttempts: readonly ReconciliationProviderAttemptRow[],
  drains: readonly DrainResultRow[],
  providerGaps: readonly ProviderGapRow[],
  directions: readonly ReconciliationDirectionRow[],
): ReconciliationSnapshot {
  return {
    archiveId,
    reconciliationDeadlineAt,
    state: "reconciling" as const,
    roomClose:
      drains[0] === undefined ? { terminal: false } : { terminal: true, result: drains[0].result },
    workerTerminal:
      checkpoints[0] === undefined
        ? { terminal: false, gaps: [{ reason: "worker_terminal_missing" }] }
        : { terminal: true, checkpoint: checkpoints[0].checkpoint },
    providerAttempts: providerAttempts.map((attempt) => ({
      attemptId: attempt.attempt_id,
      stage: attempt.stage,
    })),
    egressResults: egress.map(mapReconciliationEgress),
    providerGaps: providerGaps.map((gap) => ({
      attemptId: gap.attempt_id,
      stage: gap.stage,
      provider: gap.provider,
      directionId: gap.direction_id,
      operationId: gap.operation_id,
      attemptNumber: gap.attempt_number,
      outcome: gap.outcome,
      errorKind: gap.error_kind,
      acceptedInputWatermark: gap.accepted_input_watermark,
      receivedOutputWatermark: gap.received_output_watermark,
      emittedOutputWatermark: gap.emitted_output_watermark,
      retryDecision: gap.retry_decision,
    })),
    directions: directions.map((direction) => ({
      mode: direction.mode === "translated" ? "translated" : "same_language",
      destinationParticipantId: direction.destination_participant_id,
      emittedOutput: Number(direction.emitted_output),
    })),
    expectations: expectations.map(mapReconciliationExpectation),
    objects: objects.map(mapArchiveObject),
  };
}

function mapReconciliationEgress(row: EgressResultRow): ReconciliationEgressResult {
  return {
    egressId: row.egress_id,
    state: row.state,
    kind: row.kind,
    outputPrefix: row.output_prefix,
    terminal: row.terminal_result !== null,
    result: row.terminal_result,
  };
}

function mapReconciliationExpectation(row: ExpectationRow): ReconciliationExpectation {
  return {
    id: String(row.id),
    objectClass: String(row.object_class),
    causalKey: String(row.causal_key),
    sampleStart: row.sample_start === null ? null : Number(row.sample_start),
    sampleEnd: row.sample_end === null ? null : Number(row.sample_end),
    fulfilledObjectId: nullableString(row.fulfilled_object_id),
  };
}

function mapArchiveObject(row: ArchiveObjectRow): ArchivedObject {
  return {
    id: String(row.id),
    objectClass: String(row.object_class),
    key: String(row.key),
    versionId: String(row.version_id),
    size: Number(row.size),
    sha256: String(row.sha256),
    s3Checksum: String(row.s3_checksum),
    contentType: String(row.content_type),
  };
}
