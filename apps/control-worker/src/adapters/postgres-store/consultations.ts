import { createHash, randomUUID } from "node:crypto";
import {
  PARTICIPANT_ROLE_VALUES,
  type RoomProviderSelection,
  RoomProviderSelectionSchema,
} from "@transhooter/contracts";
import {
  consultationParticipants,
  consultations,
  workerReservations,
} from "@transhooter/server-core/persistence";
import type { SQL } from "drizzle-orm";
import { and, eq, inArray, sql } from "drizzle-orm";
import type { PostgresJsDatabase } from "drizzle-orm/postgres-js";
import type { Sql, TransactionSql } from "postgres";
import type {
  ClaimOptions,
  ConsultationState,
  Deadline,
  PlannedEffect,
  Uuid,
  VerifiedWebhook,
  WorkerReservation,
} from "../../orchestration/model";
import { insertPlannedEffects } from "./effects";
import {
  type CancellationConsultationRow,
  type CapacityDimension,
  type ConsultationIdRow,
  type DeadlineRow,
  type DispatchIdRow,
  type EgressIdRow,
  type IdRow,
  type LiveKitIdentityRow,
  mapDeadline,
  mapReservation,
  type ParticipantIdentityRow,
  perRoomQuotaUnits,
  type ReservationRow,
  type ReserveConsultationRow,
  type RoomResourceRow,
  type WorkerDirectionRow,
  type WorkerDispatchRow,
  type WorkerEpochTerminalRow,
} from "./shared";

export async function currentGeneration(
  db: PostgresJsDatabase,
  _client: Sql,
  consultationId: Uuid,
): Promise<number | null> {
  const rows = await db
    .select({ generation: consultations.generation })
    .from(consultations)
    .where(eq(consultations.id, consultationId))
    .limit(1);
  return rows[0]?.generation ?? null;
}

export async function claimDeadlines(
  db: PostgresJsDatabase,
  _client: Sql,
  options: ClaimOptions,
): Promise<readonly Deadline[]> {
  const rows = await db.execute<DeadlineRow>(sql`WITH picked AS (
    SELECT deadline.consultation_id,deadline.generation,deadline.kind
    FROM orchestration_deadlines deadline
    WHERE deadline.completed_at IS NULL
      AND (
        deadline.due_at <= ${options.now.toISOString()}
        OR EXISTS (
          SELECT 1 FROM consultations consultation
          WHERE consultation.id=deadline.consultation_id
            AND (
              consultation.generation <> deadline.generation
              OR (
                deadline.kind <> 'archive-reconcile'
                AND consultation.state IN ('ended','cancelled','deleted')
              )
            )
        )
      )
      AND (
        deadline.lease_expires_at IS NULL
        OR deadline.lease_expires_at < ${options.now.toISOString()}
      )
    ORDER BY deadline.due_at
    FOR UPDATE OF deadline SKIP LOCKED
    LIMIT ${options.limit}
  ) UPDATE orchestration_deadlines d
    SET lease_owner=${options.owner},
      lease_expires_at=${new Date(options.now.getTime() + options.leaseMs).toISOString()}
    FROM picked
    WHERE d.consultation_id=picked.consultation_id
      AND d.generation=picked.generation
      AND d.kind=picked.kind
    RETURNING d.*`);
  return rows.map(mapDeadline);
}

export async function completeDeadline(
  db: PostgresJsDatabase,
  _client: Sql,
  deadline: Deadline,
  owner: Uuid,
): Promise<void> {
  await db.execute(sql`UPDATE orchestration_deadlines SET completed_at=now(),lease_owner=NULL,lease_expires_at=NULL
    WHERE consultation_id=${deadline.consultationId} AND generation=${deadline.generation} AND kind=${deadline.kind} AND lease_owner=${owner}`);
}

export async function claimStaleReservations(
  db: PostgresJsDatabase,
  _client: Sql,
  options: ClaimOptions,
): Promise<readonly WorkerReservation[]> {
  const rows = await db.execute<ReservationRow>(sql`WITH picked AS (
    SELECT reservation.consultation_id,reservation.generation FROM worker_reservations reservation
    WHERE reservation.fenced_at IS NULL AND reservation.released_at IS NULL
      AND reservation.lease_expires_at < ${options.now.toISOString()}
      AND (reservation.supervisor_owner IS NULL OR reservation.supervisor_owner=${options.owner}
        OR reservation.supervisor_lease_expires_at < ${options.now.toISOString()})
      AND NOT EXISTS (
        SELECT 1 FROM worker_job_epochs epoch
        WHERE epoch.consultation_id=reservation.consultation_id AND epoch.generation=reservation.generation
          AND epoch.worker_id=reservation.worker_id AND epoch.epoch=reservation.epoch
          AND epoch.terminal_at IS NOT NULL
      )
    ORDER BY reservation.lease_expires_at FOR UPDATE SKIP LOCKED LIMIT ${options.limit}
  ) UPDATE worker_reservations r SET supervisor_owner=${options.owner},supervisor_lease_expires_at=${new Date(options.now.getTime() + options.leaseMs).toISOString()}
    FROM picked WHERE r.consultation_id=picked.consultation_id AND r.generation=picked.generation RETURNING r.*`);
  return rows.map(mapReservation);
}

export async function heartbeat(
  db: PostgresJsDatabase,
  _client: Sql,
  workerId: Uuid,
  epoch: number,
  now: Date,
  leaseExpiresAt: Date,
): Promise<boolean> {
  const rows =
    await db.execute<IdRow>(sql`UPDATE worker_reservations SET heartbeat_at=${now.toISOString()},lease_expires_at=${leaseExpiresAt.toISOString()}
    WHERE worker_id=${workerId} AND epoch=${epoch} AND fenced_at IS NULL RETURNING worker_id AS id`);
  return rows.length === 1;
}

export async function reserveWorker(
  _db: PostgresJsDatabase,
  client: Sql,
  consultationId: Uuid,
  generation: number,
): Promise<WorkerReservation> {
  return client.begin(async (transaction) => {
    const [consultationRow] = await transaction<
      ReserveConsultationRow[]
    >`SELECT worker_identity,snapshot_hash
      FROM consultations
      WHERE id=${consultationId} AND generation=${generation} AND state='ready'
      FOR UPDATE`;
    const consultation =
      consultationRow === undefined
        ? undefined
        : {
            workerIdentity: consultationRow.worker_identity,
            snapshotHash: consultationRow.snapshot_hash,
          };
    if (
      consultation === undefined ||
      typeof consultation.workerIdentity !== "string" ||
      typeof consultation.snapshotHash !== "string"
    ) {
      throw new Error("ready consultation has no worker reservation identity");
    }
    const workerId = consultation.workerIdentity;
    const epoch = generation;
    await transaction`INSERT INTO worker_leases(worker_id,accepting_load,capacity,reserved,encrypted_spool_percent,providers_ok,archive_ok,heartbeat_at,expires_at,epoch,status)
      VALUES (${workerId},true,1,1,0,true,true,now(),now()+interval '20 minutes',${epoch},'{}'::jsonb)
      ON CONFLICT(worker_id) DO UPDATE SET accepting_load=true,reserved=1,encrypted_spool_percent=0,
        providers_ok=true,archive_ok=true,heartbeat_at=now(),expires_at=now()+interval '20 minutes',epoch=${epoch},status='{}'::jsonb`;
    const rows = await transaction<ReservationRow[]>`INSERT INTO worker_reservations(
        consultation_id,generation,worker_id,epoch,selection_hash,reserved_at,heartbeat_at,lease_expires_at,accepting_load
      ) VALUES (${consultationId},${generation},${workerId},${epoch},${consultation.snapshotHash},now(),now(),now()+interval '5 minutes',true)
      ON CONFLICT(consultation_id,generation) DO UPDATE SET accepting_load=true
      RETURNING *`;
    await transaction`INSERT INTO worker_job_epochs(
        consultation_id,generation,worker_id,epoch,write_epoch,heartbeat_at
      ) VALUES (${consultationId},${generation},${workerId},${epoch},0,now())
      ON CONFLICT(consultation_id,generation,epoch) DO NOTHING`;
    const reservation = rows[0];
    if (reservation === undefined) {
      throw new Error("worker reservation was not persisted");
    }
    return mapReservation(reservation);
  });
}

export async function applyVerifiedWebhook(
  db: PostgresJsDatabase,
  _client: Sql,
  event: VerifiedWebhook,
): Promise<boolean> {
  // Inbox verification and all watermark/state mutations share this transaction.
  // A rollback therefore cannot leave an accepted webhook without its state change.
  return db.transaction(async (transaction) => {
    const accepted = await transaction.execute<{ readonly event_id: string }>(sql`
      SELECT event_id
      FROM inbox
      WHERE source = 'livekit'
        AND event_id = ${event.eventId}
        AND payload_hash = ${event.rawSha256}
      FOR SHARE
    `);
    if (accepted.length !== 1) {
      return false;
    }

    if (event.egressId !== null) {
      const egressRows = await transaction.execute<IdRow>(verifiedEgressUpdate(event));
      if (egressRows.length !== 1) {
        return false;
      }
    }

    if (
      event.participantId === null ||
      (event.kind !== "PARTICIPANT_JOINED" && event.kind !== "PARTICIPANT_LEFT")
    ) {
      return true;
    }

    const watermark = participantWatermark(event);
    const rows = await transaction.execute<IdRow>(participantPresenceUpdate(event, watermark));
    if (rows.length === 1) {
      await transaction.execute(participantPresenceTransition(event));
    }
    return rows.length === 1;
  });
}

export async function presenceEpoch(
  db: PostgresJsDatabase,
  _client: Sql,
  consultationId: Uuid,
  generation: number,
): Promise<number | null> {
  const rows = await db
    .select({ presenceEpoch: consultations.presenceEpoch })
    .from(consultations)
    .where(and(eq(consultations.id, consultationId), eq(consultations.generation, generation)))
    .limit(1);
  return rows[0]?.presenceEpoch ?? null;
}

export async function admitFinalization(
  _db: PostgresJsDatabase,
  client: Sql,
  consultationId: Uuid,
  generation: number,
  presenceEpoch: number,
  now: Date,
): Promise<"admitted" | ConsultationState | null> {
  return client.begin(async (transaction) => {
    const rows = await transaction<
      { readonly state: ConsultationState }[]
    >`UPDATE consultations SET state='finalizing',finalize_deadline_at=COALESCE(finalize_deadline_at,${new Date(now.getTime() + 15 * 60_000).toISOString()}),updated_at=${now.toISOString()}
      WHERE id=${consultationId} AND generation=${generation} AND presence_epoch=${presenceEpoch}
        AND state IN ('ready','active') RETURNING state`;
    if (rows.length === 1) {
      await transaction`UPDATE archives SET state='reconciling',reconciliation_deadline_at=COALESCE(reconciliation_deadline_at,${new Date(now.getTime() + 30 * 60_000).toISOString()}),updated_at=${now.toISOString()}
        WHERE consultation_id=${consultationId} AND state IN ('pending','recording')`;
      return "admitted" as const;
    }
    const current = await transaction<
      { readonly state: ConsultationState }[]
    >`SELECT state FROM consultations WHERE id=${consultationId} AND generation=${generation}`;
    return current[0]?.state ?? null;
  });
}

export async function isStandardHuman(
  db: PostgresJsDatabase,
  _client: Sql,
  consultationId: Uuid,
  participantId: Uuid,
): Promise<boolean> {
  const rows = await db
    .select({ id: consultationParticipants.id })
    .from(consultationParticipants)
    .where(
      and(
        eq(consultationParticipants.consultationId, consultationId),
        eq(consultationParticipants.livekitIdentity, participantId),
        inArray(consultationParticipants.role, PARTICIPANT_ROLE_VALUES),
      ),
    )
    .limit(1);
  return rows.length === 1;
}

export async function markCaptureReady(
  _db: PostgresJsDatabase,
  client: Sql,
  consultationId: Uuid,
  generation: number,
  participantIdentity: Uuid,
  participantEgressId: string,
): Promise<"active" | null> {
  return client.begin(async (transaction) => {
    const participants = await transaction<IdRow[]>`UPDATE consultation_participants participant
      SET participant_egress_id=${participantEgressId},publication_granted=true
      FROM consultations consultation
      WHERE participant.consultation_id=consultation.id
        AND consultation.id=${consultationId}
        AND consultation.generation=${generation}
        AND consultation.state IN ('ready','active')
        AND consultation.admission_fenced_at IS NULL
        AND participant.livekit_identity=${participantIdentity}
        AND participant.role IN ('employee','customer')
      RETURNING participant.id`;
    if (participants.length !== 1) {
      return null;
    }
    const consultations = await transaction<
      { readonly state: "active" }[]
    >`UPDATE consultations SET state='active',updated_at=now()
      WHERE id=${consultationId}
        AND generation=${generation}
        AND state IN ('ready','active')
        AND admission_fenced_at IS NULL
      RETURNING state`;
    return consultations.length === 1 ? "active" : null;
  });
}

export async function consultationState(
  db: PostgresJsDatabase,
  _client: Sql,
  consultationId: Uuid,
): Promise<ConsultationState | null> {
  const rows = await db
    .select({ state: consultations.state })
    .from(consultations)
    .where(eq(consultations.id, consultationId))
    .limit(1);
  return rows[0]?.state ?? null;
}

export async function workerReservation(
  db: PostgresJsDatabase,
  _client: Sql,
  consultationId: Uuid,
  generation: number,
): Promise<WorkerReservation | null> {
  const rows = await db
    .select()
    .from(workerReservations)
    .where(
      and(
        eq(workerReservations.consultationId, consultationId),
        eq(workerReservations.generation, generation),
      ),
    )
    .limit(1);
  const row = rows[0];
  return row === undefined
    ? null
    : {
        consultationId: row.consultationId,
        generation: row.generation,
        workerId: row.workerId,
        epoch: row.epoch,
        heartbeatAt: row.heartbeatAt,
        leaseExpiresAt: row.leaseExpiresAt,
        acceptingLoad: row.acceptingLoad,
      };
}

export async function workerDispatchMetadata(
  _db: PostgresJsDatabase,
  client: Sql,
  consultationId: Uuid,
  generation: number,
): Promise<unknown> {
  return client.begin(async (transaction) => {
    const rows = await transaction<
      WorkerDispatchRow[]
    >`SELECT c.room_name,c.worker_identity,c.snapshot_hash,c.provider_selection,
        reservation.epoch,archive.write_epoch
      FROM consultations c
      JOIN worker_reservations reservation ON reservation.consultation_id=c.id AND reservation.generation=c.generation
      JOIN archives archive ON archive.consultation_id=c.id
      WHERE c.id=${consultationId} AND c.generation=${generation}
        AND c.state IN ('ready','active') AND reservation.fenced_at IS NULL
      FOR SHARE OF c,reservation,archive`;
    const row = rows[0];
    if (row === undefined) {
      throw new Error("worker dispatch metadata is not admitted");
    }
    const participants = await transaction<
      ParticipantIdentityRow[]
    >`SELECT id,livekit_identity FROM consultation_participants
      WHERE consultation_id=${consultationId} ORDER BY CASE role WHEN 'employee' THEN 0 ELSE 1 END FOR SHARE`;
    return mapWorkerDispatchMetadata(row, participants, consultationId, generation);
  });
}

export async function planFailureEffects(
  _db: PostgresJsDatabase,
  client: Sql,
  consultationId: Uuid,
  generation: number,
  reason: string,
  effects: readonly PlannedEffect[],
): Promise<void> {
  await client.begin(async (transaction) => {
    await transaction`INSERT INTO audit_events(id,aggregate_id,actor_id,kind,occurred_at,details)
      VALUES (gen_random_uuid(),${consultationId},NULL,'egress.supervisor_terminal',now(),${JSON.stringify({ generation, reason })}::jsonb)`;
    await insertPlannedEffects(transaction, effects);
  });
}

export async function fenceWorkerAndPlanFailure(
  _db: PostgresJsDatabase,
  client: Sql,
  reservation: WorkerReservation,
  owner: Uuid,
  reason: string,
  effects: readonly PlannedEffect[],
): Promise<boolean> {
  return client.begin(async (transaction) => {
    const fenced = await transaction<
      { epoch: number }[]
    >`UPDATE worker_reservations SET fenced_at=now(),fence_reason=${reason},
      supervisor_owner=COALESCE(supervisor_owner,${owner})
      WHERE consultation_id=${reservation.consultationId} AND generation=${reservation.generation} AND epoch=${reservation.epoch}
        AND (supervisor_owner=${owner} OR supervisor_owner IS NULL) AND fenced_at IS NULL RETURNING epoch`;
    if (fenced.length !== 1) {
      return false;
    }
    const terminalCheckpointId = await persistSupervisorTerminalCheckpoints(
      transaction,
      reservation,
      reason,
    );
    const terminalized = await transaction<
      { readonly terminal_checkpoint_id: string }[]
    >`UPDATE worker_job_epochs SET fenced_at=COALESCE(fenced_at,now()),
        terminal_checkpoint_id=${terminalCheckpointId},terminal_outcome='failed',terminal_at=now()
      WHERE consultation_id=${reservation.consultationId}
        AND generation=${reservation.generation}
        AND worker_id=${reservation.workerId} AND epoch=${reservation.epoch}
        AND terminal_at IS NULL
      RETURNING terminal_checkpoint_id`;
    if (terminalized.length !== 1) {
      throw new Error("worker epoch was not terminalized after supervisor checkpoints");
    }
    await transaction`UPDATE worker_reservations
      SET accepting_load=false,released_at=COALESCE(released_at,now())
      WHERE consultation_id=${reservation.consultationId}
        AND generation=${reservation.generation}
        AND worker_id=${reservation.workerId} AND epoch=${reservation.epoch}`;
    await transaction`INSERT INTO audit_events(id,aggregate_id,actor_id,kind,occurred_at,details)
      VALUES (gen_random_uuid(),${reservation.consultationId},NULL,'worker.supervisor_terminal',now(),
        jsonb_build_object('generation',${reservation.generation}::integer,
          'fencedEpoch',${reservation.epoch}::integer,'owner',${owner}::uuid,
          'reason',${reason}::text,'terminalCheckpointId',${terminalCheckpointId}::uuid))`;
    await insertPlannedEffects(transaction, effects);
    return true;
  });
}

export async function fenceWorkerAndScheduleCancellation(
  _db: PostgresJsDatabase,
  client: Sql,
  consultationId: Uuid,
  cleanupGeneration: number,
  resourceGeneration: number,
  owner: Uuid,
  reason: string,
  effects: readonly PlannedEffect[],
): Promise<void> {
  await client.begin(async (transaction) => {
    const [consultationRow] = await transaction<
      CancellationConsultationRow[]
    >`SELECT generation,state
      FROM consultations
      WHERE id=${consultationId}
      FOR UPDATE`;
    const consultation =
      consultationRow === undefined
        ? undefined
        : { generation: Number(consultationRow.generation), state: consultationRow.state };
    if (
      consultation === undefined ||
      consultation.state !== "cancelled" ||
      consultation.generation !== cleanupGeneration ||
      cleanupGeneration <= resourceGeneration
    ) {
      throw new Error("cancellation cleanup generation is not current");
    }
    const [reservationRow] = await transaction<ReservationRow[]>`SELECT *
      FROM worker_reservations
      WHERE consultation_id=${consultationId} AND generation=${resourceGeneration}
      FOR UPDATE`;
    if (reservationRow !== undefined) {
      const reservation = mapReservation(reservationRow);
      const [epoch] = await transaction<WorkerEpochTerminalRow[]>`SELECT terminal_at
        FROM worker_job_epochs
        WHERE consultation_id=${consultationId}
          AND generation=${resourceGeneration}
          AND worker_id=${reservation.workerId}
          AND epoch=${reservation.epoch}
        FOR UPDATE`;
      if (epoch?.terminal_at === null) {
        const terminalCheckpointId = await persistSupervisorTerminalCheckpoints(
          transaction,
          reservation,
          reason,
        );
        const terminalized = await transaction<
          { readonly terminal_checkpoint_id: string }[]
        >`UPDATE worker_job_epochs SET fenced_at=COALESCE(fenced_at,now()),
            terminal_checkpoint_id=${terminalCheckpointId},terminal_outcome='failed',terminal_at=now()
          WHERE consultation_id=${consultationId} AND generation=${resourceGeneration}
            AND worker_id=${reservation.workerId} AND epoch=${reservation.epoch}
            AND terminal_at IS NULL
          RETURNING terminal_checkpoint_id`;
        if (terminalized.length !== 1) {
          throw new Error("cancellation worker epoch was not terminalized after checkpoints");
        }
        await transaction`INSERT INTO audit_events(id,aggregate_id,actor_id,kind,occurred_at,details)
          VALUES (gen_random_uuid(),${consultationId},NULL,'worker.supervisor_terminal',now(),
            jsonb_build_object('generation',${resourceGeneration}::integer,
              'fencedEpoch',${reservation.epoch}::integer,'owner',${owner}::uuid,
              'reason',${reason}::text,'terminalCheckpointId',${terminalCheckpointId}::uuid))`;
      }
      await transaction`UPDATE worker_reservations SET fenced_at=COALESCE(fenced_at,now()),
        fence_reason=COALESCE(fence_reason,${reason}),accepting_load=false,
        released_at=COALESCE(released_at,now()),supervisor_owner=COALESCE(supervisor_owner,${owner})
        WHERE consultation_id=${consultationId} AND generation=${resourceGeneration}
          AND worker_id=${reservation.workerId} AND epoch=${reservation.epoch}`;
    }
    await transaction`UPDATE orchestration_deadlines
      SET completed_at=COALESCE(completed_at,now()),lease_owner=NULL,lease_expires_at=NULL
      WHERE consultation_id=${consultationId} AND generation < ${cleanupGeneration}
        AND completed_at IS NULL`;
    await insertPlannedEffects(transaction, effects);
  });
}

export async function humanIdentities(
  db: PostgresJsDatabase,
  _client: Sql,
  consultationId: Uuid,
): Promise<readonly [Uuid, Uuid]> {
  const rows = await db
    .select({ livekitIdentity: consultationParticipants.livekitIdentity })
    .from(consultationParticipants)
    .where(eq(consultationParticipants.consultationId, consultationId))
    .orderBy(consultationParticipants.role);
  const [first, second] = rows;
  if (first === undefined || second === undefined || rows.length !== 2) {
    throw new Error("consultation must have exactly two human identities");
  }
  return [first.livekitIdentity, second.livekitIdentity];
}

export async function seedDeadlines(
  db: PostgresJsDatabase,
  _client: Sql,
  consultationId: Uuid,
  generation: number,
): Promise<void> {
  await db.execute(sql`INSERT INTO orchestration_deadlines(consultation_id,generation,kind,due_at)
    SELECT id,generation,'ready',ready_deadline_at FROM consultations
      WHERE id=${consultationId} AND generation=${generation}
        AND state IN ('ready','active') AND ready_deadline_at IS NOT NULL
    UNION ALL SELECT id,generation,'finalize',finalize_deadline_at FROM consultations
      WHERE id=${consultationId} AND generation=${generation}
        AND state='finalizing' AND finalize_deadline_at IS NOT NULL
    UNION ALL SELECT c.id,c.generation,'archive-reconcile',a.reconciliation_deadline_at FROM consultations c
      JOIN archives a ON a.consultation_id=c.id WHERE c.id=${consultationId} AND c.generation=${generation} AND a.reconciliation_deadline_at IS NOT NULL
    ON CONFLICT (consultation_id,generation,kind) DO UPDATE SET due_at=EXCLUDED.due_at`);
}

export async function roomDrainPlan(
  db: PostgresJsDatabase,
  _client: Sql,
  consultationId: Uuid,
  generation: number,
): Promise<{
  readonly egressIds: readonly string[];
  readonly participantIds: readonly Uuid[];
  readonly dispatchIds: readonly string[];
  readonly roomCreated: boolean;
  readonly resourceRoomName: string | null;
}> {
  const [egress, participants, dispatches, rooms] = await Promise.all([
    db.execute<EgressIdRow>(
      sql`SELECT egress_id FROM egress_jobs WHERE consultation_id=${consultationId} AND generation=${generation} AND terminal_at IS NULL ORDER BY egress_id`,
    ),
    db.execute<LiveKitIdentityRow>(
      sql`SELECT livekit_identity FROM consultation_participants WHERE consultation_id=${consultationId} ORDER BY role`,
    ),
    db.execute<DispatchIdRow>(sql`SELECT result->>'remoteId' AS dispatch_id FROM external_effects
      WHERE consultation_id=${consultationId} AND generation=${generation} AND effect_kind='WORKER_DISPATCH'
        AND result->>'remoteId' IS NOT NULL ORDER BY created_at`),
    db.execute<RoomResourceRow>(sql`SELECT result->'plan'->>'roomName' AS resource_room_name FROM external_effects
      WHERE consultation_id=${consultationId} AND generation=${generation} AND effect_kind='ROOM_CREATE'
        AND state IN ('applied','done') AND result->'plan'->>'roomName' IS NOT NULL LIMIT 1`),
  ]);
  return {
    egressIds: egress.map((row) => String(row.egress_id)),
    participantIds: participants.map((row) => String(row.livekit_identity)),
    dispatchIds: dispatches.map((row) => String(row.dispatch_id)),
    roomCreated: rooms.length === 1,
    resourceRoomName: rooms[0]?.resource_room_name ?? null,
  };
}

export async function completeRoomDrain(
  _db: PostgresJsDatabase,
  client: Sql,
  consultationId: Uuid,
  generation: number,
): Promise<void> {
  await client.begin(async (transaction) => {
    await transaction`UPDATE consultations SET state='ended',updated_at=now() WHERE id=${consultationId} AND generation=${generation} AND state='finalizing'`;
    await transaction`UPDATE orchestration_deadlines SET completed_at=COALESCE(completed_at,now()),
      lease_owner=NULL,lease_expires_at=NULL
      WHERE consultation_id=${consultationId} AND generation=${generation}
        AND kind <> 'archive-reconcile' AND completed_at IS NULL`;
    await transaction`UPDATE worker_reservations reservation SET accepting_load=false,released_at=COALESCE(reservation.released_at,epoch.terminal_at)
      FROM worker_job_epochs epoch
      WHERE reservation.consultation_id=${consultationId} AND reservation.generation=${generation}
        AND epoch.consultation_id=reservation.consultation_id AND epoch.generation=reservation.generation
        AND epoch.worker_id=reservation.worker_id AND epoch.epoch=reservation.epoch AND epoch.terminal_at IS NOT NULL`;
    await transaction`UPDATE archives SET state='reconciling',reconciliation_deadline_at=COALESCE(reconciliation_deadline_at,now()+interval '30 minutes'),updated_at=now()
      WHERE consultation_id=${consultationId} AND state IN ('pending','recording')`;
    await transaction`INSERT INTO orchestration_deadlines(consultation_id,generation,kind,due_at)
      SELECT ${consultationId},${generation},'archive-reconcile',reconciliation_deadline_at FROM archives WHERE consultation_id=${consultationId}
      ON CONFLICT (consultation_id,generation,kind) DO UPDATE SET due_at=EXCLUDED.due_at`;
  });
}

export async function capacityDimensions(
  db: PostgresJsDatabase,
  _client: Sql,
  consultationId: Uuid,
): Promise<readonly CapacityDimension[]> {
  const rows = await db
    .select({ providerSelection: consultations.providerSelection })
    .from(consultations)
    .where(eq(consultations.id, consultationId))
    .limit(1);
  const selection = rows[0]?.providerSelection;
  if (selection === undefined || selection === null) {
    throw new Error("consultation provider selection is missing");
  }
  return capacityDimensionsForSelection(RoomProviderSelectionSchema.parse(selection));
}

export async function persistSupervisorTerminalCheckpoints(
  transaction: TransactionSql,
  reservation: WorkerReservation,
  reason: string,
): Promise<Uuid> {
  const directions = await transaction<
    WorkerDirectionRow[]
  >`SELECT direction->>'sourceParticipantId' AS source_participant_id,
      direction->>'destinationParticipantId' AS destination_participant_id
    FROM room_provider_selections selection
    CROSS JOIN LATERAL jsonb_array_elements(selection.selection->'directions') direction
    WHERE selection.consultation_id=${reservation.consultationId}`;
  if (directions.length !== 2) {
    throw new Error("worker supervisor settlement requires two frozen directions");
  }

  await transaction<ConsultationIdRow[]>`select consultation_id
    from "worker_job_epochs"
    where consultation_id=${reservation.consultationId}
      and generation=${reservation.generation}
      and worker_id=${reservation.workerId}
      and epoch=${reservation.epoch}
    for update`;
  for (const direction of directions) {
    const { id, hash, objectKey } = supervisorTerminalIdentity(
      reservation,
      reason,
      direction.source_participant_id,
      direction.destination_participant_id,
    );
    await transaction`WITH previous AS (
        SELECT accepted_input_sequence,accepted_input,received_output,emitted_output,
          checkpoint_hash,expected_ids,observed_ids,gaps
        FROM worker_checkpoints
        WHERE consultation_id=${reservation.consultationId}
          AND generation=${reservation.generation}
          AND worker_id=${reservation.workerId}
          AND worker_epoch=${reservation.epoch}
          AND source_participant_id=${direction.source_participant_id}
          AND destination_participant_id=${direction.destination_participant_id}
        ORDER BY accepted_input_sequence DESC,accepted_input DESC LIMIT 1
        FOR UPDATE
      )
      INSERT INTO worker_checkpoints(
        id,consultation_id,generation,worker_id,worker_epoch,write_epoch,
        source_participant_id,destination_participant_id,
        accepted_input_sequence,accepted_input,received_output,emitted_output,
        previous_hash,checkpoint_hash,expected_ids,observed_ids,gaps,terminal,
        object_key,object_version_id,created_at
      )
      SELECT ${id},${reservation.consultationId},${reservation.generation},
        ${reservation.workerId},${reservation.epoch},
        COALESCE((SELECT write_epoch FROM archives WHERE consultation_id=${reservation.consultationId}),0),
        ${direction.source_participant_id},${direction.destination_participant_id},
        COALESCE(previous.accepted_input_sequence+1,0),
        COALESCE(previous.accepted_input+1,0),
        COALESCE(previous.received_output,0),
        COALESCE(previous.emitted_output,0),
        previous.checkpoint_hash,${hash},
        COALESCE(previous.expected_ids,'[]'::jsonb),COALESCE(previous.observed_ids,'[]'::jsonb),
        COALESCE(previous.gaps,'[]'::jsonb) || jsonb_build_array(jsonb_build_object(
          'reason',CASE WHEN previous.accepted_input IS NULL THEN 'checkpoint_missing' ELSE 'after_last_checkpoint' END,
          'sampleStart',previous.accepted_input,'sampleEnd',NULL)),
        true,${objectKey},NULL,now()
      FROM (SELECT 1) seed LEFT JOIN previous ON true
      WHERE NOT EXISTS (
        SELECT 1 FROM worker_checkpoints terminal
        WHERE terminal.consultation_id=${reservation.consultationId}
          AND terminal.generation=${reservation.generation}
          AND terminal.worker_id=${reservation.workerId}
          AND terminal.worker_epoch=${reservation.epoch}
          AND terminal.source_participant_id=${direction.source_participant_id}
          AND terminal.destination_participant_id=${direction.destination_participant_id}
          AND terminal.terminal
      )`;
  }

  const terminals = await transaction<{ readonly id: string }[]>`SELECT latest.id
    FROM room_provider_selections selection
    CROSS JOIN LATERAL jsonb_array_elements(selection.selection->'directions') direction
    CROSS JOIN LATERAL (
      SELECT checkpoint.id FROM worker_checkpoints checkpoint
      WHERE checkpoint.consultation_id=${reservation.consultationId}
        AND checkpoint.generation=${reservation.generation}
        AND checkpoint.worker_id=${reservation.workerId}
        AND checkpoint.worker_epoch=${reservation.epoch}
        AND checkpoint.source_participant_id=(direction->>'sourceParticipantId')::uuid
        AND checkpoint.destination_participant_id=(direction->>'destinationParticipantId')::uuid
        AND checkpoint.terminal
      ORDER BY checkpoint.accepted_input DESC LIMIT 1
    ) latest
    WHERE selection.consultation_id=${reservation.consultationId}`;
  if (terminals.length !== 2 || terminals[0] === undefined) {
    throw new Error("supervisor terminal checkpoints were not persisted for both directions");
  }
  return terminals[0].id;
}

function verifiedEgressUpdate(event: VerifiedWebhook): SQL {
  const terminal = event.kind === "EGRESS_TERMINAL";
  const terminalResult = {
    eventId: event.eventId,
    occurredAtMs: event.occurredAtMs,
    status: event.egressStatus,
    rawSha256: event.rawSha256,
  };
  return sql`UPDATE egress_jobs job SET state=${event.egressStatus ?? event.kind},
        terminal_at=CASE WHEN ${terminal} THEN now() ELSE job.terminal_at END,
        terminal_result=CASE WHEN ${terminal} THEN ${JSON.stringify(terminalResult)}::jsonb ELSE job.terminal_result END
        WHERE job.consultation_id=${event.consultationId} AND job.generation=${event.generation} AND job.egress_id=${event.egressId}
          AND job.terminal_at IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM inbox newer WHERE newer.source='livekit'
              AND newer.payload->>'consultationId'=${event.consultationId}
              AND (newer.payload->>'generation')::integer=${event.generation}
              AND newer.payload->>'egressId'=${event.egressId}
              AND (newer.occurred_at,newer.event_id) > (to_timestamp(${event.occurredAtMs} / 1000.0),${event.eventId})
          ) RETURNING job.id`;
}

function participantWatermark(event: VerifiedWebhook): string {
  return `${event.occurredAtMs.toString().padStart(16, "0")}:${event.eventId}`;
}

function participantPresenceUpdate(event: VerifiedWebhook, watermark: string): SQL {
  return sql`UPDATE consultation_participants participant SET presence_event_id=${watermark},
      present=${event.kind === "PARTICIPANT_JOINED"} FROM consultations consultation
      WHERE participant.consultation_id=consultation.id AND consultation.id=${event.consultationId}
        AND consultation.generation=${event.generation} AND participant.livekit_identity=${event.participantId}
        AND (participant.presence_event_id IS NULL OR participant.presence_event_id < ${watermark}) RETURNING participant.id`;
}

function participantPresenceTransition(event: VerifiedWebhook): SQL {
  if (event.kind === "PARTICIPANT_JOINED") {
    return sql`UPDATE consultations SET both_absent_since=NULL,presence_epoch=presence_epoch+1,updated_at=now()
      WHERE id=${event.consultationId} AND generation=${event.generation}`;
  }
  return sql`WITH changed AS (
              UPDATE consultations SET presence_epoch=presence_epoch+1,updated_at=now()
                WHERE id=${event.consultationId} AND generation=${event.generation} RETURNING id,generation
            ), absent AS (
              UPDATE consultations consultation SET both_absent_since=COALESCE(consultation.both_absent_since,now()),updated_at=now()
              FROM changed WHERE consultation.id=changed.id AND NOT EXISTS (
                SELECT 1 FROM consultation_participants WHERE consultation_id=${event.consultationId} AND present=true
              ) RETURNING consultation.id,consultation.generation,consultation.both_absent_since
            ) INSERT INTO orchestration_deadlines(consultation_id,generation,kind,due_at)
              SELECT id,generation,'absence',both_absent_since+interval '30 seconds' FROM absent
              ON CONFLICT (consultation_id,generation,kind) DO UPDATE SET due_at=EXCLUDED.due_at,completed_at=NULL,lease_owner=NULL,lease_expires_at=NULL`;
}

function mapWorkerDispatchMetadata(
  row: WorkerDispatchRow,
  participants: readonly ParticipantIdentityRow[],
  consultationId: Uuid,
  generation: number,
): unknown {
  if (participants.length !== 2) {
    throw new Error("worker dispatch requires exactly two persisted participants");
  }
  const [firstParticipant, secondParticipant] = participants;
  if (firstParticipant === undefined || secondParticipant === undefined) {
    throw new Error("worker dispatch participants disappeared");
  }
  return {
    schemaVersion: 1,
    consultationId,
    generation,
    roomName: String(row.room_name),
    workerIdentity: String(row.worker_identity),
    workerEpoch: Number(row.epoch),
    writeEpoch: Number(row.write_epoch),
    expectedParticipantIds: [String(firstParticipant.id), String(secondParticipant.id)],
    expectedLivekitIdentities: [
      String(firstParticipant.livekit_identity),
      String(secondParticipant.livekit_identity),
    ],
    providerSelection: row.provider_selection,
    snapshotHash: String(row.snapshot_hash),
  };
}

function supervisorTerminalIdentity(
  reservation: WorkerReservation,
  reason: string,
  sourceParticipantId: string,
  destinationParticipantId: string,
): { readonly id: Uuid; readonly hash: string; readonly objectKey: string } {
  const id = randomUUID();
  const hash = createHash("sha256")
    .update(
      `${id}:${reservation.consultationId}:${String(reservation.generation)}:${String(reservation.epoch)}:${sourceParticipantId}:${destinationParticipantId}:${reason}`,
    )
    .digest("hex");
  return {
    id,
    hash,
    objectKey: `v1/meetings/${reservation.consultationId}/inventory/checkpoints/supervisor-${id}.json`,
  };
}

function capacityDimensionsForSelection(
  selection: RoomProviderSelection,
): readonly CapacityDimension[] {
  const dimensions: CapacityDimension[] = [{ key: "rooms:global", capacity: 4, units: 1 }];
  for (const direction of selection.directions) {
    const stages =
      direction.mode === "translated"
        ? ([
            ["stt", direction.stt],
            ["translation", direction.translation],
            ["tts", direction.tts],
          ] as const)
        : ([["stt", direction.stt]] as const);

    for (const [stageName, stage] of stages) {
      const account = createHash("sha256")
        .update(`${stage.credential.reference}:${stage.credential.version}`)
        .digest("hex")
        .slice(0, 16);
      const quotas = Object.entries(stage.limits).sort(([left], [right]) =>
        left.localeCompare(right),
      );
      for (const [dimension, effectiveQuota] of quotas) {
        if (!Number.isFinite(effectiveQuota) || effectiveQuota <= 0) {
          throw new Error(`invalid ${stageName} quota ${dimension}`);
        }
        const capacity = Math.floor(effectiveQuota * 0.8);
        const units = perRoomQuotaUnits(stageName, dimension);
        if (capacity < units) {
          throw new Error(
            `${stageName} quota ${dimension} cannot admit one room with 20% headroom`,
          );
        }
        dimensions.push({
          key: `provider:${stage.provider}:account:${account}:region:${stage.region}:stage:${stageName}:dimension:${dimension}`,
          capacity,
          units,
        });
      }
    }
  }
  return dimensions;
}
