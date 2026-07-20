import { createHash, randomUUID } from "node:crypto";
import {
  ArchiveStateSchema,
  ExternalEffectStateSchema,
  PARTICIPANT_ROLE_VALUES,
  type RoomProviderSelection,
  RoomProviderSelectionSchema,
} from "@transhooter/contracts";
import {
  consultationParticipants,
  consultations,
  workerReservations,
} from "@transhooter/server-core/persistence";
import { and, eq, inArray, type SQL, sql } from "drizzle-orm";
import { drizzle, type PostgresJsDatabase } from "drizzle-orm/postgres-js";
import postgres, { type Row as PostgresRow, type Sql, type TransactionSql } from "postgres";
import type {
  ArchivedObject,
  ClaimOptions,
  ConsultationState,
  Deadline,
  DerivedArchiveObject,
  DurableStore,
  Effect,
  FinalInventoryObject,
  OutboxItem,
  PlannedEffect,
  ReconciliationExpectation,
  ReconciliationSnapshot,
  Uuid,
  VerifiedWebhook,
  WorkerReservation,
} from "../orchestration/model";

interface ExternalEffectRow extends PostgresRow {
  readonly id: string;
  readonly consultation_id: string;
  readonly generation: number;
  readonly effect_kind: string;
  readonly subject_id: string;
  readonly occurrence_key: string;
  readonly state: unknown;
  readonly request_bytes: Uint8Array | null;
  readonly request_hash: string | null;
  readonly lease_owner: string | null;
  readonly lease_expires_at: Date | string | null;
  readonly result: unknown;
  readonly attempts: number;
}

interface OutboxRow extends PostgresRow {
  readonly id: string;
  readonly aggregate_id: string;
  readonly generation: number;
  readonly topic: string;
  readonly payload: unknown;
  readonly attempts: number;
}

interface DeadlineRow extends PostgresRow {
  readonly consultation_id: string;
  readonly generation: number;
  readonly kind: Deadline["kind"];
  readonly due_at: Date | string;
}

interface ReservationRow extends PostgresRow {
  readonly consultation_id: string;
  readonly generation: number;
  readonly worker_id: string;
  readonly epoch: number;
  readonly heartbeat_at: Date | string;
  readonly lease_expires_at: Date | string;
  readonly accepting_load: boolean;
}

interface IdRow extends PostgresRow {
  readonly id: string;
}

interface RoomResourceRow extends PostgresRow {
  readonly resource_room_name: string | null;
}

interface ReserveConsultationRow extends PostgresRow {
  readonly worker_identity: string | null;
  readonly snapshot_hash: string | null;
}

interface CancellationConsultationRow extends PostgresRow {
  readonly generation: number;
  readonly state: ConsultationState;
}

interface WorkerEpochTerminalRow extends PostgresRow {
  readonly terminal_at: Date | string | null;
}

interface ArchiveStateRow extends PostgresRow {
  readonly id: string;
  readonly state: unknown;
}

interface ReconciliationArchiveRow extends ArchiveStateRow {
  readonly reconciliation_deadline_at: Date | string | null;
}

interface ConsultationIdRow extends PostgresRow {
  readonly consultation_id: string;
}

interface WorkerDispatchRow extends PostgresRow {
  readonly room_name: string;
  readonly worker_identity: string;
  readonly snapshot_hash: string;
  readonly provider_selection: unknown;
  readonly epoch: number;
  readonly write_epoch: number;
}

interface WorkerDirectionRow extends PostgresRow {
  readonly source_participant_id: string;
  readonly destination_participant_id: string;
}

interface ParticipantIdentityRow extends PostgresRow {
  readonly id: string;
  readonly livekit_identity: string;
}

interface ExpectationRow extends PostgresRow {
  readonly id: string;
  readonly object_class: string;
  readonly causal_key: string;
  readonly sample_start: number | null;
  readonly sample_end: number | null;
  readonly fulfilled_object_id: string | null;
}

interface ArchiveObjectRow extends PostgresRow {
  readonly id: string;
  readonly object_class: string;
  readonly key: string;
  readonly version_id: string;
  readonly size: number;
  readonly sha256: string;
  readonly s3_checksum: string;
  readonly content_type: string;
}

interface ProviderGapRow extends PostgresRow {
  readonly attempt_id: string;
  readonly stage: string;
  readonly provider: string;
  readonly direction_id: string;
  readonly operation_id: string;
  readonly attempt_number: number;
  readonly outcome: string;
  readonly error_kind: string | null;
  readonly accepted_input_watermark: number | null;
  readonly received_output_watermark: number | null;
  readonly emitted_output_watermark: number | null;
  readonly retry_decision: unknown;
}

interface CheckpointRow extends PostgresRow {
  readonly checkpoint: unknown;
}

interface EgressResultRow extends PostgresRow {
  readonly id: string;
  readonly egress_id: string | null;
  readonly kind: string;
  readonly state: string;
  readonly output_prefix: string;
  readonly terminal_result: unknown;
}

interface DrainResultRow extends PostgresRow {
  readonly result: unknown;
}
interface EgressIdRow extends PostgresRow {
  readonly egress_id: string;
}

interface LiveKitIdentityRow extends PostgresRow {
  readonly livekit_identity: string;
}

interface DispatchIdRow extends PostgresRow {
  readonly dispatch_id: string;
}

type CapacityDimension = {
  readonly key: string;
  readonly capacity: number;
  readonly units: number;
};

type ReconciliationEgressResult = {
  readonly egressId: unknown;
  readonly state: unknown;
  readonly terminal: boolean;
  readonly result: unknown;
  readonly kind: unknown;
  readonly outputPrefix: unknown;
};

export class PostgresStore implements DurableStore {
  private readonly db: PostgresJsDatabase;
  private constructor(private readonly client: Sql) {
    this.db = drizzle({ client });
  }

  static connect(url: string): PostgresStore {
    const client = postgres(url, {
      max: 10,
      prepare: false,
      transform: { undefined: null },
    });
    return new PostgresStore(client);
  }

  async close(): Promise<void> {
    await this.client.end({ timeout: 5 });
  }

  async readiness(): Promise<void> {
    await this.db.execute(sql`SELECT 1`);
  }

  async claimOutbox(options: ClaimOptions): Promise<readonly OutboxItem[]> {
    const rows = await this.db.execute<OutboxRow>(sql`WITH picked AS (
      SELECT id FROM outbox WHERE delivered_at IS NULL AND available_at <= ${options.now.toISOString()}
        AND (lease_expires_at IS NULL OR lease_expires_at < ${options.now.toISOString()})
      ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT ${options.limit}
    ) UPDATE outbox o SET lease_owner=${options.owner}, lease_expires_at=${new Date(options.now.getTime() + options.leaseMs).toISOString()}, attempts=o.attempts+1
      FROM picked WHERE o.id=picked.id RETURNING o.*`);
    return rows.map(mapOutboxItem);
  }

  async completeOutbox(id: Uuid, owner: Uuid): Promise<void> {
    await this.db.execute(
      sql`UPDATE outbox SET delivered_at=now(), lease_owner=NULL, lease_expires_at=NULL WHERE id=${id} AND lease_owner=${owner}`,
    );
  }

  async retryOutbox(id: Uuid, owner: Uuid, error: string, nextAt: Date): Promise<void> {
    await this.db.execute(sql`UPDATE outbox SET available_at=${nextAt.toISOString()}, lease_owner=NULL, lease_expires_at=NULL,
      payload=jsonb_set(payload,'{lastDispatchError}',to_jsonb(${error}::text),true) WHERE id=${id} AND lease_owner=${owner}`);
  }

  async claimEffects(options: ClaimOptions): Promise<readonly Effect[]> {
    const rows = await this.db.execute<ExternalEffectRow>(sql`WITH picked AS (
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
      ORDER BY CASE candidate.effect_kind WHEN 'STATUS_PACKET' THEN 0 ELSE 1 END,candidate.created_at
      FOR UPDATE SKIP LOCKED LIMIT ${options.limit}
    ) UPDATE external_effects e SET lease_owner=${options.owner}, lease_expires_at=${new Date(options.now.getTime() + options.leaseMs).toISOString()}
      FROM picked WHERE e.id=picked.id RETURNING e.*`);
    return rows.map(mapEffect);
  }

  async persistCalling(
    effectId: Uuid,
    owner: Uuid,
    requestBytes: Uint8Array,
    requestSha256: string,
  ): Promise<Effect | null> {
    const rows =
      await this.db.execute<ExternalEffectRow>(sql`UPDATE external_effects SET state='calling', request_bytes=COALESCE(request_bytes, ${Buffer.from(requestBytes)}),
      request_hash=COALESCE(request_hash, ${requestSha256}), attempts=attempts+1,updated_at=now()
      WHERE id=${effectId} AND lease_owner=${owner} AND state IN ('planned','calling') AND (request_hash IS NULL OR request_hash=${requestSha256}) RETURNING *`);
    return rows[0] === undefined ? null : mapEffect(rows[0]);
  }

  async markApplied(
    effectId: Uuid,
    owner: Uuid,
    remoteId: string | null,
    result: unknown,
  ): Promise<void> {
    await this.db.execute(markAppliedStatement(effectId, owner, remoteId, result));
  }

  async markDone(effectId: Uuid, owner: Uuid): Promise<void> {
    await this.db.execute(
      sql`UPDATE external_effects SET state='done',updated_at=now(),lease_owner=NULL,lease_expires_at=NULL
        WHERE id=${effectId} AND lease_owner=${owner}
          AND lease_expires_at > now()
          AND state IN ('calling','applied','compensating')`,
    );
  }

  async markFailed(
    effectId: Uuid,
    owner: Uuid,
    error: string,
    retryAt: Date | null,
  ): Promise<void> {
    await this.db.execute(sql`UPDATE external_effects SET state=${retryAt === null ? "failed" : "calling"},
      result=COALESCE(result,'{}'::jsonb) || ${JSON.stringify({ error })}::jsonb,
      updated_at=now(),lease_owner=NULL,lease_expires_at=${retryAt?.toISOString() ?? null} WHERE id=${effectId} AND lease_owner=${owner}`);
  }

  async renewEffectLease(effectId: Uuid, owner: Uuid, leaseExpiresAt: Date): Promise<boolean> {
    const rows =
      await this.db.execute<IdRow>(sql`UPDATE external_effects SET lease_expires_at=${leaseExpiresAt.toISOString()},updated_at=now()
      WHERE id=${effectId} AND lease_owner=${owner} AND state IN ('calling','applied','compensating') RETURNING id`);
    return rows.length === 1;
  }

  async markCompensating(effectId: Uuid, owner: Uuid, reason: string): Promise<void> {
    await this.db.execute(
      sql`UPDATE external_effects SET state='compensating',
        result=COALESCE(result,'{}'::jsonb) || ${JSON.stringify({ reason })}::jsonb,updated_at=now()
        WHERE id=${effectId} AND lease_owner=${owner} AND lease_expires_at > now()`,
    );
  }

  async currentGeneration(consultationId: Uuid): Promise<number | null> {
    const rows = await this.db
      .select({ generation: consultations.generation })
      .from(consultations)
      .where(eq(consultations.id, consultationId))
      .limit(1);
    return rows[0]?.generation ?? null;
  }

  async scheduleEffect(input: PlannedEffect): Promise<void> {
    await this.client.begin(async (transaction) => {
      await insertPlannedEffects(transaction, [input]);
    });
  }

  async claimDeadlines(options: ClaimOptions): Promise<readonly Deadline[]> {
    const rows = await this.db.execute<DeadlineRow>(sql`WITH picked AS (
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

  async completeDeadline(deadline: Deadline, owner: Uuid): Promise<void> {
    await this.db.execute(sql`UPDATE orchestration_deadlines SET completed_at=now(),lease_owner=NULL,lease_expires_at=NULL
      WHERE consultation_id=${deadline.consultationId} AND generation=${deadline.generation} AND kind=${deadline.kind} AND lease_owner=${owner}`);
  }

  async claimStaleReservations(options: ClaimOptions): Promise<readonly WorkerReservation[]> {
    const rows = await this.db.execute<ReservationRow>(sql`WITH picked AS (
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

  async heartbeat(
    workerId: Uuid,
    epoch: number,
    now: Date,
    leaseExpiresAt: Date,
  ): Promise<boolean> {
    const rows =
      await this.db.execute<IdRow>(sql`UPDATE worker_reservations SET heartbeat_at=${now.toISOString()},lease_expires_at=${leaseExpiresAt.toISOString()}
      WHERE worker_id=${workerId} AND epoch=${epoch} AND fenced_at IS NULL RETURNING worker_id AS id`);
    return rows.length === 1;
  }

  async reserveWorker(consultationId: Uuid, generation: number): Promise<WorkerReservation> {
    return this.client.begin(async (transaction) => {
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

  async applyVerifiedWebhook(event: VerifiedWebhook): Promise<boolean> {
    // Inbox verification and all watermark/state mutations share this transaction.
    // A rollback therefore cannot leave an accepted webhook without its state change.
    return this.db.transaction(async (transaction) => {
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

  async presenceEpoch(consultationId: Uuid, generation: number): Promise<number | null> {
    const rows = await this.db
      .select({ presenceEpoch: consultations.presenceEpoch })
      .from(consultations)
      .where(and(eq(consultations.id, consultationId), eq(consultations.generation, generation)))
      .limit(1);
    return rows[0]?.presenceEpoch ?? null;
  }

  async admitFinalization(
    consultationId: Uuid,
    generation: number,
    presenceEpoch: number,
    now: Date,
  ): Promise<"admitted" | ConsultationState | null> {
    return this.client.begin(async (transaction) => {
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

  async isStandardHuman(consultationId: Uuid, participantId: Uuid): Promise<boolean> {
    const rows = await this.db
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
  async markCaptureReady(
    consultationId: Uuid,
    generation: number,
    participantIdentity: Uuid,
    participantEgressId: string,
  ): Promise<"active" | null> {
    return this.client.begin(async (transaction) => {
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

  async consultationState(consultationId: Uuid): Promise<ConsultationState | null> {
    const rows = await this.db
      .select({ state: consultations.state })
      .from(consultations)
      .where(eq(consultations.id, consultationId))
      .limit(1);
    return rows[0]?.state ?? null;
  }

  async workerReservation(
    consultationId: Uuid,
    generation: number,
  ): Promise<WorkerReservation | null> {
    const rows = await this.db
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

  async workerDispatchMetadata(consultationId: Uuid, generation: number): Promise<unknown> {
    return this.client.begin(async (transaction) => {
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

  async planFailureEffects(
    consultationId: Uuid,
    generation: number,
    reason: string,
    effects: readonly PlannedEffect[],
  ): Promise<void> {
    await this.client.begin(async (transaction) => {
      await transaction`INSERT INTO audit_events(id,aggregate_id,actor_id,kind,occurred_at,details)
        VALUES (gen_random_uuid(),${consultationId},NULL,'egress.supervisor_terminal',now(),${JSON.stringify({ generation, reason })}::jsonb)`;
      await insertPlannedEffects(transaction, effects);
    });
  }

  async fenceWorkerAndPlanFailure(
    reservation: WorkerReservation,
    owner: Uuid,
    reason: string,
    effects: readonly PlannedEffect[],
  ): Promise<boolean> {
    return this.client.begin(async (transaction) => {
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
  async fenceWorkerAndScheduleCancellation(
    consultationId: Uuid,
    cleanupGeneration: number,
    resourceGeneration: number,
    owner: Uuid,
    reason: string,
    effects: readonly PlannedEffect[],
  ): Promise<void> {
    await this.client.begin(async (transaction) => {
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

  async humanIdentities(consultationId: Uuid): Promise<readonly [Uuid, Uuid]> {
    const rows = await this.db
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

  async seedDeadlines(consultationId: Uuid, generation: number): Promise<void> {
    await this.db.execute(sql`INSERT INTO orchestration_deadlines(consultation_id,generation,kind,due_at)
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
  async roomDrainPlan(
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
      this.db.execute<EgressIdRow>(
        sql`SELECT egress_id FROM egress_jobs WHERE consultation_id=${consultationId} AND generation=${generation} AND terminal_at IS NULL ORDER BY egress_id`,
      ),
      this.db.execute<LiveKitIdentityRow>(
        sql`SELECT livekit_identity FROM consultation_participants WHERE consultation_id=${consultationId} ORDER BY role`,
      ),
      this.db.execute<DispatchIdRow>(sql`SELECT result->>'remoteId' AS dispatch_id FROM external_effects
        WHERE consultation_id=${consultationId} AND generation=${generation} AND effect_kind='WORKER_DISPATCH'
          AND result->>'remoteId' IS NOT NULL ORDER BY created_at`),
      this.db.execute<RoomResourceRow>(sql`SELECT result->'plan'->>'roomName' AS resource_room_name FROM external_effects
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

  async completeRoomDrain(consultationId: Uuid, generation: number): Promise<void> {
    await this.client.begin(async (transaction) => {
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

  async preparePendingArchiveDeletes(): Promise<void> {
    await this.db.execute(claimPendingArchiveDeletesStatement());
  }

  async capacityDimensions(consultationId: Uuid): Promise<readonly CapacityDimension[]> {
    const rows = await this.db
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

  async reconciliationSnapshot(
    consultationId: Uuid,
    cleanupGeneration: number,
    resourceGeneration: number,
  ): Promise<ReconciliationSnapshot | null> {
    return this.client.begin(async (transaction) => {
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
      const [expectations, objects, checkpoints, egress, drains, providerGaps] = await Promise.all([
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
        transaction<
          ProviderGapRow[]
        >`SELECT attempt.id AS attempt_id,attempt.stage,attempt.provider,
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
      ]);
      return mapReconciliationSnapshot(
        archiveId,
        archive.reconciliationDeadlineAt,
        expectations,
        objects,
        checkpoints,
        egress,
        drains,
        providerGaps,
      );
    });
  }

  async completeReconciliation(
    effect: Effect,
    owner: Uuid,
    now: Date,
    snapshot: ReconciliationSnapshot,
    inventory: Readonly<Record<string, unknown>>,
    sha256: string,
    finalObject: FinalInventoryObject,
    derivedObjects: readonly DerivedArchiveObject[],
  ): Promise<boolean> {
    return this.client.begin(async (transaction) => {
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

  async finishArchiveDeletionIfEmpty(
    consultationId: Uuid,
    generation: number,
    writeEpoch: number,
  ): Promise<boolean> {
    const rows =
      await this.db.execute<IdRow>(sql`UPDATE archives a SET state='deleted',updated_at=now() FROM consultations c
      WHERE a.consultation_id=${consultationId} AND a.state='deleting' AND a.write_epoch=${writeEpoch}
        AND c.id=a.consultation_id AND c.generation=${generation} AND c.state='ended'
      RETURNING a.consultation_id AS id`);
    return rows.length === 1;
  }
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
        SELECT accepted_input_sequence,high_watermark,received_output,emitted_output,
          checkpoint_hash,expected_ids,observed_ids,gaps
        FROM worker_checkpoints
        WHERE consultation_id=${reservation.consultationId}
          AND generation=${reservation.generation}
          AND worker_id=${reservation.workerId}
          AND worker_epoch=${reservation.epoch}
          AND source_participant_id=${direction.source_participant_id}
          AND destination_participant_id=${direction.destination_participant_id}
        ORDER BY accepted_input_sequence DESC,high_watermark DESC LIMIT 1
        FOR UPDATE
      )
      INSERT INTO worker_checkpoints(
        id,consultation_id,generation,worker_id,worker_epoch,write_epoch,
        source_participant_id,destination_participant_id,
        accepted_input_sequence,high_watermark,received_output,emitted_output,
        previous_hash,checkpoint_hash,expected_ids,observed_ids,gaps,terminal,
        object_key,object_version_id,created_at
      )
      SELECT ${id},${reservation.consultationId},${reservation.generation},
        ${reservation.workerId},${reservation.epoch},
        COALESCE((SELECT write_epoch FROM archives WHERE consultation_id=${reservation.consultationId}),0),
        ${direction.source_participant_id},${direction.destination_participant_id},
        COALESCE(previous.accepted_input_sequence+1,0),
        COALESCE(previous.high_watermark+1,0),
        COALESCE(previous.received_output,0),
        COALESCE(previous.emitted_output,0),
        previous.checkpoint_hash,${hash},
        COALESCE(previous.expected_ids,'[]'::jsonb),COALESCE(previous.observed_ids,'[]'::jsonb),
        COALESCE(previous.gaps,'[]'::jsonb) || jsonb_build_array(jsonb_build_object(
          'reason',CASE WHEN previous.high_watermark IS NULL THEN 'checkpoint_missing' ELSE 'after_last_checkpoint' END,
          'sampleStart',previous.high_watermark,'sampleEnd',NULL)),
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
      ORDER BY checkpoint.high_watermark DESC LIMIT 1
    ) latest
    WHERE selection.consultation_id=${reservation.consultationId}`;
  if (terminals.length !== 2 || terminals[0] === undefined) {
    throw new Error("supervisor terminal checkpoints were not persisted for both directions");
  }
  return terminals[0].id;
}

async function insertPlannedEffects(
  transaction: TransactionSql,
  effects: readonly PlannedEffect[],
): Promise<void> {
  for (const effect of effects) {
    const authoritative = await transaction<
      IdRow[]
    >`INSERT INTO external_effects(id,consultation_id,generation,effect_kind,subject_id,occurrence_key,state,result,attempts,created_at,updated_at)
      VALUES (${effect.id},${effect.consultationId},${effect.generation},${effect.kind},${effect.subjectId},${effect.occurrenceKey},'planned',
        ${JSON.stringify({ plan: effect.plan })}::jsonb,0,now(),now())
      ON CONFLICT (consultation_id,generation,effect_kind,subject_id,occurrence_key)
      DO UPDATE SET updated_at=external_effects.updated_at
      RETURNING id`;
    const effectId = authoritative[0]?.id;
    if (effectId === undefined) {
      throw new Error("authoritative effect was not persisted");
    }
    if (effect.kind === "ROOM_COMPOSITE_EGRESS" || effect.kind === "PARTICIPANT_EGRESS") {
      const outputPrefix = effect.plan.outputPrefix;
      if (typeof outputPrefix !== "string" || outputPrefix.length === 0) {
        throw new Error("Egress effect omitted its immutable output prefix");
      }
      const objectClass =
        effect.kind === "ROOM_COMPOSITE_EGRESS" ? "room_composite" : "participant_original";
      await transaction`INSERT INTO expected_archive_artifacts(
          id,archive_id,effect_id,profile_id,profile_revision,object_class,causal_key,
          sample_start,sample_end,owner_epoch,disposition,created_at
        )
        SELECT ${effectId},archive.id,${effectId},consultation.provider_profile_id,
          consultation.provider_profile_revision,${objectClass},${outputPrefix},
          NULL,NULL,archive.write_epoch,'expected',now()
        FROM consultations consultation
        JOIN archives archive ON archive.consultation_id=consultation.id
        WHERE consultation.id=${effect.consultationId}
          AND consultation.generation=${effect.generation}
          AND archive.state NOT IN ('deleting','deleted')
        ON CONFLICT (archive_id,object_class,causal_key) DO UPDATE
        SET effect_id=COALESCE(expected_archive_artifacts.effect_id,EXCLUDED.effect_id)
        WHERE expected_archive_artifacts.effect_id IS NULL
          OR expected_archive_artifacts.effect_id=EXCLUDED.effect_id`;
    }
  }
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

function markAppliedStatement(
  effectId: Uuid,
  owner: Uuid,
  remoteId: string | null,
  result: unknown,
): SQL {
  return sql`WITH changed AS (
      UPDATE external_effects SET state='applied',
        result=COALESCE(result,'{}'::jsonb) || ${JSON.stringify({ remoteId, value: result })}::jsonb,updated_at=now()
      WHERE id=${effectId} AND lease_owner=${owner} AND lease_expires_at > now()
        AND state='calling' RETURNING *
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
    ) INSERT INTO outbox(id,topic,aggregate_id,generation,payload,available_at,attempts)
      SELECT gen_random_uuid(),'orchestration.effect.applied',consultation_id,generation,
        jsonb_build_object('consultationId',consultation_id,'generation',generation,'subjectId',subject_id,'kind',effect_kind,
          'resourceGeneration',COALESCE((result->'plan'->>'resourceGeneration')::integer,generation),
          'participantEgressId',CASE WHEN effect_kind IN ('ROOM_COMPOSITE_EGRESS','PARTICIPANT_EGRESS') THEN result->>'remoteId' ELSE result->'plan'->>'barrierEgressId' END),now(),0 FROM changed
      ON CONFLICT (id) DO NOTHING`;
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

function mapReconciliationSnapshot(
  archiveId: string,
  reconciliationDeadlineAt: Date,
  expectations: readonly ExpectationRow[],
  objects: readonly ArchiveObjectRow[],
  checkpoints: readonly CheckpointRow[],
  egress: readonly EgressResultRow[],
  drains: readonly DrainResultRow[],
  providerGaps: readonly ProviderGapRow[],
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

function mapOutboxItem(row: OutboxRow): OutboxItem {
  return {
    id: String(row.id),
    aggregateId: String(row.aggregate_id),
    generation: Number(row.generation),
    type: String(row.topic),
    payload: row.payload,
    attempts: Number(row.attempts),
  };
}

function mapDeadline(row: DeadlineRow): Deadline {
  return {
    consultationId: String(row.consultation_id),
    generation: Number(row.generation),
    kind: row.kind as Deadline["kind"],
    dueAt: new Date(String(row.due_at)),
  };
}

function perRoomQuotaUnits(stage: "stt" | "translation" | "tts", dimension: string): number {
  const normalized = dimension.toLowerCase().replaceAll("_", "-");
  const isAudioDuration =
    normalized.includes("audio") || normalized.includes("second") || normalized.includes("minute");

  if (stage === "stt") {
    if (normalized.includes("message")) {
      return 250;
    }
    if (isAudioDuration) {
      return 60;
    }
    return 1;
  }

  if (stage === "translation") {
    if (normalized.includes("character") || normalized.includes("char")) {
      return 50_000;
    }
    if (normalized.includes("request")) {
      return 50;
    }
    return 1;
  }

  if (normalized.includes("character") || normalized.includes("char")) {
    return 1_200;
  }
  if (normalized.includes("start")) {
    return 20;
  }
  if (normalized.includes("flush")) {
    return 10;
  }
  if (isAudioDuration) {
    return 60;
  }
  return 1;
}

function nullableString(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value !== "string") {
    throw new Error("database value must be a string");
  }
  return value;
}

function nullableDate(value: unknown): Date | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (value instanceof Date) {
    return value;
  }
  if (typeof value !== "string" && typeof value !== "number") {
    throw new Error("database timestamp has invalid type");
  }
  return new Date(value);
}

function mapEffect(row: ExternalEffectRow): Effect {
  const result =
    row.result !== null && typeof row.result === "object" && !Array.isArray(row.result)
      ? (row.result as Readonly<Record<string, unknown>>)
      : {};
  const plan =
    result.plan !== null && typeof result.plan === "object" && !Array.isArray(result.plan)
      ? (result.plan as Readonly<Record<string, unknown>>)
      : {};
  return {
    id: String(row.id),
    consultationId: String(row.consultation_id),
    generation: Number(row.generation),
    kind: String(row.effect_kind) as Effect["kind"],
    subjectId: String(row.subject_id),
    occurrenceKey: String(row.occurrence_key),
    plan,
    state: ExternalEffectStateSchema.parse(row.state),
    requestBytes: row.request_bytes instanceof Uint8Array ? row.request_bytes : null,
    requestSha256: nullableString(row.request_hash),
    remoteId: typeof result.remoteId === "string" ? result.remoteId : null,
    appliedResult: "value" in result ? result.value : null,
    attempt: Number(row.attempts),
    leaseOwner: nullableString(row.lease_owner),
    leaseExpiresAt: nullableDate(row.lease_expires_at),
  };
}

function mapReservation(row: ReservationRow): WorkerReservation {
  return {
    consultationId: String(row.consultation_id),
    generation: Number(row.generation),
    workerId: String(row.worker_id),
    epoch: Number(row.epoch),
    heartbeatAt: new Date(String(row.heartbeat_at)),
    leaseExpiresAt: new Date(String(row.lease_expires_at)),
    acceptingLoad: Boolean(row.accepting_load),
  };
}
