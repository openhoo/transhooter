import { createHash } from "node:crypto";
import { RoomProviderSelectionSchema, type WorkerCheckpoint } from "@transhooter/contracts";
import type {
  AbandonWorkerEpochInput,
  CheckpointInput,
  CompleteWorkerEpochInput,
  ProviderAttemptInput,
  WorkerEpochTuple,
} from "./application-operations";
import { type Clock, DomainError, type UUID } from "./domain/model";
import { Prisma, type PrismaClient } from "./persistence/database";
import { retryPostgresContention } from "./persistence/postgres-contention";

function serializeCheckpointObjectIds(checkpoint: WorkerCheckpoint): {
  expected: string;
  observed: string;
  gaps: string;
} {
  return {
    expected: JSON.stringify(checkpoint.expectedObjectIds),
    observed: JSON.stringify(checkpoint.observedObjectIds),
    gaps: JSON.stringify(checkpoint.gaps),
  };
}

export function checkpointPersistenceValues(checkpoint: WorkerCheckpoint): {
  acceptedInputSequence: number;
  acceptedInput: number;
  receivedOutput: number;
  emittedOutput: number;
  sourceParticipantId: UUID;
  destinationParticipantId: UUID;
  createdAt: Date;
} {
  return {
    acceptedInputSequence: checkpoint.acceptedInputSequence,
    acceptedInput: checkpoint.acceptedInput,
    receivedOutput: checkpoint.receivedOutput,
    emittedOutput: checkpoint.emittedOutput,
    sourceParticipantId: checkpoint.sourceParticipantId,
    destinationParticipantId: checkpoint.destinationParticipantId,
    createdAt: new Date(checkpoint.occurredAtMs),
  };
}

function safeDatabaseInteger(value: bigint | number, column: string): number {
  const converted = typeof value === "bigint" ? Number(value) : value;
  if (!Number.isSafeInteger(converted)) {
    throw new Error(`${column} is outside the JavaScript safe integer range`);
  }
  return converted;
}
export class ApplicationOperationsWorkers {
  constructor(
    private readonly database: PrismaClient,
    private readonly clock: Clock,
  ) {}

  async heartbeat(
    consultationId: UUID,
    generation: number,
    workerId: UUID,
    epoch: number,
  ): Promise<boolean> {
    const now = this.clock.now();
    const leaseExpiresAt = new Date(now.getTime() + 30_000);
    const result = await this.database.$queryRaw<Record<string, unknown>[]>(
      Prisma.sql`WITH active_job AS (
        SELECT reservation.consultation_id,reservation.generation,reservation.worker_id,reservation.epoch
        FROM consultations consultation
        JOIN worker_reservations reservation
          ON reservation.consultation_id=consultation.id
          AND reservation.generation=consultation.generation
        JOIN worker_job_epochs job
          ON job.consultation_id=reservation.consultation_id
          AND job.generation=reservation.generation
          AND job.worker_id=reservation.worker_id
          AND job.epoch=reservation.epoch
        WHERE consultation.id=${consultationId} AND consultation.generation=${generation}
          AND consultation.state IN ('ready','active')
          AND reservation.worker_id=${workerId} AND reservation.epoch=${epoch}
          AND reservation.accepting_load AND reservation.released_at IS NULL AND reservation.fenced_at IS NULL
          AND job.fenced_at IS NULL AND job.terminal_at IS NULL
        FOR UPDATE OF consultation,reservation,job
      ), reservation AS (
        UPDATE worker_reservations reservation
        SET heartbeat_at=${now},lease_expires_at=${leaseExpiresAt}
        FROM active_job
        WHERE reservation.consultation_id=active_job.consultation_id
          AND reservation.generation=active_job.generation
          AND reservation.worker_id=active_job.worker_id
          AND reservation.epoch=active_job.epoch
        RETURNING reservation.consultation_id,reservation.generation,reservation.worker_id,reservation.epoch
      )
      UPDATE worker_job_epochs job SET heartbeat_at=${now}
      FROM reservation
      WHERE job.consultation_id=reservation.consultation_id AND job.generation=reservation.generation
        AND job.worker_id=reservation.worker_id AND job.epoch=reservation.epoch
      RETURNING job.worker_id`,
    );
    return result.length === 1;
  }

  async checkpoint(input: CheckpointInput): Promise<boolean> {
    const checkpoint = input.checkpoint;
    const { expected, observed, gaps } = serializeCheckpointObjectIds(checkpoint);
    const persisted = checkpointPersistenceValues(checkpoint);
    return retryPostgresContention(() =>
      this.database.$transaction(async (transaction) => {
        const replay = await transaction.$queryRaw<Record<string, unknown>[]>(
          Prisma.sql`SELECT 1 FROM worker_checkpoints checkpoint
            WHERE checkpoint.id=${checkpoint.checkpointId}
              AND checkpoint.consultation_id=${input.consultationId}
              AND checkpoint.generation=${input.generation}
              AND checkpoint.worker_id=${input.workerId}
              AND checkpoint.worker_epoch=${checkpoint.workerEpoch}
              AND checkpoint.write_epoch=${input.writeEpoch}
              AND checkpoint.source_participant_id=${persisted.sourceParticipantId}
              AND checkpoint.destination_participant_id=${persisted.destinationParticipantId}
              AND checkpoint.accepted_input=${persisted.acceptedInput}
              AND checkpoint.accepted_input_sequence=${persisted.acceptedInputSequence}
              AND checkpoint.received_output=${persisted.receivedOutput}
              AND checkpoint.emitted_output=${persisted.emittedOutput}
              AND checkpoint.previous_hash IS NOT DISTINCT FROM ${checkpoint.previousCheckpointSha256}
              AND checkpoint.checkpoint_hash=${checkpoint.highWatermarkSha256}
              AND checkpoint.expected_ids=${expected}::jsonb
              AND checkpoint.observed_ids=${observed}::jsonb
              AND checkpoint.gaps=${gaps}::jsonb
              AND checkpoint.terminal=${checkpoint.terminal}
              AND checkpoint.object_key=${input.objectKey}
              AND checkpoint.created_at=${persisted.createdAt}`,
        );
        if (replay.length === 1) {
          return true;
        }

        const consultation = await transaction.$queryRaw<Record<string, unknown>[]>(
          Prisma.sql`SELECT id FROM consultations WHERE id=${input.consultationId} AND generation=${input.generation} FOR UPDATE`,
        );
        if (consultation.length !== 1) {
          throw new DomainError("CHECKPOINT_CONFLICT");
        }

        const parents = await transaction.$queryRaw<Record<string, unknown>[]>(
          Prisma.sql`SELECT consultation_id FROM worker_job_epochs WHERE consultation_id=${input.consultationId} AND generation=${input.generation} AND worker_id=${input.workerId} AND epoch=${checkpoint.workerEpoch} FOR UPDATE`,
        );
        if (parents.length !== 1) {
          throw new DomainError("CHECKPOINT_CONFLICT");
        }
        const result = await transaction.$queryRaw<Record<string, unknown>[]>(
          Prisma.sql`WITH chain_heads AS (
            SELECT head.checkpoint_hash
            FROM worker_checkpoints head
            WHERE head.consultation_id=${input.consultationId}
              AND head.generation=${input.generation}
              AND head.worker_id=${input.workerId}
              AND head.worker_epoch=${checkpoint.workerEpoch}
              AND head.source_participant_id=${persisted.sourceParticipantId}
              AND head.destination_participant_id=${persisted.destinationParticipantId}
              AND NOT EXISTS (
                SELECT 1 FROM worker_checkpoints child
                WHERE child.consultation_id=head.consultation_id
                  AND child.generation=head.generation
                  AND child.worker_id=head.worker_id
                  AND child.worker_epoch=head.worker_epoch
                  AND child.source_participant_id=head.source_participant_id
                  AND child.destination_participant_id=head.destination_participant_id
                  AND child.previous_hash=head.checkpoint_hash
              )
          )
          INSERT INTO worker_checkpoints(id,consultation_id,generation,worker_id,worker_epoch,write_epoch,source_participant_id,destination_participant_id,accepted_input_sequence,accepted_input,received_output,emitted_output,previous_hash,checkpoint_hash,expected_ids,observed_ids,gaps,terminal,object_key,created_at)
          SELECT ${checkpoint.checkpointId},${input.consultationId},${input.generation},${input.workerId},${checkpoint.workerEpoch},${input.writeEpoch},${persisted.sourceParticipantId},${persisted.destinationParticipantId},${persisted.acceptedInputSequence},${persisted.acceptedInput},${persisted.receivedOutput},${persisted.emittedOutput},${checkpoint.previousCheckpointSha256},${checkpoint.highWatermarkSha256},${expected}::jsonb,${observed}::jsonb,${gaps}::jsonb,${checkpoint.terminal},${input.objectKey},${persisted.createdAt}
          FROM consultations consultation
          JOIN worker_reservations reservation ON reservation.consultation_id=consultation.id
            AND reservation.generation=consultation.generation
          JOIN worker_job_epochs job ON job.consultation_id=reservation.consultation_id
            AND job.generation=reservation.generation AND job.worker_id=reservation.worker_id
            AND job.epoch=reservation.epoch
          JOIN archives archive ON archive.consultation_id=consultation.id
          WHERE consultation.id=${input.consultationId} AND consultation.generation=${input.generation}
            AND reservation.worker_id=${input.workerId} AND reservation.epoch=${checkpoint.workerEpoch}
            AND reservation.fenced_at IS NULL AND reservation.released_at IS NULL
            AND job.fenced_at IS NULL AND job.terminal_at IS NULL
            AND job.write_epoch=${input.writeEpoch} AND archive.write_epoch=${input.writeEpoch}
            AND archive.state NOT IN ('deleting','deleted')
            AND (SELECT count(*) FROM chain_heads) <= 1
            AND ${checkpoint.previousCheckpointSha256} IS NOT DISTINCT FROM (
              SELECT max(chain_heads.checkpoint_hash) FROM chain_heads
            )
            AND NOT EXISTS (
              SELECT 1 FROM worker_checkpoints prior
              WHERE prior.consultation_id=${input.consultationId}
                AND prior.generation=${input.generation}
                AND prior.worker_id=${input.workerId}
                AND prior.worker_epoch=${checkpoint.workerEpoch}
                AND prior.source_participant_id=${persisted.sourceParticipantId}
                AND prior.destination_participant_id=${persisted.destinationParticipantId}
                AND prior.terminal
            )
          ON CONFLICT DO NOTHING RETURNING id`,
        );
        if (result.length !== 1) {
          throw new DomainError("CHECKPOINT_CONFLICT");
        }
        return true;
      }),
    );
  }

  async providerAttempt(input: ProviderAttemptInput): Promise<boolean> {
    const report = input.report;
    const retryOfAttemptId = report.retryOfAttemptId ?? null;
    const contextRows = await this.database.$queryRaw<Record<string, unknown>[]>(
      Prisma.sql`SELECT selection.selection,selection.profile_id,selection.profile_revision,profile.name AS profile_name,archive.id AS archive_id,capability.capability_version
          FROM consultations consultation
          JOIN room_provider_selections selection ON selection.consultation_id=consultation.id
          JOIN provider_profiles profile ON profile.id=selection.profile_id
          JOIN archives archive ON archive.consultation_id=consultation.id
          JOIN worker_reservations reservation ON reservation.consultation_id=consultation.id AND reservation.generation=consultation.generation
          JOIN worker_job_epochs job ON job.consultation_id=consultation.id AND job.generation=consultation.generation
            AND job.worker_id=reservation.worker_id AND job.epoch=reservation.epoch
          JOIN language_capabilities capability ON capability.id=${report.directionId}
            AND capability.profile_id=selection.profile_id AND capability.revision=selection.profile_revision
          WHERE consultation.id=${input.consultationId} AND consultation.generation=${input.generation}
            AND consultation.worker_identity=${input.workerId}
            AND reservation.worker_id=${input.workerId} AND reservation.epoch=${input.epoch}
            AND reservation.selection_hash=selection.selection_hash
            AND reservation.fenced_at IS NULL AND reservation.released_at IS NULL
            AND job.fenced_at IS NULL AND job.terminal_at IS NULL
            AND job.write_epoch=archive.write_epoch
            AND archive.state NOT IN ('deleting','deleted')`,
    );
    const context = contextRows[0];
    if (!context) {
      throw new DomainError("PROVIDER_ATTEMPT_FENCED");
    }

    const selection = RoomProviderSelectionSchema.parse(context.selection);
    if (
      selection.profileRevision !== Number(context.profile_revision) ||
      selection.profileId !== context.profile_name
    ) {
      throw new DomainError("PROVIDER_SELECTION_MISMATCH");
    }
    const direction = selection.directions.find(
      (candidate) => candidate.capabilityRowId === report.directionId,
    );
    if (!direction) {
      throw new DomainError("PROVIDER_DIRECTION_MISMATCH");
    }
    const stageSelection =
      report.stage === "stt"
        ? direction.stt
        : direction.mode === "translated"
          ? direction[report.stage]
          : undefined;
    if (!stageSelection) {
      throw new DomainError("PROVIDER_STAGE_MISMATCH");
    }
    if (stageSelection.credential.version !== report.credentialVersion) {
      throw new DomainError("PROVIDER_CREDENTIAL_MISMATCH");
    }

    const error = report.error;
    const watermarks = report.watermarks;
    const acceptedInputWatermark =
      watermarks.acceptedInputSampleEnd ?? watermarks.acceptedInputSequence;
    const receivedOutputWatermark =
      watermarks.receivedOutputSampleEnd ?? watermarks.receivedOutputSequence;
    const emittedOutputWatermark =
      watermarks.emittedOutputSampleEnd ?? watermarks.emittedOutputSequence;
    const retryDecision = JSON.stringify(report.retryDecision);
    const rawTransport = JSON.stringify(report.rawReferences);
    const rawHttp = report.transport === "http" ? rawTransport : null;
    const rawWebsocket = report.transport === "websocket" ? rawTransport : null;
    const rawGrpc = report.transport === "grpc" ? rawTransport : null;
    const voice = "voice" in stageSelection ? stageSelection.voice : null;
    const apiVersion = String(context.capability_version);

    const inserted = await retryPostgresContention(() =>
      this.database.$queryRaw<Record<string, unknown>[]>(
        Prisma.sql`INSERT INTO provider_attempts(
            id,archive_id,consultation_id,profile_id,profile_revision,stage,provider,direction_id,
            operation_id,attempt_number,retry_of,credential_reference,credential_version,
            credential_fingerprint,endpoint,api_version,model,voice,outcome,error_kind,error_scope,
            provider_retry_advice,provider_code,provider_request_id,retry_delay_ms,
            accepted_input_watermark,received_output_watermark,emitted_output_watermark,
            retry_decision,transport,raw_http,raw_websocket,raw_grpc,terminal_hash,started_at,terminal_at
          )
          SELECT ${report.attemptId},archive.id,${input.consultationId},selection.profile_id,
            selection.profile_revision,${report.stage},${stageSelection.provider},${report.directionId},
            ${report.operationId},${report.attemptNumber},${retryOfAttemptId},
            ${stageSelection.credential.reference},${report.credentialVersion},
            ${report.credentialFingerprint},${stageSelection.endpoint},${apiVersion},
            ${stageSelection.model},${voice},${report.outcome},${error?.kind ?? null},
            ${error?.scope ?? null},${error?.providerRetryAdvice ?? null},
            ${error?.providerCode ?? null},${error?.providerRequestId ?? null},
            ${error?.retryDelayMs ?? null},${acceptedInputWatermark},${receivedOutputWatermark},
            ${emittedOutputWatermark},${retryDecision}::jsonb,${report.transport},
            ${rawHttp}::jsonb,${rawWebsocket}::jsonb,${rawGrpc}::jsonb,${report.terminalHash},
            to_timestamp(${report.startedAtMs}/1000.0),to_timestamp(${report.occurredAtMs}/1000.0)
          FROM consultations consultation
          JOIN room_provider_selections selection ON selection.consultation_id=consultation.id
          JOIN archives archive ON archive.consultation_id=consultation.id
          JOIN worker_reservations reservation ON reservation.consultation_id=consultation.id
            AND reservation.generation=consultation.generation
          JOIN worker_job_epochs job ON job.consultation_id=consultation.id
            AND job.generation=consultation.generation AND job.worker_id=reservation.worker_id
            AND job.epoch=reservation.epoch
          LEFT JOIN provider_attempts predecessor ON predecessor.id=${retryOfAttemptId}
          WHERE consultation.id=${input.consultationId} AND consultation.generation=${input.generation}
            AND consultation.worker_identity=${input.workerId}
            AND reservation.worker_id=${input.workerId} AND reservation.epoch=${input.epoch}
            AND reservation.selection_hash=selection.selection_hash
            AND reservation.fenced_at IS NULL AND reservation.released_at IS NULL
            AND job.fenced_at IS NULL AND job.terminal_at IS NULL
            AND job.write_epoch=archive.write_epoch AND archive.state NOT IN ('deleting','deleted')
            AND (
              (${report.attemptNumber}=1 AND ${retryOfAttemptId}::uuid IS NULL)
              OR (${report.attemptNumber}>1 AND predecessor.id=${retryOfAttemptId}
                AND predecessor.consultation_id=${input.consultationId}
                AND predecessor.direction_id=${report.directionId}
                AND predecessor.operation_id=${report.operationId}
                AND predecessor.attempt_number=${report.attemptNumber - 1})
            )
          ON CONFLICT DO NOTHING
          RETURNING id`,
      ),
    );
    if (inserted.length === 1) {
      return true;
    }

    const replay = await this.database.$queryRaw<Record<string, unknown>[]>(
      Prisma.sql`SELECT 1 FROM provider_attempts WHERE id=${report.attemptId}
          AND archive_id=${String(context.archive_id)} AND consultation_id=${input.consultationId}
          AND profile_id=${String(context.profile_id)}
          AND profile_revision=${Number(context.profile_revision)} AND stage=${report.stage}
          AND provider=${stageSelection.provider} AND direction_id=${report.directionId}
          AND operation_id=${report.operationId} AND attempt_number=${report.attemptNumber}
          AND retry_of IS NOT DISTINCT FROM ${retryOfAttemptId}
          AND credential_reference=${stageSelection.credential.reference}
          AND credential_version=${report.credentialVersion}
          AND credential_fingerprint=${report.credentialFingerprint}
          AND endpoint=${stageSelection.endpoint} AND api_version=${apiVersion}
          AND model=${stageSelection.model} AND voice IS NOT DISTINCT FROM ${voice}
          AND outcome=${report.outcome} AND error_kind IS NOT DISTINCT FROM ${error?.kind ?? null}
          AND error_scope IS NOT DISTINCT FROM ${error?.scope ?? null}
          AND provider_retry_advice IS NOT DISTINCT FROM ${error?.providerRetryAdvice ?? null}
          AND provider_code IS NOT DISTINCT FROM ${error?.providerCode ?? null}
          AND provider_request_id IS NOT DISTINCT FROM ${error?.providerRequestId ?? null}
          AND retry_delay_ms IS NOT DISTINCT FROM ${error?.retryDelayMs ?? null}
          AND accepted_input_watermark IS NOT DISTINCT FROM ${acceptedInputWatermark}
          AND received_output_watermark IS NOT DISTINCT FROM ${receivedOutputWatermark}
          AND emitted_output_watermark IS NOT DISTINCT FROM ${emittedOutputWatermark}
          AND retry_decision=${retryDecision}::jsonb AND transport=${report.transport}
          AND raw_http IS NOT DISTINCT FROM ${rawHttp}::jsonb
          AND raw_websocket IS NOT DISTINCT FROM ${rawWebsocket}::jsonb
          AND raw_grpc IS NOT DISTINCT FROM ${rawGrpc}::jsonb
          AND terminal_hash=${report.terminalHash}
          AND started_at=to_timestamp(${report.startedAtMs}/1000.0)
          AND terminal_at=to_timestamp(${report.occurredAtMs}/1000.0)`,
    );
    if (replay.length === 1) {
      return true;
    }
    throw new DomainError("PROVIDER_ATTEMPT_CONFLICT");
  }

  async expiredWorkerEpochs(): Promise<readonly WorkerEpochTuple[]> {
    const rows = await this.database.$queryRaw<
      {
        consultation_id: UUID;
        generation: number;
        worker_id: UUID;
        epoch: bigint;
        write_epoch: number;
      }[]
    >(Prisma.sql`SELECT reservation.consultation_id,reservation.generation,
        reservation.worker_id,reservation.epoch,job.write_epoch
      FROM worker_reservations reservation
      JOIN consultations consultation ON consultation.id=reservation.consultation_id
        AND consultation.generation=reservation.generation
      JOIN worker_job_epochs job ON job.consultation_id=reservation.consultation_id
        AND job.generation=reservation.generation AND job.worker_id=reservation.worker_id
        AND job.epoch=reservation.epoch
      JOIN archives archive ON archive.consultation_id=reservation.consultation_id
      WHERE reservation.lease_expires_at < now()
        AND reservation.released_at IS NULL AND reservation.fenced_at IS NULL
        AND job.fenced_at IS NULL AND job.terminal_at IS NULL
        AND job.write_epoch=archive.write_epoch
      ORDER BY reservation.lease_expires_at,reservation.consultation_id`);
    return rows.map((row) => ({
      consultationId: row.consultation_id,
      generation: row.generation,
      workerId: row.worker_id,
      epoch: safeDatabaseInteger(row.epoch, "worker_reservations.epoch"),
      writeEpoch: row.write_epoch,
    }));
  }

  async completeWorkerEpoch(input: CompleteWorkerEpochInput): Promise<boolean> {
    const request = JSON.stringify(input);
    const [first, second] = input.terminalCheckpoints;
    if (
      first.checkpointId === second.checkpointId ||
      (input.outcome === "clean") !== (input.failure === null)
    ) {
      throw new DomainError("WORKER_COMPLETION_CONFLICT");
    }
    return retryPostgresContention(() =>
      this.database.$transaction(async (transaction) => {
        const replay = await transaction.$queryRaw<Record<string, unknown>[]>(
          Prisma.sql`SELECT 1 FROM audit_events WHERE id=${input.completionEventId}
            AND aggregate_id=${input.consultationId} AND kind='worker.epoch_completed'
            AND details=${request}::jsonb`,
        );
        if (replay.length === 1) {
          return true;
        }
        const completed = await transaction.$queryRaw<
          { terminal_at: Date; next_generation: number }[]
        >(Prisma.sql`WITH locked AS (
            SELECT reservation.consultation_id,reservation.generation,reservation.worker_id,
              reservation.epoch,job.write_epoch
            FROM worker_reservations reservation
            JOIN consultations consultation ON consultation.id=reservation.consultation_id
              AND consultation.generation=reservation.generation
            JOIN worker_job_epochs job ON job.consultation_id=reservation.consultation_id
              AND job.generation=reservation.generation AND job.worker_id=reservation.worker_id
              AND job.epoch=reservation.epoch
            JOIN archives archive ON archive.consultation_id=reservation.consultation_id
            WHERE reservation.consultation_id=${input.consultationId}
              AND reservation.generation=${input.generation}
              AND reservation.worker_id=${input.workerId} AND reservation.epoch=${input.epoch}
              AND reservation.released_at IS NULL AND reservation.fenced_at IS NULL
              AND job.fenced_at IS NULL AND job.terminal_at IS NULL
              AND job.write_epoch=${input.writeEpoch} AND archive.write_epoch=${input.writeEpoch}
              AND archive.state NOT IN ('deleting','deleted')
            FOR UPDATE OF consultation,reservation,job
          ), accepted_pair AS (
            SELECT min(checkpoint.id::text)::uuid AS designated_id,
              min(checkpoint.created_at) FILTER (
                WHERE checkpoint.id::text=(SELECT min(candidate.id::text)
                  FROM worker_checkpoints candidate
                  WHERE candidate.id IN (${first.checkpointId},${second.checkpointId}))
              ) AS terminal_at
            FROM worker_checkpoints checkpoint,locked
            WHERE checkpoint.id IN (${first.checkpointId},${second.checkpointId})
              AND checkpoint.consultation_id=locked.consultation_id
              AND checkpoint.generation=locked.generation AND checkpoint.worker_id=locked.worker_id
              AND checkpoint.worker_epoch=locked.epoch AND checkpoint.write_epoch=locked.write_epoch
              AND checkpoint.terminal
              AND ((checkpoint.id=${first.checkpointId} AND checkpoint.checkpoint_hash=${first.checkpointHash})
                OR (checkpoint.id=${second.checkpointId} AND checkpoint.checkpoint_hash=${second.checkpointHash}))
              AND EXISTS (
                SELECT 1 FROM room_provider_selections selection
                CROSS JOIN LATERAL jsonb_array_elements(selection.selection->'directions') direction
                WHERE selection.consultation_id=locked.consultation_id
                  AND (direction->>'sourceParticipantId')::uuid=checkpoint.source_participant_id
                  AND (direction->>'destinationParticipantId')::uuid=checkpoint.destination_participant_id
              )
            HAVING count(*)=2 AND count(DISTINCT checkpoint.source_participant_id)=2
              AND count(DISTINCT checkpoint.destination_participant_id)=2
          ), terminalized AS (
            UPDATE worker_job_epochs job SET terminal_checkpoint_id=accepted_pair.designated_id,
              terminal_outcome=${input.outcome},terminal_at=accepted_pair.terminal_at
            FROM locked,accepted_pair
            WHERE job.consultation_id=locked.consultation_id AND job.generation=locked.generation
              AND job.worker_id=locked.worker_id AND job.epoch=locked.epoch
              AND accepted_pair.terminal_at IS NOT NULL
            RETURNING job.consultation_id,job.generation,job.worker_id,job.epoch,job.terminal_at
          ), released AS (
            UPDATE worker_reservations reservation SET accepting_load=false,
              released_at=terminalized.terminal_at
            FROM terminalized WHERE reservation.consultation_id=terminalized.consultation_id
              AND reservation.generation=terminalized.generation
              AND reservation.worker_id=terminalized.worker_id AND reservation.epoch=terminalized.epoch
            RETURNING terminalized.*
          ), advanced AS (
            UPDATE consultations consultation SET state='finalizing',generation=consultation.generation+1,
              finalize_deadline_at=COALESCE(consultation.finalize_deadline_at,now()+interval '15 minutes'),
              updated_at=now()
            FROM released WHERE ${input.outcome}='failed'
              AND consultation.id=released.consultation_id AND consultation.generation=released.generation
            RETURNING consultation.generation
          ) SELECT released.terminal_at,
            COALESCE((SELECT generation FROM advanced),released.generation) AS next_generation
          FROM released`);
        const terminal = completed[0];
        if (!terminal) {
          throw new DomainError("WORKER_COMPLETION_FENCED");
        }
        if (input.outcome === "failed") {
          await transaction.$executeRaw(
            Prisma.sql`UPDATE archives SET state='reconciling',
              reconciliation_deadline_at=COALESCE(reconciliation_deadline_at,now()+interval '30 minutes'),
              updated_at=now() WHERE consultation_id=${input.consultationId}
              AND state IN ('pending','recording','reconciling')`,
          );
          await transaction.$executeRaw(
            Prisma.sql`INSERT INTO outbox(id,topic,aggregate_id,generation,payload,available_at,attempts)
              VALUES (${input.completionEventId},'archive.failed',${input.consultationId},
                ${terminal.next_generation},${JSON.stringify({
                  reasonCode: "ARCHIVE_FAILED",
                  egressId: input.workerId,
                  resourceGeneration: input.generation,
                })}::jsonb,now(),0)`,
          );
        }
        await transaction.$executeRaw(
          Prisma.sql`INSERT INTO audit_events(id,aggregate_id,actor_id,kind,occurred_at,details)
            VALUES (${input.completionEventId},${input.consultationId},NULL,
              'worker.epoch_completed',${terminal.terminal_at},${request}::jsonb)`,
        );
        return true;
      }),
    );
  }

  async abandonWorkerEpoch(input: AbandonWorkerEpochInput): Promise<boolean> {
    if ((input.sealId === undefined) !== (input.completionEventId === undefined)) {
      throw new DomainError("WORKER_ABANDONMENT_CONFLICT");
    }
    const request = JSON.stringify(input);
    return retryPostgresContention(() =>
      this.database.$transaction(async (transaction) => {
        const replay = await transaction.$queryRaw<Record<string, unknown>[]>(
          Prisma.sql`SELECT 1 FROM audit_events WHERE id=${input.abandonmentEventId}
            AND aggregate_id=${input.consultationId} AND kind='worker.epoch_abandoned'
            AND details=${request}::jsonb`,
        );
        if (replay.length === 1) {
          return true;
        }
        const locked = await transaction.$queryRaw<Record<string, unknown>[]>(
          Prisma.sql`SELECT reservation.consultation_id FROM worker_reservations reservation
            JOIN consultations consultation ON consultation.id=reservation.consultation_id
              AND consultation.generation=reservation.generation
            JOIN worker_job_epochs job ON job.consultation_id=reservation.consultation_id
              AND job.generation=reservation.generation AND job.worker_id=reservation.worker_id
              AND job.epoch=reservation.epoch
            JOIN archives archive ON archive.consultation_id=reservation.consultation_id
            WHERE reservation.consultation_id=${input.consultationId}
              AND reservation.generation=${input.generation}
              AND reservation.worker_id=${input.workerId} AND reservation.epoch=${input.epoch}
              AND reservation.lease_expires_at < now() AND reservation.released_at IS NULL
              AND reservation.fenced_at IS NULL AND job.fenced_at IS NULL AND job.terminal_at IS NULL
              AND job.write_epoch=${input.writeEpoch} AND archive.write_epoch=${input.writeEpoch}
              AND (SELECT count(*) FROM worker_checkpoints checkpoint
                WHERE checkpoint.consultation_id=reservation.consultation_id
                  AND checkpoint.generation=reservation.generation
                  AND checkpoint.worker_id=reservation.worker_id
                  AND checkpoint.worker_epoch=reservation.epoch AND checkpoint.terminal) < 2
            FOR UPDATE OF consultation,reservation,job`,
        );
        if (locked.length !== 1) {
          throw new DomainError("WORKER_ABANDONMENT_FENCED");
        }
        const directions = await transaction.$queryRaw<
          { source_participant_id: UUID; destination_participant_id: UUID }[]
        >(Prisma.sql`SELECT (direction->>'sourceParticipantId')::uuid AS source_participant_id,
            (direction->>'destinationParticipantId')::uuid AS destination_participant_id
          FROM room_provider_selections selection
          CROSS JOIN LATERAL jsonb_array_elements(selection.selection->'directions') direction
          WHERE selection.consultation_id=${input.consultationId}
          ORDER BY source_participant_id`);
        if (directions.length !== 2) {
          throw new DomainError("WORKER_ABANDONMENT_CONFLICT");
        }
        const checkpoints = directions.map((direction) => {
          const seed = `${input.abandonmentEventId}:${direction.source_participant_id}:${direction.destination_participant_id}`;
          const bytes = createHash("sha256").update(seed).digest().subarray(0, 16);
          bytes[6] = ((bytes[6] ?? 0) & 0x0f) | 0x50;
          bytes[8] = ((bytes[8] ?? 0) & 0x3f) | 0x80;
          const hex = bytes.toString("hex");
          const id = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
          return {
            ...direction,
            id,
            hash: createHash("sha256")
              .update(`${seed}:${input.handoffDigest}:${input.permanentOutcomeDigest}`)
              .digest("hex"),
          };
        });
        for (const checkpoint of checkpoints) {
          await transaction.$executeRaw(
            Prisma.sql`WITH previous AS (
                SELECT accepted_input_sequence,accepted_input,received_output,emitted_output,
                  checkpoint_hash,expected_ids,observed_ids,gaps
                FROM worker_checkpoints
                WHERE consultation_id=${input.consultationId} AND generation=${input.generation}
                  AND worker_id=${input.workerId} AND worker_epoch=${input.epoch}
                  AND source_participant_id=${checkpoint.source_participant_id}
                  AND destination_participant_id=${checkpoint.destination_participant_id}
                ORDER BY accepted_input_sequence DESC,accepted_input DESC LIMIT 1
                FOR UPDATE
              ) INSERT INTO worker_checkpoints(id,consultation_id,generation,worker_id,
                worker_epoch,write_epoch,source_participant_id,destination_participant_id,
                accepted_input_sequence,accepted_input,received_output,emitted_output,
                previous_hash,checkpoint_hash,expected_ids,observed_ids,gaps,terminal,
                object_key,object_version_id,created_at)
              SELECT ${checkpoint.id},${input.consultationId},${input.generation},${input.workerId},
                ${input.epoch},${input.writeEpoch},${checkpoint.source_participant_id},
                ${checkpoint.destination_participant_id},COALESCE(previous.accepted_input_sequence+1,0),
                COALESCE(previous.accepted_input+1,0),COALESCE(previous.received_output,0),
                COALESCE(previous.emitted_output,0),previous.checkpoint_hash,${checkpoint.hash},
                COALESCE(previous.expected_ids,'[]'::jsonb),COALESCE(previous.observed_ids,'[]'::jsonb),
                COALESCE(previous.gaps,'[]'::jsonb) || ${JSON.stringify([{ reason: input.reason, sampleStart: null, sampleEnd: null }])}::jsonb,
                true,${`v1/meetings/${input.consultationId}/inventory/checkpoints/supervisor-${checkpoint.id}.json`},
                NULL,now() FROM (SELECT 1) seed LEFT JOIN previous ON true`,
          );
        }
        const designated = [...checkpoints].sort((left, right) =>
          left.id.localeCompare(right.id),
        )[0];
        if (!designated) {
          throw new DomainError("WORKER_ABANDONMENT_CONFLICT");
        }
        const terminalized = await transaction.$queryRaw<{ next_generation: number }[]>(
          Prisma.sql`WITH terminalized AS (
              UPDATE worker_job_epochs SET fenced_at=now(),terminal_checkpoint_id=${designated.id},
                terminal_outcome='failed',terminal_at=(SELECT created_at FROM worker_checkpoints WHERE id=${designated.id})
              WHERE consultation_id=${input.consultationId} AND generation=${input.generation}
                AND worker_id=${input.workerId} AND epoch=${input.epoch} AND terminal_at IS NULL
              RETURNING consultation_id,generation,worker_id,epoch,terminal_at
            ), released AS (
              UPDATE worker_reservations reservation SET accepting_load=false,fenced_at=now(),
                fence_reason=${input.reason},released_at=terminalized.terminal_at
              FROM terminalized WHERE reservation.consultation_id=terminalized.consultation_id
                AND reservation.generation=terminalized.generation
                AND reservation.worker_id=terminalized.worker_id AND reservation.epoch=terminalized.epoch
              RETURNING terminalized.*
            ) UPDATE consultations consultation SET state='finalizing',generation=consultation.generation+1,
              finalize_deadline_at=COALESCE(consultation.finalize_deadline_at,now()+interval '15 minutes'),updated_at=now()
            FROM released WHERE consultation.id=released.consultation_id
              AND consultation.generation=released.generation RETURNING consultation.generation AS next_generation`,
        );
        const terminal = terminalized[0];
        if (!terminal) {
          throw new DomainError("WORKER_ABANDONMENT_FENCED");
        }
        await transaction.$executeRaw(
          Prisma.sql`UPDATE archives SET state='reconciling',
            reconciliation_deadline_at=COALESCE(reconciliation_deadline_at,now()+interval '30 minutes'),
            updated_at=now() WHERE consultation_id=${input.consultationId}
            AND state IN ('pending','recording','reconciling')`,
        );
        await transaction.$executeRaw(
          Prisma.sql`INSERT INTO audit_events(id,aggregate_id,actor_id,kind,occurred_at,details)
            VALUES (${input.abandonmentEventId},${input.consultationId},NULL,
              'worker.epoch_abandoned',now(),${request}::jsonb)`,
        );
        await transaction.$executeRaw(
          Prisma.sql`INSERT INTO outbox(id,topic,aggregate_id,generation,payload,available_at,attempts)
            VALUES (${input.abandonmentEventId},'archive.failed',${input.consultationId},
              ${terminal.next_generation},${JSON.stringify({
                reasonCode: "ARCHIVE_FAILED",
                egressId: input.workerId,
                resourceGeneration: input.generation,
              })}::jsonb,now(),0)`,
        );
        return true;
      }),
    );
  }
}
