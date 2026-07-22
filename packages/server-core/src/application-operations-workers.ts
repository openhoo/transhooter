import { RoomProviderSelectionSchema, type WorkerCheckpoint } from "@transhooter/contracts";
import { and, eq, sql } from "drizzle-orm";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";
import type {
  CheckpointInput,
  ProviderAttemptInput,
  WorkerFailureInput,
} from "./application-operations";
import { type Clock, DomainError, type UUID } from "./domain/model";
import type { DrizzleSchema } from "./persistence/repositories";
import { consultations, workerJobEpochs } from "./persistence/schema";

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

function postgresErrorCode(error: unknown): string | null {
  let current = error;
  for (let depth = 0; depth < 4; depth += 1) {
    if (typeof current !== "object" || current === null) {
      return null;
    }
    if ("code" in current && typeof current.code === "string") {
      return current.code;
    }
    current = "cause" in current ? current.cause : null;
  }
  return null;
}

async function retryPostgresContention<T>(operation: () => Promise<T>): Promise<T> {
  for (let attempt = 1; ; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      const code = postgresErrorCode(error);
      if (attempt >= 5 || (code !== "40P01" && code !== "40001")) {
        throw error;
      }
      const backoffMs = Math.min(250, 10 * 2 ** (attempt - 1)) + Math.floor(Math.random() * 10);
      await new Promise<void>((resolve) => setTimeout(resolve, backoffMs));
    }
  }
}

export class ApplicationOperationsWorkers {
  constructor(
    private readonly database: NodePgDatabase<DrizzleSchema>,
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
    const result = await this.database.execute(
      sql`WITH active_job AS (
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
    return result.rowCount === 1;
  }

  async checkpoint(input: CheckpointInput): Promise<boolean> {
    const checkpoint = input.checkpoint;
    const { expected, observed, gaps } = serializeCheckpointObjectIds(checkpoint);
    const persisted = checkpointPersistenceValues(checkpoint);
    return retryPostgresContention(() =>
      this.database.transaction(async (transaction) => {
        const consultation = await transaction
          .select({ id: consultations.id })
          .from(consultations)
          .where(
            and(
              eq(consultations.id, input.consultationId),
              eq(consultations.generation, input.generation),
            ),
          )
          .for("update");
        if (consultation.length !== 1) {
          throw new DomainError("CHECKPOINT_CONFLICT");
        }

        const parents = await transaction
          .select({ consultationId: workerJobEpochs.consultationId })
          .from(workerJobEpochs)
          .where(
            and(
              eq(workerJobEpochs.consultationId, input.consultationId),
              eq(workerJobEpochs.generation, input.generation),
              eq(workerJobEpochs.workerId, input.workerId),
              eq(workerJobEpochs.epoch, checkpoint.workerEpoch),
            ),
          )
          .for("update");
        if (parents.length !== 1) {
          throw new DomainError("CHECKPOINT_CONFLICT");
        }

        const result = await transaction.execute(
          sql`WITH chain_heads AS (
            SELECT head.checkpoint_hash
            FROM worker_checkpoints head
            WHERE head.consultation_id=${input.consultationId}
              AND head.generation=${input.generation}
              AND head.worker_id=${input.workerId}
              AND head.worker_epoch=${checkpoint.workerEpoch}
              AND head.source_participant_id=${persisted.sourceParticipantId}
              AND head.destination_participant_id=${persisted.destinationParticipantId}
              AND NOT EXISTS (
                SELECT 1
                FROM worker_checkpoints child
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
            AND archive.write_epoch=${input.writeEpoch}
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

        if (result.rowCount !== 1) {
          const replay = await transaction.execute(
            sql`SELECT 1 FROM worker_checkpoints checkpoint
              JOIN consultations consultation ON consultation.id=checkpoint.consultation_id
                AND consultation.generation=checkpoint.generation
              JOIN worker_reservations reservation ON reservation.consultation_id=checkpoint.consultation_id
                AND reservation.generation=checkpoint.generation
                AND reservation.worker_id=checkpoint.worker_id AND reservation.epoch=checkpoint.worker_epoch
              JOIN worker_job_epochs job ON job.consultation_id=checkpoint.consultation_id
                AND job.generation=checkpoint.generation AND job.worker_id=checkpoint.worker_id
                AND job.epoch=checkpoint.worker_epoch
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
                AND checkpoint.expected_ids=${expected}::jsonb AND checkpoint.observed_ids=${observed}::jsonb
                AND checkpoint.gaps=${gaps}::jsonb AND checkpoint.terminal=${checkpoint.terminal}
                AND checkpoint.object_key=${input.objectKey} AND checkpoint.created_at=${persisted.createdAt}
                AND reservation.fenced_at IS NULL AND job.fenced_at IS NULL
                AND (
                  (reservation.released_at IS NULL AND job.terminal_at IS NULL)
                  OR (
                    checkpoint.terminal
                    AND reservation.released_at IS NOT NULL
                    AND job.terminal_at IS NOT NULL
                    AND job.terminal_outcome='clean'
                  )
                )`,
          );
          if (replay.rowCount !== 1) {
            throw new DomainError("CHECKPOINT_CONFLICT");
          }
        }

        if (checkpoint.terminal) {
          await transaction.execute(
            sql`WITH frozen_directions AS (
              SELECT direction->>'sourceParticipantId' AS source_participant_id,
                direction->>'destinationParticipantId' AS destination_participant_id
              FROM room_provider_selections selection
              CROSS JOIN LATERAL jsonb_array_elements(selection.selection->'directions') direction
              WHERE selection.consultation_id=${input.consultationId}
            ), complete AS (
              SELECT count(*)=2 AND bool_and(EXISTS (
                SELECT 1 FROM worker_checkpoints terminal
                WHERE terminal.consultation_id=${input.consultationId}
                  AND terminal.generation=${input.generation}
                  AND terminal.worker_id=${input.workerId}
                  AND terminal.worker_epoch=${checkpoint.workerEpoch}
                  AND terminal.source_participant_id=frozen_directions.source_participant_id::uuid
                  AND terminal.destination_participant_id=frozen_directions.destination_participant_id::uuid
                  AND terminal.terminal
              )) AS settled
              FROM frozen_directions
            ), terminal_epoch AS (
              UPDATE worker_job_epochs job
              SET terminal_checkpoint_id=${checkpoint.checkpointId},
                terminal_outcome=COALESCE(job.terminal_outcome,'clean'),
                terminal_at=COALESCE(job.terminal_at,to_timestamp(${checkpoint.occurredAtMs}/1000.0))
              FROM complete,consultations consultation
              WHERE complete.settled AND consultation.id=${input.consultationId}
                AND consultation.generation=${input.generation}
                AND job.consultation_id=consultation.id AND job.generation=consultation.generation
                AND job.worker_id=${input.workerId} AND job.epoch=${checkpoint.workerEpoch}
                AND job.fenced_at IS NULL AND job.terminal_at IS NULL
              RETURNING job.consultation_id,job.generation,job.worker_id,job.epoch,job.terminal_at
            )
            UPDATE worker_reservations reservation
            SET accepting_load=false,released_at=COALESCE(reservation.released_at,terminal_epoch.terminal_at)
            FROM terminal_epoch
            WHERE reservation.consultation_id=terminal_epoch.consultation_id
              AND reservation.generation=terminal_epoch.generation
              AND reservation.worker_id=terminal_epoch.worker_id
              AND reservation.epoch=terminal_epoch.epoch`,
          );
        }
        return true;
      }),
    );
  }

  async providerAttempt(input: ProviderAttemptInput): Promise<boolean> {
    const report = input.report;
    const contextResult = await this.database.execute(
      sql`SELECT selection.selection,selection.profile_id,selection.profile_revision,profile.name AS profile_name,archive.id AS archive_id,capability.capability_version
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
    const context = contextResult.rows[0];
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
      this.database.execute(
        sql`INSERT INTO provider_attempts(
            id,archive_id,consultation_id,profile_id,profile_revision,stage,provider,direction_id,
            operation_id,attempt_number,retry_of,credential_reference,credential_version,
            credential_fingerprint,endpoint,api_version,model,voice,outcome,error_kind,error_scope,
            provider_retry_advice,provider_code,provider_request_id,retry_delay_ms,
            accepted_input_watermark,received_output_watermark,emitted_output_watermark,
            retry_decision,transport,raw_http,raw_websocket,raw_grpc,terminal_hash,started_at,terminal_at
          )
          SELECT ${report.attemptId},archive.id,${input.consultationId},selection.profile_id,
            selection.profile_revision,${report.stage},${stageSelection.provider},${report.directionId},
            ${report.operationId},${report.attemptNumber},${report.retryOfAttemptId},
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
          LEFT JOIN provider_attempts predecessor ON predecessor.id=${report.retryOfAttemptId}
          WHERE consultation.id=${input.consultationId} AND consultation.generation=${input.generation}
            AND consultation.worker_identity=${input.workerId}
            AND reservation.worker_id=${input.workerId} AND reservation.epoch=${input.epoch}
            AND reservation.selection_hash=selection.selection_hash
            AND reservation.fenced_at IS NULL AND reservation.released_at IS NULL
            AND job.fenced_at IS NULL AND job.terminal_at IS NULL
            AND job.write_epoch=archive.write_epoch AND archive.state NOT IN ('deleting','deleted')
            AND (
              (${report.attemptNumber}=1 AND ${report.retryOfAttemptId}::uuid IS NULL)
              OR (${report.attemptNumber}>1 AND predecessor.id=${report.retryOfAttemptId}
                AND predecessor.consultation_id=${input.consultationId}
                AND predecessor.direction_id=${report.directionId}
                AND predecessor.operation_id=${report.operationId}
                AND predecessor.attempt_number=${report.attemptNumber - 1})
            )
          ON CONFLICT DO NOTHING
          RETURNING id`,
      ),
    );
    if (inserted.rowCount === 1) {
      return true;
    }

    const replay = await this.database.execute(
      sql`SELECT 1 FROM provider_attempts WHERE id=${report.attemptId}
          AND archive_id=${String(context.archive_id)} AND consultation_id=${input.consultationId}
          AND profile_id=${String(context.profile_id)}
          AND profile_revision=${Number(context.profile_revision)} AND stage=${report.stage}
          AND provider=${stageSelection.provider} AND direction_id=${report.directionId}
          AND operation_id=${report.operationId} AND attempt_number=${report.attemptNumber}
          AND retry_of IS NOT DISTINCT FROM ${report.retryOfAttemptId}
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
    if (replay.rowCount === 1) {
      return true;
    }
    throw new DomainError("PROVIDER_ATTEMPT_CONFLICT");
  }

  async workerFailure(input: WorkerFailureInput): Promise<boolean> {
    return this.database.transaction(async (transaction) => {
      const replay = await transaction.execute(
        sql`SELECT 1 FROM outbox WHERE id=${input.eventId} AND topic='archive.failed' AND aggregate_id=${input.consultationId}`,
      );
      if (replay.rowCount === 1) {
        return true;
      }
      const now = this.clock.now();
      const reason = {
        kind: input.kindName,
        message: input.message,
        phase: input.phase ?? null,
        snapshotHash: input.snapshotHash ?? null,
        lastCheckpointHashes: input.lastCheckpointHashes,
      };
      const fenced = await transaction.execute<{ generation: number }>(
        sql`WITH locked AS (
          SELECT consultation.generation
          FROM consultations consultation
          JOIN worker_reservations reservation
            ON reservation.consultation_id=consultation.id
            AND reservation.generation=${input.generation}
            AND reservation.worker_id=${input.workerId}
            AND reservation.epoch=${input.epoch}
          JOIN worker_job_epochs job
            ON job.consultation_id=reservation.consultation_id
            AND job.generation=reservation.generation
            AND job.worker_id=reservation.worker_id
            AND job.epoch=reservation.epoch
          WHERE consultation.id=${input.consultationId}
            AND consultation.generation=${input.generation}
            AND consultation.state IN ('ready','active')
            AND reservation.fenced_at IS NULL
            AND reservation.released_at IS NULL
            AND job.fenced_at IS NULL
            AND job.terminal_at IS NULL
          FOR UPDATE OF consultation,reservation,job
        ), stopped_reservation AS (
          UPDATE worker_reservations reservation
          SET accepting_load=false,lease_expires_at=${now},
            fence_reason=COALESCE(reservation.fence_reason,${input.message})
          FROM locked
          WHERE reservation.consultation_id=${input.consultationId}
            AND reservation.generation=${input.generation}
            AND reservation.worker_id=${input.workerId}
            AND reservation.epoch=${input.epoch}
          RETURNING reservation.consultation_id
        )
        UPDATE consultations consultation
        SET state='finalizing',generation=consultation.generation+1,
          finalize_deadline_at=${new Date(now.getTime() + 900_000)},updated_at=${now}
        FROM locked,stopped_reservation
        WHERE consultation.id=${input.consultationId}
        RETURNING consultation.generation`,
      );
      const nextGeneration = fenced.rows[0]?.generation;
      if (nextGeneration === undefined) {
        throw new DomainError("WORKER_FAILURE_FENCED");
      }
      await transaction.execute(
        sql`UPDATE archives SET state='reconciling',reconciliation_deadline_at=COALESCE(reconciliation_deadline_at,${new Date(now.getTime() + 1_800_000)}),updated_at=${now} WHERE consultation_id=${input.consultationId} AND state IN ('pending','recording','reconciling')`,
      );
      await transaction.execute(
        sql`INSERT INTO audit_events(id,aggregate_id,actor_id,kind,occurred_at,details)
          VALUES (${input.eventId},${input.consultationId},NULL,'worker.failure_reported',${now},${JSON.stringify(reason)}::jsonb)`,
      );
      await transaction.execute(
        sql`INSERT INTO outbox(id,topic,aggregate_id,generation,payload,available_at,attempts)
          VALUES (${input.eventId},'archive.failed',${input.consultationId},${nextGeneration},${JSON.stringify(
            {
              reasonCode: "ARCHIVE_FAILED",
              egressId: input.workerId,
              resourceGeneration: input.generation,
            },
          )}::jsonb,${now},0)`,
      );
      return true;
    });
  }
}
