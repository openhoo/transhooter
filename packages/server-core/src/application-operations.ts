import {
  type ProviderAttemptReport,
  RoomProviderSelectionSchema,
  type WorkerCheckpoint,
} from "@transhooter/contracts";
import { sql } from "drizzle-orm";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";
import { type Clock, DomainError, type StaffRole, type UUID } from "./domain/model";
import type { DrizzleSchema } from "./persistence/repositories";
import type { ObjectStoragePort } from "./ports/index";

export interface StaffPrincipal {
  userId: UUID;
  role: StaffRole;
}

export interface ArchiveObjectPage {
  objects: readonly Record<string, unknown>[];
  cursor: string | null;
}

export interface AdminLanguageRow {
  id: UUID;
  sourceLocale: string;
  targetLocale: string;
  mode: "translated" | "same_language";
  snapshot: unknown;
  profileName: string;
  revision: number;
  freshUntil: Date;
  enabled: boolean;
}

interface ActiveHoldProof {
  id: UUID;
  reason: string;
}

interface ProviderAttemptGroup {
  stage: "stt" | "translation" | "tts";
  provider: string;
  direction: string;
  attemptIds: readonly UUID[];
}

export interface ArchiveDetailProof {
  activeHolds: readonly ActiveHoldProof[];
  inventoryVersionId: string | null;
  inventorySha256: string | null;
  egressIds: readonly string[];
  providerAttemptIds: readonly UUID[];
  providerAttemptGroups: readonly ProviderAttemptGroup[];
}

export interface ArchiveDetail extends ArchiveDetailProof {
  id: UUID;
  consultationId: UUID;
  state: string;
  inventory: unknown;
}

export interface CheckpointInput {
  workerId: UUID;
  consultationId: UUID;
  generation: number;
  writeEpoch: number;
  objectKey: string;
  checkpoint: WorkerCheckpoint;
}

export interface WorkerFailureInput {
  consultationId: UUID;
  generation: number;
  workerId: UUID;
  epoch: number;
  eventId: UUID;
  kindName: string;
  message: string;
  phase?: string;
  snapshotHash?: string;
  lastCheckpointHashes: Readonly<Record<UUID, string>>;
}

export interface ProviderAttemptInput {
  consultationId: UUID;
  generation: number;
  workerId: UUID;
  epoch: number;
  eventId: UUID;
  report: ProviderAttemptReport;
}

export interface ApplicationOperations {
  consultationOptions(profileId: string): Promise<readonly Record<string, unknown>[]>;
  consultationRoom(consultationId: UUID, userId: UUID): Promise<Record<string, unknown>>;
  consultationInviteRecipient(consultationId: UUID, employeeUserId: UUID): Promise<string>;
  archiveList(principal: StaffPrincipal): Promise<readonly Record<string, unknown>[]>;
  archiveGet(principal: StaffPrincipal, archiveId: UUID): Promise<ArchiveDetail>;
  archiveObjects(
    principal: StaffPrincipal,
    archiveId: UUID,
    cursor: string | null,
    limit: number,
  ): Promise<ArchiveObjectPage>;
  archiveDownload(principal: StaffPrincipal, archiveId: UUID, objectId: UUID): Promise<string>;
  adminFailures(principal: StaffPrincipal): Promise<readonly Record<string, unknown>[]>;
  adminLanguages(
    principal: StaffPrincipal,
    profileId: string,
  ): Promise<readonly AdminLanguageRow[]>;
  egressLayout(consultationId: UUID, generation: number): Promise<Record<string, unknown>>;
  logout(sessionId: UUID, userId: UUID): Promise<void>;
  heartbeat(
    consultationId: UUID,
    generation: number,
    workerId: UUID,
    epoch: number,
  ): Promise<boolean>;
  checkpoint(input: CheckpointInput): Promise<boolean>;
  providerAttempt(input: ProviderAttemptInput): Promise<boolean>;
  workerFailure(input: WorkerFailureInput): Promise<boolean>;
}

function mapArchiveDetail(row: Record<string, unknown>): ArchiveDetail {
  return {
    id: row.id as UUID,
    consultationId: row.consultation_id as UUID,
    state: String(row.state),
    inventory: row.inventory ?? null,
    activeHolds: row.active_holds as ArchiveDetailProof["activeHolds"],
    inventoryVersionId: row.inventory_version_id as string | null,
    inventorySha256: row.sha256 as string | null,
    egressIds: row.egress_ids as string[],
    providerAttemptIds: row.provider_attempt_ids as UUID[],
    providerAttemptGroups:
      row.provider_attempt_groups as ArchiveDetailProof["providerAttemptGroups"],
  };
}

function mapAdminLanguage(row: Record<string, unknown>): AdminLanguageRow {
  return {
    id: row.id as UUID,
    sourceLocale: String(row.source_locale),
    targetLocale: String(row.target_locale),
    mode: row.mode as AdminLanguageRow["mode"],
    snapshot: row.snapshot,
    profileName: String(row.profile_name),
    revision: Number(row.revision),
    freshUntil: row.fresh_until as Date,
    enabled: Boolean(row.enabled),
  };
}

function presentArchiveObjectPage(
  rows: readonly Record<string, unknown>[],
  limit: number,
): ArchiveObjectPage {
  const hasMore = rows.length > limit;
  const objects = hasMore ? rows.slice(0, limit) : rows;
  return {
    objects,
    cursor: hasMore ? String(objects.at(-1)?.id) : null,
  };
}

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

export class DrizzleApplicationOperations implements ApplicationOperations {
  constructor(
    private readonly database: NodePgDatabase<DrizzleSchema>,
    private readonly storage: ObjectStoragePort,
    private readonly clock: Clock,
  ) {}

  async consultationOptions(profileId: string): Promise<readonly Record<string, unknown>[]> {
    const result = await this.database.execute(
      sql`SELECT l.id,p.id AS profile_id,l.source_locale,l.target_locale,l.mode,l.snapshot,p.name AS profile_name,l.revision,l.fresh_until FROM language_capabilities l JOIN provider_profiles p ON p.id=l.profile_id WHERE (p.id::text=${profileId} OR p.name=${profileId}) AND p.enabled AND l.enabled AND l.revision=p.current_revision AND l.fresh_until>now() ORDER BY l.source_locale,l.target_locale`,
    );
    return result.rows;
  }

  async consultationRoom(consultationId: UUID, userId: UUID): Promise<Record<string, unknown>> {
    const result = await this.database.execute(
      sql`SELECT c.id AS consultation_id,c.state,c.generation,c.worker_identity,c.room_sid,c.dispatch_id,c.composite_egress_id,mine.id AS participant_id,mine.livekit_identity AS participant_identity,mine.role,mine.display_name,other.id AS other_participant_id,other.livekit_identity AS other_identity,other.display_name AS other_display_name FROM consultations c JOIN consultation_participants mine ON mine.consultation_id=c.id AND mine.user_id=${userId} JOIN consultation_participants other ON other.consultation_id=c.id AND other.id<>mine.id WHERE c.id=${consultationId}`,
    );
    const row = result.rows[0];
    if (!row) {
      throw new DomainError("NOT_FOUND");
    }
    if (!row.worker_identity || !row.room_sid || !row.dispatch_id || !row.composite_egress_id) {
      throw new DomainError("PROVISIONING");
    }
    return row;
  }

  async consultationInviteRecipient(consultationId: UUID, employeeUserId: UUID): Promise<string> {
    const result = await this.database.execute(
      sql`SELECT customer.email FROM consultation_participants employee JOIN consultation_participants slot ON slot.consultation_id=employee.consultation_id AND slot.role='customer' JOIN users customer ON customer.id=slot.user_id JOIN consultations c ON c.id=employee.consultation_id WHERE c.id=${consultationId} AND c.state='invited' AND employee.user_id=${employeeUserId} AND employee.role='employee'`,
    );
    const email = result.rows[0]?.email;
    if (typeof email !== "string") {
      throw new DomainError("NOT_FOUND");
    }
    return email;
  }

  async archiveList(principal: StaffPrincipal): Promise<readonly Record<string, unknown>[]> {
    const result = await this.database.execute(
      sql`SELECT DISTINCT a.id,a.consultation_id,a.state,a.final_inventory_hash,a.updated_at FROM archives a LEFT JOIN consultation_participants p ON p.consultation_id=a.consultation_id AND p.user_id=${principal.userId} AND p.role='employee' WHERE ${principal.role}='admin' OR p.id IS NOT NULL ORDER BY a.updated_at DESC`,
    );
    return result.rows;
  }

  async archiveGet(principal: StaffPrincipal, routeId: UUID): Promise<ArchiveDetail> {
    const result = await this.database.execute(
      sql`SELECT a.id,a.consultation_id,a.state,f.inventory,f.sha256,inventory_object.version_id AS inventory_version_id,COALESCE((SELECT jsonb_agg(jsonb_build_object('id',h.id,'reason',h.reason) ORDER BY h.placed_at,h.id) FROM legal_holds h WHERE h.archive_id=a.id AND h.released_at IS NULL),'[]'::jsonb) AS active_holds,COALESCE((SELECT jsonb_agg(j.egress_id ORDER BY j.id) FROM egress_jobs j WHERE j.consultation_id=a.consultation_id AND j.egress_id IS NOT NULL),'[]'::jsonb) AS egress_ids,COALESCE((SELECT jsonb_agg(pa.id ORDER BY pa.id) FROM provider_attempts pa WHERE pa.archive_id=a.id),'[]'::jsonb) AS provider_attempt_ids,COALESCE((SELECT jsonb_agg(jsonb_build_object('stage',g.stage,'provider',g.provider,'direction',g.direction_id,'attemptIds',g.attempt_ids) ORDER BY g.stage,g.provider,g.direction_id) FROM (SELECT stage,provider,direction_id,jsonb_agg(id ORDER BY attempt_number,id) AS attempt_ids FROM provider_attempts WHERE archive_id=a.id GROUP BY stage,provider,direction_id) g),'[]'::jsonb) AS provider_attempt_groups FROM archives a LEFT JOIN consultation_participants p ON p.consultation_id=a.consultation_id AND p.user_id=${principal.userId} AND p.role='employee' LEFT JOIN final_inventories f ON f.archive_id=a.id LEFT JOIN archive_objects inventory_object ON inventory_object.id=f.object_id WHERE (a.id=${routeId} OR a.consultation_id=${routeId}) AND (${principal.role}='admin' OR p.id IS NOT NULL)`,
    );
    const row = result.rows[0];
    if (!row) {
      throw new DomainError("NOT_FOUND");
    }
    return mapArchiveDetail(row);
  }

  async archiveObjects(
    principal: StaffPrincipal,
    routeId: UUID,
    cursor: string | null,
    limit: number,
  ): Promise<ArchiveObjectPage> {
    const boundedLimit = Math.max(1, Math.min(limit, 100));
    const result = await this.database.execute(
      sql`SELECT DISTINCT o.* FROM archive_objects o JOIN archives a ON a.id=o.archive_id LEFT JOIN consultation_participants p ON p.consultation_id=a.consultation_id AND p.user_id=${principal.userId} AND p.role='employee' WHERE (a.id=${routeId} OR a.consultation_id=${routeId}) AND (${principal.role}='admin' OR p.id IS NOT NULL) AND (${cursor}::uuid IS NULL OR o.id>${cursor}::uuid) ORDER BY o.id LIMIT ${boundedLimit + 1}`,
    );
    return presentArchiveObjectPage(result.rows, boundedLimit);
  }

  async archiveDownload(principal: StaffPrincipal, routeId: UUID, objectId: UUID): Promise<string> {
    const result = await this.database.execute(
      sql`SELECT o.key,o.version_id FROM archive_objects o JOIN archives a ON a.id=o.archive_id LEFT JOIN consultation_participants p ON p.consultation_id=a.consultation_id AND p.user_id=${principal.userId} AND p.role='employee' WHERE (a.id=${routeId} OR a.consultation_id=${routeId}) AND o.id=${objectId} AND (${principal.role}='admin' OR p.id IS NOT NULL)`,
    );
    const row = result.rows[0];
    if (!row) {
      throw new DomainError("NOT_FOUND");
    }
    return this.storage.presignGet(String(row.key), String(row.version_id), 300);
  }

  async adminFailures(principal: StaffPrincipal): Promise<readonly Record<string, unknown>[]> {
    this.assertAdmin(principal);
    const result = await this.database.execute(
      sql`SELECT id,consultation_id,state,result,updated_at FROM external_effects WHERE state='failed' UNION ALL SELECT id,consultation_id,'failed',terminal_result,terminal_at FROM egress_jobs WHERE state IN ('EGRESS_FAILED','EGRESS_ABORTED','EGRESS_LIMIT_REACHED') ORDER BY updated_at DESC NULLS LAST`,
    );
    return result.rows;
  }

  async adminLanguages(
    principal: StaffPrincipal,
    profileId: string,
  ): Promise<readonly AdminLanguageRow[]> {
    this.assertAdmin(principal);
    const result = await this.database.execute(
      sql`SELECT l.id,l.source_locale,l.target_locale,l.mode,l.snapshot,p.name AS profile_name,l.revision,l.fresh_until,l.enabled FROM language_capabilities l JOIN provider_profiles p ON p.id=l.profile_id WHERE p.name=${profileId} AND l.revision=p.current_revision ORDER BY l.source_locale,l.target_locale,l.mode`,
    );
    return result.rows.map(mapAdminLanguage);
  }

  async egressLayout(consultationId: UUID, generation: number): Promise<Record<string, unknown>> {
    const result = await this.database.execute(
      sql`SELECT c.id,c.room_name,c.generation,jsonb_agg(jsonb_build_object('identity',p.livekit_identity,'role',p.role,'displayName',p.display_name) ORDER BY p.role) AS participants FROM consultations c JOIN consultation_participants p ON p.consultation_id=c.id WHERE c.id=${consultationId} AND c.generation=${generation} GROUP BY c.id`,
    );
    const row = result.rows[0];
    if (!row) {
      throw new DomainError("FENCED_GENERATION");
    }
    return row;
  }

  async logout(sessionId: UUID, userId: UUID): Promise<void> {
    await this.database.execute(
      sql`UPDATE sessions SET revoked_at=now() WHERE id=${sessionId} AND user_id=${userId} AND revoked_at IS NULL`,
    );
  }

  async heartbeat(
    consultationId: UUID,
    generation: number,
    workerId: UUID,
    epoch: number,
  ): Promise<boolean> {
    const now = this.clock.now();
    const leaseExpiresAt = new Date(now.getTime() + 30_000);
    const result = await this.database.execute(
      sql`WITH reservation AS (
        UPDATE worker_reservations SET heartbeat_at=${now},lease_expires_at=${leaseExpiresAt}
        WHERE consultation_id=${consultationId} AND generation=${generation} AND worker_id=${workerId} AND epoch=${epoch}
          AND accepting_load AND released_at IS NULL AND fenced_at IS NULL
        RETURNING consultation_id,generation,worker_id,epoch
      )
      UPDATE worker_job_epochs job SET heartbeat_at=${now}
      FROM reservation
      WHERE job.consultation_id=reservation.consultation_id AND job.generation=reservation.generation
        AND job.worker_id=reservation.worker_id AND job.epoch=reservation.epoch AND job.fenced_at IS NULL AND job.terminal_at IS NULL
      RETURNING job.worker_id`,
    );
    return result.rowCount === 1;
  }

  async checkpoint(input: CheckpointInput): Promise<boolean> {
    const checkpoint = input.checkpoint;
    const { expected, observed, gaps } = serializeCheckpointObjectIds(checkpoint);
    const persisted = checkpointPersistenceValues(checkpoint);
    const result = await retryPostgresContention(() =>
      this.database.execute(
        sql`INSERT INTO worker_checkpoints(id,consultation_id,generation,worker_id,worker_epoch,write_epoch,source_participant_id,destination_participant_id,accepted_input_sequence,high_watermark,received_output,emitted_output,previous_hash,checkpoint_hash,expected_ids,observed_ids,gaps,terminal,object_key,created_at)
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
        ON CONFLICT (id) DO NOTHING RETURNING id`,
      ),
    );

    if (result.rowCount !== 1) {
      const replay = await this.database.execute(
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
            AND checkpoint.high_watermark=${persisted.acceptedInput}
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
      await this.database.execute(
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

  private assertAdmin(principal: StaffPrincipal): void {
    if (principal.role !== "admin") {
      throw new DomainError("FORBIDDEN");
    }
  }
}
