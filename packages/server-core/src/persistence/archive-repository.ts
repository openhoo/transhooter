import { type FinalInventory, FinalInventorySchema } from "@transhooter/contracts";
import type { Consultation } from "../consultations/domain";
import { type ArchiveState, DomainError, type Instant, type UUID } from "../domain/model";
import type { ArchiveObject, ArchiveRepository, Transaction } from "../ports/index";
import { Prisma, type PrismaClient } from "./database";
import { TransactionHandle, unwrap } from "./transaction";

type ArchiveLockRow = {
  id: UUID;
  state: ArchiveState;
  consultation_state: Consultation["state"];
  write_epoch: number;
  completed_deletion_epoch: number | null;
  final_inventory_hash: string | null;
  reconciliation_deadline_at: Date | null;
};

type ArchiveObjectLockRow = {
  id: UUID;
  consultation_id: UUID;
  object_class: string;
  causal_key: string;
  key: string;
  version_id: string;
  size: bigint;
  sha256: string;
  s3_checksum: string;
  content_type: string;
  sample_start: bigint | null;
  sample_end: bigint | null;
  attempt: number | null;
  sequence: bigint | null;
  writer_epoch: number;
};

type IdRow = { id: UUID };

function safeDatabaseInteger(value: bigint | number, column: string): number {
  const converted = typeof value === "bigint" ? Number(value) : value;
  if (!Number.isSafeInteger(converted)) {
    throw new Error(`${column} is outside the JavaScript safe integer range`);
  }
  return converted;
}

function nullableSafeDatabaseInteger(value: bigint | number | null, column: string): number | null {
  return value === null ? null : safeDatabaseInteger(value, column);
}

export class PrismaArchiveRepository implements ArchiveRepository {
  constructor(private readonly database: PrismaClient) {}

  transaction<T>(work: (tx: Transaction) => Promise<T>): Promise<T> {
    return this.database.$transaction((database) => work(new TransactionHandle(database)), {
      maxWait: 5_000,
      timeout: 2_147_483_647,
    });
  }

  async lockByConsultation(consultationId: UUID, tx: Transaction) {
    const database = unwrap(tx);
    // Coupled consultation/archive operations always lock the consultation first.
    // This matches consultation lifecycle transactions and prevents inverse-order deadlocks.
    await database.$queryRaw<{ id: UUID }[]>(Prisma.sql`
      SELECT id
      FROM consultations
      WHERE id = ${consultationId}
      FOR UPDATE
    `);
    const rows = await database.$queryRaw<ArchiveLockRow[]>(Prisma.sql`
      SELECT
        a.id,
        a.state,
        c.state AS consultation_state,
        a.write_epoch,
        a.completed_deletion_epoch,
        a.final_inventory_hash,
        a.reconciliation_deadline_at
      FROM archives a
      JOIN consultations c ON c.id = a.consultation_id
      WHERE a.consultation_id = ${consultationId}
      FOR UPDATE OF a
    `);
    const row = rows[0];
    return row
      ? {
          id: row.id,
          state: row.state,
          consultationState: row.consultation_state,
          writeEpoch: Number(row.write_epoch),
          completedDeletionEpoch:
            row.completed_deletion_epoch === null ? null : Number(row.completed_deletion_epoch),
          finalInventoryHash: row.final_inventory_hash,
          reconciliationDeadlineAt: row.reconciliation_deadline_at,
        }
      : null;
  }

  async transition(
    id: UUID,
    from: readonly ArchiveState[],
    to: ArchiveState,
    tx: Transaction,
  ): Promise<boolean> {
    if (!from.length) {
      return false;
    }
    const rows = await unwrap(tx).$queryRaw<IdRow[]>(Prisma.sql`
      UPDATE archives
      SET state = ${to}::archive_state, updated_at = now()
      WHERE id = ${id}
        AND state IN (${Prisma.join(from.map((state) => Prisma.sql`${state}::archive_state`))})
      RETURNING id
    `);
    return rows.length === 1;
  }

  async createExpectedArtifact(
    input: {
      id: UUID;
      archiveId: UUID;
      objectClass: string;
      causalKey: string;
      sampleStart: number | null;
      sampleEnd: number | null;
      ownerEpoch: number;
    },
    tx: Transaction,
  ): Promise<UUID> {
    const database = unwrap(tx);
    const inserted = await database.$queryRaw<IdRow[]>(Prisma.sql`
      INSERT INTO expected_archive_artifacts(
        id,
        archive_id,
        profile_id,
        profile_revision,
        object_class,
        causal_key,
        sample_start,
        sample_end,
        owner_epoch,
        created_at
      )
      SELECT
        ${input.id},
        ${input.archiveId},
        c.provider_profile_id,
        c.provider_profile_revision,
        ${input.objectClass},
        ${input.causalKey},
        ${input.sampleStart},
        ${input.sampleEnd},
        ${input.ownerEpoch},
        now()
      FROM archives a
      JOIN consultations c ON c.id = a.consultation_id
      WHERE a.id = ${input.archiveId}
      ON CONFLICT(archive_id, object_class, causal_key) DO NOTHING
      RETURNING id
    `);
    if (inserted.length === 1) {
      return input.id;
    }

    const rows = await database.$queryRaw<
      {
        id: UUID;
        sample_start: bigint | null;
        sample_end: bigint | null;
        owner_epoch: bigint;
      }[]
    >(Prisma.sql`
      SELECT id, sample_start, sample_end, owner_epoch
      FROM expected_archive_artifacts
      WHERE archive_id = ${input.archiveId}
        AND object_class = ${input.objectClass}
        AND causal_key = ${input.causalKey}
      FOR UPDATE
    `);
    const row = rows[0];
    if (
      !row ||
      safeDatabaseInteger(row.owner_epoch, "expected_archive_artifacts.owner_epoch") !==
        input.ownerEpoch ||
      nullableSafeDatabaseInteger(row.sample_start, "expected_archive_artifacts.sample_start") !==
        input.sampleStart ||
      nullableSafeDatabaseInteger(row.sample_end, "expected_archive_artifacts.sample_end") !==
        input.sampleEnd
    ) {
      throw new DomainError("EXPECTED_ARTIFACT_CONFLICT");
    }
    return row.id;
  }

  async hasExactObject(object: ArchiveObject, tx: Transaction): Promise<boolean> {
    const rows = await unwrap(tx).$queryRaw<ArchiveObjectLockRow[]>(Prisma.sql`
      SELECT o.id, a.consultation_id, o.object_class, o.causal_key, o.key, o.version_id,
        o.size, o.sha256, o.s3_checksum, o.content_type, o.sample_start, o.sample_end,
        o.attempt, o.sequence, o.writer_epoch
      FROM archive_objects o
      JOIN archives a ON a.id = o.archive_id
      WHERE o.id = ${object.id}
        OR (o.key = ${object.key} AND o.version_id = ${object.versionId})
      FOR UPDATE
    `);
    const row = rows[0];
    if (!row) {
      return false;
    }
    if (
      row.id !== object.id ||
      row.consultation_id !== object.consultationId ||
      row.object_class !== object.objectClass ||
      row.causal_key !== object.causalKey ||
      row.key !== object.key ||
      row.version_id !== object.versionId ||
      safeDatabaseInteger(row.size, "archive_objects.size") !== object.size ||
      row.sha256 !== object.sha256 ||
      row.s3_checksum !== object.s3Checksum ||
      row.content_type !== object.contentType ||
      nullableSafeDatabaseInteger(row.sample_start, "archive_objects.sample_start") !==
        object.sampleStart ||
      nullableSafeDatabaseInteger(row.sample_end, "archive_objects.sample_end") !==
        object.sampleEnd ||
      row.attempt !== object.attempt ||
      nullableSafeDatabaseInteger(row.sequence, "archive_objects.sequence") !== object.sequence ||
      row.writer_epoch !== object.writerEpoch
    ) {
      throw new DomainError("ARCHIVE_WRITER_FENCED");
    }
    return true;
  }
  async recordObject(object: ArchiveObject, tx: Transaction): Promise<void> {
    const database = unwrap(tx);
    const inserted = await database.$queryRaw<IdRow[]>(Prisma.sql`
      INSERT INTO archive_objects(
        id,
        archive_id,
        object_class,
        causal_key,
        key,
        version_id,
        size,
        sha256,
        s3_checksum,
        content_type,
        sample_start,
        sample_end,
        attempt,
        sequence,
        writer_epoch,
        created_at
      )
      SELECT
        ${object.id},
        id,
        ${object.objectClass},
        ${object.causalKey},
        ${object.key},
        ${object.versionId},
        ${object.size},
        ${object.sha256},
        ${object.s3Checksum},
        ${object.contentType},
        ${object.sampleStart},
        ${object.sampleEnd},
        ${object.attempt},
        ${object.sequence},
        ${object.writerEpoch},
        now()
      FROM archives
      WHERE consultation_id = ${object.consultationId}
        AND write_epoch = ${object.writerEpoch}
        AND state IN ('pending', 'recording', 'reconciling')
      ON CONFLICT DO NOTHING
      RETURNING id
    `);
    if (inserted.length === 1) {
      return;
    }

    const existing = await database.$queryRaw<ArchiveObjectLockRow[]>(Prisma.sql`
      SELECT o.id, a.consultation_id, o.object_class, o.causal_key, o.key, o.version_id,
        o.size, o.sha256, o.s3_checksum, o.content_type, o.sample_start, o.sample_end,
        o.attempt, o.sequence, o.writer_epoch
      FROM archive_objects o
      JOIN archives a ON a.id = o.archive_id
      WHERE o.id = ${object.id}
        OR (o.key = ${object.key} AND o.version_id = ${object.versionId})
      FOR UPDATE
    `);
    const row = existing[0];
    if (
      !row ||
      row.id !== object.id ||
      row.consultation_id !== object.consultationId ||
      row.object_class !== object.objectClass ||
      row.causal_key !== object.causalKey ||
      row.key !== object.key ||
      row.version_id !== object.versionId ||
      safeDatabaseInteger(row.size, "archive_objects.size") !== object.size ||
      row.sha256 !== object.sha256 ||
      row.s3_checksum !== object.s3Checksum ||
      row.content_type !== object.contentType ||
      nullableSafeDatabaseInteger(row.sample_start, "archive_objects.sample_start") !==
        object.sampleStart ||
      nullableSafeDatabaseInteger(row.sample_end, "archive_objects.sample_end") !==
        object.sampleEnd ||
      row.attempt !== object.attempt ||
      nullableSafeDatabaseInteger(row.sequence, "archive_objects.sequence") !== object.sequence ||
      row.writer_epoch !== object.writerEpoch
    ) {
      throw new DomainError("ARCHIVE_WRITER_FENCED");
    }
  }

  async lockSpoolProducerTuple(
    input: {
      consultationId: UUID;
      generation: number;
      workerId: UUID;
      workerEpoch: number;
      writerEpoch: number;
    },
    tx: Transaction,
  ): Promise<boolean> {
    const rows = await unwrap(tx).$queryRaw<{ worker_id: UUID }[]>(Prisma.sql`
      SELECT r.worker_id
      FROM worker_reservations r
      JOIN worker_job_epochs j
        ON j.consultation_id = r.consultation_id
       AND j.generation = r.generation
       AND j.worker_id = r.worker_id
       AND j.epoch = r.epoch
      JOIN consultations c
        ON c.id = r.consultation_id
       AND c.generation = r.generation
      JOIN archives a ON a.consultation_id = r.consultation_id
      WHERE r.consultation_id = ${input.consultationId}
        AND r.generation = ${input.generation}
        AND r.worker_id = ${input.workerId}
        AND r.epoch = ${input.workerEpoch}
        AND r.released_at IS NULL
        AND r.fenced_at IS NULL
        AND j.fenced_at IS NULL
        AND j.terminal_at IS NULL
        AND j.write_epoch = ${input.writerEpoch}
        AND a.write_epoch = ${input.writerEpoch}
        AND a.state NOT IN ('deleting', 'deleted')
      FOR UPDATE OF r, j
    `);
    return rows.length === 1;
  }

  async fulfillExpectedArtifact(
    expectedId: UUID,
    objectId: UUID,
    writerEpoch: number,
    tx: Transaction,
  ): Promise<boolean> {
    const database = unwrap(tx);
    const changed = await database.$queryRaw<IdRow[]>(Prisma.sql`
      UPDATE expected_archive_artifacts e
      SET fulfilled_object_id = ${objectId}, disposition = 'fulfilled'
      FROM archives a, archive_objects o
      WHERE e.id = ${expectedId}
        AND a.id = e.archive_id
        AND a.write_epoch = ${writerEpoch}
        AND a.state NOT IN ('deleting', 'deleted')
        AND o.id = ${objectId}
        AND o.archive_id = e.archive_id
        AND o.object_class = e.object_class
        AND o.causal_key = e.causal_key
        AND o.sample_start IS NOT DISTINCT FROM e.sample_start
        AND o.sample_end IS NOT DISTINCT FROM e.sample_end
        AND e.owner_epoch = ${writerEpoch}
        AND o.writer_epoch = ${writerEpoch}
        AND e.fulfilled_object_id IS NULL
      RETURNING e.id
    `);
    if (changed.length === 1) {
      return true;
    }

    const existing = await database.$queryRaw<{ matched: number }[]>(Prisma.sql`
      SELECT 1 AS matched
      FROM expected_archive_artifacts e
      JOIN archives a ON a.id = e.archive_id
      JOIN archive_objects o ON o.id = e.fulfilled_object_id
      WHERE e.id = ${expectedId}
        AND e.fulfilled_object_id = ${objectId}
        AND a.write_epoch = ${writerEpoch}
        AND e.owner_epoch = ${writerEpoch}
        AND o.writer_epoch = ${writerEpoch}
        AND o.object_class = e.object_class
        AND o.causal_key = e.causal_key
        AND o.sample_start IS NOT DISTINCT FROM e.sample_start
        AND o.sample_end IS NOT DISTINCT FROM e.sample_end
        AND a.state NOT IN ('deleting', 'deleted')
    `);
    return existing.length === 1;
  }

  async unresolvedExpectations(archiveId: UUID, tx: Transaction) {
    const rows = await unwrap(tx).$queryRaw<
      {
        id: UUID;
        object_class: string;
        causal_key: string;
        sample_start: bigint | null;
        sample_end: bigint | null;
      }[]
    >(Prisma.sql`
      SELECT id, object_class, causal_key, sample_start, sample_end
      FROM expected_archive_artifacts
      WHERE archive_id = ${archiveId}
        AND fulfilled_object_id IS NULL
      ORDER BY id
      FOR UPDATE SKIP LOCKED
    `);
    return rows.map((row) => ({
      id: row.id,
      objectClass: row.object_class,
      causalKey: row.causal_key,
      sampleStart: nullableSafeDatabaseInteger(
        row.sample_start,
        "expected_archive_artifacts.sample_start",
      ),
      sampleEnd: nullableSafeDatabaseInteger(
        row.sample_end,
        "expected_archive_artifacts.sample_end",
      ),
    }));
  }

  async inventoryObjects(archiveId: UUID, tx: Transaction): Promise<readonly ArchiveObject[]> {
    const rows = await unwrap(tx).$queryRaw<ArchiveObjectLockRow[]>(Prisma.sql`
      SELECT o.id, a.consultation_id, o.object_class, o.causal_key, o.key, o.version_id,
        o.size, o.sha256, o.s3_checksum, o.content_type, o.sample_start, o.sample_end,
        o.attempt, o.sequence, o.writer_epoch
      FROM archive_objects o
      JOIN archives a ON a.id = o.archive_id
      WHERE o.archive_id = ${archiveId}
      ORDER BY o.key, o.version_id
    `);
    return rows.map((row) => ({
      id: row.id,
      consultationId: row.consultation_id,
      objectClass: row.object_class,
      causalKey: row.causal_key,
      key: row.key,
      versionId: row.version_id,
      size: safeDatabaseInteger(row.size, "archive_objects.size"),
      sha256: row.sha256,
      s3Checksum: row.s3_checksum,
      contentType: row.content_type,
      sampleStart: nullableSafeDatabaseInteger(row.sample_start, "archive_objects.sample_start"),
      sampleEnd: nullableSafeDatabaseInteger(row.sample_end, "archive_objects.sample_end"),
      attempt: row.attempt,
      sequence: nullableSafeDatabaseInteger(row.sequence, "archive_objects.sequence"),
      writerEpoch: row.writer_epoch,
    }));
  }

  async completePrerequisites(
    archiveId: UUID,
    inventory: FinalInventory,
    tx: Transaction,
  ): Promise<boolean> {
    const egress = JSON.stringify(inventory.egressResults);
    const worker = inventory.workerTerminal;
    const room = inventory.roomClose;
    const rows = await unwrap(tx).$queryRaw<{ complete: boolean }[]>(Prisma.sql`
      SELECT
        c.state = 'ended'
        AND c.room_sid = ${room.roomId}
        AND c.generation = ${room.generation}
        AND floor(extract(epoch FROM c.updated_at) * 1000) = ${room.closedAtMs}
        AND EXISTS(
          SELECT 1
          FROM worker_checkpoints w
          JOIN worker_job_epochs j ON j.terminal_checkpoint_id = w.id
          WHERE w.consultation_id = c.id
            AND w.id = ${worker.checkpointId}
            AND w.worker_epoch = ${worker.workerEpoch}
            AND w.terminal
            AND floor(extract(epoch FROM w.created_at) * 1000) = ${worker.occurredAtMs}
            AND j.terminal_outcome = ${worker.outcome}
            AND floor(extract(epoch FROM j.terminal_at) * 1000) = ${worker.occurredAtMs}
        )
        AND NOT EXISTS(
          SELECT 1
          FROM egress_jobs e
          WHERE e.consultation_id = c.id
            AND (
              e.terminal_at IS NULL
              OR NOT EXISTS(
                SELECT 1
                FROM jsonb_array_elements(${egress}::jsonb) proposed
                WHERE proposed.value = e.terminal_result
              )
            )
        )
        AND NOT EXISTS(
          SELECT 1
          FROM jsonb_array_elements(${egress}::jsonb) proposed
          WHERE NOT EXISTS(
            SELECT 1
            FROM egress_jobs e
            WHERE e.consultation_id = c.id
              AND e.terminal_at IS NOT NULL
              AND e.terminal_result = proposed.value
          )
        ) AS complete
      FROM archives a
      JOIN consultations c ON c.id = a.consultation_id
      WHERE a.id = ${archiveId}
    `);
    return rows[0]?.complete === true;
  }

  async createFinalInventory(
    archiveId: UUID,
    inventory: FinalInventory,
    sha256: string,
    objectId: UUID,
    tx: Transaction,
  ): Promise<boolean> {
    const database = unwrap(tx);
    const inserted = await database.$queryRaw<{ archive_id: UUID }[]>(Prisma.sql`
      INSERT INTO final_inventories(
        archive_id,
        status,
        inventory,
        sha256,
        object_id,
        room_close,
        worker_terminal,
        egress_results,
        missing,
        errors,
        created_at
      ) VALUES (
        ${archiveId},
        ${inventory.status},
        ${JSON.stringify(inventory)}::jsonb,
        ${sha256},
        ${objectId},
        ${JSON.stringify(inventory.roomClose)}::jsonb,
        ${JSON.stringify(inventory.workerTerminal)}::jsonb,
        ${JSON.stringify(inventory.egressResults)}::jsonb,
        ${JSON.stringify(inventory.missing)}::jsonb,
        ${JSON.stringify(inventory.errors)}::jsonb,
        now()
      )
      ON CONFLICT DO NOTHING
      RETURNING archive_id
    `);
    if (inserted.length === 1) {
      await database.$executeRaw(Prisma.sql`
        UPDATE archives
        SET final_inventory_hash = ${sha256}, updated_at = now()
        WHERE id = ${archiveId}
          AND final_inventory_hash IS NULL
      `);
    }
    return inserted.length === 1;
  }

  async finalInventoryHash(archiveId: UUID, tx: Transaction): Promise<string | null> {
    const rows = await unwrap(tx).$queryRaw<{ sha256: string }[]>(Prisma.sql`
      SELECT sha256
      FROM final_inventories
      WHERE archive_id = ${archiveId}
    `);
    return rows[0]?.sha256 ?? null;
  }

  async finalInventory(archiveId: UUID, tx: Transaction): Promise<FinalInventory | null> {
    const rows = await unwrap(tx).$queryRaw<{ inventory: unknown }[]>(Prisma.sql`
      SELECT inventory
      FROM final_inventories
      WHERE archive_id = ${archiveId}
    `);
    const row = rows[0];
    if (row === undefined) {
      return null;
    }
    return FinalInventorySchema.parse(row.inventory);
  }

  async supplementClaims(
    archiveId: UUID,
    tx: Transaction,
  ): Promise<{ closedGapIndexes: readonly number[]; objectIds: readonly UUID[] }> {
    const rows = await unwrap(tx).$queryRaw<{ gaps: unknown; objects: unknown }[]>(Prisma.sql`
      SELECT
        COALESCE(jsonb_agg(DISTINCT gap.value), '[]'::jsonb) AS gaps,
        COALESCE(
          jsonb_agg(DISTINCT object.value ->> 'objectId')
            FILTER (WHERE object.value IS NOT NULL),
          '[]'::jsonb
        ) AS objects
      FROM inventory_supplements s
      LEFT JOIN LATERAL
        jsonb_array_elements(s.supplement -> 'closedGapIndexes') gap(value)
        ON true
      LEFT JOIN LATERAL
        jsonb_array_elements(s.supplement -> 'addedObjects') object(value)
        ON true
      WHERE s.archive_id = ${archiveId}
    `);
    const row = rows[0];
    if (row === undefined || !Array.isArray(row.gaps) || !Array.isArray(row.objects)) {
      throw new Error("inventory supplement claims are malformed");
    }
    if (!row.gaps.every((value) => typeof value === "number" && Number.isSafeInteger(value))) {
      throw new Error("inventory supplement gap indexes are malformed");
    }
    if (!row.objects.every((value) => typeof value === "string")) {
      throw new Error("inventory supplement object ids are malformed");
    }
    return {
      closedGapIndexes: row.gaps,
      objectIds: row.objects as UUID[],
    };
  }

  async createSupplement(
    input: {
      id: UUID;
      archiveId: UUID;
      finalHash: string;
      supplement: unknown;
      sha256: string;
      objectId: UUID | null;
      at: Instant;
    },
    tx: Transaction,
  ): Promise<boolean> {
    const rows = await unwrap(tx).$queryRaw<IdRow[]>(Prisma.sql`
      INSERT INTO inventory_supplements(
        id,
        archive_id,
        final_inventory_sha256,
        supplement,
        sha256,
        object_id,
        created_at
      )
      SELECT
        ${input.id},
        ${input.archiveId},
        ${input.finalHash},
        ${JSON.stringify(input.supplement)}::jsonb,
        ${input.sha256},
        ${input.objectId},
        ${input.at}
      FROM final_inventories
      WHERE archive_id = ${input.archiveId}
        AND sha256 = ${input.finalHash}
      ON CONFLICT DO NOTHING
      RETURNING id
    `);
    return rows.length === 1;
  }

  async addHold(
    input: {
      id: UUID;
      archiveId: UUID;
      reason: string;
      actorId: UUID;
      sessionId: UUID;
      reauthenticatedAt: Instant;
      at: Instant;
    },
    tx: Transaction,
  ): Promise<void> {
    await unwrap(tx).$executeRaw(Prisma.sql`
      INSERT INTO legal_holds(
        id,
        archive_id,
        reason,
        actor_id,
        session_id,
        reauthenticated_at,
        placed_at
      ) VALUES (
        ${input.id},
        ${input.archiveId},
        ${input.reason},
        ${input.actorId},
        ${input.sessionId},
        ${input.reauthenticatedAt},
        ${input.at}
      )
    `);
  }

  async removeHold(id: UUID, actorId: UUID, at: Instant, tx: Transaction): Promise<void> {
    await unwrap(tx).$executeRaw(Prisma.sql`
      UPDATE legal_holds
      SET state = 'released', released_at = ${at}, released_by = ${actorId}
      WHERE id = ${id}
        AND released_at IS NULL
    `);
  }

  async activeHolds(archiveId: UUID, tx: Transaction) {
    const rows = await unwrap(tx).$queryRaw<
      {
        id: UUID;
        reason: string;
        actor_id: UUID;
        state: "applying" | "active" | "releasing" | "failed";
        per_version_results: unknown;
      }[]
    >(Prisma.sql`
      SELECT id, reason, actor_id, state, per_version_results
      FROM legal_holds
      WHERE archive_id = ${archiveId}
        AND released_at IS NULL
      FOR UPDATE
    `);
    return rows.map((row) => ({
      id: row.id,
      reason: row.reason,
      actorId: row.actor_id,
      state: row.state,
      perVersionResults: row.per_version_results,
    }));
  }

  async completeDeletion(
    archiveId: UUID,
    writeEpoch: number,
    at: Instant,
    tx: Transaction,
  ): Promise<boolean> {
    const rows = await unwrap(tx).$queryRaw<IdRow[]>(Prisma.sql`
      WITH changed AS (
        UPDATE archives
        SET state = 'deleted',
            completed_deletion_epoch = ${writeEpoch},
            deleted_at = ${at},
            updated_at = ${at}
        WHERE id = ${archiveId}
          AND write_epoch = ${writeEpoch}
          AND state = 'deleting'
        RETURNING consultation_id
      ),
      tombstone AS (
        UPDATE consultations c
        SET state = 'deleted',
            deleted_at = ${at},
            provider_selection = NULL,
            snapshot_hash = NULL,
            room_name = NULL,
            worker_identity = NULL,
            updated_at = ${at}
        FROM changed
        WHERE c.id = changed.consultation_id
        RETURNING c.id
      )
      UPDATE consultation_participants p
      SET display_name = NULL,
          language = NULL,
          consent_version = NULL,
          consent_copy_hash = NULL,
          consent_snapshot_hash = NULL,
          consented_at = NULL
      FROM tombstone
      WHERE p.consultation_id = tombstone.id
      RETURNING p.id
    `);
    return rows.length === 2;
  }

  async recordHoldResults(
    id: UUID,
    aggregate: unknown,
    perVersion: unknown,
    tx: Transaction,
  ): Promise<void> {
    await unwrap(tx).$executeRaw(Prisma.sql`
      UPDATE legal_holds
      SET aggregate_result = ${JSON.stringify(aggregate)}::jsonb,
          per_version_results = ${JSON.stringify(perVersion)}::jsonb
      WHERE id = ${id}
    `);
  }

  async transitionHoldState(
    id: UUID,
    from: readonly ("applying" | "active" | "releasing" | "failed")[],
    to: "active" | "releasing" | "failed",
    tx: Transaction,
  ): Promise<boolean> {
    if (!from.length) {
      return false;
    }
    const rows = await unwrap(tx).$queryRaw<IdRow[]>(Prisma.sql`
      UPDATE legal_holds
      SET state = ${to}
      WHERE id = ${id}
        AND released_at IS NULL
        AND state IN (${Prisma.join(from)})
      RETURNING id
    `);
    return rows.length === 1;
  }

  async beginHoldOperation(
    archiveId: UUID,
    operationId: UUID,
    owner: UUID,
    kind: "add" | "release",
    at: Instant,
    leaseExpiresAt: Instant,
    tx: Transaction,
  ): Promise<boolean> {
    const rows = await unwrap(tx).$queryRaw<IdRow[]>(Prisma.sql`
      UPDATE archives
      SET hold_operation_id = ${operationId},
          hold_operation_owner = ${owner},
          hold_operation_kind = ${kind},
          hold_operation_started_at = ${at},
          hold_operation_lease_expires_at = ${leaseExpiresAt}
      WHERE id = ${archiveId}
        AND hold_operation_id IS NULL
        AND NOT EXISTS(
          SELECT 1
          FROM legal_holds
          WHERE archive_id = ${archiveId}
            AND released_at IS NULL
            AND state IN ('applying', 'releasing')
        )
      RETURNING id
    `);
    return rows.length === 1;
  }

  async claimStaleHoldOperation(
    archiveId: UUID,
    owner: UUID,
    now: Instant,
    leaseExpiresAt: Instant,
    tx: Transaction,
  ): Promise<{ operationId: UUID; kind: "add" | "release" } | null> {
    const rows = await unwrap(tx).$queryRaw<
      { operation_id: UUID | null; kind: string | null }[]
    >(Prisma.sql`
      UPDATE archives
      SET hold_operation_owner = ${owner},
          hold_operation_lease_expires_at = ${leaseExpiresAt}
      WHERE id = ${archiveId}
        AND hold_operation_id IS NOT NULL
        AND hold_operation_lease_expires_at <= ${now}
      RETURNING hold_operation_id AS operation_id, hold_operation_kind AS kind
    `);
    const row = rows[0];
    return row?.operation_id && (row.kind === "add" || row.kind === "release")
      ? { operationId: row.operation_id, kind: row.kind }
      : null;
  }

  async renewHoldOperation(
    archiveId: UUID,
    operationId: UUID,
    owner: UUID,
    leaseExpiresAt: Instant,
    tx: Transaction,
  ): Promise<boolean> {
    const rows = await unwrap(tx).$queryRaw<IdRow[]>(Prisma.sql`
      UPDATE archives
      SET hold_operation_lease_expires_at = ${leaseExpiresAt}
      WHERE id = ${archiveId}
        AND hold_operation_id = ${operationId}
        AND hold_operation_owner = ${owner}
      RETURNING id
    `);
    return rows.length === 1;
  }

  async completeHoldOperation(
    archiveId: UUID,
    operationId: UUID,
    owner: UUID,
    tx: Transaction,
  ): Promise<boolean> {
    const rows = await unwrap(tx).$queryRaw<IdRow[]>(Prisma.sql`
      UPDATE archives
      SET hold_operation_id = NULL,
          hold_operation_owner = NULL,
          hold_operation_kind = NULL,
          hold_operation_started_at = NULL,
          hold_operation_lease_expires_at = NULL
      WHERE id = ${archiveId}
        AND hold_operation_id = ${operationId}
        AND hold_operation_owner = ${owner}
      RETURNING id
    `);
    return rows.length === 1;
  }

  async deletionWritersDrained(
    consultationId: UUID,
    writeEpoch: number,
    tx: Transaction,
  ): Promise<boolean> {
    const rows = await unwrap(tx).$queryRaw<{ drained: boolean }[]>(Prisma.sql`
      SELECT
        NOT EXISTS(
          SELECT 1
          FROM worker_reservations
          WHERE consultation_id = ${consultationId}
            AND released_at IS NULL
        )
        AND NOT EXISTS(
          SELECT 1
          FROM worker_job_epochs
          WHERE consultation_id = ${consultationId}
            AND write_epoch < ${writeEpoch}
            AND terminal_at IS NULL
        )
        AND NOT EXISTS(
          SELECT 1
          FROM egress_jobs
          WHERE consultation_id = ${consultationId}
            AND terminal_at IS NULL
        )
        AND EXISTS(
          SELECT 1
          FROM archives a
          JOIN final_inventories f ON f.archive_id = a.id
          WHERE a.consultation_id = ${consultationId}
        ) AS drained
    `);
    return rows[0]?.drained === true;
  }

  async incrementWriteEpoch(archiveId: UUID, tx: Transaction): Promise<number> {
    const rows = await unwrap(tx).$queryRaw<{ write_epoch: number }[]>(Prisma.sql`
      UPDATE archives
      SET write_epoch = write_epoch + 1
      WHERE id = ${archiveId}
      RETURNING write_epoch
    `);
    return rows[0]?.write_epoch ?? Number.NaN;
  }

  async fenceWritersForDeletion(
    consultationId: UUID,
    writeEpoch: number,
    at: Instant,
    tx: Transaction,
  ): Promise<void> {
    const database = unwrap(tx);
    await database.$executeRaw(Prisma.sql`
      UPDATE worker_reservations
      SET fenced_at = ${at}, accepting_load = false
      WHERE consultation_id = ${consultationId}
        AND fenced_at IS NULL
    `);
    await database.$executeRaw(Prisma.sql`
      UPDATE worker_job_epochs
      SET fenced_at = ${at}
      WHERE consultation_id = ${consultationId}
        AND write_epoch < ${writeEpoch}
        AND fenced_at IS NULL
    `);
  }

  async recordDeletionFailure(archiveId: UUID, failure: unknown, tx: Transaction): Promise<void> {
    await unwrap(tx).$executeRaw(Prisma.sql`
      UPDATE archives
      SET deletion_failure = ${JSON.stringify(failure)}::jsonb
      WHERE id = ${archiveId}
    `);
  }

  async recordDeletionScan(
    input: {
      id: UUID;
      archiveId: UUID;
      writeEpoch: number;
      versionCount: number;
      multipartCount: number;
      consecutiveEmpty: number;
      result: unknown;
      at: Instant;
    },
    tx: Transaction,
  ): Promise<void> {
    await unwrap(tx).$executeRaw(Prisma.sql`
      INSERT INTO deletion_scans(
        id,
        archive_id,
        write_epoch,
        version_count,
        multipart_count,
        consecutive_empty,
        result,
        scanned_at
      )
      SELECT
        ${input.id},
        ${input.archiveId},
        ${input.writeEpoch},
        ${input.versionCount},
        ${input.multipartCount},
        ${input.consecutiveEmpty},
        ${JSON.stringify(input.result)}::jsonb,
        ${input.at}
      FROM archives
      WHERE id = ${input.archiveId}
        AND write_epoch = ${input.writeEpoch}
    `);
  }
}
