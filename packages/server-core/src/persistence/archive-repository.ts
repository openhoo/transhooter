import { type FinalInventory, FinalInventorySchema } from "@transhooter/contracts";
import { and, asc, eq, gt, inArray, isNull, lte, sql } from "drizzle-orm";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";
import type { Consultation } from "../consultations/domain";
import { type ArchiveState, DomainError, type Instant, type UUID } from "../domain/model";
import type { ArchiveObject, ArchiveRepository, Transaction } from "../ports/index";
import { type DrizzleSchema, TransactionHandle, unwrap } from "./repositories";
import {
  archiveObjects,
  archives,
  consultations,
  expectedArchiveArtifacts,
  finalInventories,
  legalHolds,
  workerJobEpochs,
  workerReservations,
} from "./schema";

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
  size: number;
  sha256: string;
  s3_checksum: string;
  content_type: string;
  sample_start: number | null;
  sample_end: number | null;
  attempt: number | null;
  sequence: number | null;
  writer_epoch: number;
};

abstract class DrizzleRepository {
  constructor(protected readonly database: NodePgDatabase<DrizzleSchema>) {}

  transaction<T>(work: (tx: Transaction) => Promise<T>): Promise<T> {
    return this.database.transaction((database) => work(new TransactionHandle(database)));
  }
}

export class DrizzleArchiveRepository extends DrizzleRepository implements ArchiveRepository {
  async lockByConsultation(consultationId: UUID, tx: Transaction) {
    const database = unwrap(tx);
    // Coupled consultation/archive operations always lock the consultation first.
    // This matches consultation lifecycle transactions and prevents inverse-order deadlocks.
    await database
      .select({ id: consultations.id })
      .from(consultations)
      .where(eq(consultations.id, consultationId))
      .for("update");
    const result = await database.execute<ArchiveLockRow>(
      sql`
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
      `,
    );
    const row = result.rows[0];
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
    const updated = await unwrap(tx)
      .update(archives)
      .set({ state: to, updatedAt: sql`now()` })
      .where(and(eq(archives.id, id), inArray(archives.state, from)))
      .returning({ id: archives.id });
    return updated.length === 1;
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
    const inserted = await database.execute<{ id: UUID }>(sql`
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
    if (inserted.rowCount === 1) {
      return input.id;
    }

    const [row] = await database
      .select({
        id: expectedArchiveArtifacts.id,
        sampleStart: expectedArchiveArtifacts.sampleStart,
        sampleEnd: expectedArchiveArtifacts.sampleEnd,
        ownerEpoch: expectedArchiveArtifacts.ownerEpoch,
      })
      .from(expectedArchiveArtifacts)
      .where(
        and(
          eq(expectedArchiveArtifacts.archiveId, input.archiveId),
          eq(expectedArchiveArtifacts.objectClass, input.objectClass),
          eq(expectedArchiveArtifacts.causalKey, input.causalKey),
        ),
      )
      .for("update");
    if (
      !row ||
      row.ownerEpoch !== input.ownerEpoch ||
      row.sampleStart !== input.sampleStart ||
      row.sampleEnd !== input.sampleEnd
    ) {
      throw new DomainError("EXPECTED_ARTIFACT_CONFLICT");
    }
    return row.id;
  }

  async recordObject(object: ArchiveObject, tx: Transaction): Promise<void> {
    const database = unwrap(tx);
    const result = await database.execute<{ id: UUID }>(sql`
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
      WHERE
        consultation_id = ${object.consultationId}
        AND write_epoch = ${object.writerEpoch}
        AND state IN ('pending', 'recording', 'reconciling')
      ON CONFLICT DO NOTHING
      RETURNING id
    `);
    if (result.rowCount === 1) {
      return;
    }

    const existing = await database.execute<ArchiveObjectLockRow>(
      sql`
      SELECT o.*, a.consultation_id
      FROM archive_objects o
      JOIN archives a ON a.id = o.archive_id
      WHERE
        o.id = ${object.id}
        OR (o.key = ${object.key} AND o.version_id = ${object.versionId})
        FOR UPDATE
      `,
    );
    const row = existing.rows[0];
    if (
      !row ||
      row.id !== object.id ||
      row.consultation_id !== object.consultationId ||
      row.object_class !== object.objectClass ||
      row.causal_key !== object.causalKey ||
      row.key !== object.key ||
      row.version_id !== object.versionId ||
      Number(row.size) !== object.size ||
      row.sha256 !== object.sha256 ||
      row.s3_checksum !== object.s3Checksum ||
      row.content_type !== object.contentType ||
      (row.sample_start === null ? null : Number(row.sample_start)) !== object.sampleStart ||
      (row.sample_end === null ? null : Number(row.sample_end)) !== object.sampleEnd ||
      (row.attempt === null ? null : Number(row.attempt)) !== object.attempt ||
      (row.sequence === null ? null : Number(row.sequence)) !== object.sequence ||
      Number(row.writer_epoch) !== object.writerEpoch
    ) {
      throw new DomainError("ARCHIVE_WRITER_FENCED");
    }
  }
  async lockActiveWorkerWriter(
    input: {
      consultationId: UUID;
      generation: number;
      workerId: UUID;
      workerEpoch: number;
      writerEpoch: number;
    },
    tx: Transaction,
  ): Promise<boolean> {
    const rows = await unwrap(tx)
      .select({ workerId: workerReservations.workerId })
      .from(workerReservations)
      .innerJoin(
        workerJobEpochs,
        and(
          eq(workerJobEpochs.consultationId, workerReservations.consultationId),
          eq(workerJobEpochs.generation, workerReservations.generation),
          eq(workerJobEpochs.workerId, workerReservations.workerId),
          eq(workerJobEpochs.epoch, workerReservations.epoch),
        ),
      )
      .innerJoin(
        consultations,
        and(
          eq(consultations.id, workerReservations.consultationId),
          eq(consultations.generation, workerReservations.generation),
        ),
      )
      .innerJoin(archives, eq(archives.consultationId, workerReservations.consultationId))
      .where(
        and(
          eq(workerReservations.consultationId, input.consultationId),
          eq(workerReservations.generation, input.generation),
          eq(workerReservations.workerId, input.workerId),
          eq(workerReservations.epoch, input.workerEpoch),
          isNull(workerReservations.releasedAt),
          gt(workerReservations.leaseExpiresAt, sql`now()`),
          isNull(workerReservations.fencedAt),
          isNull(workerJobEpochs.fencedAt),
          isNull(workerJobEpochs.terminalAt),
          eq(workerJobEpochs.writeEpoch, input.writerEpoch),
          eq(archives.writeEpoch, input.writerEpoch),
        ),
      )
      .for("update", { of: [workerReservations, workerJobEpochs] });
    return rows.length === 1;
  }

  async fulfillExpectedArtifact(
    expectedId: UUID,
    objectId: UUID,
    writerEpoch: number,
    tx: Transaction,
  ): Promise<boolean> {
    const database = unwrap(tx);
    const result = await database.execute<{ id: UUID }>(sql`
      UPDATE expected_archive_artifacts e
      SET fulfilled_object_id = ${objectId}, disposition = 'fulfilled'
      FROM archives a, archive_objects o
      WHERE
        e.id = ${expectedId}
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
    if (result.rowCount === 1) {
      return true;
    }

    const existing = await database.execute<{ matched: number }>(sql`
      SELECT 1 AS matched
      FROM expected_archive_artifacts e
      JOIN archives a ON a.id = e.archive_id
      JOIN archive_objects o ON o.id = e.fulfilled_object_id
      WHERE
        e.id = ${expectedId}
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
    return existing.rowCount === 1;
  }

  async unresolvedExpectations(archiveId: UUID, tx: Transaction) {
    const rows = await unwrap(tx)
      .select({
        id: expectedArchiveArtifacts.id,
        objectClass: expectedArchiveArtifacts.objectClass,
        causalKey: expectedArchiveArtifacts.causalKey,
        sampleStart: expectedArchiveArtifacts.sampleStart,
        sampleEnd: expectedArchiveArtifacts.sampleEnd,
      })
      .from(expectedArchiveArtifacts)
      .where(
        and(
          eq(expectedArchiveArtifacts.archiveId, archiveId),
          isNull(expectedArchiveArtifacts.fulfilledObjectId),
        ),
      )
      .orderBy(asc(expectedArchiveArtifacts.id))
      .for("update", { skipLocked: true });
    return rows;
  }

  async inventoryObjects(archiveId: UUID, tx: Transaction): Promise<readonly ArchiveObject[]> {
    const rows = await unwrap(tx)
      .select({
        object: archiveObjects,
        consultationId: archives.consultationId,
      })
      .from(archiveObjects)
      .innerJoin(archives, eq(archives.id, archiveObjects.archiveId))
      .where(eq(archiveObjects.archiveId, archiveId))
      .orderBy(asc(archiveObjects.key), asc(archiveObjects.versionId));

    return rows.map(({ object, consultationId }) => ({
      id: object.id,
      consultationId,
      objectClass: object.objectClass,
      causalKey: object.causalKey,
      key: object.key,
      versionId: object.versionId,
      size: object.size,
      sha256: object.sha256,
      s3Checksum: object.s3Checksum,
      contentType: object.contentType,
      sampleStart: object.sampleStart,
      sampleEnd: object.sampleEnd,
      attempt: object.attempt,
      sequence: object.sequence,
      writerEpoch: object.writerEpoch,
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
    const result = await unwrap(tx).execute<{ complete: boolean }>(
      sql`
      SELECT
        c.state = 'ended'
        AND c.room_sid = ${room.roomId}
        AND c.generation = ${room.generation}
        AND floor(extract(epoch FROM c.updated_at) * 1000) = ${room.closedAtMs}
        AND EXISTS(
          SELECT 1
          FROM worker_checkpoints w
          JOIN worker_job_epochs j ON j.terminal_checkpoint_id = w.id
          WHERE
            w.consultation_id = c.id
            AND w.id = ${worker.checkpointId}
            AND w.worker_epoch = ${worker.workerEpoch}
            AND w.terminal
            AND floor(extract(epoch FROM w.created_at) * 1000) =
              ${worker.occurredAtMs}
            AND j.terminal_outcome = ${worker.outcome}
            AND floor(extract(epoch FROM j.terminal_at) * 1000) =
              ${worker.occurredAtMs}
        )
        AND NOT EXISTS(
          SELECT 1
          FROM egress_jobs e
          WHERE
            e.consultation_id = c.id
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
            WHERE
              e.consultation_id = c.id
              AND e.terminal_at IS NOT NULL
              AND e.terminal_result = proposed.value
          )
        ) AS complete
      FROM archives a
      JOIN consultations c ON c.id = a.consultation_id
      WHERE a.id = ${archiveId}
      `,
    );
    return result.rows[0]?.complete === true;
  }

  async createFinalInventory(
    archiveId: UUID,
    inventory: FinalInventory,
    sha256: string,
    objectId: UUID,
    tx: Transaction,
  ): Promise<boolean> {
    const inserted = await unwrap(tx)
      .insert(finalInventories)
      .values({
        archiveId,
        status: inventory.status,
        inventory,
        sha256,
        objectId,
        roomClose: inventory.roomClose,
        workerTerminal: inventory.workerTerminal,
        egressResults: inventory.egressResults,
        missing: inventory.missing,
        errors: inventory.errors,
        createdAt: sql`now()`,
      })
      .onConflictDoNothing()
      .returning({ archiveId: finalInventories.archiveId });
    if (inserted.length === 1) {
      await unwrap(tx)
        .update(archives)
        .set({ finalInventoryHash: sha256, updatedAt: sql`now()` })
        .where(and(eq(archives.id, archiveId), isNull(archives.finalInventoryHash)));
    }
    return inserted.length === 1;
  }

  async finalInventoryHash(archiveId: UUID, tx: Transaction): Promise<string | null> {
    const rows = await unwrap(tx)
      .select({ sha256: finalInventories.sha256 })
      .from(finalInventories)
      .where(eq(finalInventories.archiveId, archiveId));
    return rows[0]?.sha256 ?? null;
  }

  async finalInventory(archiveId: UUID, tx: Transaction): Promise<FinalInventory | null> {
    const rows = await unwrap(tx)
      .select({ inventory: finalInventories.inventory })
      .from(finalInventories)
      .where(eq(finalInventories.archiveId, archiveId));
    const parsed = FinalInventorySchema.safeParse(rows[0]?.inventory);
    return parsed.success ? parsed.data : null;
  }

  async supplementClaims(
    archiveId: UUID,
    tx: Transaction,
  ): Promise<{
    closedGapIndexes: readonly number[];
    objectIds: readonly UUID[];
  }> {
    const result = await unwrap(tx).execute<{ gaps: unknown; objects: unknown }>(
      sql`
      SELECT
        COALESCE(
          jsonb_agg(DISTINCT gap.value),
          '[]'::jsonb
        ) AS gaps,
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
      `,
    );
    const gaps = result.rows[0]?.gaps;
    const objects = result.rows[0]?.objects;
    return {
      closedGapIndexes: Array.isArray(gaps)
        ? gaps.filter((value): value is number => typeof value === "number")
        : [],
      objectIds: Array.isArray(objects)
        ? objects.filter((value): value is UUID => typeof value === "string")
        : [],
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
    const result = await unwrap(tx).execute<{ id: UUID }>(sql`
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
      WHERE
        archive_id = ${input.archiveId}
        AND sha256 = ${input.finalHash}
      ON CONFLICT DO NOTHING
      RETURNING id
    `);
    return result.rowCount === 1;
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
    await unwrap(tx).insert(legalHolds).values({
      id: input.id,
      archiveId: input.archiveId,
      reason: input.reason,
      actorId: input.actorId,
      sessionId: input.sessionId,
      reauthenticatedAt: input.reauthenticatedAt,
      placedAt: input.at,
    });
  }

  async removeHold(id: UUID, actorId: UUID, at: Instant, tx: Transaction): Promise<void> {
    await unwrap(tx)
      .update(legalHolds)
      .set({
        state: "released",
        releasedAt: at,
        releasedBy: actorId,
      })
      .where(and(eq(legalHolds.id, id), isNull(legalHolds.releasedAt)));
  }

  async activeHolds(archiveId: UUID, tx: Transaction) {
    const rows = await unwrap(tx)
      .select({
        id: legalHolds.id,
        reason: legalHolds.reason,
        actorId: legalHolds.actorId,
        state: legalHolds.state,
        perVersionResults: legalHolds.perVersionResults,
      })
      .from(legalHolds)
      .where(and(eq(legalHolds.archiveId, archiveId), isNull(legalHolds.releasedAt)))
      .for("update");
    return rows.map((row) => ({
      ...row,
      state: row.state as "applying" | "active" | "releasing" | "failed",
    }));
  }

  async completeDeletion(
    archiveId: UUID,
    writeEpoch: number,
    at: Instant,
    tx: Transaction,
  ): Promise<boolean> {
    const result = await unwrap(tx).execute<{ id: UUID }>(sql`
      WITH changed AS (
        UPDATE archives
        SET
          state = 'deleted',
          completed_deletion_epoch = ${writeEpoch},
          deleted_at = ${at},
          updated_at = ${at}
        WHERE
          id = ${archiveId}
          AND write_epoch = ${writeEpoch}
          AND state = 'deleting'
        RETURNING consultation_id
      ),
      tombstone AS (
        UPDATE consultations c
        SET
          state = 'deleted',
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
      SET
        display_name = NULL,
        language = NULL,
        consent_version = NULL,
        consent_copy_hash = NULL,
        consent_snapshot_hash = NULL,
        consented_at = NULL
      FROM tombstone
      WHERE p.consultation_id = tombstone.id
      RETURNING p.id
    `);
    return result.rowCount === 2;
  }

  async recordHoldResults(
    id: UUID,
    aggregate: unknown,
    perVersion: unknown,
    tx: Transaction,
  ): Promise<void> {
    await unwrap(tx)
      .update(legalHolds)
      .set({
        aggregateResult: aggregate,
        perVersionResults: perVersion,
      })
      .where(eq(legalHolds.id, id));
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
    const updated = await unwrap(tx)
      .update(legalHolds)
      .set({ state: to })
      .where(
        and(eq(legalHolds.id, id), isNull(legalHolds.releasedAt), inArray(legalHolds.state, from)),
      )
      .returning({ id: legalHolds.id });
    return updated.length === 1;
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
    const result = await unwrap(tx).execute<{ id: UUID }>(sql`
      UPDATE archives
      SET
        hold_operation_id = ${operationId},
        hold_operation_owner = ${owner},
        hold_operation_kind = ${kind},
        hold_operation_started_at = ${at},
        hold_operation_lease_expires_at = ${leaseExpiresAt}
      WHERE
        id = ${archiveId}
        AND hold_operation_id IS NULL
        AND NOT EXISTS(
          SELECT 1
          FROM legal_holds
          WHERE
            archive_id = ${archiveId}
            AND released_at IS NULL
            AND state IN ('applying', 'releasing')
        )
      RETURNING id
    `);
    return result.rowCount === 1;
  }

  async claimStaleHoldOperation(
    archiveId: UUID,
    owner: UUID,
    now: Instant,
    leaseExpiresAt: Instant,
    tx: Transaction,
  ): Promise<{ operationId: UUID; kind: "add" | "release" } | null> {
    const updated = await unwrap(tx)
      .update(archives)
      .set({
        holdOperationOwner: owner,
        holdOperationLeaseExpiresAt: leaseExpiresAt,
      })
      .where(
        and(
          eq(archives.id, archiveId),
          sql`${archives.holdOperationId} IS NOT NULL`,
          lte(archives.holdOperationLeaseExpiresAt, now),
        ),
      )
      .returning({
        operationId: archives.holdOperationId,
        kind: archives.holdOperationKind,
      });
    const row = updated[0];
    return row?.operationId && (row.kind === "add" || row.kind === "release")
      ? { operationId: row.operationId, kind: row.kind }
      : null;
  }

  async renewHoldOperation(
    archiveId: UUID,
    operationId: UUID,
    owner: UUID,
    leaseExpiresAt: Instant,
    tx: Transaction,
  ): Promise<boolean> {
    const updated = await unwrap(tx)
      .update(archives)
      .set({ holdOperationLeaseExpiresAt: leaseExpiresAt })
      .where(
        and(
          eq(archives.id, archiveId),
          eq(archives.holdOperationId, operationId),
          eq(archives.holdOperationOwner, owner),
        ),
      )
      .returning({ id: archives.id });
    return updated.length === 1;
  }

  async completeHoldOperation(
    archiveId: UUID,
    operationId: UUID,
    owner: UUID,
    tx: Transaction,
  ): Promise<boolean> {
    const updated = await unwrap(tx)
      .update(archives)
      .set({
        holdOperationId: null,
        holdOperationOwner: null,
        holdOperationKind: null,
        holdOperationStartedAt: null,
        holdOperationLeaseExpiresAt: null,
      })
      .where(
        and(
          eq(archives.id, archiveId),
          eq(archives.holdOperationId, operationId),
          eq(archives.holdOperationOwner, owner),
        ),
      )
      .returning({ id: archives.id });
    return updated.length === 1;
  }

  async deletionWritersDrained(
    consultationId: UUID,
    writeEpoch: number,
    tx: Transaction,
  ): Promise<boolean> {
    const result = await unwrap(tx).execute<{ drained: boolean }>(
      sql`
      SELECT
        NOT EXISTS(
          SELECT 1
          FROM worker_reservations
          WHERE
            consultation_id = ${consultationId}
            AND released_at IS NULL
        )
        AND NOT EXISTS(
          SELECT 1
          FROM worker_job_epochs
          WHERE
            consultation_id = ${consultationId}
            AND write_epoch < ${writeEpoch}
            AND terminal_at IS NULL
        )
        AND NOT EXISTS(
          SELECT 1
          FROM egress_jobs
          WHERE
            consultation_id = ${consultationId}
            AND terminal_at IS NULL
        )
        AND EXISTS(
          SELECT 1
          FROM archives a
          JOIN final_inventories f ON f.archive_id = a.id
          WHERE a.consultation_id = ${consultationId}
        ) AS drained
      `,
    );
    return result.rows[0]?.drained === true;
  }

  async incrementWriteEpoch(archiveId: UUID, tx: Transaction): Promise<number> {
    const updated = await unwrap(tx)
      .update(archives)
      .set({ writeEpoch: sql`${archives.writeEpoch} + 1` })
      .where(eq(archives.id, archiveId))
      .returning({ writeEpoch: archives.writeEpoch });
    return updated[0]?.writeEpoch ?? Number.NaN;
  }

  async fenceWritersForDeletion(
    consultationId: UUID,
    writeEpoch: number,
    at: Instant,
    tx: Transaction,
  ): Promise<void> {
    const database = unwrap(tx);
    await database.execute(sql`
      UPDATE worker_reservations
      SET fenced_at = ${at}, accepting_load = false
      WHERE
        consultation_id = ${consultationId}
        AND fenced_at IS NULL
    `);
    await database.execute(sql`
      UPDATE worker_job_epochs
      SET fenced_at = ${at}
      WHERE
        consultation_id = ${consultationId}
        AND write_epoch < ${writeEpoch}
        AND fenced_at IS NULL
    `);
  }

  async recordDeletionFailure(archiveId: UUID, failure: unknown, tx: Transaction): Promise<void> {
    await unwrap(tx)
      .update(archives)
      .set({ deletionFailure: failure })
      .where(eq(archives.id, archiveId));
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
    await unwrap(tx).execute(sql`
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
      WHERE id = ${input.archiveId} AND write_epoch = ${input.writeEpoch}
    `);
  }
}
