import { RoomProviderSelectionSchema } from "@transhooter/contracts";
import type { ConsentRecord, Consultation, ParticipantSlot } from "../consultations/domain";
import { DomainError, type Instant, type UUID } from "../domain/model";
import type { ConsultationRepository, EgressEventEarlySource, Transaction } from "../ports/index";
import { Prisma, type PrismaClient } from "./database";
import { TransactionHandle, unwrap } from "./repositories";

abstract class PrismaRepository {
  constructor(protected readonly database: PrismaClient) {}

  transaction<T>(work: (tx: Transaction) => Promise<T>): Promise<T> {
    return this.database.$transaction((database) => work(new TransactionHandle(database)));
  }
}

export class PrismaConsultationRepository
  extends PrismaRepository
  implements ConsultationRepository
{
  async lock(id: UUID, tx: Transaction): Promise<Consultation | null> {
    const database = unwrap(tx);
    await database.$queryRaw(Prisma.sql`
      SELECT id
      FROM consultations
      WHERE id = ${id}::uuid
      FOR UPDATE
    `);
    return this.load(id, database);
  }

  get(id: UUID): Promise<Consultation | null> {
    return this.load(id, this.database);
  }

  async findByCreationIdempotencyKey(
    employeeUserId: UUID,
    creationIdempotencyKey: UUID,
    tx: Transaction,
  ): Promise<Consultation | null> {
    const database = unwrap(tx);
    const row = await database.consultation.findFirst({
      where: { employeeUserId, creationIdempotencyKey },
      select: { id: true },
    });
    return row ? this.load(row.id, database) : null;
  }

  async listForUser(userId: UUID): Promise<readonly Consultation[]> {
    const rows = await this.database.consultationParticipant.findMany({
      where: { userId },
      select: { consultationId: true },
      orderBy: { consultationId: "asc" },
    });
    const consultationsForUser = await Promise.all(
      rows.map(({ consultationId }) => this.load(consultationId, this.database)),
    );

    return consultationsForUser.filter((value): value is Consultation => value !== null);
  }

  async create(
    value: Consultation,
    employeeUserId: UUID,
    creationIdempotencyKey: UUID,
    tx: Transaction,
  ): Promise<boolean> {
    const database = unwrap(tx);
    const inserted = await database.$queryRaw<IdRow[]>(Prisma.sql`
      INSERT INTO consultations (
        id,
        state,
        provider_profile_id,
        provider_profile_revision,
        provider_selection,
        snapshot_hash,
        generation,
        room_name,
        room_sid,
        dispatch_id,
        composite_egress_id,
        worker_identity,
        ready_deadline_at,
        finalize_deadline_at,
        both_absent_since,
        admission_fenced_at,
        employee_user_id,
        creation_idempotency_key,
        created_at,
        updated_at
      )
      VALUES (
        ${value.id}::uuid,
        ${value.state}::consultation_state,
        ${value.providerProfileId}::uuid,
        ${value.providerProfileRevision},
        ${value.providerSelection === null ? null : JSON.stringify(value.providerSelection)}::jsonb,
        ${value.snapshotHash},
        ${value.generation},
        ${value.roomName},
        ${value.roomSid},
        ${value.dispatchId},
        ${value.compositeEgressId},
        ${value.workerIdentity}::uuid,
        ${value.readyDeadlineAt},
        ${value.finalizeDeadlineAt},
        ${value.bothAbsentSince},
        ${value.admissionFencedAt},
        ${employeeUserId}::uuid,
        ${creationIdempotencyKey},
        ${value.createdAt},
        ${value.updatedAt}
      )
      ON CONFLICT (employee_user_id, creation_idempotency_key) DO NOTHING
      RETURNING id
    `);
    if (inserted.length === 0) {
      return false;
    }

    for (const slot of value.participants) {
      await database.consultationParticipant.create({
        data: {
          id: slot.id,
          consultationId: value.id,
          userId: slot.userId,
          role: slot.role,
          livekitIdentity: slot.livekitIdentity,
          displayName: slot.displayName,
          language: slot.language,
          consentVersion: slot.consent?.version ?? null,
          consentCopyHash: slot.consent?.copyHash ?? null,
          consentSnapshotHash: slot.consent?.snapshotHash ?? null,
          consentedAt: slot.consent?.consentedAt ?? null,
          present: slot.present,
          presenceEventId: slot.eventWatermark,
          presenceEventTime: slot.eventOccurredAt,
          publicationGranted: slot.publicationGranted,
          participantEgressId: slot.participantEgressId,
        },
      });
    }
    await database.archive.create({
      data: {
        id: value.id,
        consultationId: value.id,
        state: value.archiveState,
        writeEpoch: 0,
        reconciliationDeadlineAt: null,
        createdAt: value.createdAt,
        updatedAt: value.updatedAt,
      },
    });
    return true;
  }

  async save(value: Consultation, expectedUpdatedAt: Instant, tx: Transaction): Promise<boolean> {
    const database = unwrap(tx);
    const updated = await database.$queryRaw<IdRow[]>(Prisma.sql`
      UPDATE consultations
      SET
        state = ${value.state}::consultation_state,
        provider_profile_revision = ${value.providerProfileRevision},
        provider_selection = ${value.providerSelection === null ? null : JSON.stringify(value.providerSelection)}::jsonb,
        snapshot_hash = ${value.snapshotHash},
        generation = ${value.generation},
        room_name = ${value.roomName},
        room_sid = ${value.roomSid},
        dispatch_id = ${value.dispatchId},
        composite_egress_id = ${value.compositeEgressId},
        worker_identity = ${value.workerIdentity}::uuid,
        ready_deadline_at = ${value.readyDeadlineAt},
        finalize_deadline_at = ${value.finalizeDeadlineAt},
        both_absent_since = ${value.bothAbsentSince},
        admission_fenced_at = ${value.admissionFencedAt},
        updated_at = GREATEST(now(), updated_at + interval '1 microsecond')
      WHERE
        id = ${value.id}::uuid
        AND date_trunc('milliseconds', updated_at) = ${expectedUpdatedAt}::timestamptz
      RETURNING id
    `);
    if (updated.length !== 1) {
      return false;
    }

    if (value.providerSelection && value.snapshotHash) {
      const frozen = await database.$queryRaw<ConsultationIdRow[]>(Prisma.sql`
        INSERT INTO room_provider_selections (
          consultation_id,
          profile_id,
          profile_revision,
          capability_hash,
          selection_hash,
          selection,
          created_at
        )
        VALUES (
          ${value.id}::uuid,
          ${value.providerProfileId}::uuid,
          ${value.providerProfileRevision},
          ${value.providerSelection.capabilityHash},
          ${value.snapshotHash},
          ${JSON.stringify(value.providerSelection)}::jsonb,
          ${value.updatedAt}
        )
        ON CONFLICT (consultation_id) DO UPDATE
        SET
          profile_id = EXCLUDED.profile_id,
          profile_revision = EXCLUDED.profile_revision,
          capability_hash = EXCLUDED.capability_hash,
          selection_hash = EXCLUDED.selection_hash,
          selection = EXCLUDED.selection,
          created_at = EXCLUDED.created_at
        WHERE
          room_provider_selections.selection_hash = EXCLUDED.selection_hash
          OR EXISTS (
            SELECT 1
            FROM consultations
            WHERE
              consultations.id = EXCLUDED.consultation_id
              AND consultations.state = 'invited'
          )
        RETURNING consultation_id AS "consultationId"
      `);
      if (frozen.length !== 1) {
        throw new DomainError("PROVIDER_SELECTION_CONFLICT");
      }
    }

    for (const slot of value.participants) {
      const capabilityRowId =
        value.providerSelection?.directions.find(
          (direction) => direction.sourceParticipantId === slot.id,
        )?.capabilityRowId ?? null;
      await database.consultationParticipant.updateMany({
        where: { id: slot.id, consultationId: value.id },
        data: {
          displayName: slot.displayName,
          language: slot.language,
          capabilityRowId,
          consentVersion: slot.consent?.version ?? null,
          consentCopyHash: slot.consent?.copyHash ?? null,
          consentSnapshotHash: slot.consent?.snapshotHash ?? null,
          consentedAt: slot.consent?.consentedAt ?? null,
          present: slot.present,
          presenceEventId: slot.eventWatermark,
          presenceEventTime: slot.eventOccurredAt,
          publicationGranted: slot.publicationGranted,
          participantEgressId: slot.participantEgressId,
        },
      });
    }

    await database.$executeRaw(Prisma.sql`
      UPDATE archives
      SET
        state = ${value.archiveState}::archive_state,
        reconciliation_deadline_at = CASE
          WHEN ${value.archiveState}::archive_state = 'reconciling'
            AND reconciliation_deadline_at IS NULL
          THEN ${new Date(value.updatedAt.getTime() + 30 * 60_000)}
          ELSE reconciliation_deadline_at
        END,
        updated_at = ${value.updatedAt}
      WHERE consultation_id = ${value.id}::uuid
    `);
    return true;
  }

  async isCurrentEgress(
    consultationId: UUID,
    generation: number,
    egressId: string,
    tx: Transaction,
  ): Promise<boolean> {
    const database = unwrap(tx);
    const job = await database.egressJob.findFirst({
      where: { consultationId, generation, egressId },
      select: { id: true },
    });
    if (job) {
      return true;
    }
    const participant = await database.consultationParticipant.findFirst({
      where: {
        consultationId,
        participantEgressId: egressId,
        consultation: { generation },
      },
      select: { id: true },
    });
    return participant !== null;
  }

  async resolveCurrentEgressSubject(
    consultationId: UUID,
    generation: number,
    egressId: string,
    tx: Transaction,
  ): Promise<{ participantId: UUID | null } | null> {
    const database = unwrap(tx);
    const job = await database.egressJob.findFirst({
      where: { consultationId, generation, egressId },
      select: { kind: true, subjectId: true },
    });
    if (job) {
      return { participantId: job.kind === "participant" ? job.subjectId : null };
    }

    const participant = await database.consultationParticipant.findFirst({
      where: {
        consultationId,
        participantEgressId: egressId,
        consultation: { generation },
      },
      select: { id: true },
    });
    return participant ? { participantId: participant.id } : null;
  }

  async resolveEgressEvent(
    egressId: string,
    earlySource?: EgressEventEarlySource,
  ): Promise<{
    consultationId: UUID;
    generation: number;
    roomName: string;
    earlySubject?: { participantId: UUID | null };
  } | null> {
    const rows = await this.database.$queryRaw<EgressEventRow[]>(Prisma.sql`
      SELECT
        e.consultation_id AS "consultationId",
        e.generation,
        c.room_name AS "roomName"
      FROM egress_jobs e
      INNER JOIN consultations c ON c.id = e.consultation_id
      WHERE
        e.egress_id = ${egressId}
        AND c.generation = e.generation
    `);
    const row = rows[0];
    if (row && rows.length === 1 && row.roomName !== null) {
      return {
        consultationId: row.consultationId,
        generation: row.generation,
        roomName: row.roomName,
      };
    }
    if (!earlySource) {
      return null;
    }

    if (earlySource.kind === "room_composite") {
      const earlyRows = await this.database.$queryRaw<EgressEventRow[]>(Prisma.sql`
        SELECT
          e.consultation_id AS "consultationId",
          e.generation,
          c.room_name AS "roomName"
        FROM external_effects e
        INNER JOIN consultations c ON c.id = e.consultation_id
        WHERE
          c.room_name = ${earlySource.roomName}
          AND c.generation = e.generation
          AND e.effect_kind = 'ROOM_COMPOSITE_EGRESS'
          AND e.state IN ('planned', 'calling', 'applied', 'done')
        LIMIT 2
      `);
      const earlyRow = earlyRows[0];
      if (!earlyRow || earlyRows.length !== 1 || earlyRow.roomName === null) {
        return null;
      }
      return {
        consultationId: earlyRow.consultationId,
        generation: earlyRow.generation,
        roomName: earlyRow.roomName,
        earlySubject: { participantId: null },
      };
    }

    const earlyRows = await this.database.$queryRaw<ParticipantEgressEventRow[]>(Prisma.sql`
      SELECT
        e.consultation_id AS "consultationId",
        e.generation,
        c.room_name AS "roomName",
        p.id AS "participantId"
      FROM external_effects e
      INNER JOIN consultations c ON c.id = e.consultation_id
      INNER JOIN consultation_participants p
        ON p.consultation_id = e.consultation_id
        AND p.id = e.subject_id
      WHERE
        c.room_name = ${earlySource.roomName}
        AND c.generation = e.generation
        AND e.effect_kind = 'PARTICIPANT_EGRESS'
        AND p.livekit_identity = ${earlySource.identity}::uuid
        AND e.state IN ('planned', 'calling', 'applied', 'done')
      LIMIT 2
    `);
    const earlyRow = earlyRows[0];
    if (!earlyRow || earlyRows.length !== 1 || earlyRow.roomName === null) {
      return null;
    }
    return {
      consultationId: earlyRow.consultationId,
      generation: earlyRow.generation,
      roomName: earlyRow.roomName,
      earlySubject: { participantId: earlyRow.participantId },
    };
  }

  async clearParticipantEgressBinding(
    consultationId: UUID,
    generation: number,
    participantId: UUID,
    egressId: string,
    tx: Transaction,
  ): Promise<boolean> {
    const rows = await unwrap(tx).$queryRaw<IdRow[]>(Prisma.sql`
      UPDATE consultation_participants p
      SET participant_egress_id = NULL, publication_granted = false
      FROM consultations c
      WHERE
        p.id = ${participantId}::uuid
        AND p.consultation_id = c.id
        AND c.id = ${consultationId}::uuid
        AND c.generation = ${generation}
        AND p.participant_egress_id = ${egressId}
      RETURNING p.id
    `);
    return rows.length === 1;
  }

  async persistProvisioningIds(
    consultationId: UUID,
    generation: number,
    ids: {
      roomSid?: string;
      dispatchId?: string;
      compositeEgressId?: string;
    },
    tx: Transaction,
  ): Promise<boolean> {
    const roomSid = ids.roomSid ?? null;
    const dispatchId = ids.dispatchId ?? null;
    const compositeEgressId = ids.compositeEgressId ?? null;
    const rows = await unwrap(tx).$queryRaw<IdRow[]>(Prisma.sql`
      UPDATE consultations
      SET
        room_sid = COALESCE(room_sid, ${roomSid}::text),
        dispatch_id = COALESCE(dispatch_id, ${dispatchId}::text),
        composite_egress_id = COALESCE(composite_egress_id, ${compositeEgressId}::text),
        updated_at = now()
      WHERE
        id = ${consultationId}::uuid
        AND generation = ${generation}
        AND (
          room_sid IS NULL
          OR room_sid IS NOT DISTINCT FROM ${roomSid}::text
          OR ${roomSid}::text IS NULL
        )
        AND (
          dispatch_id IS NULL
          OR dispatch_id IS NOT DISTINCT FROM ${dispatchId}::text
          OR ${dispatchId}::text IS NULL
        )
        AND (
          composite_egress_id IS NULL
          OR composite_egress_id IS NOT DISTINCT FROM ${compositeEgressId}::text
          OR ${compositeEgressId}::text IS NULL
        )
      RETURNING id
    `);
    return rows.length === 1;
  }
  private async load(
    id: UUID,
    database: PrismaClient | Prisma.TransactionClient,
  ): Promise<Consultation | null> {
    const row = await database.consultation.findUnique({
      where: { id },
      include: {
        archive: { select: { state: true } },
        participants: { orderBy: { role: "asc" } },
      },
    });
    if (!row?.archive) {
      return null;
    }

    const [firstParticipant, secondParticipant] = row.participants;
    if (
      firstParticipant === undefined ||
      secondParticipant === undefined ||
      row.participants.length !== 2
    ) {
      throw new DomainError("INVALID_PARTICIPANTS");
    }
    const participants: [ParticipantSlot, ParticipantSlot] = [
      mapParticipant(firstParticipant),
      mapParticipant(secondParticipant),
    ];

    return {
      id: row.id,
      state: row.state,
      archiveState: row.archive.state,
      providerProfileId: row.providerProfileId,
      providerProfileRevision: row.providerProfileRevision,
      participants,
      providerSelection: row.providerSelection
        ? RoomProviderSelectionSchema.parse(row.providerSelection)
        : null,
      snapshotHash: row.snapshotHash,
      generation: row.generation,
      roomName: row.roomName,
      roomSid: row.roomSid,
      dispatchId: row.dispatchId,
      compositeEgressId: row.compositeEgressId,
      workerIdentity: row.workerIdentity,
      readyDeadlineAt: row.readyDeadlineAt,
      finalizeDeadlineAt: row.finalizeDeadlineAt,
      bothAbsentSince: row.bothAbsentSince,
      admissionFencedAt: row.admissionFencedAt,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
    };
  }
}

interface IdRow {
  id: UUID;
}

interface ConsultationIdRow {
  consultationId: UUID;
}

interface EgressEventRow {
  consultationId: UUID;
  generation: number;
  roomName: string | null;
}

interface ParticipantEgressEventRow extends EgressEventRow {
  participantId: UUID;
}

function mapParticipant(row: Prisma.ConsultationParticipantModel): ParticipantSlot {
  const consent =
    row.consentVersion === null
      ? null
      : ({
          version: 1,
          copyHash: String(row.consentCopyHash),
          snapshotHash: String(row.consentSnapshotHash),
          consentedAt: new Date(String(row.consentedAt)),
        } satisfies ConsentRecord);

  return {
    id: row.id,
    userId: row.userId,
    role: row.role,
    livekitIdentity: row.livekitIdentity,
    displayName: row.displayName,
    language: row.language,
    consent,
    present: row.present,
    eventWatermark: row.presenceEventId,
    eventOccurredAt: row.presenceEventTime,
    publicationGranted: row.publicationGranted,
    participantEgressId: row.participantEgressId,
  };
}
