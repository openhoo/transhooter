import { RoomProviderSelectionSchema } from "@transhooter/contracts";
import { and, asc, eq, inArray, sql } from "drizzle-orm";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";
import type { ConsentRecord, Consultation, ParticipantSlot } from "../consultations/domain";
import { DomainError, type Instant, type UUID } from "../domain/model";
import type { ConsultationRepository, EgressEventEarlySource, Transaction } from "../ports/index";
import {
  type DrizzleSchema,
  type DrizzleTransaction,
  TransactionHandle,
  unwrap,
} from "./repositories";
import {
  archives,
  consultationParticipants,
  consultations,
  egressJobs,
  externalEffects,
  roomProviderSelections,
} from "./schema";

abstract class DrizzleRepository {
  constructor(protected readonly database: NodePgDatabase<DrizzleSchema>) {}

  transaction<T>(work: (tx: Transaction) => Promise<T>): Promise<T> {
    return this.database.transaction((database) => work(new TransactionHandle(database)));
  }
}

export class DrizzleConsultationRepository
  extends DrizzleRepository
  implements ConsultationRepository
{
  async lock(id: UUID, tx: Transaction): Promise<Consultation | null> {
    await unwrap(tx)
      .select({ id: consultations.id })
      .from(consultations)
      .where(eq(consultations.id, id))
      .for("update");
    return this.load(id, unwrap(tx));
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
    const rows = await database
      .select({ id: consultations.id })
      .from(consultations)
      .where(
        and(
          eq(consultations.employeeUserId, employeeUserId),
          eq(consultations.creationIdempotencyKey, creationIdempotencyKey),
        ),
      )
      .limit(1);
    const id = rows[0]?.id;
    return id ? this.load(id, database) : null;
  }

  async listForUser(userId: UUID): Promise<readonly Consultation[]> {
    const rows = await this.database
      .select({ consultationId: consultationParticipants.consultationId })
      .from(consultationParticipants)
      .where(eq(consultationParticipants.userId, userId))
      .orderBy(asc(consultationParticipants.consultationId));
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
    const inserted = await database
      .insert(consultations)
      .values({
        id: value.id,
        state: value.state,
        providerProfileId: value.providerProfileId,
        providerProfileRevision: value.providerProfileRevision,
        providerSelection: value.providerSelection,
        snapshotHash: value.snapshotHash,
        generation: value.generation,
        roomName: value.roomName,
        roomSid: value.roomSid,
        dispatchId: value.dispatchId,
        compositeEgressId: value.compositeEgressId,
        workerIdentity: value.workerIdentity,
        readyDeadlineAt: value.readyDeadlineAt,
        finalizeDeadlineAt: value.finalizeDeadlineAt,
        bothAbsentSince: value.bothAbsentSince,
        admissionFencedAt: value.admissionFencedAt,
        employeeUserId,
        creationIdempotencyKey,
        createdAt: value.createdAt,
        updatedAt: value.updatedAt,
      })
      .onConflictDoNothing({
        target: [consultations.employeeUserId, consultations.creationIdempotencyKey],
      })
      .returning({ id: consultations.id });
    if (inserted.length === 0) {
      return false;
    }

    for (const slot of value.participants) {
      await database.insert(consultationParticipants).values({
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
      });
    }
    await database.insert(archives).values({
      id: value.id,
      consultationId: value.id,
      state: value.archiveState,
      writeEpoch: 0,
      reconciliationDeadlineAt: null,
      createdAt: value.createdAt,
      updatedAt: value.updatedAt,
    });
    return true;
  }
  async save(value: Consultation, expectedUpdatedAt: Instant, tx: Transaction): Promise<boolean> {
    const database = unwrap(tx);
    const updated = await database
      .update(consultations)
      .set({
        state: value.state,
        providerProfileRevision: value.providerProfileRevision,
        providerSelection: value.providerSelection,
        snapshotHash: value.snapshotHash,
        generation: value.generation,
        roomName: value.roomName,
        roomSid: value.roomSid,
        dispatchId: value.dispatchId,
        compositeEgressId: value.compositeEgressId,
        workerIdentity: value.workerIdentity,
        readyDeadlineAt: value.readyDeadlineAt,
        finalizeDeadlineAt: value.finalizeDeadlineAt,
        bothAbsentSince: value.bothAbsentSince,
        admissionFencedAt: value.admissionFencedAt,
        updatedAt: sql`GREATEST(now(), ${consultations.updatedAt} + interval '1 microsecond')`,
      })
      .where(
        and(
          eq(consultations.id, value.id),
          sql`date_trunc('milliseconds', ${consultations.updatedAt}) = ${expectedUpdatedAt}`,
        ),
      )
      .returning({ id: consultations.id });
    if (updated.length !== 1) {
      return false;
    }

    if (value.providerSelection && value.snapshotHash) {
      const frozen = await database
        .insert(roomProviderSelections)
        .values({
          consultationId: value.id,
          profileId: value.providerProfileId,
          profileRevision: value.providerProfileRevision,
          capabilityHash: value.providerSelection.capabilityHash,
          selectionHash: value.snapshotHash,
          selection: value.providerSelection,
          createdAt: value.updatedAt,
        })
        .onConflictDoUpdate({
          target: roomProviderSelections.consultationId,
          set: {
            profileId: value.providerProfileId,
            profileRevision: value.providerProfileRevision,
            capabilityHash: value.providerSelection.capabilityHash,
            selectionHash: value.snapshotHash,
            selection: value.providerSelection,
            createdAt: value.updatedAt,
          },
          setWhere: sql`
            ${roomProviderSelections.selectionHash} = excluded.selection_hash
            OR EXISTS(
              SELECT 1
              FROM ${consultations}
              WHERE
                ${consultations.id} = excluded.consultation_id
                AND ${consultations.state} = 'invited'
            )
          `,
        })
        .returning({ consultationId: roomProviderSelections.consultationId });
      if (frozen.length !== 1) {
        throw new DomainError("PROVIDER_SELECTION_CONFLICT");
      }
    }

    for (const slot of value.participants) {
      const capabilityRowId =
        value.providerSelection?.directions.find(
          (direction) => direction.sourceParticipantId === slot.id,
        )?.capabilityRowId ?? null;
      await database
        .update(consultationParticipants)
        .set({
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
        })
        .where(
          and(
            eq(consultationParticipants.id, slot.id),
            eq(consultationParticipants.consultationId, value.id),
          ),
        );
    }

    await database
      .update(archives)
      .set({
        state: value.archiveState,
        reconciliationDeadlineAt: sql`
          CASE
            WHEN ${value.archiveState} = 'reconciling'
              AND ${archives.reconciliationDeadlineAt} IS NULL
            THEN ${new Date(value.updatedAt.getTime() + 30 * 60_000)}
            ELSE ${archives.reconciliationDeadlineAt}
          END
        `,
        updatedAt: value.updatedAt,
      })
      .where(eq(archives.consultationId, value.id));
    return true;
  }

  async isCurrentEgress(
    consultationId: UUID,
    generation: number,
    egressId: string,
    tx: Transaction,
  ): Promise<boolean> {
    const database = unwrap(tx);
    const jobs = await database
      .select({ id: egressJobs.id })
      .from(egressJobs)
      .where(
        and(
          eq(egressJobs.consultationId, consultationId),
          eq(egressJobs.generation, generation),
          eq(egressJobs.egressId, egressId),
        ),
      )
      .limit(1);
    if (jobs.length === 1) {
      return true;
    }
    const participants = await database
      .select({ id: consultationParticipants.id })
      .from(consultationParticipants)
      .innerJoin(consultations, eq(consultations.id, consultationParticipants.consultationId))
      .where(
        and(
          eq(consultations.id, consultationId),
          eq(consultations.generation, generation),
          eq(consultationParticipants.participantEgressId, egressId),
        ),
      )
      .limit(1);
    return participants.length === 1;
  }

  async resolveCurrentEgressSubject(
    consultationId: UUID,
    generation: number,
    egressId: string,
    tx: Transaction,
  ): Promise<{ participantId: UUID | null } | null> {
    const database = unwrap(tx);
    const jobs = await database
      .select({
        kind: egressJobs.kind,
        subjectId: egressJobs.subjectId,
      })
      .from(egressJobs)
      .where(
        and(
          eq(egressJobs.consultationId, consultationId),
          eq(egressJobs.generation, generation),
          eq(egressJobs.egressId, egressId),
        ),
      )
      .limit(1);
    const job = jobs[0];
    if (job) {
      return { participantId: job.kind === "participant" ? job.subjectId : null };
    }

    const participants = await database
      .select({ participantId: consultationParticipants.id })
      .from(consultationParticipants)
      .innerJoin(consultations, eq(consultations.id, consultationParticipants.consultationId))
      .where(
        and(
          eq(consultations.id, consultationId),
          eq(consultations.generation, generation),
          eq(consultationParticipants.participantEgressId, egressId),
        ),
      )
      .limit(1);
    return participants[0] ?? null;
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
    const rows = await this.database
      .select({
        consultationId: egressJobs.consultationId,
        generation: egressJobs.generation,
        roomName: consultations.roomName,
      })
      .from(egressJobs)
      .innerJoin(consultations, eq(consultations.id, egressJobs.consultationId))
      .where(
        and(eq(egressJobs.egressId, egressId), eq(consultations.generation, egressJobs.generation)),
      );
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
      const earlyRows = await this.database
        .select({
          consultationId: externalEffects.consultationId,
          generation: externalEffects.generation,
          roomName: consultations.roomName,
        })
        .from(externalEffects)
        .innerJoin(consultations, eq(consultations.id, externalEffects.consultationId))
        .where(
          and(
            eq(consultations.roomName, earlySource.roomName),
            eq(consultations.generation, externalEffects.generation),
            eq(externalEffects.effectKind, "ROOM_COMPOSITE_EGRESS"),
            inArray(externalEffects.state, ["planned", "calling", "applied", "done"]),
          ),
        )
        .limit(2);
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

    const earlyRows = await this.database
      .select({
        consultationId: externalEffects.consultationId,
        generation: externalEffects.generation,
        roomName: consultations.roomName,
        participantId: consultationParticipants.id,
      })
      .from(externalEffects)
      .innerJoin(consultations, eq(consultations.id, externalEffects.consultationId))
      .innerJoin(
        consultationParticipants,
        and(
          eq(consultationParticipants.consultationId, externalEffects.consultationId),
          eq(consultationParticipants.id, externalEffects.subjectId),
        ),
      )
      .where(
        and(
          eq(consultations.roomName, earlySource.roomName),
          eq(consultations.generation, externalEffects.generation),
          eq(externalEffects.effectKind, "PARTICIPANT_EGRESS"),
          eq(consultationParticipants.livekitIdentity, earlySource.identity),
          inArray(externalEffects.state, ["planned", "calling", "applied", "done"]),
        ),
      )
      .limit(2);
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
    const result = await unwrap(tx).execute<{ id: UUID }>(sql`
      UPDATE consultation_participants p
      SET participant_egress_id = NULL, publication_granted = false
      FROM consultations c
      WHERE
        p.id = ${participantId}
        AND p.consultation_id = c.id
        AND c.id = ${consultationId}
        AND c.generation = ${generation}
        AND p.participant_egress_id = ${egressId}
      RETURNING p.id
    `);
    return result.rowCount === 1;
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
    const result = await unwrap(tx).execute<{ id: UUID }>(sql`
      UPDATE consultations
      SET
        room_sid = COALESCE(room_sid, ${roomSid}),
        dispatch_id = COALESCE(dispatch_id, ${dispatchId}),
        composite_egress_id = COALESCE(composite_egress_id, ${compositeEgressId}),
        updated_at = now()
      WHERE
        id = ${consultationId}
        AND generation = ${generation}
        AND (
          room_sid IS NULL
          OR room_sid IS NOT DISTINCT FROM ${roomSid}
          OR ${roomSid} IS NULL
        )
        AND (
          dispatch_id IS NULL
          OR dispatch_id IS NOT DISTINCT FROM ${dispatchId}
          OR ${dispatchId} IS NULL
        )
        AND (
          composite_egress_id IS NULL
          OR composite_egress_id IS NOT DISTINCT FROM ${compositeEgressId}
          OR ${compositeEgressId} IS NULL
        )
      RETURNING id
    `);
    return result.rowCount === 1;
  }

  private async load(
    id: UUID,
    database: NodePgDatabase<DrizzleSchema> | DrizzleTransaction,
  ): Promise<Consultation | null> {
    const rows = await database
      .select({
        consultation: consultations,
        archiveState: archives.state,
      })
      .from(consultations)
      .innerJoin(archives, eq(archives.consultationId, consultations.id))
      .where(eq(consultations.id, id))
      .limit(1);
    const row = rows[0];
    if (!row) {
      return null;
    }

    const participantRows = await database
      .select()
      .from(consultationParticipants)
      .where(eq(consultationParticipants.consultationId, id))
      .orderBy(asc(consultationParticipants.role));
    const [firstParticipant, secondParticipant] = participantRows;
    if (
      firstParticipant === undefined ||
      secondParticipant === undefined ||
      participantRows.length !== 2
    ) {
      throw new DomainError("INVALID_PARTICIPANTS");
    }
    const participants: [ParticipantSlot, ParticipantSlot] = [
      mapParticipant(firstParticipant),
      mapParticipant(secondParticipant),
    ];
    const consultation = row.consultation;

    return {
      id: consultation.id,
      state: consultation.state,
      archiveState: row.archiveState,
      providerProfileId: consultation.providerProfileId,
      providerProfileRevision: consultation.providerProfileRevision,
      participants,
      providerSelection: consultation.providerSelection
        ? RoomProviderSelectionSchema.parse(consultation.providerSelection)
        : null,
      snapshotHash: consultation.snapshotHash,
      generation: consultation.generation,
      roomName: consultation.roomName,
      roomSid: consultation.roomSid,
      dispatchId: consultation.dispatchId,
      compositeEgressId: consultation.compositeEgressId,
      workerIdentity: consultation.workerIdentity,
      readyDeadlineAt: consultation.readyDeadlineAt,
      finalizeDeadlineAt: consultation.finalizeDeadlineAt,
      bothAbsentSince: consultation.bothAbsentSince,
      admissionFencedAt: consultation.admissionFencedAt,
      createdAt: consultation.createdAt,
      updatedAt: consultation.updatedAt,
    };
  }
}

function mapParticipant(row: typeof consultationParticipants.$inferSelect): ParticipantSlot {
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
