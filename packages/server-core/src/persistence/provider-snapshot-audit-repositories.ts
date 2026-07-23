import {
  type DirectionSelection,
  type RoomProviderSelection,
  RoomProviderSelectionSchema,
} from "@transhooter/contracts";
import { DomainError, type Instant, type UUID } from "../domain/model";
import type { AuditPort, ProviderSnapshotPort, Transaction } from "../ports/index";
import { Prisma, type PrismaClient } from "./database";
import { TransactionHandle, unwrap } from "./repositories";

type CapabilitySnapshot = (
  | Omit<
      Extract<DirectionSelection, { mode: "same_language" }>,
      "sourceParticipantId" | "destinationParticipantId" | "capabilityRowId"
    >
  | Omit<
      Extract<DirectionSelection, { mode: "translated" }>,
      "sourceParticipantId" | "destinationParticipantId" | "capabilityRowId"
    >
) & { capabilityVersion?: string };
type BypassSelectionRow = {
  profile_name: string;
  current_revision: number;
  capability_hash: string;
  id: UUID;
  snapshot: CapabilitySnapshot;
};
type CurrentProfileRow = { id: UUID; current_revision: number };
type TranslatedSelectionRow = {
  profileName: string;
  currentRevision: number;
  capabilityHash: string;
  id: UUID;
  sourceLocale: string;
  snapshot: CapabilitySnapshot;
};

abstract class PrismaRepository {
  constructor(protected readonly database: PrismaClient) {}

  transaction<T>(work: (tx: Transaction) => Promise<T>): Promise<T> {
    return this.database.$transaction((database) => work(new TransactionHandle(database)));
  }
}

export class PrismaProviderSnapshotRepository
  extends PrismaRepository
  implements ProviderSnapshotPort
{
  constructor(
    database: PrismaClient,
    private readonly canonicalHash: (value: unknown) => string,
  ) {
    super(database);
  }

  async resolve(
    profileId: UUID,
    participants: readonly [{ id: UUID; language: string }, { id: UUID; language: string }],
    tx: Transaction,
  ): Promise<{
    selection: RoomProviderSelection;
    hash: string;
    profileRevision: number;
  }> {
    if (participants[0].language === participants[1].language) {
      const bypass = await unwrap(tx).$queryRaw<BypassSelectionRow[]>(Prisma.sql`
        SELECT
          p.name AS profile_name,
          p.current_revision,
          r.capability_hash,
          l.id,
          l.snapshot
        FROM provider_profiles p
        JOIN provider_profile_revisions r
          ON r.profile_id = p.id AND r.revision = p.current_revision
        JOIN language_capabilities l
          ON l.profile_id = p.id AND l.revision = p.current_revision
        WHERE
          p.id = ${profileId}::uuid
          AND p.enabled
          AND l.enabled
          AND l.fresh_until > now()
          AND l.mode = 'same_language'
          AND l.source_locale = ${participants[0].language}
          AND l.target_locale = ${participants[0].language}
          FOR SHARE
      `);
      const row = bypass[0];
      if (!row) {
        throw new DomainError("PROFILE_INCOMPATIBLE");
      }

      const snapshot = row.snapshot;
      const directions = participants.map((participant, index) => ({
        ...snapshot,
        sourceParticipantId: participant.id,
        destinationParticipantId: participants[index === 0 ? 1 : 0].id,
        capabilityRowId: row.id,
      }));
      const profileRevision = Number(row.current_revision);
      const selection = RoomProviderSelectionSchema.parse({
        profileId: row.profile_name,
        profileRevision,
        capabilityHash: row.capability_hash,
        participantIds: [participants[0].id, participants[1].id],
        directions,
      });
      return {
        selection,
        hash: this.canonicalHash(selection),
        profileRevision,
      };
    }

    const rows = await unwrap(tx).$queryRaw<TranslatedSelectionRow[]>(Prisma.sql`
      SELECT
        p.name AS "profileName",
        p.current_revision AS "currentRevision",
        r.capability_hash AS "capabilityHash",
        l.id,
        l.source_locale AS "sourceLocale",
        l.snapshot
      FROM provider_profiles p
      JOIN provider_profile_revisions r
        ON r.profile_id = p.id AND r.revision = p.current_revision
      JOIN language_capabilities l
        ON l.profile_id = p.id
        AND l.revision = p.current_revision
        AND l.enabled = true
      WHERE p.id = ${profileId}::uuid
        AND p.enabled = true
        AND l.fresh_until > now()
        AND (
          (l.source_locale = ${participants[0].language}
            AND l.target_locale = ${participants[1].language})
          OR
          (l.source_locale = ${participants[1].language}
            AND l.target_locale = ${participants[0].language})
        )
    `);
    if (rows.length !== 2) {
      throw new DomainError("PROFILE_INCOMPATIBLE");
    }
    rows.sort((left) => (left.sourceLocale === participants[0].language ? -1 : 1));
    const first = rows[0];
    if (!first) {
      throw new DomainError("PROFILE_INCOMPATIBLE");
    }
    const directions = rows.map((row, index) => ({
      ...row.snapshot,
      sourceParticipantId: participants[index]?.id,
      destinationParticipantId: participants[index === 0 ? 1 : 0]?.id,
      capabilityRowId: row.id,
    }));
    const profileRevision = first.currentRevision;
    const selection = RoomProviderSelectionSchema.parse({
      profileId: first.profileName,
      profileRevision,
      capabilityHash: first.capabilityHash,
      participantIds: [participants[0].id, participants[1].id],
      directions,
    });
    return {
      selection,
      hash: this.canonicalHash(selection),
      profileRevision,
    };
  }

  async assertFreshAndHealthy(selection: RoomProviderSelection): Promise<void> {
    const rowIds = [...new Set(selection.directions.map((direction) => direction.capabilityRowId))];
    const rows = await this.database.$queryRaw<{ id: UUID }[]>(Prisma.sql`
      SELECT l.id
      FROM language_capabilities l
      JOIN provider_profiles p ON p.id = l.profile_id
      JOIN provider_profile_revisions r
        ON r.profile_id = p.id AND r.revision = l.revision
      WHERE l.id IN (${Prisma.join(rowIds.map((id) => Prisma.sql`${id}::uuid`))})
        AND p.name = ${selection.profileId}
        AND l.revision = ${selection.profileRevision}
        AND r.capability_hash = ${selection.capabilityHash}
        AND p.enabled = true
        AND l.enabled = true
        AND l.fresh_until > now()
    `);
    if (rows.length !== rowIds.length) {
      throw new DomainError("PROFILE_STALE_OR_DISABLED");
    }
  }

  async currentEnabledRevision(
    profileReference: string,
    tx: Transaction,
  ): Promise<{ profileId: UUID; revision: number }> {
    const result = await unwrap(tx).$queryRaw<CurrentProfileRow[]>(Prisma.sql`
      SELECT id, current_revision
      FROM provider_profiles
      WHERE
        (id::text = ${profileReference} OR name = ${profileReference})
        AND enabled
      ORDER BY (id::text = ${profileReference}) DESC
      LIMIT 1
      FOR SHARE
    `);
    const row = result[0];
    if (!row) {
      throw new DomainError("PROFILE_DISABLED_OR_NOT_FOUND");
    }
    return {
      profileId: row.id,
      revision: Number(row.current_revision),
    };
  }
}

export class PrismaAuditRepository extends PrismaRepository implements AuditPort {
  async append(
    input: {
      id: UUID;
      aggregateId: UUID;
      actorId: UUID | null;
      kind: string;
      occurredAt: Instant;
      details: unknown;
    },
    tx: Transaction,
  ): Promise<void> {
    await unwrap(tx).$executeRaw(Prisma.sql`
      INSERT INTO audit_events (id, aggregate_id, actor_id, kind, occurred_at, details)
      VALUES (
        ${input.id}::uuid,
        ${input.aggregateId}::uuid,
        ${input.actorId}::uuid,
        ${input.kind},
        ${input.occurredAt},
        ${JSON.stringify(input.details)}::jsonb
      )
    `);
  }
}
