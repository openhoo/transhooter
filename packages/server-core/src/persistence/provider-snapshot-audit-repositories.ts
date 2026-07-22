import {
  type DirectionSelection,
  type RoomProviderSelection,
  RoomProviderSelectionSchema,
} from "@transhooter/contracts";
import { and, eq, gt, inArray, or, sql } from "drizzle-orm";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";
import { DomainError, type Instant, type UUID } from "../domain/model";
import type { AuditPort, ProviderSnapshotPort, Transaction } from "../ports/index";
import { type DrizzleSchema, TransactionHandle, unwrap } from "./repositories";
import {
  auditEvents,
  languageCapabilities,
  providerProfileRevisions,
  providerProfiles,
} from "./schema";

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

abstract class DrizzleRepository {
  constructor(protected readonly database: NodePgDatabase<DrizzleSchema>) {}

  transaction<T>(work: (tx: Transaction) => Promise<T>): Promise<T> {
    return this.database.transaction((database) => work(new TransactionHandle(database)));
  }
}

export class DrizzleProviderSnapshotRepository
  extends DrizzleRepository
  implements ProviderSnapshotPort
{
  constructor(
    database: NodePgDatabase<DrizzleSchema>,
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
      const bypass = await unwrap(tx).execute<BypassSelectionRow>(
        sql`
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
          p.id = ${profileId}
          AND p.enabled
          AND l.enabled
          AND l.fresh_until > now()
          AND l.mode = 'same_language'
          AND l.source_locale = ${participants[0].language}
          AND l.target_locale = ${participants[0].language}
          FOR SHARE
        `,
      );
      const row = bypass.rows[0];
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

    const rows = await unwrap(tx)
      .select({
        profileName: providerProfiles.name,
        currentRevision: providerProfiles.currentRevision,
        capabilityHash: providerProfileRevisions.capabilityHash,
        id: languageCapabilities.id,
        sourceLocale: languageCapabilities.sourceLocale,
        snapshot: languageCapabilities.snapshot,
      })
      .from(providerProfiles)
      .innerJoin(
        providerProfileRevisions,
        and(
          eq(providerProfileRevisions.profileId, providerProfiles.id),
          eq(providerProfileRevisions.revision, providerProfiles.currentRevision),
        ),
      )
      .innerJoin(
        languageCapabilities,
        and(
          eq(languageCapabilities.profileId, providerProfiles.id),
          eq(languageCapabilities.revision, providerProfiles.currentRevision),
          eq(languageCapabilities.enabled, true),
        ),
      )
      .where(
        and(
          eq(providerProfiles.id, profileId),
          eq(providerProfiles.enabled, true),
          gt(languageCapabilities.freshUntil, sql`now()`),
          or(
            and(
              eq(languageCapabilities.sourceLocale, participants[0].language),
              eq(languageCapabilities.targetLocale, participants[1].language),
            ),
            and(
              eq(languageCapabilities.sourceLocale, participants[1].language),
              eq(languageCapabilities.targetLocale, participants[0].language),
            ),
          ),
        ),
      );
    if (rows.length !== 2) {
      throw new DomainError("PROFILE_INCOMPATIBLE");
    }
    rows.sort((left) => (left.sourceLocale === participants[0].language ? -1 : 1));
    const first = rows[0];
    if (!first) {
      throw new DomainError("PROFILE_INCOMPATIBLE");
    }
    const directions = rows.map((row, index) => ({
      ...(row.snapshot as CapabilitySnapshot),
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
    const rows = await this.database
      .select({ id: languageCapabilities.id })
      .from(languageCapabilities)
      .innerJoin(providerProfiles, eq(providerProfiles.id, languageCapabilities.profileId))
      .innerJoin(
        providerProfileRevisions,
        and(
          eq(providerProfileRevisions.profileId, providerProfiles.id),
          eq(providerProfileRevisions.revision, languageCapabilities.revision),
        ),
      )
      .where(
        and(
          inArray(languageCapabilities.id, rowIds),
          eq(providerProfiles.name, selection.profileId),
          eq(languageCapabilities.revision, selection.profileRevision),
          eq(providerProfileRevisions.capabilityHash, selection.capabilityHash),
          eq(providerProfiles.enabled, true),
          eq(languageCapabilities.enabled, true),
          gt(languageCapabilities.freshUntil, sql`now()`),
        ),
      );
    if (rows.length !== rowIds.length) {
      throw new DomainError("PROFILE_STALE_OR_DISABLED");
    }
  }

  async currentEnabledRevision(
    profileReference: string,
    tx: Transaction,
  ): Promise<{ profileId: UUID; revision: number }> {
    const result = await unwrap(tx).execute<CurrentProfileRow>(
      sql`
        SELECT id, current_revision
        FROM provider_profiles
        WHERE
          (id::text = ${profileReference} OR name = ${profileReference})
          AND enabled
        ORDER BY (id::text = ${profileReference}) DESC
        LIMIT 1
        FOR SHARE
      `,
    );
    const row = result.rows[0];
    if (!row) {
      throw new DomainError("PROFILE_DISABLED_OR_NOT_FOUND");
    }
    return {
      profileId: row.id,
      revision: Number(row.current_revision),
    };
  }
}

export class DrizzleAuditRepository extends DrizzleRepository implements AuditPort {
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
    await unwrap(tx).insert(auditEvents).values({
      id: input.id,
      aggregateId: input.aggregateId,
      actorId: input.actorId,
      kind: input.kind,
      occurredAt: input.occurredAt,
      details: input.details,
    });
  }
}
