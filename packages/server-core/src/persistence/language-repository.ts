import type { DirectionSelection } from "@transhooter/contracts";
import { DomainError, type UUID } from "../domain/model";
import type {
  LanguageCapability,
  LanguageRepository,
  ProviderProfileRevision,
  Transaction,
} from "../ports/index";
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

type LockedProfileRow = { id: UUID; name: string; currentRevision: number };

abstract class PrismaRepository {
  constructor(protected readonly database: PrismaClient) {}

  transaction<T>(work: (tx: Transaction) => Promise<T>): Promise<T> {
    return this.database.$transaction((database) => work(new TransactionHandle(database)));
  }
}

export class PrismaLanguageRepository extends PrismaRepository implements LanguageRepository {
  async replaceProfileRevision(
    profileId: UUID,
    requestedRevision: number,
    profile: ProviderProfileRevision,
    rows: readonly LanguageCapability[],
    tx: Transaction,
  ): Promise<void> {
    const database = unwrap(tx);
    const currentRows = await database.$queryRaw<LockedProfileRow[]>(Prisma.sql`
      SELECT id, name, current_revision AS "currentRevision"
      FROM provider_profiles
      WHERE id = ${profileId}::uuid
      FOR UPDATE
    `);
    const current = currentRows[0];
    if (current && current.name !== profile.name) {
      throw new DomainError("PROFILE_IDENTITY_MISMATCH");
    }

    const duplicate = current
      ? await database.providerProfileRevision.findFirst({
          where: { profileId, capabilityHash: profile.capabilityHash },
          select: { revision: true },
        })
      : null;
    if (duplicate) {
      const revision = duplicate.revision;
      for (const row of rows) {
        const renewed = await database.$queryRaw<{ id: UUID }[]>(Prisma.sql`
          UPDATE language_capabilities
          SET fresh_until = GREATEST(fresh_until, ${row.freshUntil})
          WHERE profile_id = ${profileId}::uuid
            AND revision = ${revision}
            AND source_locale = ${row.sourceLocale}
            AND target_locale = ${row.targetLocale}
            AND mode = ${row.mode}
            AND capability_hash = ${profile.capabilityHash}
          RETURNING id
        `);
        if (renewed.length !== 1) {
          throw new DomainError("CAPABILITY_RENEWAL_MISMATCH");
        }
      }
      await database.$executeRaw(Prisma.sql`
        UPDATE provider_profiles
        SET enabled = true, current_revision = GREATEST(current_revision, ${revision})
        WHERE id = ${profileId}::uuid
      `);
      return;
    }

    const revision = current
      ? Math.max(current.currentRevision + 1, requestedRevision)
      : requestedRevision;
    if (!Number.isSafeInteger(revision) || revision < 1) {
      throw new DomainError("INVALID_PROFILE_REVISION");
    }

    if (!current) {
      await database.$executeRaw(Prisma.sql`
        INSERT INTO provider_profiles (id, name, enabled, current_revision, created_at)
        VALUES (${profileId}::uuid, ${profile.name}, true, ${revision}, now())
      `);
    }
    await database.$executeRaw(Prisma.sql`
      INSERT INTO provider_profile_revisions (
        profile_id,
        revision,
        capability_hash,
        adapter_builds,
        policy,
        credential_references,
        created_at
      ) VALUES (
        ${profileId}::uuid,
        ${revision},
        ${profile.capabilityHash},
        ${JSON.stringify(profile.adapterBuilds)}::jsonb,
        ${JSON.stringify(profile.policy)}::jsonb,
        ${JSON.stringify(profile.credentialReferences)}::jsonb,
        now()
      )
    `);
    for (const row of rows) {
      const snapshot = row.snapshot as CapabilitySnapshot;
      await database.$executeRaw(Prisma.sql`
        INSERT INTO language_capabilities (
          id,
          profile_id,
          revision,
          source_locale,
          target_locale,
          mode,
          stt_provider,
          stt_endpoint,
          stt_model,
          stt_encoding,
          stt_limits,
          translation_provider,
          translation_endpoint,
          translation_model,
          translation_code,
          tts_provider,
          tts_endpoint,
          tts_model,
          tts_voice,
          tts_format,
          tts_limits,
          region,
          adapter_version,
          capability_version,
          capability_hash,
          enabled,
          fresh_until,
          snapshot
        ) VALUES (
          ${row.id}::uuid,
          ${profileId}::uuid,
          ${revision},
          ${row.sourceLocale},
          ${row.targetLocale},
          ${row.mode},
          ${snapshot.stt.provider},
          ${snapshot.stt.endpoint},
          ${snapshot.stt.model},
          ${snapshot.stt.encoding},
          ${JSON.stringify(snapshot.stt.limits)}::jsonb,
          ${snapshot.mode === "translated" ? snapshot.translation.provider : null},
          ${snapshot.mode === "translated" ? snapshot.translation.endpoint : null},
          ${snapshot.mode === "translated" ? snapshot.translation.model : null},
          ${snapshot.mode === "translated" ? snapshot.translation.targetCode : null},
          ${snapshot.mode === "translated" ? snapshot.tts.provider : null},
          ${snapshot.mode === "translated" ? snapshot.tts.endpoint : null},
          ${snapshot.mode === "translated" ? snapshot.tts.model : null},
          ${snapshot.mode === "translated" ? snapshot.tts.voice : null},
          ${snapshot.mode === "translated" ? snapshot.tts.encoding : null},
          ${snapshot.mode === "translated" ? JSON.stringify(snapshot.tts.limits) : null}::jsonb,
          ${snapshot.stt.region},
          ${snapshot.stt.adapterBuild},
          ${snapshot.capabilityVersion ?? "1"},
          ${row.capabilityHash},
          ${row.enabled},
          ${row.freshUntil},
          ${JSON.stringify(snapshot)}::jsonb
        )
      `);
    }
    await database.providerProfile.update({
      where: { id: profileId },
      data: { enabled: true, currentRevision: revision },
    });
  }

  async list(profileId: UUID, revision?: number): Promise<readonly LanguageCapability[]> {
    const rows = await this.database.languageCapability.findMany({
      where: revision === undefined ? { profileId } : { profileId, revision },
    });

    return rows.map((row) => ({
      id: row.id,
      profileId: row.profileId,
      revision: row.revision,
      sourceLocale: row.sourceLocale,
      targetLocale: row.targetLocale,
      mode: row.mode as LanguageCapability["mode"],
      enabled: row.enabled,
      snapshot: row.snapshot,
      capabilityHash: row.capabilityHash,
      freshUntil: row.freshUntil,
    }));
  }

  async setEnabled(
    id: UUID,
    profileId: UUID,
    profileRevision: number,
    enabled: boolean,
    tx: Transaction,
  ): Promise<void> {
    const updated = await unwrap(tx).$queryRaw<{ id: UUID }[]>(Prisma.sql`
      UPDATE language_capabilities
      SET enabled = ${enabled}
      WHERE id = ${id}::uuid
        AND profile_id = ${profileId}::uuid
        AND revision = ${profileRevision}
        AND EXISTS (
          SELECT 1
          FROM provider_profiles
          WHERE id = ${profileId}::uuid
            AND current_revision = ${profileRevision}
        )
      RETURNING id
    `);
    if (updated.length !== 1) {
      throw new DomainError("CAPABILITY_REVISION_CONFLICT");
    }
  }
}
