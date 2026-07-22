import type { DirectionSelection } from "@transhooter/contracts";
import { and, eq, sql } from "drizzle-orm";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";
import { DomainError, type UUID } from "../domain/model";
import type {
  LanguageCapability,
  LanguageRepository,
  ProviderProfileRevision,
  Transaction,
} from "../ports/index";
import { type DrizzleSchema, TransactionHandle, unwrap } from "./repositories";
import { languageCapabilities, providerProfileRevisions, providerProfiles } from "./schema";

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

abstract class DrizzleRepository {
  constructor(protected readonly database: NodePgDatabase<DrizzleSchema>) {}

  transaction<T>(work: (tx: Transaction) => Promise<T>): Promise<T> {
    return this.database.transaction((database) => work(new TransactionHandle(database)));
  }
}

export class DrizzleLanguageRepository extends DrizzleRepository implements LanguageRepository {
  async replaceProfileRevision(
    profileId: UUID,
    requestedRevision: number,
    profile: ProviderProfileRevision,
    rows: readonly LanguageCapability[],
    tx: Transaction,
  ): Promise<void> {
    const database = unwrap(tx);
    const [current] = await database
      .select({
        id: providerProfiles.id,
        name: providerProfiles.name,
        currentRevision: providerProfiles.currentRevision,
      })
      .from(providerProfiles)
      .where(eq(providerProfiles.id, profileId))
      .for("update");
    if (current && current.name !== profile.name) {
      throw new DomainError("PROFILE_IDENTITY_MISMATCH");
    }

    const duplicate = current
      ? await database
          .select({ revision: providerProfileRevisions.revision })
          .from(providerProfileRevisions)
          .where(
            and(
              eq(providerProfileRevisions.profileId, profileId),
              eq(providerProfileRevisions.capabilityHash, profile.capabilityHash),
            ),
          )
          .limit(1)
      : null;
    if (duplicate?.[0]) {
      const revision = duplicate[0].revision;
      for (const row of rows) {
        const renewed = await database
          .update(languageCapabilities)
          .set({
            freshUntil: sql`GREATEST(${languageCapabilities.freshUntil}, ${row.freshUntil})`,
          })
          .where(
            and(
              eq(languageCapabilities.profileId, profileId),
              eq(languageCapabilities.revision, revision),
              eq(languageCapabilities.sourceLocale, row.sourceLocale),
              eq(languageCapabilities.targetLocale, row.targetLocale),
              eq(languageCapabilities.mode, row.mode),
              eq(languageCapabilities.capabilityHash, profile.capabilityHash),
            ),
          )
          .returning({ id: languageCapabilities.id });
        if (renewed.length !== 1) {
          throw new DomainError("CAPABILITY_RENEWAL_MISMATCH");
        }
      }
      await database
        .update(providerProfiles)
        .set({
          enabled: true,
          currentRevision: sql`GREATEST(${providerProfiles.currentRevision}, ${revision})`,
        })
        .where(eq(providerProfiles.id, profileId));
      return;
    }

    const revision = current
      ? Math.max(current.currentRevision + 1, requestedRevision)
      : requestedRevision;
    if (!Number.isSafeInteger(revision) || revision < 1) {
      throw new DomainError("INVALID_PROFILE_REVISION");
    }

    if (!current) {
      await database.insert(providerProfiles).values({
        id: profileId,
        name: profile.name,
        enabled: true,
        currentRevision: revision,
        createdAt: sql`now()`,
      });
    }
    await database.insert(providerProfileRevisions).values({
      profileId,
      revision,
      capabilityHash: profile.capabilityHash,
      adapterBuilds: profile.adapterBuilds,
      policy: profile.policy,
      credentialReferences: profile.credentialReferences,
      createdAt: sql`now()`,
    });
    for (const row of rows) {
      const snapshot = row.snapshot as CapabilitySnapshot;
      await database.insert(languageCapabilities).values({
        id: row.id,
        profileId,
        revision,
        sourceLocale: row.sourceLocale,
        targetLocale: row.targetLocale,
        mode: row.mode,
        sttProvider: snapshot.stt.provider,
        sttEndpoint: snapshot.stt.endpoint,
        sttModel: snapshot.stt.model,
        sttEncoding: snapshot.stt.encoding,
        sttLimits: snapshot.stt.limits,
        translationProvider: snapshot.mode === "translated" ? snapshot.translation.provider : null,
        translationEndpoint: snapshot.mode === "translated" ? snapshot.translation.endpoint : null,
        translationModel: snapshot.mode === "translated" ? snapshot.translation.model : null,
        translationCode: snapshot.mode === "translated" ? snapshot.translation.targetCode : null,
        ttsProvider: snapshot.mode === "translated" ? snapshot.tts.provider : null,
        ttsEndpoint: snapshot.mode === "translated" ? snapshot.tts.endpoint : null,
        ttsModel: snapshot.mode === "translated" ? snapshot.tts.model : null,
        ttsVoice: snapshot.mode === "translated" ? snapshot.tts.voice : null,
        ttsFormat: snapshot.mode === "translated" ? snapshot.tts.encoding : null,
        ttsLimits: snapshot.mode === "translated" ? snapshot.tts.limits : null,
        region: snapshot.stt.region,
        adapterVersion: snapshot.stt.adapterBuild,
        capabilityVersion: snapshot.capabilityVersion ?? "1",
        capabilityHash: row.capabilityHash,
        enabled: row.enabled,
        freshUntil: row.freshUntil,
        snapshot,
      });
    }
    await database
      .update(providerProfiles)
      .set({ enabled: true, currentRevision: revision })
      .where(eq(providerProfiles.id, profileId));
  }

  async list(profileId: UUID, revision?: number): Promise<readonly LanguageCapability[]> {
    const rows =
      revision === undefined
        ? await this.database
            .select()
            .from(languageCapabilities)
            .where(eq(languageCapabilities.profileId, profileId))
        : await this.database
            .select()
            .from(languageCapabilities)
            .where(
              and(
                eq(languageCapabilities.profileId, profileId),
                eq(languageCapabilities.revision, revision),
              ),
            );

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
    const updated = await unwrap(tx)
      .update(languageCapabilities)
      .set({ enabled })
      .where(
        and(
          eq(languageCapabilities.id, id),
          eq(languageCapabilities.profileId, profileId),
          eq(languageCapabilities.revision, profileRevision),
          sql`EXISTS (
            SELECT 1
            FROM ${providerProfiles}
            WHERE ${providerProfiles.id} = ${profileId}
              AND ${providerProfiles.currentRevision} = ${profileRevision}
          )`,
        ),
      )
      .returning({ id: languageCapabilities.id });
    if (updated.length !== 1) {
      throw new DomainError("CAPABILITY_REVISION_CONFLICT");
    }
  }
}
