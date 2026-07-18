import { type Clock, DomainError, type IdGenerator, type UUID } from "../domain/model";
import type {
  LanguageCapability,
  LanguageRepository,
  ProviderProfileRevision,
} from "../ports/index";

export interface CapabilityRefresh {
  profileId: UUID;
  revision: number;
  profileName: string;
  capabilityHash: string;
  adapterBuilds: unknown;
  policy: unknown;
  credentialReferences: unknown;
  complete: boolean;
  rows: readonly Omit<LanguageCapability, "id" | "profileId" | "revision">[];
}

export class LanguageService {
  constructor(
    private readonly repository: LanguageRepository,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
  ) {}

  async publishRevision(refresh: CapabilityRefresh): Promise<void> {
    if (!refresh.complete || !refresh.rows.length)
      throw new DomainError("INCOMPLETE_CAPABILITY_REFRESH");
    if (refresh.rows.some((row) => row.freshUntil <= this.clock.now()))
      throw new DomainError("STALE_CAPABILITY_REFRESH");
    if (refresh.rows.some((row) => row.capabilityHash !== refresh.capabilityHash))
      throw new DomainError("CAPABILITY_HASH_MISMATCH");
    const directionKeys = new Set<string>();
    const rows = refresh.rows.map((row): LanguageCapability => {
      const key = `${row.sourceLocale}\0${row.targetLocale}\0${row.mode}`;
      if (directionKeys.has(key)) throw new DomainError("DUPLICATE_CAPABILITY_DIRECTION");
      directionKeys.add(key);
      return {
        ...row,
        id: this.ids.uuid(),
        profileId: refresh.profileId,
        revision: refresh.revision,
      };
    });
    const profile: ProviderProfileRevision = {
      name: refresh.profileName,
      capabilityHash: refresh.capabilityHash,
      adapterBuilds: refresh.adapterBuilds,
      policy: refresh.policy,
      credentialReferences: refresh.credentialReferences,
    };
    await this.repository.transaction((tx) =>
      this.repository.replaceProfileRevision(
        refresh.profileId,
        refresh.revision,
        profile,
        rows,
        tx,
      ),
    );
  }

  async setEnabled(id: UUID, enabled: boolean): Promise<void> {
    await this.repository.transaction((tx) => this.repository.setEnabled(id, enabled, tx));
  }
}
