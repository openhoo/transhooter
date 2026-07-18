import type { Effect, Uuid } from "./model";

export interface Adoption {
  readonly remoteId: string;
  readonly matchesRequest: boolean;
  readonly terminal: boolean;
  readonly result?: unknown;
}

export interface RemoteResult {
  readonly remoteId: string | null;
  readonly result: unknown;
}

export interface ArchiveObjectIdentity {
  readonly key: string;
  readonly versionId: string;
}

export interface ArchiveObjectVerification extends ArchiveObjectIdentity {
  readonly size: number;
  readonly checksum: string;
}

export interface ArchiveObjectUpload {
  readonly key: string;
  readonly body: Uint8Array;
  readonly contentType: string;
  readonly sha256: string;
}

export interface UploadedArchiveObject {
  readonly versionId: string;
  readonly size: number;
  readonly checksum: string;
}

export interface DiscoveredArchiveObject extends ArchiveObjectVerification {
  readonly contentType: string;
  readonly sha256: string;
}

export interface RemoteEffects {
  readiness(): Promise<void>;
  adopt(effect: Effect, request: Readonly<Record<string, unknown>>): Promise<Adoption | null>;
  canonicalRequest?(effect: Effect, request: Readonly<Record<string, unknown>>): Uint8Array;
  execute(effect: Effect, request: Readonly<Record<string, unknown>>): Promise<RemoteResult>;
  compensate(effect: Effect): Promise<void>;
  areHumansAbsent(roomName: string, identities: readonly Uuid[]): Promise<boolean>;
  verifyArchiveObject(input: ArchiveObjectVerification): Promise<boolean>;
  readArchiveObject(input: ArchiveObjectIdentity): Promise<Uint8Array>;
  notifyArchiveRecording(consultationId: Uuid): Promise<void>;
  notifyDeleteDrain(consultationId: Uuid, writeEpoch: number): Promise<boolean>;
  putArchiveObject(input: ArchiveObjectUpload): Promise<UploadedArchiveObject>;
  discoverArchiveObjects(prefix: string): Promise<readonly DiscoveredArchiveObject[]>;
  drainArchive(consultationId: Uuid): Promise<boolean>;
}
