import type { ProviderAttemptReport, WorkerCheckpoint } from "@transhooter/contracts";
import { ApplicationOperationsQueries } from "./application-operations-queries";
import { ApplicationOperationsWorkers } from "./application-operations-workers";
import type { Clock, StaffRole, UUID } from "./domain/model";
import { Prisma, type PrismaClient } from "./persistence/database";
import type { ObjectStoragePort } from "./ports/index";

export interface StaffPrincipal {
  userId: UUID;
  role: StaffRole;
}

export interface ArchiveObjectPage {
  objects: readonly ArchiveObjectRow[];
  cursor: string | null;
}
export interface ArchiveObjectRow {
  id: UUID;
  objectClass: string;
  key: string;
  contentType: string;
  size: string;
  sha256: string;
  s3Checksum: string;
  versionId: string;
}

export interface ArchivePresentation {
  archive: ArchiveDetail;
  objectPage: ArchiveObjectPage;
}

export interface ConsultationOptionRow {
  id: UUID;
  profileId: UUID;
  sourceLocale: string;
  targetLocale: string;
  mode: "translated" | "same_language";
  snapshot: unknown;
  profileName: string;
  revision: number;
  freshUntil: Date;
}

export interface ProviderProfileMetadata {
  id: UUID;
  name: string;
  revision: number;
}

export interface AdminLanguageRow {
  id: UUID;
  profileId: UUID;
  sourceLocale: string;
  targetLocale: string;
  mode: "translated" | "same_language";
  snapshot: unknown;
  profileName: string;
  revision: number;
  freshUntil: Date;
  enabled: boolean;
}

interface ActiveHoldProof {
  id: UUID;
  reason: string;
}

interface ProviderAttemptGroup {
  stage: "stt" | "translation" | "tts";
  provider: string;
  direction: string;
  attemptIds: readonly UUID[];
}

export interface ArchiveDetailProof {
  activeHolds: readonly ActiveHoldProof[];
  inventoryVersionId: string | null;
  inventorySha256: string | null;
  egressIds: readonly string[];
  providerAttemptIds: readonly UUID[];
  providerAttemptGroups: readonly ProviderAttemptGroup[];
}

export interface ArchiveDetail extends ArchiveDetailProof {
  id: UUID;
  consultationId: UUID;
  state: string;
  inventory: unknown;
}

export interface CheckpointInput {
  workerId: UUID;
  consultationId: UUID;
  generation: number;
  writeEpoch: number;
  objectKey: string;
  checkpoint: WorkerCheckpoint;
}

export interface WorkerFailureInput {
  consultationId: UUID;
  generation: number;
  workerId: UUID;
  epoch: number;
  eventId: UUID;
  kindName: string;
  message: string;
  phase?: string;
  snapshotHash?: string;
  lastCheckpointHashes: Readonly<Record<UUID, string>>;
}

export interface ProviderAttemptInput {
  consultationId: UUID;
  generation: number;
  workerId: UUID;
  epoch: number;
  eventId: UUID;
  report: ProviderAttemptReport;
}

export interface ApplicationOperations {
  consultationOptions(profileId: string): Promise<readonly ConsultationOptionRow[]>;
  providerProfileMetadata(): Promise<readonly ProviderProfileMetadata[]>;
  consultationRoom(consultationId: UUID, userId: UUID): Promise<Record<string, unknown>>;
  consultationInviteRecipient(consultationId: UUID, employeeUserId: UUID): Promise<string>;
  archiveList(principal: StaffPrincipal): Promise<readonly Record<string, unknown>[]>;
  archiveGet(principal: StaffPrincipal, archiveId: UUID): Promise<ArchiveDetail>;
  archiveObjects(
    principal: StaffPrincipal,
    archiveId: UUID,
    cursor: string | null,
    limit: number,
  ): Promise<ArchiveObjectPage>;
  archivePresentation(
    principal: StaffPrincipal,
    archiveId: UUID,
    cursor: string | null,
    limit: number,
  ): Promise<ArchivePresentation>;
  archiveDownload(principal: StaffPrincipal, archiveId: UUID, objectId: UUID): Promise<string>;
  adminFailures(principal: StaffPrincipal): Promise<readonly Record<string, unknown>[]>;
  adminLanguages(
    principal: StaffPrincipal,
    profileId: string,
  ): Promise<readonly AdminLanguageRow[]>;
  egressLayout(consultationId: UUID, generation: number): Promise<Record<string, unknown>>;
  logout(sessionId: UUID, userId: UUID): Promise<void>;
  heartbeat(
    consultationId: UUID,
    generation: number,
    workerId: UUID,
    epoch: number,
  ): Promise<boolean>;
  checkpoint(input: CheckpointInput): Promise<boolean>;
  providerAttempt(input: ProviderAttemptInput): Promise<boolean>;
  workerFailure(input: WorkerFailureInput): Promise<boolean>;
}

export class PrismaApplicationOperations implements ApplicationOperations {
  private readonly queries: ApplicationOperationsQueries;
  private readonly workers: ApplicationOperationsWorkers;

  constructor(
    private readonly database: PrismaClient,
    storage: ObjectStoragePort,
    clock: Clock,
  ) {
    this.queries = new ApplicationOperationsQueries(database, storage);
    this.workers = new ApplicationOperationsWorkers(database, clock);
  }

  consultationOptions(profileId: string): Promise<readonly ConsultationOptionRow[]> {
    return this.queries.consultationOptions(profileId);
  }

  providerProfileMetadata(): Promise<readonly ProviderProfileMetadata[]> {
    return this.queries.providerProfileMetadata();
  }

  consultationRoom(consultationId: UUID, userId: UUID): Promise<Record<string, unknown>> {
    return this.queries.consultationRoom(consultationId, userId);
  }

  consultationInviteRecipient(consultationId: UUID, employeeUserId: UUID): Promise<string> {
    return this.queries.consultationInviteRecipient(consultationId, employeeUserId);
  }

  archiveList(principal: StaffPrincipal): Promise<readonly Record<string, unknown>[]> {
    return this.queries.archiveList(principal);
  }

  archiveGet(principal: StaffPrincipal, archiveId: UUID): Promise<ArchiveDetail> {
    return this.queries.archiveGet(principal, archiveId);
  }

  archiveObjects(
    principal: StaffPrincipal,
    archiveId: UUID,
    cursor: string | null,
    limit: number,
  ): Promise<ArchiveObjectPage> {
    return this.queries.archiveObjects(principal, archiveId, cursor, limit);
  }
  archivePresentation(
    principal: StaffPrincipal,
    archiveId: UUID,
    cursor: string | null,
    limit: number,
  ): Promise<ArchivePresentation> {
    return this.queries.archivePresentation(principal, archiveId, cursor, limit);
  }

  archiveDownload(principal: StaffPrincipal, archiveId: UUID, objectId: UUID): Promise<string> {
    return this.queries.archiveDownload(principal, archiveId, objectId);
  }

  adminFailures(principal: StaffPrincipal): Promise<readonly Record<string, unknown>[]> {
    return this.queries.adminFailures(principal);
  }

  adminLanguages(
    principal: StaffPrincipal,
    profileId: string,
  ): Promise<readonly AdminLanguageRow[]> {
    return this.queries.adminLanguages(principal, profileId);
  }

  egressLayout(consultationId: UUID, generation: number): Promise<Record<string, unknown>> {
    return this.queries.egressLayout(consultationId, generation);
  }

  async logout(sessionId: UUID, userId: UUID): Promise<void> {
    await this.database.$executeRaw(
      Prisma.sql`UPDATE sessions SET revoked_at=now() WHERE id=${sessionId} AND user_id=${userId} AND revoked_at IS NULL`,
    );
  }

  heartbeat(
    consultationId: UUID,
    generation: number,
    workerId: UUID,
    epoch: number,
  ): Promise<boolean> {
    return this.workers.heartbeat(consultationId, generation, workerId, epoch);
  }

  checkpoint(input: CheckpointInput): Promise<boolean> {
    return this.workers.checkpoint(input);
  }

  providerAttempt(input: ProviderAttemptInput): Promise<boolean> {
    return this.workers.providerAttempt(input);
  }

  workerFailure(input: WorkerFailureInput): Promise<boolean> {
    return this.workers.workerFailure(input);
  }
}
