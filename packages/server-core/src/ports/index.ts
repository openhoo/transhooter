import type { FinalInventory, RoomProviderSelection, StatusPacket } from "@transhooter/contracts";
import type { Consultation } from "../consultations/domain";
import type {
  ArchiveState,
  EffectState,
  Instant,
  MagicLinkPurpose,
  StaffRole,
  UUID,
} from "../domain/model";

export interface Transaction {
  readonly opaque: symbol;
}

export interface TransactionManager {
  transaction<T>(work: (tx: Transaction) => Promise<T>): Promise<T>;
}

export interface UserRecord {
  id: UUID;
  email: string;
  displayName: string;
  staffRole: StaffRole | null;
}

export interface SessionRecord {
  id: UUID;
  userId: UUID;
  tokenHash: string;
  csrfHash: string;
  expiresAt: Instant;
  reauthenticatedAt: Instant | null;
  reauthConsultationId: UUID | null;
}

export interface MagicLinkRecord {
  id: UUID;
  userId: UUID | null;
  consultationId: UUID | null;
  sessionId: UUID | null;
  purpose: MagicLinkPurpose;
  tokenHash: string;
  expiresAt: Instant;
  consumedAt: Instant | null;
  revokedAt: Instant | null;
}

export interface PendingExchangeRecord {
  id: UUID;
  magicLinkId: UUID;
  nonceHash: string;
  csrfHash: string;
  expiresAt: Instant;
  consumedAt: Instant | null;
}

export interface AuthRepository extends TransactionManager {
  findUserByEmail(email: string, tx?: Transaction): Promise<UserRecord | null>;
  findUserById(id: UUID, tx?: Transaction): Promise<UserRecord | null>;
  findOrCreateCustomer(
    id: UUID,
    email: string,
    displayName: string,
    createdAt: Instant,
  ): Promise<UserRecord>;
  findSessionByTokenHash(hash: string): Promise<SessionRecord | null>;
  createMagicLink(link: MagicLinkRecord, tx?: Transaction): Promise<void>;
  lockMagicLinkByTokenHash(hash: string, tx: Transaction): Promise<MagicLinkRecord | null>;
  lockMagicLinkById(id: UUID, tx: Transaction): Promise<MagicLinkRecord | null>;
  createPendingExchange(exchange: PendingExchangeRecord, tx: Transaction): Promise<void>;
  lockPendingExchangeByNonceHash(
    hash: string,
    tx: Transaction,
  ): Promise<PendingExchangeRecord | null>;
  consumeExchangeAndLink(
    exchangeId: UUID,
    linkId: UUID,
    at: Instant,
    tx: Transaction,
  ): Promise<boolean>;
  createSession(session: SessionRecord, tx: Transaction): Promise<void>;
  rotateSession(session: SessionRecord, replacesSessionId: UUID, tx: Transaction): Promise<void>;
  admitMagicLinkRequest(
    emailHash: string | null,
    ipHash: string,
    since: Instant,
    at: Instant,
    emailLimit: number,
    ipLimit: number,
  ): Promise<boolean>;
  revokeConsultationLinks(consultationId: UUID, at: Instant, tx: Transaction): Promise<void>;
}

export interface MailPort {
  sendMagicLink(input: {
    to: string;
    purpose: MagicLinkPurpose;
    url: string;
    expiresAt: Instant;
  }): Promise<void>;
}

export interface ConsultationRepository extends TransactionManager {
  lock(id: UUID, tx: Transaction): Promise<Consultation | null>;
  get(id: UUID): Promise<Consultation | null>;
  listForUser(userId: UUID): Promise<readonly Consultation[]>;
  save(value: Consultation, expectedUpdatedAt: Instant, tx: Transaction): Promise<boolean>;
  create(value: Consultation, tx: Transaction): Promise<void>;
  isCurrentEgress(
    consultationId: UUID,
    generation: number,
    egressId: string,
    tx: Transaction,
  ): Promise<boolean>;
  resolveCurrentEgressSubject(
    consultationId: UUID,
    generation: number,
    egressId: string,
    tx: Transaction,
  ): Promise<{ participantId: UUID | null } | null>;
  resolveEgressEvent(egressId: string): Promise<{
    consultationId: UUID;
    generation: number;
    roomName: string;
  } | null>;
  persistProvisioningIds(
    consultationId: UUID,
    generation: number,
    ids: {
      roomSid?: string;
      dispatchId?: string;
      compositeEgressId?: string;
    },
    tx: Transaction,
  ): Promise<boolean>;
  clearParticipantEgressBinding(
    consultationId: UUID,
    generation: number,
    participantId: UUID,
    egressId: string,
    tx: Transaction,
  ): Promise<boolean>;
}

export interface ProviderSnapshotPort {
  resolve(
    profileId: UUID,
    participants: readonly [{ id: UUID; language: string }, { id: UUID; language: string }],
    tx: Transaction,
  ): Promise<{
    selection: RoomProviderSelection;
    hash: string;
    profileRevision: number;
  }>;
  assertFreshAndHealthy(selection: RoomProviderSelection): Promise<void>;
  currentEnabledRevision(
    profileReference: string,
    tx: Transaction,
  ): Promise<{ profileId: UUID; revision: number }>;
}

export interface ProviderProfileRevision {
  name: string;
  capabilityHash: string;
  adapterBuilds: unknown;
  policy: unknown;
  credentialReferences: unknown;
}

export interface LanguageCapability {
  id: UUID;
  profileId: UUID;
  revision: number;
  sourceLocale: string;
  targetLocale: string;
  mode: "translated" | "same_language";
  enabled: boolean;
  snapshot: unknown;
  capabilityHash: string;
  freshUntil: Instant;
}

export interface LanguageRepository extends TransactionManager {
  replaceProfileRevision(
    profileId: UUID,
    revision: number,
    profile: ProviderProfileRevision,
    rows: readonly LanguageCapability[],
    tx: Transaction,
  ): Promise<void>;
  list(profileId: UUID, revision?: number): Promise<readonly LanguageCapability[]>;
  setEnabled(id: UUID, enabled: boolean, tx: Transaction): Promise<void>;
}

export interface ExternalEffect {
  id: UUID;
  consultationId: UUID;
  generation: number;
  kind: string;
  subjectId: UUID;
  state: EffectState;
  requestBytes: Uint8Array | null;
  requestHash: string | null;
  leaseOwner: UUID | null;
  leaseExpiresAt: Instant | null;
  result: unknown;
  attempts: number;
}

export interface OutboxMessage {
  id: UUID;
  topic: string;
  aggregateId: UUID;
  generation: number;
  payload: unknown;
  availableAt: Instant;
  attempts: number;
}

export interface EffectRepository extends TransactionManager {
  plan(effect: ExternalEffect, tx: Transaction): Promise<ExternalEffect>;
  lock(effectId: UUID, tx: Transaction): Promise<ExternalEffect | null>;
  beginCall(
    effectId: UUID,
    requestBytes: Uint8Array,
    requestHash: string,
    owner: UUID,
    leaseUntil: Instant,
    tx: Transaction,
  ): Promise<boolean>;
  beginCompensation(
    effectId: UUID,
    owner: UUID,
    leaseUntil: Instant,
    tx: Transaction,
  ): Promise<boolean>;
  complete(
    effectId: UUID,
    owner: UUID,
    requestHash: string,
    state: "applied" | "done" | "failed" | "compensating",
    result: unknown,
    tx: Transaction,
  ): Promise<boolean>;
  completeCompensation(
    effectId: UUID,
    owner: UUID,
    requestHash: string,
    result: unknown,
    tx: Transaction,
  ): Promise<boolean>;
  recordCompensationAttempt(
    effectId: UUID,
    owner: UUID,
    requestHash: string,
    result: unknown,
    tx: Transaction,
  ): Promise<void>;
  claimOutbox(
    owner: UUID,
    now: Instant,
    leaseUntil: Instant,
    limit: number,
    tx: Transaction,
  ): Promise<readonly OutboxMessage[]>;
  enqueue(message: OutboxMessage, tx: Transaction): Promise<void>;
  acceptInbox(
    source: string,
    eventId: string,
    occurredAt: Instant,
    payloadHash: string,
    payload: unknown,
    tx: Transaction,
  ): Promise<boolean>;
}

export interface LiveKitRoomPort {
  findRoomByName(name: string): Promise<{ name: string; metadata: string } | null>;
  createRoom(input: { name: string; metadata: string }): Promise<{ sid: string }>;
  closeRoom(name: string): Promise<void>;
  listAllowedParticipants(roomName: string): Promise<readonly UUID[]>;
  updateParticipant(input: {
    roomName: string;
    identity: UUID;
    canPublish: boolean;
    canPublishData: false;
  }): Promise<void>;
  sendStatus(roomName: string, packet: StatusPacket, destinations: readonly UUID[]): Promise<void>;
  removeParticipant(roomName: string, identity: UUID): Promise<void>;
}

export interface LiveKitTokenPort {
  issue(input: {
    identity: UUID;
    roomName: string;
    ttlSeconds: 600;
    attributes: Record<string, string>;
    grants: {
      roomJoin: true;
      canPublish: false;
      canPublishData: false;
      canSubscribe: true;
    };
  }): Promise<string>;
}

export interface EgressPort {
  startRoomComposite(input: {
    roomName: string;
    outputPrefix: string;
    layoutUrl: string;
    requestIdentity: string;
  }): Promise<{ egressId: string; state: string }>;
  startParticipant(input: {
    roomName: string;
    identity: UUID;
    outputPrefix: string;
    requestIdentity: string;
  }): Promise<{ egressId: string; state: string }>;
  get(egressId: string): Promise<{
    egressId: string;
    state: string;
    output: unknown;
  }>;
  stop(egressId: string): Promise<void>;
}

export interface DispatchPort {
  dispatch(input: {
    roomName: string;
    metadata: string;
    requestIdentity: string;
  }): Promise<{ dispatchId: string }>;
}

export interface WorkerCapacityPort {
  reserve(input: { consultationId: UUID; generation: number; selectionHash: string }): Promise<{
    workerId: UUID;
    epoch: number;
    expiresAt: Instant;
  }>;
}

export interface RedisCoordinationPort {
  reserve(key: string, capacity: number, ttlMs: number): Promise<boolean>;
  release(key: string): Promise<void>;
}

export interface ArchiveObject {
  id: UUID;
  consultationId: UUID;
  objectClass: string;
  causalKey: string;
  key: string;
  versionId: string;
  size: number;
  sha256: string;
  s3Checksum: string;
  contentType: string;
  sampleStart: number | null;
  sampleEnd: number | null;
  attempt: number | null;
  sequence: number | null;
  writerEpoch: number;
}

export interface ArchiveRepository extends TransactionManager {
  lockByConsultation(
    consultationId: UUID,
    tx: Transaction,
  ): Promise<{
    id: UUID;
    state: ArchiveState;
    consultationState: Consultation["state"];
    writeEpoch: number;
    completedDeletionEpoch: number | null;
    finalInventoryHash: string | null;
    reconciliationDeadlineAt: Instant | null;
  } | null>;
  transition(
    id: UUID,
    from: readonly ArchiveState[],
    to: ArchiveState,
    tx: Transaction,
  ): Promise<boolean>;
  createExpectedArtifact(
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
  ): Promise<UUID>;
  recordObject(object: ArchiveObject, tx: Transaction): Promise<void>;
  lockActiveWorkerWriter(
    input: {
      consultationId: UUID;
      generation: number;
      workerId: UUID;
      workerEpoch: number;
      writerEpoch: number;
    },
    tx: Transaction,
  ): Promise<boolean>;
  fulfillExpectedArtifact(
    expectedId: UUID,
    objectId: UUID,
    writerEpoch: number,
    tx: Transaction,
  ): Promise<boolean>;
  unresolvedExpectations(
    archiveId: UUID,
    tx: Transaction,
  ): Promise<
    readonly {
      id: UUID;
      objectClass: string;
      causalKey: string;
      sampleStart: number | null;
      sampleEnd: number | null;
    }[]
  >;
  inventoryObjects(archiveId: UUID, tx: Transaction): Promise<readonly ArchiveObject[]>;
  completePrerequisites(
    archiveId: UUID,
    inventory: FinalInventory,
    tx: Transaction,
  ): Promise<boolean>;
  createFinalInventory(
    archiveId: UUID,
    inventory: FinalInventory,
    sha256: string,
    objectId: UUID,
    tx: Transaction,
  ): Promise<boolean>;
  finalInventory(archiveId: UUID, tx: Transaction): Promise<FinalInventory | null>;
  finalInventoryHash(archiveId: UUID, tx: Transaction): Promise<string | null>;
  supplementClaims(
    archiveId: UUID,
    tx: Transaction,
  ): Promise<{
    closedGapIndexes: readonly number[];
    objectIds: readonly UUID[];
  }>;
  createSupplement(
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
  ): Promise<boolean>;
  addHold(
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
  ): Promise<void>;
  removeHold(id: UUID, actorId: UUID, at: Instant, tx: Transaction): Promise<void>;
  activeHolds(
    archiveId: UUID,
    tx: Transaction,
  ): Promise<
    readonly {
      id: UUID;
      reason: string;
      actorId: UUID;
      perVersionResults: unknown;
      state: "applying" | "active" | "releasing" | "failed";
    }[]
  >;
  recordHoldResults(
    id: UUID,
    aggregate: unknown,
    perVersion: unknown,
    tx: Transaction,
  ): Promise<void>;
  transitionHoldState(
    id: UUID,
    from: readonly ("applying" | "active" | "releasing" | "failed")[],
    to: "active" | "releasing" | "failed",
    tx: Transaction,
  ): Promise<boolean>;
  beginHoldOperation(
    archiveId: UUID,
    operationId: UUID,
    owner: UUID,
    kind: "add" | "release",
    at: Instant,
    leaseExpiresAt: Instant,
    tx: Transaction,
  ): Promise<boolean>;
  claimStaleHoldOperation(
    archiveId: UUID,
    owner: UUID,
    now: Instant,
    leaseExpiresAt: Instant,
    tx: Transaction,
  ): Promise<{
    operationId: UUID;
    kind: "add" | "release";
  } | null>;
  renewHoldOperation(
    archiveId: UUID,
    operationId: UUID,
    owner: UUID,
    leaseExpiresAt: Instant,
    tx: Transaction,
  ): Promise<boolean>;
  completeHoldOperation(
    archiveId: UUID,
    operationId: UUID,
    owner: UUID,
    tx: Transaction,
  ): Promise<boolean>;
  deletionWritersDrained(
    consultationId: UUID,
    writeEpoch: number,
    tx: Transaction,
  ): Promise<boolean>;
  incrementWriteEpoch(archiveId: UUID, tx: Transaction): Promise<number>;
  fenceWritersForDeletion(
    consultationId: UUID,
    writeEpoch: number,
    at: Instant,
    tx: Transaction,
  ): Promise<void>;
  recordDeletionFailure(archiveId: UUID, failure: unknown, tx: Transaction): Promise<void>;
  recordDeletionScan(
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
  ): Promise<void>;
  completeDeletion(
    archiveId: UUID,
    writeEpoch: number,
    at: Instant,
    tx: Transaction,
  ): Promise<boolean>;
}

export interface ObjectStoragePort {
  putCreateOnce(input: {
    key: string;
    body: Uint8Array;
    contentType: string;
    checksum: string;
  }): Promise<{
    versionId: string;
    size: number;
    checksum: string;
  }>;
  head(key: string): Promise<{
    versionId: string;
    size: number;
    checksum: string;
    sha256: string;
  } | null>;
  verify(input: {
    key: string;
    versionId: string;
    size: number;
    checksum: string;
  }): Promise<boolean>;
  listMeetingVersions(
    consultationId: UUID,
    cursor?: string,
  ): Promise<{
    versions: readonly {
      key: string;
      versionId: string;
    }[];
    cursor: string | null;
  }>;
  deleteVersions(
    versions: readonly {
      key: string;
      versionId: string;
    }[],
  ): Promise<void>;
  listMultipart(consultationId: UUID): Promise<
    readonly {
      key: string;
      uploadId: string;
    }[]
  >;
  abortMultipart(key: string, uploadId: string): Promise<void>;
  presignGet(key: string, versionId: string, expiresSeconds: 300): Promise<string>;
  setLegalHold(key: string, versionId: string, enabled: boolean): Promise<void>;
}

export interface InternalPrincipal {
  service: "web" | "control-worker" | "translation-worker" | "language-refresh" | "spool-drainer";
  subject: string;
  permissions: readonly string[];
}

export interface InternalPrincipalVerifier {
  verify(
    headers: Readonly<Record<string, string | readonly string[] | undefined>>,
  ): Promise<InternalPrincipal>;
}

export interface AuditPort {
  append(
    input: {
      id: UUID;
      aggregateId: UUID;
      actorId: UUID | null;
      kind: string;
      occurredAt: Instant;
      details: unknown;
    },
    tx: Transaction,
  ): Promise<void>;
}
