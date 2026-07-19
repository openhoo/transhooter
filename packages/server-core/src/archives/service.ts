import {
  type FinalInventory,
  FinalInventorySchema,
  type InventorySupplement,
  InventorySupplementSchema,
} from "@transhooter/contracts";
import {
  addMilliseconds,
  type Clock,
  DomainError,
  type IdGenerator,
  type UUID,
} from "../domain/model";
import type {
  ArchiveObject,
  ArchiveRepository,
  AuditPort,
  EffectRepository,
  ObjectStoragePort,
  SessionRecord,
  Transaction,
} from "../ports/index";

export interface InventoryHasher {
  sha256Canonical(value: unknown): string;
}

interface ExpectedArtifactInput {
  objectClass: string;
  causalKey: string;
  sampleStart: number | null;
  sampleEnd: number | null;
  ownerEpoch: number;
}

interface ObjectVersion {
  key: string;
  versionId: string;
}

interface MultipartUpload {
  key: string;
  uploadId: string;
}

interface UploadedObject {
  versionId: string;
  size: number;
  checksum: string;
}
interface RecordObjectInput {
  objectId: UUID;
  class: ArchiveObject["objectClass"];
  key: string;
  versionId: string;
  size: number;
  sha256: string;
  s3Checksum: string;
  contentType: string;
  sampleRange: { start: number; end: number } | null;
  attempt: number | null;
  sequence: number | null;
}
interface LockedArchive {
  id: UUID;
  state:
    | "pending"
    | "recording"
    | "reconciling"
    | "complete"
    | "incomplete"
    | "deleting"
    | "deleted";
  consultationState:
    | "invited"
    | "ready"
    | "active"
    | "finalizing"
    | "ended"
    | "cancelled"
    | "deleted";
  writeEpoch: number;
  completedDeletionEpoch: number | null;
  finalInventoryHash: string | null;
  reconciliationDeadlineAt: Date | null;
}

interface HoldProgress extends ObjectVersion {
  phase: "apply" | "retry" | "release" | "compensate";
  ok: boolean;
  error: string | null;
}

interface HoldRecoveryResult {
  id: UUID;
  failed: boolean;
}

interface SupplementClaims {
  closedGapIndexes: readonly number[];
  objectIds: readonly UUID[];
}

function holdProgress(value: unknown): HoldProgress[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter(
    (entry): entry is HoldProgress =>
      typeof entry === "object" &&
      entry !== null &&
      typeof (entry as Partial<HoldProgress>).key === "string" &&
      typeof (entry as Partial<HoldProgress>).versionId === "string" &&
      typeof (entry as Partial<HoldProgress>).phase === "string" &&
      typeof (entry as Partial<HoldProgress>).ok === "boolean",
  );
}

export class ArchiveService {
  constructor(
    private readonly archives: ArchiveRepository,
    private readonly storage: ObjectStoragePort,
    private readonly audit: AuditPort,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
    private readonly hash: InventoryHasher,
    private readonly effects: EffectRepository,
  ) {}

  async adoptCompositeRecording(consultationId: UUID): Promise<boolean> {
    return this.archives.transaction(async (tx) => {
      const archive = await this.required(consultationId, tx);
      return archive.state === "pending"
        ? this.archives.transition(archive.id, ["pending"], "recording", tx)
        : archive.state === "recording";
    });
  }

  async expectArtifact(consultationId: UUID, input: ExpectedArtifactInput): Promise<UUID> {
    return this.archives.transaction(async (tx) => {
      const archive = await this.required(consultationId, tx);
      if (
        archive.writeEpoch !== input.ownerEpoch ||
        archive.state === "deleting" ||
        archive.state === "deleted"
      ) {
        throw new DomainError("ARCHIVE_WRITER_FENCED");
      }
      return this.archives.createExpectedArtifact(
        {
          id: this.ids.uuid(),
          archiveId: archive.id,
          ...input,
        },
        tx,
      );
    });
  }

  async recordObjectAndFulfill(object: ArchiveObject, expectedId: UUID): Promise<void> {
    await this.archives.transaction(async (tx) => {
      await this.recordProtectedObject(object, tx);
      const fulfilled = await this.archives.fulfillExpectedArtifact(
        expectedId,
        object.id,
        object.writerEpoch,
        tx,
      );
      if (!fulfilled) {
        throw new DomainError("EXPECTED_ARTIFACT_FENCED");
      }
    });
  }

  async recordObject(
    consultationId: UUID,
    writerEpoch: number,
    causalKey: string,
    object: RecordObjectInput,
  ): Promise<void> {
    await this.archives.transaction((tx) =>
      this.recordProtectedObject(
        {
          id: object.objectId,
          consultationId,
          objectClass: object.class,
          causalKey,
          key: object.key,
          versionId: object.versionId,
          size: object.size,
          sha256: object.sha256,
          s3Checksum: object.s3Checksum,
          contentType: object.contentType,
          sampleStart: object.sampleRange?.start ?? null,
          sampleEnd: object.sampleRange?.end ?? null,
          attempt: object.attempt,
          sequence: object.sequence,
          writerEpoch,
        },
        tx,
      ),
    );
  }
  async recordWorkerObject(
    consultationId: UUID,
    worker: {
      generation: number;
      workerId: UUID;
      workerEpoch: number;
    },
    writerEpoch: number,
    causalKey: string,
    object: RecordObjectInput,
  ): Promise<void> {
    await this.archives.transaction(async (tx) => {
      const archiveObject = this.toArchiveObject(consultationId, writerEpoch, causalKey, object);
      const archive = await this.required(consultationId, tx);
      const active = await this.archives.lockActiveWorkerWriter(
        {
          consultationId,
          generation: worker.generation,
          workerId: worker.workerId,
          workerEpoch: worker.workerEpoch,
          writerEpoch,
        },
        tx,
      );
      if (!active) {
        throw new DomainError("ARCHIVE_WRITER_FENCED");
      }
      await this.recordProtectedObject(archiveObject, tx, archive);
    });
  }

  async recordDrainerObject(
    consultationId: UUID,
    causalKey: string,
    object: RecordObjectInput,
  ): Promise<void> {
    await this.archives.transaction(async (tx) => {
      const archive = await this.required(consultationId, tx);
      if (
        archive.state !== "pending" &&
        archive.state !== "recording" &&
        archive.state !== "reconciling"
      ) {
        throw new DomainError("ARCHIVE_WRITER_FENCED");
      }
      await this.recordProtectedObject(
        this.toArchiveObject(consultationId, archive.writeEpoch, causalKey, object),
        tx,
        archive,
      );
    });
  }

  async addSupplement(
    consultationId: UUID,
    proposed: InventorySupplement,
  ): Promise<{ created: boolean; sha256: string }> {
    const supplement = InventorySupplementSchema.parse(proposed);
    if (supplement.consultationId !== consultationId) {
      throw new DomainError("SUPPLEMENT_CONSULTATION_MISMATCH");
    }
    const sha256 = this.hash.sha256Canonical(supplement);
    const archiveId = await this.validateSupplementClaims(consultationId, supplement);
    const key = `v1/meetings/${consultationId}/inventory/supplements/${supplement.supplementId}.json`;
    const body = new TextEncoder().encode(JSON.stringify(supplement));
    const uploaded = await this.putCreateOnceOrRecover(key, body, sha256, "SUPPLEMENT_CONFLICT");
    const verified = await this.storage.verify({
      key,
      versionId: uploaded.versionId,
      size: uploaded.size,
      checksum: uploaded.checksum,
    });
    if (!verified) {
      throw new DomainError("SUPPLEMENT_VERIFICATION_FAILED");
    }

    const created = await this.commitSupplement(
      consultationId,
      archiveId,
      supplement,
      sha256,
      key,
      uploaded,
    );
    return { created, sha256 };
  }

  async finalizeInventory(
    consultationId: UUID,
    proposed: FinalInventory,
  ): Promise<{ created: boolean; sha256: string }> {
    const inventory = FinalInventorySchema.parse(proposed);
    if (inventory.consultationId !== consultationId) {
      throw new DomainError("INVENTORY_CONSULTATION_MISMATCH");
    }
    const sha256 = this.hash.sha256Canonical(inventory);
    const key = `v1/meetings/${consultationId}/inventory/final.json`;
    const preflight = await this.preflightFinalInventory(consultationId, inventory, sha256);
    if (preflight.existing) {
      return { created: false, sha256 };
    }

    const body = new TextEncoder().encode(JSON.stringify(inventory));
    const uploaded = await this.putCreateOnceOrRecover(
      key,
      body,
      sha256,
      "FINAL_INVENTORY_CONFLICT",
    );
    const verified = await this.storage.verify({
      key,
      versionId: uploaded.versionId,
      size: uploaded.size,
      checksum: uploaded.checksum,
    });
    if (!verified) {
      throw new DomainError("INVENTORY_VERIFICATION_FAILED");
    }

    const created = await this.commitFinalInventory(
      consultationId,
      preflight.archiveId,
      inventory,
      sha256,
      key,
      uploaded,
    );
    return { created, sha256 };
  }

  async forceIncomplete(
    consultationId: UUID,
    build: (
      missing: readonly {
        id: UUID;
        objectClass: string;
        causalKey: string;
      }[],
    ) => FinalInventory,
  ): Promise<void> {
    const proposed = await this.archives.transaction(async (tx) => {
      const archive = await this.required(consultationId, tx);
      if (
        archive.state !== "reconciling" ||
        !archive.reconciliationDeadlineAt ||
        archive.reconciliationDeadlineAt > this.clock.now()
      ) {
        throw new DomainError("RECONCILIATION_NOT_DUE");
      }
      return build(await this.archives.unresolvedExpectations(archive.id, tx));
    });
    await this.finalizeInventory(consultationId, proposed);
  }

  async addHold(consultationId: UUID, session: SessionRecord, reason: string): Promise<UUID> {
    this.assertFreshReauth(consultationId, session);
    const reauthenticatedAt = session.reauthenticatedAt;
    if (reauthenticatedAt === null) {
      throw new DomainError("REAUTH_REQUIRED");
    }
    const trimmedReason = reason.trim();
    if (!trimmedReason) {
      throw new DomainError("HOLD_REASON_REQUIRED");
    }

    await this.recoverStaleHold(consultationId);
    const owner = this.ids.uuid();
    const planned = await this.archives.transaction(async (tx) => {
      const archive = await this.required(consultationId, tx);
      if (archive.state !== "complete" && archive.state !== "incomplete") {
        throw new DomainError("INVALID_ARCHIVE_STATE");
      }
      const id = this.ids.uuid();
      const now = this.clock.now();
      const began = await this.archives.beginHoldOperation(
        archive.id,
        id,
        owner,
        "add",
        now,
        addMilliseconds(now, 300_000),
        tx,
      );
      if (!began) {
        throw new DomainError("HOLD_OPERATION_IN_PROGRESS");
      }
      await this.archives.addHold(
        {
          id,
          archiveId: archive.id,
          reason: trimmedReason,
          actorId: session.userId,
          sessionId: session.id,
          reauthenticatedAt,
          at: now,
        },
        tx,
      );
      return { id, archiveId: archive.id };
    });

    const failed = await this.applyHoldToStorage(
      consultationId,
      planned.archiveId,
      planned.id,
      owner,
      session.userId,
      trimmedReason,
    );
    if (failed) {
      throw new DomainError("HOLD_APPLICATION_FAILED");
    }
    return planned.id;
  }

  async releaseHold(consultationId: UUID, holdId: UUID, session: SessionRecord): Promise<void> {
    this.assertFreshReauth(consultationId, session);
    const recovered = await this.recoverStaleHold(consultationId);
    if (recovered?.id === holdId) {
      if (recovered.failed) {
        throw new DomainError("HOLD_RELEASE_FAILED");
      }
      return;
    }

    const owner = this.ids.uuid();
    const planned = await this.archives.transaction(async (tx) => {
      const archive = await this.required(consultationId, tx);
      if (archive.state !== "complete" && archive.state !== "incomplete") {
        throw new DomainError("INVALID_ARCHIVE_STATE");
      }
      const holds = await this.archives.activeHolds(archive.id, tx);
      const target = holds.find((hold) => hold.id === holdId);
      if (!target) {
        throw new DomainError("HOLD_NOT_FOUND");
      }

      const now = this.clock.now();
      const began = await this.archives.beginHoldOperation(
        archive.id,
        holdId,
        owner,
        "release",
        now,
        addMilliseconds(now, 300_000),
        tx,
      );
      if (!began) {
        throw new DomainError("HOLD_OPERATION_IN_PROGRESS");
      }

      const anotherHoldIsActive = holds.some((hold) => hold.id !== holdId);
      if (anotherHoldIsActive) {
        const completed = await this.archives.completeHoldOperation(archive.id, holdId, owner, tx);
        if (!completed) {
          throw new DomainError("HOLD_OPERATION_FENCED");
        }
        await this.archives.removeHold(holdId, session.userId, now, tx);
        await this.audit.append(
          {
            id: this.ids.uuid(),
            aggregateId: archive.id,
            actorId: session.userId,
            kind: "archive.hold_released",
            occurredAt: now,
            details: {
              holdId,
              storageRetainedForOtherHolds: true,
            },
          },
          tx,
        );
        return { skip: true, archiveId: archive.id };
      }

      const transitioned = await this.archives.transitionHoldState(
        holdId,
        [target.state],
        "releasing",
        tx,
      );
      if (!transitioned) {
        throw new DomainError("HOLD_OPERATION_FENCED");
      }
      return { skip: false, archiveId: archive.id };
    });

    if (planned.skip) {
      return;
    }
    const failed = await this.releaseHoldFromStorage(
      consultationId,
      planned.archiveId,
      holdId,
      owner,
      session.userId,
    );
    if (failed) {
      throw new DomainError("HOLD_RELEASE_FAILED");
    }
  }

  async beginDelete(consultationId: UUID, session: SessionRecord): Promise<void> {
    this.assertFreshReauth(consultationId, session);
    await this.recoverStaleHold(consultationId);
    await this.archives.transaction(async (tx) => {
      const archive = await this.required(consultationId, tx);
      if (archive.state === "deleting" || archive.state === "deleted") {
        return;
      }
      if (archive.consultationState !== "ended") {
        throw new DomainError("CONSULTATION_NOT_ENDED");
      }
      if (archive.state !== "complete" && archive.state !== "incomplete") {
        throw new DomainError("INVALID_ARCHIVE_STATE");
      }
      if ((await this.archives.activeHolds(archive.id, tx)).length) {
        throw new DomainError("ARCHIVE_HELD");
      }

      const writeEpoch = await this.archives.incrementWriteEpoch(archive.id, tx);
      const now = this.clock.now();
      await this.archives.fenceWritersForDeletion(consultationId, writeEpoch, now, tx);
      const transitioned = await this.archives.transition(
        archive.id,
        ["complete", "incomplete"],
        "deleting",
        tx,
      );
      if (!transitioned) {
        throw new DomainError("CONCURRENT_MODIFICATION");
      }
      await this.audit.append(
        {
          id: this.ids.uuid(),
          aggregateId: archive.id,
          actorId: session.userId,
          kind: "archive.deletion_admitted",
          occurredAt: this.clock.now(),
          details: { writeEpoch },
        },
        tx,
      );
      await this.effects.enqueue(
        {
          id: this.ids.uuid(),
          topic: "archive.deletion_requested",
          aggregateId: archive.id,
          generation: writeEpoch,
          payload: {
            consultationId,
            archiveId: archive.id,
            writeEpoch,
          },
          availableAt: this.clock.now(),
          attempts: 0,
        },
        tx,
      );
    });
  }

  async drainDeletion(consultationId: UUID, writeEpoch: number): Promise<boolean> {
    const archive = await this.archives.transaction(async (tx) => {
      const value = await this.required(consultationId, tx);
      if (value.state === "deleted" && value.completedDeletionEpoch === writeEpoch) {
        return value;
      }
      if (value.state !== "deleting") {
        throw new DomainError("INVALID_ARCHIVE_STATE");
      }
      if (value.writeEpoch !== writeEpoch) {
        throw new DomainError("ARCHIVE_FENCED");
      }
      return value;
    });
    if (archive.state === "deleted") {
      return true;
    }

    const initiallyDrained = await this.deletionWritersAreDrained(consultationId, writeEpoch);
    if (!initiallyDrained) {
      return false;
    }

    try {
      let consecutiveEmpty = 0;
      while (consecutiveEmpty < 2) {
        const scan = await this.performDeletionScan(consultationId);
        consecutiveEmpty = scan.empty ? consecutiveEmpty + 1 : 0;
        const writersStillDrained = await this.recordDeletionScan(
          consultationId,
          archive.writeEpoch,
          scan,
          consecutiveEmpty,
        );
        if (!writersStillDrained) {
          return false;
        }
      }
      return await this.completeDeletion(consultationId, archive.writeEpoch);
    } catch (error) {
      await this.recordDeletionFailure(consultationId, archive.id, archive.writeEpoch, error);
      return false;
    }
  }

  private async validateSupplementClaims(
    consultationId: UUID,
    supplement: InventorySupplement,
  ): Promise<UUID> {
    return this.archives.transaction(async (tx) => {
      const archive = await this.required(consultationId, tx);
      if (archive.state !== "complete" && archive.state !== "incomplete") {
        throw new DomainError("INVALID_ARCHIVE_STATE");
      }
      const final = await this.archives.finalInventory(archive.id, tx);
      if (!final || archive.finalInventoryHash !== supplement.finalInventorySha256) {
        throw new DomainError("FINAL_INVENTORY_CONFLICT");
      }
      const persisted = await this.archives.inventoryObjects(archive.id, tx);
      const claims = await this.archives.supplementClaims(archive.id, tx);
      this.assertClaimedObjects(supplement.addedObjects, persisted, false);
      this.assertSupplementClaims(supplement, final, claims);
      return archive.id;
    });
  }

  private async commitSupplement(
    consultationId: UUID,
    archiveId: UUID,
    supplement: InventorySupplement,
    sha256: string,
    key: string,
    uploaded: UploadedObject,
  ): Promise<boolean> {
    return this.archives.transaction(async (tx) => {
      const locked = await this.required(consultationId, tx);
      const final = await this.archives.finalInventory(locked.id, tx);
      const claims = await this.archives.supplementClaims(locked.id, tx);
      if (!final) {
        throw new DomainError("FINAL_INVENTORY_CONFLICT");
      }
      this.assertSupplementClaims(supplement, final, claims);

      const object: ArchiveObject = {
        id: supplement.supplementId,
        consultationId,
        objectClass: "inventory_supplement",
        causalKey: `inventory:supplement:${supplement.supplementId}`,
        key,
        versionId: uploaded.versionId,
        size: uploaded.size,
        sha256,
        s3Checksum: uploaded.checksum,
        contentType: "application/json",
        sampleStart: null,
        sampleEnd: null,
        attempt: null,
        sequence: null,
        writerEpoch: locked.writeEpoch,
      };
      await this.recordProtectedObject(object, tx);
      return this.archives.createSupplement(
        {
          id: supplement.supplementId,
          archiveId,
          finalHash: supplement.finalInventorySha256,
          supplement,
          sha256,
          objectId: object.id,
          at: this.clock.now(),
        },
        tx,
      );
    });
  }

  private async preflightFinalInventory(
    consultationId: UUID,
    inventory: FinalInventory,
    sha256: string,
  ): Promise<{ archiveId: UUID; existing: boolean }> {
    return this.archives.transaction(async (tx) => {
      const archive = await this.required(consultationId, tx);
      const existingHash = await this.archives.finalInventoryHash(archive.id, tx);
      if (existingHash !== null) {
        if (existingHash !== sha256) {
          throw new DomainError("FINAL_INVENTORY_CONFLICT");
        }
        return { archiveId: archive.id, existing: true };
      }
      if (archive.state !== "reconciling") {
        throw new DomainError("INVALID_ARCHIVE_STATE");
      }

      const unresolved = await this.archives.unresolvedExpectations(archive.id, tx);
      const persisted = await this.archives.inventoryObjects(archive.id, tx);
      this.assertClaimedObjects(inventory.objects, persisted, true);
      if (!(await this.archives.completePrerequisites(archive.id, inventory, tx))) {
        throw new DomainError("INVENTORY_PREREQUISITES_MISSING");
      }
      if (inventory.status === "complete" && unresolved.length) {
        throw new DomainError("INVENTORY_INCOMPLETE");
      }
      const unresolvedGapIsMissing =
        inventory.status === "incomplete" &&
        unresolved.some((expected) => {
          const expectedRange =
            expected.sampleStart === null || expected.sampleEnd === null
              ? null
              : { start: expected.sampleStart, end: expected.sampleEnd };
          return !inventory.missing.some(
            (gap) =>
              gap.objectClass === expected.objectClass &&
              rangesEqual(gap.sampleRange, expectedRange),
          );
        });
      if (unresolvedGapIsMissing) {
        throw new DomainError("INVENTORY_GAPS_MISMATCH");
      }
      return { archiveId: archive.id, existing: false };
    });
  }

  private async commitFinalInventory(
    consultationId: UUID,
    archiveId: UUID,
    inventory: FinalInventory,
    sha256: string,
    key: string,
    uploaded: UploadedObject,
  ): Promise<boolean> {
    return this.archives.transaction(async (tx) => {
      const locked = await this.required(consultationId, tx);
      if (locked.id !== archiveId) {
        throw new DomainError("ARCHIVE_FENCED");
      }
      if (locked.state !== "reconciling") {
        const existingHash = await this.archives.finalInventoryHash(locked.id, tx);
        if (
          (locked.state === "complete" || locked.state === "incomplete") &&
          existingHash === sha256
        ) {
          return false;
        }
        if (existingHash !== null && existingHash !== sha256) {
          throw new DomainError("FINAL_INVENTORY_CONFLICT");
        }
        throw new DomainError("ARCHIVE_FENCED");
      }
      const object: ArchiveObject = {
        id: inventory.inventoryId,
        consultationId,
        objectClass: "final_inventory",
        causalKey: "inventory:final",
        key,
        versionId: uploaded.versionId,
        size: uploaded.size,
        sha256,
        s3Checksum: uploaded.checksum,
        contentType: "application/json",
        sampleStart: null,
        sampleEnd: null,
        attempt: null,
        sequence: null,
        writerEpoch: locked.writeEpoch,
      };
      await this.archives.recordObject(object, tx);
      const inserted = await this.archives.createFinalInventory(
        locked.id,
        inventory,
        sha256,
        object.id,
        tx,
      );
      if (!inserted) {
        const existingHash = await this.archives.finalInventoryHash(locked.id, tx);
        if (existingHash !== sha256) {
          throw new DomainError("FINAL_INVENTORY_CONFLICT");
        }
        return false;
      }
      const transitioned = await this.archives.transition(
        locked.id,
        ["reconciling"],
        inventory.status,
        tx,
      );
      if (!transitioned) {
        throw new DomainError("ARCHIVE_FENCED");
      }
      return true;
    });
  }

  private async putCreateOnceOrRecover(
    key: string,
    body: Uint8Array,
    sha256: string,
    conflictCode: string,
  ): Promise<UploadedObject> {
    try {
      return await this.storage.putCreateOnce({
        key,
        body,
        contentType: "application/json",
        checksum: sha256,
      });
    } catch {
      const existing = await this.storage.head(key);
      if (!existing || existing.sha256 !== sha256) {
        throw new DomainError(conflictCode);
      }
      return existing;
    }
  }

  private async recoverStaleHold(consultationId: UUID): Promise<HoldRecoveryResult | null> {
    const owner = this.ids.uuid();
    const claimed = await this.archives.transaction(async (tx) => {
      const archive = await this.required(consultationId, tx);
      const now = this.clock.now();
      const operation = await this.archives.claimStaleHoldOperation(
        archive.id,
        owner,
        now,
        addMilliseconds(now, 300_000),
        tx,
      );
      if (!operation) {
        return null;
      }
      const hold = (await this.archives.activeHolds(archive.id, tx)).find(
        (candidate) => candidate.id === operation.operationId,
      );
      if (!hold) {
        throw new DomainError("HOLD_OPERATION_CORRUPT");
      }
      return {
        archiveId: archive.id,
        owner,
        operation,
        hold,
      };
    });
    if (!claimed) {
      return null;
    }

    const failed =
      claimed.operation.kind === "add"
        ? await this.applyHoldToStorage(
            consultationId,
            claimed.archiveId,
            claimed.hold.id,
            claimed.owner,
            claimed.hold.actorId,
            claimed.hold.reason,
            claimed.hold.perVersionResults,
          )
        : await this.releaseHoldFromStorage(
            consultationId,
            claimed.archiveId,
            claimed.hold.id,
            claimed.owner,
            claimed.hold.actorId,
            claimed.hold.perVersionResults,
          );
    return { id: claimed.operation.operationId, failed };
  }

  private async applyHoldToStorage(
    consultationId: UUID,
    archiveId: UUID,
    holdId: UUID,
    owner: UUID,
    actorId: UUID,
    reason: string,
    previous?: unknown,
  ): Promise<boolean> {
    const results = holdProgress(previous);
    const failedVersions: ObjectVersion[] = [];
    for (const version of await this.allVersions(consultationId)) {
      const wasAlreadyProtected = results.some(
        (result) =>
          result.key === version.key &&
          result.versionId === version.versionId &&
          result.ok &&
          (result.phase === "apply" || result.phase === "retry"),
      );
      if (wasAlreadyProtected) {
        continue;
      }

      try {
        await this.storage.setLegalHold(version.key, version.versionId, true);
        results.push({
          ...version,
          phase: "apply",
          ok: true,
          error: null,
        });
      } catch (error) {
        results.push({
          ...version,
          phase: "apply",
          ok: false,
          error: error instanceof Error ? error.message : "unknown",
        });
        failedVersions.push(version);
      }
      await this.renewHoldLease(consultationId, archiveId, holdId, owner, results);
    }

    for (const version of failedVersions) {
      try {
        await this.storage.setLegalHold(version.key, version.versionId, true);
        results.push({
          ...version,
          phase: "retry",
          ok: true,
          error: null,
        });
      } catch (error) {
        results.push({
          ...version,
          phase: "retry",
          ok: false,
          error: error instanceof Error ? error.message : "unknown",
        });
      }
      await this.renewHoldLease(consultationId, archiveId, holdId, owner, results);
    }

    const failed = failedVersions.some(
      (version) =>
        !results.some(
          (result) =>
            result.phase === "retry" &&
            result.key === version.key &&
            result.versionId === version.versionId &&
            result.ok,
        ),
    );
    await this.completeHoldApplication(
      consultationId,
      archiveId,
      holdId,
      owner,
      actorId,
      reason,
      results,
      failed,
    );
    return failed;
  }

  private async completeHoldApplication(
    consultationId: UUID,
    archiveId: UUID,
    holdId: UUID,
    owner: UUID,
    actorId: UUID,
    reason: string,
    results: HoldProgress[],
    failed: boolean,
  ): Promise<void> {
    await this.archives.transaction(async (tx) => {
      const archive = await this.required(consultationId, tx);
      const completed =
        archive.id === archiveId &&
        (await this.archives.completeHoldOperation(archive.id, holdId, owner, tx));
      if (!completed) {
        throw new DomainError("HOLD_OPERATION_FENCED");
      }
      const transitioned = await this.archives.transitionHoldState(
        holdId,
        ["applying"],
        failed ? "failed" : "active",
        tx,
      );
      if (!transitioned) {
        throw new DomainError("HOLD_OPERATION_FENCED");
      }
      await this.archives.recordHoldResults(
        holdId,
        {
          protected: results.filter((result) => result.ok).length,
          failed,
        },
        results,
        tx,
      );
      await this.audit.append(
        {
          id: this.ids.uuid(),
          aggregateId: archive.id,
          actorId,
          kind: failed ? "archive.hold_failed" : "archive.hold_added",
          occurredAt: this.clock.now(),
          details: { holdId, reason, failed },
        },
        tx,
      );
    });
  }

  private async releaseHoldFromStorage(
    consultationId: UUID,
    archiveId: UUID,
    holdId: UUID,
    owner: UUID,
    actorId: UUID,
    previous?: unknown,
  ): Promise<boolean> {
    const results = holdProgress(previous);
    const released = new Map<string, ObjectVersion>();
    const discovered = new Map<string, ObjectVersion>();
    let previousScan: string | null = null;
    let stableScans = 0;

    while (stableScans < 2) {
      const versions = await this.allVersions(consultationId);
      const signature = versions
        .map((version) => `${version.key}\u0000${version.versionId}`)
        .sort()
        .join("\u0001");
      stableScans = signature === previousScan ? stableScans + 1 : 1;
      previousScan = signature;

      for (const version of versions) {
        const versionKey = `${version.key}\u0000${version.versionId}`;
        discovered.set(versionKey, version);
        const latest = results.findLast(
          (result) => result.key === version.key && result.versionId === version.versionId,
        );
        if (latest?.phase === "release") {
          if (latest.ok) {
            released.set(versionKey, version);
          }
          continue;
        }

        try {
          await this.storage.setLegalHold(version.key, version.versionId, false);
          results.push({
            ...version,
            phase: "release",
            ok: true,
            error: null,
          });
          released.set(versionKey, version);
        } catch (error) {
          results.push({
            ...version,
            phase: "release",
            ok: false,
            error: error instanceof Error ? error.message : "unknown",
          });
        }
        await this.renewHoldLease(consultationId, archiveId, holdId, owner, results);
      }
    }

    const releaseFailed = [...discovered.values()].some((version) => {
      const latest = results.findLast(
        (result) => result.key === version.key && result.versionId === version.versionId,
      );
      return latest?.phase !== "release" || !latest.ok;
    });
    if (releaseFailed) {
      for (const version of released.values()) {
        try {
          await this.storage.setLegalHold(version.key, version.versionId, true);
          results.push({
            ...version,
            phase: "compensate",
            ok: true,
            error: null,
          });
        } catch (error) {
          results.push({
            ...version,
            phase: "compensate",
            ok: false,
            error: error instanceof Error ? error.message : "unknown",
          });
        }
        await this.renewHoldLease(consultationId, archiveId, holdId, owner, results);
      }
    }

    const compensationFailed =
      releaseFailed &&
      [...released.values()].some((version) => {
        const latest = results.findLast(
          (result) => result.key === version.key && result.versionId === version.versionId,
        );
        return latest?.phase !== "compensate" || !latest.ok;
      });
    if (releaseFailed) {
      await this.completeHoldRelease(
        consultationId,
        archiveId,
        holdId,
        owner,
        actorId,
        results,
        true,
        compensationFailed,
      );
      return true;
    }

    while (true) {
      const pending = await this.completeHoldRelease(
        consultationId,
        archiveId,
        holdId,
        owner,
        actorId,
        results,
        false,
        false,
      );
      if (pending.length === 0) {
        return false;
      }
      for (const version of pending) {
        try {
          await this.storage.setLegalHold(version.key, version.versionId, false);
          results.push({
            ...version,
            phase: "release",
            ok: true,
            error: null,
          });
          released.set(`${version.key}\u0000${version.versionId}`, version);
        } catch (error) {
          results.push({
            ...version,
            phase: "release",
            ok: false,
            error: error instanceof Error ? error.message : "unknown",
          });
        }
        await this.renewHoldLease(consultationId, archiveId, holdId, owner, results);
      }
      const pendingReleaseFailed = pending.some((version) => {
        const latest = results.findLast(
          (result) => result.key === version.key && result.versionId === version.versionId,
        );
        return latest?.phase !== "release" || !latest.ok;
      });
      if (pendingReleaseFailed) {
        for (const version of released.values()) {
          try {
            await this.storage.setLegalHold(version.key, version.versionId, true);
            results.push({
              ...version,
              phase: "compensate",
              ok: true,
              error: null,
            });
          } catch (error) {
            results.push({
              ...version,
              phase: "compensate",
              ok: false,
              error: error instanceof Error ? error.message : "unknown",
            });
          }
          await this.renewHoldLease(consultationId, archiveId, holdId, owner, results);
        }
        await this.completeHoldRelease(
          consultationId,
          archiveId,
          holdId,
          owner,
          actorId,
          results,
          true,
          [...released.values()].some((version) => {
            const latest = results.findLast(
              (result) => result.key === version.key && result.versionId === version.versionId,
            );
            return latest?.phase !== "compensate" || !latest.ok;
          }),
        );
        return true;
      }
    }
  }

  private async completeHoldRelease(
    consultationId: UUID,
    archiveId: UUID,
    holdId: UUID,
    owner: UUID,
    actorId: UUID,
    results: HoldProgress[],
    releaseFailed: boolean,
    compensationFailed: boolean,
  ): Promise<ObjectVersion[]> {
    return this.archives.transaction(async (tx) => {
      const archive = await this.required(consultationId, tx);
      if (archive.id !== archiveId) {
        throw new DomainError("HOLD_OPERATION_FENCED");
      }

      if (!releaseFailed) {
        const recorded = new Map(
          (await this.archives.inventoryObjects(archive.id, tx)).map(({ key, versionId }) => [
            `${key}\u0000${versionId}`,
            { key, versionId },
          ]),
        );
        const pending = [...recorded.values()].filter((version) => {
          const latest = results.findLast(
            (result) => result.key === version.key && result.versionId === version.versionId,
          );
          return latest?.phase !== "release" || !latest.ok;
        });
        if (pending.length > 0) {
          const now = this.clock.now();
          const renewed = await this.archives.renewHoldOperation(
            archive.id,
            holdId,
            owner,
            addMilliseconds(now, 300_000),
            tx,
          );
          if (!renewed) {
            throw new DomainError("HOLD_OPERATION_FENCED");
          }
          await this.archives.recordHoldResults(
            holdId,
            { inProgress: true, pendingVersions: pending.length },
            results,
            tx,
          );
          return pending;
        }
      }

      const completed = await this.archives.completeHoldOperation(archive.id, holdId, owner, tx);
      if (!completed) {
        throw new DomainError("HOLD_OPERATION_FENCED");
      }
      await this.archives.recordHoldResults(
        holdId,
        { releaseFailed, compensationFailed },
        results,
        tx,
      );
      if (releaseFailed) {
        const transitioned = await this.archives.transitionHoldState(
          holdId,
          ["releasing"],
          compensationFailed ? "failed" : "active",
          tx,
        );
        if (!transitioned) {
          throw new DomainError("HOLD_OPERATION_FENCED");
        }
        return [];
      }

      const now = this.clock.now();
      await this.archives.removeHold(holdId, actorId, now, tx);
      await this.audit.append(
        {
          id: this.ids.uuid(),
          aggregateId: archive.id,
          actorId,
          kind: "archive.hold_released",
          occurredAt: now,
          details: { holdId, versions: results.length },
        },
        tx,
      );
      return [];
    });
  }

  private async renewHoldLease(
    consultationId: UUID,
    archiveId: UUID,
    operationId: UUID,
    owner: UUID,
    results: unknown,
  ): Promise<void> {
    await this.archives.transaction(async (tx) => {
      const archive = await this.required(consultationId, tx);
      const now = this.clock.now();
      const renewed =
        archive.id === archiveId &&
        (await this.archives.renewHoldOperation(
          archiveId,
          operationId,
          owner,
          addMilliseconds(now, 300_000),
          tx,
        ));
      if (!renewed) {
        throw new DomainError("HOLD_OPERATION_FENCED");
      }
      await this.archives.recordHoldResults(operationId, { inProgress: true }, results, tx);
    });
  }

  private async deletionWritersAreDrained(
    consultationId: UUID,
    writeEpoch: number,
  ): Promise<boolean> {
    return this.archives.transaction(async (tx) => {
      const locked = await this.required(consultationId, tx);
      return (
        locked.state === "deleting" &&
        locked.writeEpoch === writeEpoch &&
        this.archives.deletionWritersDrained(consultationId, writeEpoch, tx)
      );
    });
  }

  private async performDeletionScan(consultationId: UUID): Promise<{
    uploads: readonly MultipartUpload[];
    versions: ObjectVersion[];
    remainingUploads: readonly MultipartUpload[];
    remainingVersions: ObjectVersion[];
    empty: boolean;
  }> {
    const uploads = await this.storage.listMultipart(consultationId);
    for (const upload of uploads) {
      await this.storage.abortMultipart(upload.key, upload.uploadId);
    }
    const versions = await this.allVersions(consultationId);
    if (versions.length) {
      await this.storage.deleteVersions(versions);
    }
    const remainingUploads = await this.storage.listMultipart(consultationId);
    const remainingVersions = await this.allVersions(consultationId);
    return {
      uploads,
      versions,
      remainingUploads,
      remainingVersions,
      empty: remainingUploads.length === 0 && remainingVersions.length === 0,
    };
  }

  private async recordDeletionScan(
    consultationId: UUID,
    writeEpoch: number,
    scan: {
      uploads: readonly MultipartUpload[];
      versions: readonly ObjectVersion[];
      remainingUploads: readonly MultipartUpload[];
      remainingVersions: readonly ObjectVersion[];
    },
    consecutiveEmpty: number,
  ): Promise<boolean> {
    return this.archives.transaction(async (tx) => {
      const locked = await this.required(consultationId, tx);
      if (locked.state !== "deleting" || locked.writeEpoch !== writeEpoch) {
        throw new DomainError("ARCHIVE_FENCED");
      }
      const drained = await this.archives.deletionWritersDrained(
        consultationId,
        locked.writeEpoch,
        tx,
      );
      await this.archives.recordDeletionScan(
        {
          id: this.ids.uuid(),
          archiveId: locked.id,
          writeEpoch: locked.writeEpoch,
          versionCount: scan.remainingVersions.length,
          multipartCount: scan.remainingUploads.length,
          consecutiveEmpty,
          result: {
            deletedVersions: scan.versions.length,
            abortedUploads: scan.uploads.length,
            writersDrained: drained,
          },
          at: this.clock.now(),
        },
        tx,
      );
      return drained;
    });
  }

  private async completeDeletion(consultationId: UUID, writeEpoch: number): Promise<boolean> {
    return this.archives.transaction(async (tx) => {
      const locked = await this.required(consultationId, tx);
      if (locked.writeEpoch !== writeEpoch) {
        throw new DomainError("ARCHIVE_FENCED");
      }
      const drained = await this.archives.deletionWritersDrained(
        consultationId,
        locked.writeEpoch,
        tx,
      );
      if (!drained) {
        return false;
      }
      const completed = await this.archives.completeDeletion(
        locked.id,
        locked.writeEpoch,
        this.clock.now(),
        tx,
      );
      if (!completed) {
        throw new DomainError("ARCHIVE_FENCED");
      }
      return true;
    });
  }

  private async recordDeletionFailure(
    consultationId: UUID,
    archiveId: UUID,
    writeEpoch: number,
    error: unknown,
  ): Promise<void> {
    await this.archives.transaction(async (tx) => {
      const locked = await this.required(consultationId, tx);
      if (locked.state !== "deleting" || locked.writeEpoch !== writeEpoch) {
        return;
      }
      await this.archives.recordDeletionFailure(
        archiveId,
        {
          message: error instanceof Error ? error.message : "unknown",
          writeEpoch,
        },
        tx,
      );
      await this.archives.transition(locked.id, ["deleting"], "incomplete", tx);
    });
  }

  private async allVersions(consultationId: UUID): Promise<ObjectVersion[]> {
    const versions: ObjectVersion[] = [];
    let cursor: string | undefined;
    do {
      const page = await this.storage.listMeetingVersions(consultationId, cursor);
      versions.push(...page.versions);
      cursor = page.cursor ?? undefined;
    } while (cursor !== undefined);
    return versions;
  }

  private assertFreshReauth(consultationId: UUID, session: SessionRecord): void {
    const isFresh =
      session.reauthenticatedAt !== null &&
      session.reauthConsultationId === consultationId &&
      this.clock.now().getTime() - session.reauthenticatedAt.getTime() <= 300_000;
    if (!isFresh) {
      throw new DomainError("REAUTH_REQUIRED");
    }
  }

  private async required(consultationId: UUID, tx: Transaction) {
    const archive = await this.archives.lockByConsultation(consultationId, tx);
    if (!archive) {
      throw new DomainError("NOT_FOUND");
    }
    return archive;
  }

  private assertSupplementClaims(
    supplement: InventorySupplement,
    final: FinalInventory,
    previous: SupplementClaims,
  ): void {
    const indexes = new Set(supplement.closedGapIndexes);
    const claimedObjects = new Set([
      ...final.objects.map((object) => object.objectId),
      ...previous.objectIds,
    ]);
    const hasDuplicateIndexes = indexes.size !== supplement.closedGapIndexes.length;
    const hasConflictingIndex = supplement.closedGapIndexes.some(
      (index) => index >= final.missing.length || previous.closedGapIndexes.includes(index),
    );
    if (hasDuplicateIndexes || hasConflictingIndex) {
      throw new DomainError("SUPPLEMENT_GAP_CONFLICT");
    }

    for (const index of supplement.closedGapIndexes) {
      const gap = final.missing[index];
      const hasCoveringNewObject =
        gap !== undefined &&
        supplement.addedObjects.some(
          (object) =>
            !claimedObjects.has(object.objectId) &&
            object.class === gap.objectClass &&
            rangesEqual(object.sampleRange, gap.sampleRange),
        );
      if (!hasCoveringNewObject) {
        throw new DomainError("SUPPLEMENT_GAP_CONFLICT");
      }
    }
  }

  private assertClaimedObjects(
    claimed: FinalInventory["objects"],
    persisted: readonly ArchiveObject[],
    exact: boolean,
  ): void {
    const claimedIds = new Set(claimed.map((object) => object.objectId));
    const claimedVersions = new Set(
      claimed.map((object) => `${object.key}\u0000${object.versionId}`),
    );
    if (claimedIds.size !== claimed.length || claimedVersions.size !== claimed.length) {
      throw new DomainError("INVENTORY_OBJECT_MISMATCH");
    }

    const persistedById = new Map(persisted.map((object) => [object.id, object]));
    const persistedVersions = new Set(
      persisted.map((object) => `${object.key}\u0000${object.versionId}`),
    );
    if (
      exact &&
      (claimed.length !== persisted.length ||
        persistedById.size !== persisted.length ||
        persistedVersions.size !== persisted.length)
    ) {
      throw new DomainError("INVENTORY_OBJECT_MISMATCH");
    }

    const hasMismatch = claimed.some((object) => {
      const stored = persistedById.get(object.objectId);
      return (
        !stored ||
        stored.objectClass !== object.class ||
        stored.key !== object.key ||
        stored.versionId !== object.versionId ||
        stored.size !== object.size ||
        stored.sha256 !== object.sha256 ||
        stored.s3Checksum !== object.s3Checksum ||
        stored.sampleStart !== (object.sampleRange?.start ?? null) ||
        stored.sampleEnd !== (object.sampleRange?.end ?? null) ||
        stored.attempt !== object.attempt ||
        stored.sequence !== object.sequence
      );
    });
    if (hasMismatch) {
      throw new DomainError("INVENTORY_OBJECT_MISMATCH");
    }
  }

  private async recordProtectedObject(
    object: ArchiveObject,
    tx: Transaction,
    lockedArchive?: LockedArchive,
  ): Promise<void> {
    const archive = lockedArchive ?? (await this.required(object.consultationId, tx));
    if (
      archive.writeEpoch !== object.writerEpoch ||
      archive.state === "deleting" ||
      archive.state === "deleted"
    ) {
      throw new DomainError("ARCHIVE_WRITER_FENCED");
    }
    const verified = await this.storage.verify({
      key: object.key,
      versionId: object.versionId,
      size: object.size,
      checksum: object.s3Checksum,
    });
    if (!verified) {
      throw new DomainError("ARCHIVE_OBJECT_VERIFICATION_FAILED");
    }
    if ((await this.archives.activeHolds(archive.id, tx)).length > 0) {
      await this.storage.setLegalHold(object.key, object.versionId, true);
    }
    await this.archives.recordObject(object, tx);
  }

  private toArchiveObject(
    consultationId: UUID,
    writerEpoch: number,
    causalKey: string,
    object: RecordObjectInput,
  ): ArchiveObject {
    return {
      id: object.objectId,
      consultationId,
      objectClass: object.class,
      causalKey,
      key: object.key,
      versionId: object.versionId,
      size: object.size,
      sha256: object.sha256,
      s3Checksum: object.s3Checksum,
      contentType: object.contentType,
      sampleStart: object.sampleRange?.start ?? null,
      sampleEnd: object.sampleRange?.end ?? null,
      attempt: object.attempt,
      sequence: object.sequence,
      writerEpoch,
    };
  }
}

function rangesEqual(
  left: { start: number; end: number } | null,
  right: { start: number; end: number } | null,
): boolean {
  return left?.start === right?.start && left?.end === right?.end;
}
