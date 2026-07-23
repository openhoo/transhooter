import { createHash, randomUUID } from "node:crypto";
import { type CaptionPacket, CaptionPacketSchema } from "@transhooter/contracts";
import { deterministicUuid } from "@transhooter/server-core/rooms";
import { type EffectFaultControl, noEffectFaults } from "../runtime/fault-control";
import { recordEffect, recordWorkClaimed, withControlSpan } from "../runtime/telemetry";
import type {
  ArchivedObject,
  Clock,
  DerivedArchiveObject,
  DurableStore,
  Effect,
  ReconciliationSnapshot,
  Uuid,
} from "./model";
import { canonicalRequest, EFFECT_KINDS } from "./model";
import type { Adoption, RemoteEffects } from "./remote";

export interface RunnerOptions {
  readonly owner: Uuid;
  readonly leaseMs: number;
  readonly batchSize: number;
}

type MissingInventoryEntry = Readonly<Record<string, unknown>> & {
  readonly class: string;
  readonly reason: string;
};

interface ArchiveObjectEvidenceIndex {
  readonly firstObjectIdByClassAndPrefix: ReadonlyMap<string, string>;
  readonly prefixes: ReadonlySet<string>;
  readonly providerEvidence: ReadonlySet<string>;
  readonly classAndDestination: ReadonlySet<string>;
}

interface CanonicalEffectRequest {
  readonly bytes: Uint8Array;
  readonly sha256: string;
}

interface LeaseRenewal {
  timer: NodeJS.Timeout | undefined;
  pending: Promise<void>;
  requestedExpiryMs: number;
  lost: boolean;
  monitoring: boolean;
}

class LeaseLostError extends Error {}
type EffectOutcome = "compensated" | "failed" | "lease_lost" | "not_owned" | "done";
const ARCHIVE_OBJECT_IO_CONCURRENCY = 4;

export class EffectRunner {
  constructor(
    private readonly store: DurableStore,
    private readonly remote: RemoteEffects,
    private readonly clock: Clock,
    private readonly options: RunnerOptions,
    private readonly faults: EffectFaultControl = noEffectFaults,
  ) {}

  async tick(): Promise<number> {
    const effects = await this.store.claimEffects({
      owner: this.options.owner,
      now: this.clock.now(),
      leaseMs: this.options.leaseMs,
      limit: this.options.batchSize,
    });
    recordWorkClaimed("effect", effects.length);
    await settledBarrier(effects.map(async (effect) => this.observeEffect(effect)));
    return effects.length;
  }

  private async observeEffect(effect: Effect): Promise<void> {
    const startedAt = performance.now();
    const kind = EFFECT_KINDS.includes(effect.kind) ? effect.kind : "other";
    let outcome: EffectOutcome = "failed";
    let failure: unknown;
    try {
      outcome = await withControlSpan("control.effect", { "control.effect.kind": kind }, async () =>
        this.run(effect),
      );
    } catch (error) {
      failure = error;
      throw error;
    } finally {
      recordEffect(kind, outcome, (performance.now() - startedAt) / 1_000, failure);
    }
  }

  private async run(claimed: Effect): Promise<EffectOutcome> {
    if (claimed.state === "applied") {
      return await this.resumeAppliedEffect(claimed);
    }
    const generation = await this.store.currentGeneration(claimed.consultationId);
    if (claimed.state === "compensating" || generation !== claimed.generation) {
      const renewal = this.startLeaseRenewal(claimed);
      try {
        if (claimed.state !== "compensating") {
          await this.requireOwnership(claimed, renewal);
          await this.store.markCompensating(claimed.id, this.options.owner, "generation fenced");
        }
        await this.compensateOwned(claimed, renewal);
        return "compensated";
      } catch (error) {
        if (error instanceof LeaseLostError) {
          return "lease_lost";
        }
        throw error;
      } finally {
        renewal.monitoring = false;
        clearInterval(renewal.timer);
      }
    }

    const canonical = this.canonicalEffectRequest(claimed);
    if (claimed.requestSha256 !== null && claimed.requestSha256 !== canonical.sha256) {
      await this.store.markFailed(
        claimed.id,
        this.options.owner,
        "immutable request hash mismatch",
        null,
      );
      return "failed";
    }

    const calling = await this.store.persistCalling(
      claimed.id,
      this.options.owner,
      canonical.bytes,
      canonical.sha256,
    );
    if (calling === null) {
      return "not_owned";
    }

    const renewal = this.startLeaseRenewal(calling);
    try {
      await this.requireOwnership(calling, renewal);
      if ((await this.store.currentGeneration(calling.consultationId)) !== calling.generation) {
        await this.store.markCompensating(
          calling.id,
          this.options.owner,
          "generation fenced before remote call",
        );
        await this.compensateOwned(calling, renewal);
        return "compensated";
      }
      await this.faults.afterPersist(calling.kind, calling.consultationId);
      if (calling.kind === "ARCHIVE_RECONCILE") {
        return await this.reconcileArchive(calling, renewal);
      }
      if (calling.kind === "ARCHIVE_DELETE") {
        return await this.deleteArchive(calling);
      }
      return await this.executeRemoteEffect(calling, renewal);
    } catch (error) {
      if (error instanceof LeaseLostError) {
        return "lease_lost";
      }
      throw error;
    } finally {
      renewal.monitoring = false;
      clearInterval(renewal.timer);
    }
  }

  private async resumeAppliedEffect(effect: Effect): Promise<EffectOutcome> {
    const renewal = this.startLeaseRenewal(effect);
    try {
      await this.requireOwnership(effect, renewal);
      if ((await this.store.currentGeneration(effect.consultationId)) !== effect.generation) {
        await this.store.markCompensating(
          effect.id,
          this.options.owner,
          "generation fenced after applied recovery",
        );
        await this.compensateOwned(effect, renewal);
        return "compensated";
      }
      await this.store.markDone(effect.id, this.options.owner);
      return "done";
    } catch (error) {
      if (error instanceof LeaseLostError) {
        return "lease_lost";
      }
      throw error;
    } finally {
      renewal.monitoring = false;
      clearInterval(renewal.timer);
    }
  }

  private canonicalEffectRequest(effect: Effect): CanonicalEffectRequest {
    const remoteBytes = this.remote.canonicalRequest?.(effect, effect.plan);
    if (remoteBytes === undefined) {
      return canonicalRequest(effect.plan);
    }
    return {
      bytes: remoteBytes,
      sha256: createHash("sha256").update(remoteBytes).digest("hex"),
    };
  }

  private startLeaseRenewal(effect: Effect): LeaseRenewal {
    const renewal: LeaseRenewal = {
      timer: undefined,
      pending: Promise.resolve(),
      requestedExpiryMs: effect.leaseExpiresAt?.getTime() ?? 0,
      lost: false,
      monitoring: true,
    };
    renewal.timer = setInterval(
      () => {
        void this.renewLease(effect, renewal);
      },
      Math.max(100, Math.floor(this.options.leaseMs / 3)),
    );
    renewal.timer.unref();
    return renewal;
  }

  private async renewLease(effect: Effect, renewal: LeaseRenewal): Promise<boolean> {
    const requestedExpiryMs = Math.max(
      renewal.requestedExpiryMs,
      this.clock.now().getTime() + this.options.leaseMs,
    );
    renewal.requestedExpiryMs = requestedExpiryMs;
    const attempt = renewal.pending.then(async () =>
      this.store.renewEffectLease(effect.id, this.options.owner, new Date(requestedExpiryMs)),
    );
    renewal.pending = attempt.then(
      () => undefined,
      () => undefined,
    );
    try {
      const accepted = await attempt;
      if (renewal.monitoring && !accepted) {
        renewal.lost = true;
      }
      return accepted;
    } catch {
      if (renewal.monitoring) {
        renewal.lost = true;
      }
      return false;
    }
  }

  private async requireOwnership(effect: Effect, renewal: LeaseRenewal): Promise<void> {
    if (renewal.lost) {
      throw new LeaseLostError("effect lease lost");
    }
    if (!(await this.renewLease(effect, renewal))) {
      renewal.lost = true;
      throw new LeaseLostError("effect lease lost");
    }
  }

  private async executeRemoteEffect(effect: Effect, renewal: LeaseRenewal): Promise<EffectOutcome> {
    try {
      await this.faults.shouldFail(effect.kind, effect.consultationId);
      const adopted = await this.remote.adopt(effect, effect.plan);
      if (adopted !== null) {
        return await this.applyAdoption(effect, adopted, renewal);
      }
      const result = await this.remote.execute(effect, effect.plan);
      await this.faults.afterRemoteSuccess(effect.kind, effect.consultationId);
      await this.requireOwnership(effect, renewal);
      if ((await this.store.currentGeneration(effect.consultationId)) !== effect.generation) {
        await this.store.markCompensating(
          effect.id,
          this.options.owner,
          "generation fenced after remote call",
        );
        await this.compensateOwned({ ...effect, remoteId: result.remoteId }, renewal);
        return "compensated";
      }
      const rejectedOutcome = await this.persistApplied(
        effect,
        result.remoteId,
        result.result,
        renewal,
      );
      if (rejectedOutcome !== null) {
        return rejectedOutcome;
      }
      await this.requireOwnership(effect, renewal);
      await this.store.markDone(effect.id, this.options.owner);
      return "done";
    } catch (error) {
      if (error instanceof LeaseLostError) {
        throw error;
      }
      const generation = await this.store.currentGeneration(effect.consultationId);
      if (generation !== effect.generation) {
        const adoption = await this.remote.adopt(effect, effect.plan);
        if (adoption?.matchesRequest === true) {
          await this.requireOwnership(effect, renewal);
          await this.store.markCompensating(
            effect.id,
            this.options.owner,
            "ambiguous stale remote call",
          );
          await this.compensateOwned({ ...effect, remoteId: adoption.remoteId }, renewal);
          return "compensated";
        }
      }
      const message = error instanceof Error ? error.message : "unknown remote effect failure";
      const delay = Math.min(60_000, 250 * 2 ** Math.min(effect.attempt, 8));
      await this.store.markFailed(
        effect.id,
        this.options.owner,
        message,
        new Date(this.clock.now().getTime() + delay),
      );
      return "failed";
    }
  }

  private async applyAdoption(
    effect: Effect,
    adoption: Adoption,
    renewal: LeaseRenewal,
  ): Promise<EffectOutcome> {
    await this.requireOwnership(effect, renewal);
    if (!adoption.matchesRequest) {
      await this.store.markFailed(
        effect.id,
        this.options.owner,
        "deterministic identity collision",
        null,
      );
      return "failed";
    }
    if ((await this.store.currentGeneration(effect.consultationId)) !== effect.generation) {
      await this.store.markCompensating(
        effect.id,
        this.options.owner,
        "generation fenced after adoption",
      );
      await this.compensateOwned({ ...effect, remoteId: adoption.remoteId }, renewal);
      return "compensated";
    }
    const rejectedOutcome = await this.persistApplied(
      effect,
      adoption.remoteId,
      adoption.result ?? { adopted: true, terminal: adoption.terminal },
      renewal,
    );
    if (rejectedOutcome !== null) {
      return rejectedOutcome;
    }
    await this.requireOwnership(effect, renewal);
    await this.store.markDone(effect.id, this.options.owner);
    return "done";
  }

  private async persistApplied(
    effect: Effect,
    remoteId: string | null,
    result: unknown,
    renewal: LeaseRenewal,
  ): Promise<EffectOutcome | null> {
    const transition = await this.store.markApplied(
      effect.id,
      this.options.owner,
      remoteId,
      result,
    );
    if (transition === "applied") {
      await this.faults.afterMarkApplied(effect.kind, effect.consultationId);
      return null;
    }
    if ((await this.store.currentGeneration(effect.consultationId)) !== effect.generation) {
      await this.requireOwnership(effect, renewal);
      await this.store.markCompensating(
        effect.id,
        this.options.owner,
        "generation fenced during applied transition",
      );
      await this.compensateOwned({ ...effect, remoteId }, renewal);
      return "compensated";
    }
    return "not_owned";
  }

  private async compensateOwned(
    effect: Effect,
    renewal: LeaseRenewal,
    markDone = true,
  ): Promise<void> {
    await this.requireOwnership(effect, renewal);
    await this.remote.compensate(effect);
    await this.requireOwnership(effect, renewal);
    if (markDone) {
      await this.store.markDone(effect.id, this.options.owner);
    }
  }

  private async reconcileArchive(effect: Effect, renewal: LeaseRenewal): Promise<EffectOutcome> {
    try {
      const snapshot = await this.store.reconciliationSnapshot(
        effect.consultationId,
        effect.generation,
        typeof effect.plan.resourceGeneration === "number"
          ? effect.plan.resourceGeneration
          : effect.generation,
      );
      if (snapshot === null) {
        await this.store.markDone(effect.id, this.options.owner);
        return "done";
      }
      const knownObjects = new Set(snapshot.objects.map(archiveObjectIdentity));
      const discoveredObjects = (
        await this.remote.discoverArchiveObjects(`v1/meetings/${effect.consultationId}/`)
      )
        .filter(
          (object) =>
            !object.key.endsWith("/inventory/final.json") &&
            !knownObjects.has(archiveObjectIdentity(object)),
        )
        .map(
          (object): DerivedArchiveObject => ({
            id: deterministicUuid(
              effect.consultationId,
              `archive-object:${object.key}:${object.versionId}`,
            ),
            objectClass: archiveObjectClass(object.key),
            key: object.key,
            versionId: object.versionId,
            size: object.size,
            sha256: object.sha256,
            checksum: object.checksum,
            contentType: object.contentType,
          }),
        )
        .sort((left, right) =>
          `${left.key}\u0000${left.versionId}\u0000${left.id}`.localeCompare(
            `${right.key}\u0000${right.versionId}\u0000${right.id}`,
          ),
        );
      const persistedObjects = [
        ...snapshot.objects,
        ...discoveredObjects.map(derivedToArchivedObject),
      ];
      const invalidObjectIds = await this.verifyObjects(persistedObjects);
      const missing = this.missingInventoryEntries(snapshot, invalidObjectIds, persistedObjects);
      if (
        missing.length > 0 &&
        effect.plan.forceIncomplete !== true &&
        this.clock.now() < snapshot.reconciliationDeadlineAt
      ) {
        throw new Error(
          `archive evidence is still pending: ${JSON.stringify(missing.slice(0, 20))}`,
        );
      }
      const captionObjects = persistedObjects.filter(
        (object) =>
          object.contentType === "application/json" && object.objectClass.includes("caption"),
      );
      const renderedVttObjects = await this.renderVtt(effect.consultationId, captionObjects);
      const persistedIdentities = new Set(persistedObjects.map(archiveObjectIdentity));
      const vttObjects = renderedVttObjects.filter(
        (object) => !persistedIdentities.has(archiveObjectIdentity(object)),
      );
      const derivedObjects = [...discoveredObjects, ...vttObjects].sort((left, right) =>
        `${left.key}\u0000${left.versionId}\u0000${left.id}`.localeCompare(
          `${right.key}\u0000${right.versionId}\u0000${right.id}`,
        ),
      );
      const inventoryObjects = [
        ...persistedObjects,
        ...vttObjects.map(derivedToArchivedObject),
      ].sort((left, right) =>
        `${left.key}\u0000${left.versionId}\u0000${left.id}`.localeCompare(
          `${right.key}\u0000${right.versionId}\u0000${right.id}`,
        ),
      );
      const inventory = {
        consultationId: effect.consultationId,
        status: missing.length === 0 ? "complete" : "incomplete",
        roomClose: snapshot.roomClose,
        workerTerminal: snapshot.workerTerminal,
        egressResults: snapshot.egressResults,
        objects: inventoryObjects,
        missing,
        errors:
          invalidObjectIds.size === 0
            ? []
            : [
                {
                  code: "OBJECT_VERIFICATION_FAILED",
                  objectIds: [...invalidObjectIds].sort(),
                },
              ],
      } as const;
      await this.uploadAndCompleteInventory(effect, renewal, snapshot, inventory, derivedObjects);
      return "done";
    } catch (error) {
      const message = error instanceof Error ? error.message : "archive reconciliation failed";
      await this.store.markFailed(
        effect.id,
        this.options.owner,
        message,
        new Date(this.clock.now().getTime() + 1_000),
      );
      return "failed";
    }
  }

  private async verifyObjects(objects: ReconciliationSnapshot["objects"]): Promise<Set<string>> {
    const verificationResults = await mapBounded(
      objects,
      ARCHIVE_OBJECT_IO_CONCURRENCY,
      async (object) => ({
        id: object.id,
        valid: await this.remote.verifyArchiveObject({
          key: object.key,
          versionId: object.versionId,
          size: object.size,
          checksum: object.s3Checksum,
        }),
      }),
    );
    return new Set(
      verificationResults.filter((result) => !result.valid).map((result) => result.id),
    );
  }

  private missingInventoryEntries(
    snapshot: ReconciliationSnapshot,
    invalidObjectIds: ReadonlySet<string>,
    objects: readonly ArchivedObject[],
  ): MissingInventoryEntry[] {
    const objectIndex = indexArchiveObjectEvidence(objects);
    const resolvedExpectationIds = new Map(
      snapshot.expectations.map((expected) => {
        const discovered =
          expected.fulfilledObjectId === null &&
          (expected.objectClass === "room_composite" ||
            expected.objectClass === "participant_original")
            ? (objectIndex.firstObjectIdByClassAndPrefix.get(
                `${expected.objectClass}\u0000${expected.causalKey}`,
              ) ?? null)
            : null;
        return [expected.id, expected.fulfilledObjectId ?? discovered] as const;
      }),
    );
    const missing: MissingInventoryEntry[] = snapshot.expectations
      .filter((expected) => {
        const objectId = resolvedExpectationIds.get(expected.id) ?? null;
        return objectId === null || invalidObjectIds.has(objectId);
      })
      .map((expected) => {
        const objectId = resolvedExpectationIds.get(expected.id) ?? null;
        return {
          expectationId: expected.id,
          class: expected.objectClass,
          causalKey: expected.causalKey,
          sampleStart: expected.sampleStart,
          sampleEnd: expected.sampleEnd,
          reason:
            objectId !== null && invalidObjectIds.has(objectId)
              ? "object_verification_failed"
              : "unfulfilled",
        };
      });
    const expectedObjectIds = new Set(
      [...resolvedExpectationIds.values()].filter(
        (objectId): objectId is string => objectId !== null,
      ),
    );
    missing.push(
      ...snapshot.providerGaps.map((gap) => ({
        class: "provider_terminal",
        reason: "provider_attempt_failed",
        ...gap,
      })),
    );
    for (const objectId of invalidObjectIds) {
      if (!expectedObjectIds.has(objectId)) {
        missing.push({
          class: "archive_object",
          objectId,
          reason: "object_verification_failed",
        });
      }
    }
    if (!terminalMarker(snapshot.roomClose)) {
      missing.push({ class: "room_close", reason: "terminal_missing" });
    }
    if (!terminalMarker(snapshot.workerTerminal)) {
      missing.push({ class: "worker_terminal", reason: "terminal_missing" });
    }
    for (const attempt of snapshot.providerAttempts) {
      for (const evidence of [
        `/pipeline/terminal/raw/${attempt.attemptId}/`,
        `/pipeline/${attempt.stage}/raw/${attempt.attemptId}/`,
      ]) {
        if (!objectIndex.providerEvidence.has(evidence)) {
          missing.push({
            class: "provider_attempt",
            attemptId: attempt.attemptId,
            stage: attempt.stage,
            evidence,
            reason: "object_missing",
          });
        }
      }
    }

    for (const direction of snapshot.directions) {
      if (direction.mode !== "translated" || direction.emittedOutput === 0) {
        continue;
      }
      for (const objectClass of ["tts_output_pcm", "livekit_output_pcm"] as const) {
        if (
          !objectIndex.classAndDestination.has(
            `${objectClass}\u0000${direction.destinationParticipantId}`,
          )
        ) {
          missing.push({
            class: objectClass,
            destinationParticipantId: direction.destinationParticipantId,
            sampleStart: 0,
            sampleEnd: direction.emittedOutput,
            reason: "object_missing",
          });
        }
      }
    }
    snapshot.egressResults.forEach((result, index) => {
      if (!terminalMarker(result)) {
        missing.push({
          class: "egress_terminal",
          index,
          reason: "terminal_missing",
        });
        return;
      }
      const outputPrefix = asRecord(result).outputPrefix;
      if (typeof outputPrefix !== "string" || !objectIndex.prefixes.has(outputPrefix)) {
        missing.push({
          class: "egress_object",
          index,
          reason: "object_missing",
        });
      }
    });
    return missing;
  }

  private async uploadAndCompleteInventory(
    effect: Effect,
    renewal: LeaseRenewal,
    snapshot: ReconciliationSnapshot,
    inventory: Readonly<Record<string, unknown>>,
    vttObjects: readonly DerivedArchiveObject[],
  ): Promise<void> {
    const canonical = canonicalRequest(inventory);
    const uploaded = await this.remote.putArchiveObject({
      key: `v1/meetings/${effect.consultationId}/inventory/final.json`,
      body: canonical.bytes,
      contentType: "application/json",
      sha256: canonical.sha256,
    });
    await this.requireOwnership(effect, renewal);
    const completed = await this.store.completeReconciliation(
      effect,
      this.options.owner,
      this.clock.now(),
      snapshot,
      inventory,
      canonical.sha256,
      {
        id: randomUUID(),
        versionId: uploaded.versionId,
        size: uploaded.size,
        checksum: uploaded.checksum,
      },
      vttObjects,
    );
    if (!completed) {
      throw new Error("final inventory create-once fence rejected");
    }
  }

  private async renderVtt(
    consultationId: Uuid,
    captionObjects: readonly ArchivedObject[],
  ): Promise<readonly DerivedArchiveObject[]> {
    const latestPackets = await this.loadLatestFinalCaptions(consultationId, captionObjects);
    const packetsByDestination = new Map<string, CaptionPacket[]>();
    for (const packet of latestPackets) {
      const destinationPackets = packetsByDestination.get(packet.destinationParticipantId) ?? [];
      destinationPackets.push(packet);
      packetsByDestination.set(packet.destinationParticipantId, destinationPackets);
    }

    const uploads = [...packetsByDestination.entries()]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([destination, packets]) => {
        const orderedPackets = [...packets].sort(
          (left, right) =>
            left.sourceSampleStart - right.sourceSampleStart ||
            left.utteranceId.localeCompare(right.utteranceId),
        );
        const body = encodeVtt(orderedPackets);
        return {
          body,
          key: `v1/meetings/${consultationId}/captions/${destination}/final.vtt`,
          sha256: createHash("sha256").update(body).digest("hex"),
        };
      });

    return await mapBounded(uploads, ARCHIVE_OBJECT_IO_CONCURRENCY, async (upload) => {
      const uploaded = await this.remote.putArchiveObject({
        key: upload.key,
        body: upload.body,
        contentType: "text/vtt; charset=utf-8",
        sha256: upload.sha256,
      });
      return {
        id: deterministicUuid(consultationId, `archive-object:${upload.key}:${uploaded.versionId}`),
        objectClass: archiveObjectClass(upload.key),
        key: upload.key,
        versionId: uploaded.versionId,
        size: uploaded.size,
        sha256: upload.sha256,
        checksum: uploaded.checksum,
        contentType: "text/vtt; charset=utf-8",
      };
    });
  }

  private async loadLatestFinalCaptions(
    consultationId: Uuid,
    captionObjects: readonly ArchivedObject[],
  ): Promise<readonly CaptionPacket[]> {
    const orderedObjects = [...captionObjects].sort((left, right) =>
      archiveObjectIdentity(left).localeCompare(archiveObjectIdentity(right)),
    );
    const packetsByObject = await mapBounded(
      orderedObjects,
      ARCHIVE_OBJECT_IO_CONCURRENCY,
      async (object) => {
        const bytes = await this.remote.readArchiveObject({
          key: object.key,
          versionId: object.versionId,
        });
        const decoded: unknown = JSON.parse(new TextDecoder().decode(bytes));
        const candidates = Array.isArray(decoded) ? decoded : [decoded];
        const packets: CaptionPacket[] = [];
        for (const candidate of candidates) {
          const parsed = CaptionPacketSchema.safeParse(candidate);
          if (
            parsed.success &&
            parsed.data.finality === "final" &&
            parsed.data.consultationId === consultationId
          ) {
            packets.push(parsed.data);
          }
        }
        return packets;
      },
    );
    const latest = new Map<string, CaptionPacket>();
    for (const packets of packetsByObject) {
      for (const packet of packets) {
        const key = `${packet.destinationParticipantId}:${packet.utteranceId}`;
        const prior = latest.get(key);
        if (prior === undefined || packet.revision > prior.revision) {
          latest.set(key, packet);
        }
      }
    }
    return [...latest.values()].sort(
      (left, right) =>
        left.destinationParticipantId.localeCompare(right.destinationParticipantId) ||
        left.utteranceId.localeCompare(right.utteranceId),
    );
  }

  private async deleteArchive(effect: Effect): Promise<EffectOutcome> {
    try {
      const writeEpoch = effect.plan.writeEpoch;
      if (typeof writeEpoch !== "number" || !Number.isInteger(writeEpoch) || writeEpoch < 0) {
        throw new Error("archive deletion write epoch is invalid");
      }
      const reason = effect.plan.reason;
      if (typeof reason !== "string" || reason.trim().length === 0) {
        throw new Error("archive deletion reason is invalid");
      }
      const empty = await this.remote.drainArchive(effect.consultationId);
      if (!empty) {
        await this.store.markFailed(
          effect.id,
          this.options.owner,
          "archive deletion has remaining versions or multipart uploads",
          this.clock.now(),
        );
        return "failed";
      }
      const transitioned = await this.remote.notifyDeleteDrain(
        effect.consultationId,
        writeEpoch,
        reason,
      );
      if (transitioned) {
        await this.store.markDone(effect.id, this.options.owner);
        return "done";
      }
      await this.store.markFailed(
        effect.id,
        this.options.owner,
        `archive deletion failed for write epoch ${String(writeEpoch)}`,
        null,
      );
      return "failed";
    } catch (error) {
      const message = error instanceof Error ? error.message : "archive delete failed";
      await this.store.markFailed(
        effect.id,
        this.options.owner,
        message,
        new Date(this.clock.now().getTime() + 1_000),
      );
      return "failed";
    }
  }
}

async function settledBarrier(operations: readonly Promise<unknown>[]): Promise<void> {
  const results = await Promise.allSettled(operations);
  const failure = results.find(
    (result): result is PromiseRejectedResult => result.status === "rejected",
  );
  if (failure) {
    throw failure.reason;
  }
}

async function mapBounded<TInput, TResult>(
  items: readonly TInput[],
  concurrency: number,
  operation: (item: TInput, index: number) => Promise<TResult>,
): Promise<TResult[]> {
  if (items.length === 0) {
    return [];
  }
  const results = new Array<TResult>(items.length);
  let nextIndex = 0;
  let rejected = false;
  let failure: unknown;
  const workers = Array.from(
    { length: Math.min(concurrency, items.length) },
    async (): Promise<void> => {
      while (!rejected) {
        const index = nextIndex;
        if (index >= items.length) {
          return;
        }
        nextIndex += 1;
        try {
          results[index] = await operation(items[index] as TInput, index);
        } catch (error) {
          if (!rejected) {
            rejected = true;
            failure = error;
          }
          return;
        }
      }
    },
  );
  await Promise.all(workers);
  if (rejected) {
    throw failure;
  }
  return results;
}

function archiveObjectIdentity(object: {
  readonly key: string;
  readonly versionId: string;
}): string {
  return `${object.key}\u0000${object.versionId}`;
}

function indexArchiveObjectEvidence(
  objects: readonly ArchivedObject[],
): ArchiveObjectEvidenceIndex {
  const firstObjectIdByClassAndPrefix = new Map<string, string>();
  const prefixes = new Set<string>();
  const providerEvidence = new Set<string>();
  const classAndDestination = new Set<string>();
  const orderedObjects = [...objects].sort((left, right) =>
    `${left.key}\u0000${left.versionId}\u0000${left.id}`.localeCompare(
      `${right.key}\u0000${right.versionId}\u0000${right.id}`,
    ),
  );

  for (const object of orderedObjects) {
    const segments = object.key.split("/");
    let prefix = segments[0] ?? "";
    for (let index = 1; index < segments.length; index += 1) {
      prefixes.add(prefix);
      const classAndPrefix = `${object.objectClass}\u0000${prefix}`;
      if (!firstObjectIdByClassAndPrefix.has(classAndPrefix)) {
        firstObjectIdByClassAndPrefix.set(classAndPrefix, object.id);
      }
      prefix += `/${segments[index]}`;
    }

    for (let index = 1; index < segments.length - 1; index += 1) {
      const segment = segments[index];
      if (segment !== undefined) {
        classAndDestination.add(`${object.objectClass}\u0000${segment}`);
      }
      if (segment === "pipeline" && segments[index + 2] === "raw" && index + 4 < segments.length) {
        providerEvidence.add(`/pipeline/${segments[index + 1]}/raw/${segments[index + 3]}/`);
      }
    }
  }

  return {
    firstObjectIdByClassAndPrefix,
    prefixes,
    providerEvidence,
    classAndDestination,
  };
}

function derivedToArchivedObject(object: DerivedArchiveObject): ArchivedObject {
  return {
    id: object.id,
    objectClass: object.objectClass,
    key: object.key,
    versionId: object.versionId,
    size: object.size,
    sha256: object.sha256,
    s3Checksum: object.checksum,
    contentType: object.contentType,
  };
}

function archiveObjectClass(key: string): string {
  if (key.includes("/media/composite/")) {
    return "room_composite";
  }
  if (key.includes("/media/participants/")) {
    return "participant_original";
  }
  if (key.includes("/pipeline/caption/") || key.includes("/captions/")) {
    return "caption";
  }
  if (key.includes("/audio/") && key.endsWith(".json")) {
    return "pcm_sidecar";
  }
  if (key.includes("/audio/livekit-output/")) {
    return "livekit_output_pcm";
  }
  if (key.includes("/audio/tts-output/")) {
    return "tts_output_pcm";
  }
  if (key.includes("/audio/stt-input/")) {
    return "stt_input_pcm";
  }
  return "pipeline_exchange";
}

function asRecord(value: unknown): Readonly<Record<string, unknown>> {
  return value !== null && typeof value === "object"
    ? (value as Readonly<Record<string, unknown>>)
    : {};
}

function terminalMarker(value: unknown): boolean {
  return (
    value !== null && typeof value === "object" && "terminal" in value && value.terminal === true
  );
}

function encodeVtt(packets: readonly CaptionPacket[]): Uint8Array {
  const cues = packets.map(
    (packet, index) =>
      `${String(index + 1)}\n${vttTimestamp(packet.sourceSampleStart)} --> ${vttTimestamp(packet.sourceSampleEnd)}\n${packet.translatedText.replaceAll(/\s+/g, " ").trim()}\n`,
  );
  return new TextEncoder().encode(`WEBVTT\n\n${cues.join("\n")}`);
}

function vttTimestamp(sample: number): string {
  const milliseconds = Math.floor((sample * 1_000) / 16_000);
  const hours = Math.floor(milliseconds / 3_600_000);
  const minutes = Math.floor((milliseconds % 3_600_000) / 60_000);
  const seconds = Math.floor((milliseconds % 60_000) / 1_000);
  const remainder = milliseconds % 1_000;
  return `${hours.toString().padStart(2, "0")}:${minutes.toString().padStart(2, "0")}:${seconds.toString().padStart(2, "0")}.${remainder.toString().padStart(3, "0")}`;
}
