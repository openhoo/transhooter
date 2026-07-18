import { createHash, randomUUID } from "node:crypto";
import { type CaptionPacket, CaptionPacketSchema } from "@transhooter/contracts";
import { deterministicUuid } from "@transhooter/server-core/rooms";
import { type EffectFaultControl, noEffectFaults } from "../runtime/fault-control";
import type {
  ArchivedObject,
  Clock,
  DerivedArchiveObject,
  DurableStore,
  Effect,
  ReconciliationSnapshot,
  Uuid,
} from "./model";
import { canonicalRequest } from "./model";
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

interface CanonicalEffectRequest {
  readonly bytes: Uint8Array;
  readonly sha256: string;
}

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
    await Promise.all(effects.map(async (effect) => this.run(effect)));
    return effects.length;
  }

  private async run(claimed: Effect): Promise<void> {
    if (await this.compensateIfRequired(claimed)) {
      return;
    }

    const canonical = this.canonicalEffectRequest(claimed);
    if (claimed.requestSha256 !== null && claimed.requestSha256 !== canonical.sha256) {
      await this.store.markFailed(
        claimed.id,
        this.options.owner,
        "immutable request hash mismatch",
        null,
      );
      return;
    }

    const calling = await this.store.persistCalling(
      claimed.id,
      this.options.owner,
      canonical.bytes,
      canonical.sha256,
    );
    if (calling === null) {
      return;
    }
    await this.faults.afterPersist(calling.kind, calling.consultationId);

    const renewal = this.startLeaseRenewal(calling);
    try {
      if (calling.kind === "ARCHIVE_RECONCILE") {
        await this.reconcileArchive(calling);
        return;
      }
      if (calling.kind === "ARCHIVE_DELETE") {
        await this.deleteArchive(calling);
        return;
      }
      await this.executeRemoteEffect(calling);
    } finally {
      clearInterval(renewal);
    }
  }

  private async compensateIfRequired(claimed: Effect): Promise<boolean> {
    if (claimed.state === "compensating") {
      await this.remote.compensate(claimed);
      await this.store.markDone(claimed.id, this.options.owner);
      return true;
    }
    const generation = await this.store.currentGeneration(claimed.consultationId);
    if (generation === claimed.generation) {
      return false;
    }
    await this.store.markCompensating(claimed.id, this.options.owner, "generation fenced");
    await this.remote.compensate(claimed);
    await this.store.markDone(claimed.id, this.options.owner);
    return true;
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

  private startLeaseRenewal(effect: Effect): NodeJS.Timeout {
    const renewal = setInterval(
      () => {
        const leaseExpiresAt = new Date(this.clock.now().getTime() + this.options.leaseMs);
        void this.store
          .renewEffectLease(effect.id, this.options.owner, leaseExpiresAt)
          .catch(() => undefined);
      },
      Math.max(100, Math.floor(this.options.leaseMs / 3)),
    );
    renewal.unref();
    return renewal;
  }

  private async executeRemoteEffect(effect: Effect): Promise<void> {
    try {
      await this.faults.shouldFail(effect.kind, effect.consultationId);
      const adopted = await this.remote.adopt(effect, effect.plan);
      if (adopted !== null) {
        await this.applyAdoption(effect, adopted);
        return;
      }
      const result = await this.remote.execute(effect, effect.plan);
      await this.store.markApplied(effect.id, this.options.owner, result.remoteId, result.result);
      await this.store.markDone(effect.id, this.options.owner);
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown remote effect failure";
      const delay = Math.min(60_000, 250 * 2 ** Math.min(effect.attempt, 8));
      await this.store.markFailed(
        effect.id,
        this.options.owner,
        message,
        new Date(this.clock.now().getTime() + delay),
      );
    }
  }

  private async applyAdoption(effect: Effect, adoption: Adoption): Promise<void> {
    if (!adoption.matchesRequest) {
      await this.store.markCompensating(
        effect.id,
        this.options.owner,
        "deterministic identity collision",
      );
      await this.remote.compensate(effect);
      await this.store.markFailed(
        effect.id,
        this.options.owner,
        "deterministic identity collision",
        null,
      );
      return;
    }
    await this.store.markApplied(
      effect.id,
      this.options.owner,
      adoption.remoteId,
      adoption.result ?? { adopted: true, terminal: adoption.terminal },
    );
    await this.store.markDone(effect.id, this.options.owner);
  }

  private async reconcileArchive(effect: Effect): Promise<void> {
    try {
      const snapshot = await this.store.reconciliationSnapshot(
        effect.consultationId,
        effect.generation,
        typeof effect.plan.resourceGeneration === "number"
          ? effect.plan.resourceGeneration
          : effect.generation,
      );
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
        );
      const persistedObjects = [
        ...snapshot.objects,
        ...discoveredObjects.map(derivedToArchivedObject),
      ];
      const invalidObjectIds = await this.verifyObjects(persistedObjects);
      const missing = this.missingInventoryEntries(snapshot, invalidObjectIds, persistedObjects);
      if (missing.length > 0 && this.clock.now() < snapshot.reconciliationDeadlineAt) {
        throw new Error("archive evidence is still pending");
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
      const derivedObjects = [...discoveredObjects, ...vttObjects];
      const inventory = {
        status: missing.length === 0 ? "complete" : "incomplete",
        roomClose: snapshot.roomClose,
        workerTerminal: snapshot.workerTerminal,
        egressResults: snapshot.egressResults,
        objects: [...persistedObjects, ...vttObjects.map(derivedToArchivedObject)],
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
      await this.uploadAndCompleteInventory(effect, snapshot, inventory, derivedObjects);
      await this.store.markDone(effect.id, this.options.owner);
    } catch (error) {
      const message = error instanceof Error ? error.message : "archive reconciliation failed";
      await this.store.markFailed(
        effect.id,
        this.options.owner,
        message,
        new Date(this.clock.now().getTime() + 1_000),
      );
    }
  }

  private async verifyObjects(objects: ReconciliationSnapshot["objects"]): Promise<Set<string>> {
    const invalidObjectIds = new Set<string>();
    await Promise.all(
      objects.map(async (object) => {
        const valid = await this.remote.verifyArchiveObject({
          key: object.key,
          versionId: object.versionId,
          size: object.size,
          checksum: object.s3Checksum,
        });
        if (!valid) {
          invalidObjectIds.add(object.id);
        }
      }),
    );
    return invalidObjectIds;
  }

  private missingInventoryEntries(
    snapshot: ReconciliationSnapshot,
    invalidObjectIds: ReadonlySet<string>,
    objects: readonly ArchivedObject[],
  ): MissingInventoryEntry[] {
    const missing: MissingInventoryEntry[] = snapshot.expectations
      .filter(
        (expected) =>
          expected.fulfilledObjectId === null || invalidObjectIds.has(expected.fulfilledObjectId),
      )
      .map((expected) => ({
        expectationId: expected.id,
        class: expected.objectClass,
        causalKey: expected.causalKey,
        sampleStart: expected.sampleStart,
        sampleEnd: expected.sampleEnd,
        reason: expected.fulfilledObjectId === null ? "unfulfilled" : "object_verification_failed",
      }));
    const expectedObjectIds = new Set(
      snapshot.expectations.flatMap((expected) =>
        expected.fulfilledObjectId === null ? [] : [expected.fulfilledObjectId],
      ),
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
      if (
        typeof outputPrefix !== "string" ||
        !objects.some((object) => object.key.startsWith(`${outputPrefix}/`))
      ) {
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
    const completed = await this.store.completeReconciliation(
      effect.consultationId,
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
    for (const packet of latestPackets.values()) {
      const destinationPackets = packetsByDestination.get(packet.destinationParticipantId) ?? [];
      destinationPackets.push(packet);
      packetsByDestination.set(packet.destinationParticipantId, destinationPackets);
    }

    const derived: DerivedArchiveObject[] = [];
    for (const [destination, packets] of packetsByDestination) {
      packets.sort(
        (left, right) =>
          left.sourceSampleStart - right.sourceSampleStart ||
          left.utteranceId.localeCompare(right.utteranceId),
      );
      const body = encodeVtt(packets);
      const sha256 = createHash("sha256").update(body).digest("hex");
      const key = `v1/meetings/${consultationId}/captions/${destination}/final.vtt`;
      const uploaded = await this.remote.putArchiveObject({
        key,
        body,
        contentType: "text/vtt; charset=utf-8",
        sha256,
      });
      derived.push({
        id: deterministicUuid(consultationId, `vtt:${destination}`),
        objectClass: "caption_vtt",
        key,
        versionId: uploaded.versionId,
        size: uploaded.size,
        sha256,
        checksum: uploaded.checksum,
        contentType: "text/vtt; charset=utf-8",
      });
    }
    return derived;
  }

  private async loadLatestFinalCaptions(
    consultationId: Uuid,
    captionObjects: readonly ArchivedObject[],
  ): Promise<Map<string, CaptionPacket>> {
    const latest = new Map<string, CaptionPacket>();
    for (const object of captionObjects) {
      const bytes = await this.remote.readArchiveObject({
        key: object.key,
        versionId: object.versionId,
      });
      const decoded: unknown = JSON.parse(new TextDecoder().decode(bytes));
      const candidates = Array.isArray(decoded) ? decoded : [decoded];
      for (const candidate of candidates) {
        const parsed = CaptionPacketSchema.safeParse(candidate);
        if (
          !parsed.success ||
          parsed.data.finality !== "final" ||
          parsed.data.consultationId !== consultationId
        ) {
          continue;
        }
        const key = `${parsed.data.destinationParticipantId}:${parsed.data.utteranceId}`;
        const prior = latest.get(key);
        if (prior === undefined || parsed.data.revision > prior.revision) {
          latest.set(key, parsed.data);
        }
      }
    }
    return latest;
  }

  private async deleteArchive(effect: Effect): Promise<void> {
    try {
      const empty = await this.remote.drainArchive(effect.consultationId);
      if (!empty) {
        await this.store.markFailed(
          effect.id,
          this.options.owner,
          "archive deletion has remaining versions or multipart uploads",
          this.clock.now(),
        );
        return;
      }
      const writeEpoch = effect.plan.writeEpoch;
      if (typeof writeEpoch !== "number" || !Number.isInteger(writeEpoch) || writeEpoch < 0) {
        throw new Error("archive deletion write epoch is invalid");
      }
      const transitioned = await this.remote.notifyDeleteDrain(effect.consultationId, writeEpoch);
      if (transitioned) {
        await this.store.markDone(effect.id, this.options.owner);
      } else {
        await this.store.markFailed(
          effect.id,
          this.options.owner,
          `archive deletion failed for write epoch ${String(writeEpoch)}`,
          null,
        );
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "archive delete failed";
      await this.store.markFailed(
        effect.id,
        this.options.owner,
        message,
        new Date(this.clock.now().getTime() + 1_000),
      );
    }
  }
}

function archiveObjectIdentity(object: {
  readonly key: string;
  readonly versionId: string;
}): string {
  return `${object.key}\u0000${object.versionId}`;
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
