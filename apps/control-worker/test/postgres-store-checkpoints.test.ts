import { describe, expect, it } from "bun:test";
import { readFile } from "node:fs/promises";
import {
  completeReconciliation,
  reconciliationSnapshot,
} from "../src/adapters/postgres-store/archives";
import {
  capacityDimensions,
  claimStaleReservations,
  completeRoomDrain,
  persistSupervisorTerminalCheckpoints,
} from "../src/adapters/postgres-store/consultations";
import { claimEffects, insertPlannedEffects } from "../src/adapters/postgres-store/effects";
import type { Effect, PlannedEffect, WorkerReservation } from "../src/orchestration/model";

const SOURCE = "00000000-0000-4000-8000-000000000011";
const DESTINATION = "00000000-0000-4000-8000-000000000012";

const RESERVATION: WorkerReservation = {
  consultationId: "00000000-0000-4000-8000-000000000021",
  generation: 3,
  workerId: "00000000-0000-4000-8000-000000000022",
  epoch: 2,
  heartbeatAt: new Date("2026-01-01T00:00:00Z"),
  leaseExpiresAt: new Date("2026-01-01T00:00:30Z"),
  acceptingLoad: true,
};

function fakeTransaction(results: unknown[]) {
  const calls: Array<{ readonly text: string; readonly values: readonly unknown[] }> = [];
  const execute = (query: {
    readonly strings: readonly string[];
    readonly values: readonly unknown[];
  }) => {
    calls.push({ text: query.strings.join("?"), values: query.values });
    return Promise.resolve(results.shift() ?? []);
  };
  const transaction = {
    $queryRaw: execute,
    $executeRaw: execute,
  };
  return { calls, transaction };
}

describe("PostgresStore stale reservation claims", () => {
  it("leaves translation-worker reservations to the drainer while claiming other stale workers", async () => {
    const nonTranslationReservation = {
      consultation_id: RESERVATION.consultationId,
      generation: RESERVATION.generation,
      worker_id: "00000000-0000-4000-8000-000000000023",
      epoch: BigInt(RESERVATION.epoch),
      heartbeat_at: RESERVATION.heartbeatAt,
      lease_expires_at: RESERVATION.leaseExpiresAt,
      accepting_load: true,
    };
    const fake = fakeTransaction([[nonTranslationReservation]]);

    const claimed = await claimStaleReservations(fake.transaction as never, {
      owner: "00000000-0000-4000-8000-000000000024",
      now: new Date("2026-01-01T00:01:00Z"),
      leaseMs: 30_000,
      limit: 10,
    });

    expect(claimed).toEqual([
      {
        consultationId: nonTranslationReservation.consultation_id,
        generation: nonTranslationReservation.generation,
        workerId: nonTranslationReservation.worker_id,
        epoch: RESERVATION.epoch,
        heartbeatAt: RESERVATION.heartbeatAt,
        leaseExpiresAt: RESERVATION.leaseExpiresAt,
        acceptingLoad: true,
      },
    ]);
    expect(fake.calls).toHaveLength(1);
    expect(fake.calls[0]?.text).toContain(
      "JOIN consultations consultation ON consultation.id=reservation.consultation_id",
    );
    expect(fake.calls[0]?.text).toContain(
      "reservation.worker_id IS DISTINCT FROM consultation.worker_identity",
    );
    expect(fake.calls[0]?.text).toContain("UPDATE worker_reservations r SET supervisor_owner");
  });
});

describe("PostgresStore supervisor checkpoint settlement", () => {
  it("persists and verifies one terminal for each frozen direction", async () => {
    const fake = fakeTransaction([
      [
        { source_participant_id: SOURCE, destination_participant_id: DESTINATION },
        { source_participant_id: DESTINATION, destination_participant_id: SOURCE },
      ],
      [],
      [],
      [],
      [
        { id: "00000000-0000-4000-8000-000000000031" },
        { id: "00000000-0000-4000-8000-000000000032" },
      ],
    ]);

    const terminalCheckpointId = await persistSupervisorTerminalCheckpoints(
      fake.transaction as never,
      RESERVATION,
      "heartbeat expired",
    );
    expect(terminalCheckpointId).toBe("00000000-0000-4000-8000-000000000031");

    expect(fake.calls).toHaveLength(5);
    expect(fake.calls[1]?.text).toContain('from "worker_job_epochs"');
    expect(fake.calls[1]?.text).toContain("for update");
    expect(fake.calls[2]?.values).toContain(SOURCE);
    expect(fake.calls[2]?.values).toContain(DESTINATION);
    expect(fake.calls[3]?.values).toContain(DESTINATION);
    expect(fake.calls[3]?.values).toContain(SOURCE);
    expect(fake.calls[2]?.text).toContain(
      "accepted_input_sequence,accepted_input,received_output,emitted_output",
    );
    expect(fake.calls[2]?.text).toContain("COALESCE(previous.accepted_input_sequence+1,0)");
  });

  it("refuses to terminalize without exactly two frozen directions", async () => {
    const fake = fakeTransaction([
      [{ source_participant_id: SOURCE, destination_participant_id: DESTINATION }],
    ]);

    await expect(
      persistSupervisorTerminalCheckpoints(fake.transaction as never, RESERVATION, "cancelled"),
    ).rejects.toThrow("requires two frozen directions");
    expect(fake.calls).toHaveLength(1);
  });
});

describe("PostgresStore room drain settlement", () => {
  it("cannot upsert a null archive-reconciliation deadline on redelivery", async () => {
    const fake = fakeTransaction([[], [], [], [], []]);

    await completeRoomDrain(
      fake.transaction as never,
      RESERVATION.consultationId,
      RESERVATION.generation,
    );

    expect(fake.calls).toHaveLength(5);
    expect(fake.calls[3]?.text).toContain("state IN ('pending','recording')");
    expect(fake.calls[4]?.text).toContain("state='reconciling'");
    expect(fake.calls[4]?.text).toContain("reconciliation_deadline_at IS NOT NULL");
  });
});

describe("PostgresStore capacity release", () => {
  it("allows only a deleted consultation to have its provider selection scrubbed", async () => {
    const deleted = fakeTransaction([[{ state: "deleted", provider_selection: null }]]);
    await expect(
      capacityDimensions(deleted.transaction as never, RESERVATION.consultationId),
    ).resolves.toEqual([]);

    const active = fakeTransaction([[{ state: "active", provider_selection: null }]]);
    await expect(
      capacityDimensions(active.transaction as never, RESERVATION.consultationId),
    ).rejects.toThrow("consultation provider selection is missing");

    const missing = fakeTransaction([[]]);
    await expect(
      capacityDimensions(missing.transaction as never, RESERVATION.consultationId),
    ).rejects.toThrow("consultation provider selection is missing");
  });
});

describe("PostgresStore batched persistence contracts", () => {
  it("resolves authoritative conflict winners before one expected-artifact batch", async () => {
    const firstAuthoritative = "00000000-0000-4000-8000-000000000101";
    const secondAuthoritative = "00000000-0000-4000-8000-000000000102";
    const firstEffect: PlannedEffect = {
      id: "00000000-0000-4000-8000-000000000111",
      consultationId: RESERVATION.consultationId,
      generation: RESERVATION.generation,
      kind: "ROOM_COMPOSITE_EGRESS",
      subjectId: "00000000-0000-4000-8000-000000000121",
      occurrenceKey: "room",
      plan: { outputPrefix: "recordings/room" },
    };
    const effects: readonly PlannedEffect[] = [
      firstEffect,
      { ...firstEffect, id: "00000000-0000-4000-8000-000000000114" },
      {
        id: "00000000-0000-4000-8000-000000000112",
        consultationId: RESERVATION.consultationId,
        generation: RESERVATION.generation,
        kind: "PARTICIPANT_EGRESS",
        subjectId: "00000000-0000-4000-8000-000000000122",
        occurrenceKey: "participant",
        plan: { outputPrefix: "recordings/participant" },
      },
      {
        id: "00000000-0000-4000-8000-000000000113",
        consultationId: RESERVATION.consultationId,
        generation: RESERVATION.generation,
        kind: "ROOM_CREATE",
        subjectId: "00000000-0000-4000-8000-000000000123",
        occurrenceKey: "room-create",
        plan: {},
      },
    ];
    const [primaryEffect, duplicateEffect, secondEffect, roomCreateEffect] = effects;
    if (!primaryEffect || !duplicateEffect || !secondEffect || !roomCreateEffect) {
      throw new Error("planned-effect fixture requires four effects");
    }
    const fake = fakeTransaction([
      [
        {
          id: firstAuthoritative,
          effect_kind: primaryEffect.kind,
          output_prefix: primaryEffect.plan.outputPrefix,
        },
        {
          id: firstAuthoritative,
          effect_kind: duplicateEffect.kind,
          output_prefix: duplicateEffect.plan.outputPrefix,
        },
        {
          id: secondAuthoritative,
          effect_kind: secondEffect.kind,
          output_prefix: secondEffect.plan.outputPrefix,
        },
        {
          id: roomCreateEffect.id,
          effect_kind: roomCreateEffect.kind,
          output_prefix: null,
        },
      ],
      [],
    ]);

    await insertPlannedEffects(fake.transaction as never, effects);

    expect(fake.calls).toHaveLength(2);
    expect(fake.calls[0]?.text).toContain("WITH requested");
    expect(fake.calls[0]?.text.match(/INSERT INTO external_effects/g)).toHaveLength(1);
    expect(fake.calls[0]?.text).toContain("JOIN inserted USING");
    expect(fake.calls[0]?.text).toContain("SELECT DISTINCT ON");
    expect(fake.calls[0]?.text).toContain("ORDER BY requested.ordinal");
    expect(fake.calls[0]?.values.filter((value) => value === 0)).toHaveLength(1);
    expect(fake.calls[0]?.values.filter((value) => value === 1)).toHaveLength(1);
    expect(fake.calls[1]?.text.match(/INSERT INTO expected_archive_artifacts/g)).toHaveLength(1);
    expect(fake.calls[1]?.values).toContain(firstAuthoritative);
    expect(fake.calls[1]?.values).toContain(secondAuthoritative);
    expect(fake.calls[1]?.values.filter((value) => value === firstAuthoritative)).toHaveLength(1);
    expect(fake.calls[1]?.values.filter((value) => value === secondAuthoritative)).toHaveLength(1);
    expect(fake.calls[1]?.values).not.toContain(primaryEffect.id);
    expect(fake.calls[1]?.values).not.toContain(duplicateEffect.id);
    expect(fake.calls[1]?.values).not.toContain(secondEffect.id);
    expect(fake.calls[1]?.text).toContain("JOIN external_effects effect ON effect.id");
  });

  it("bounds large planned-effect inserts into complete batches", async () => {
    const effects: readonly PlannedEffect[] = Array.from({ length: 501 }, (_, index) => ({
      id: `00000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`,
      consultationId: RESERVATION.consultationId,
      generation: RESERVATION.generation,
      kind: "ROOM_CREATE",
      subjectId: `00000000-0000-4000-8001-${String(index + 1).padStart(12, "0")}`,
      occurrenceKey: `room-${index}`,
      plan: {},
    }));
    const finalEffect = effects[500];
    if (!finalEffect) {
      throw new Error("planned-effect batch fixture requires 501 effects");
    }
    const fake = fakeTransaction([
      effects.slice(0, 500).map((effect) => ({
        id: effect.id,
        effect_kind: effect.kind,
        output_prefix: null,
      })),
      [
        {
          id: finalEffect.id,
          effect_kind: finalEffect.kind,
          output_prefix: null,
        },
      ],
    ]);

    await insertPlannedEffects(fake.transaction as never, effects);

    expect(fake.calls).toHaveLength(2);
    expect(fake.calls[0]?.text.match(/INSERT INTO external_effects/g)).toHaveLength(1);
    expect(fake.calls[1]?.text.match(/INSERT INTO external_effects/g)).toHaveLength(1);
    expect(fake.calls[0]?.values).toContain(499);
    expect(fake.calls[1]?.values).toContain(0);
  });

  it("locks and reads reconciling archives with one primary lookup", async () => {
    const archiveId = "00000000-0000-4000-8000-000000000190";
    const fake = fakeTransaction([
      [
        {
          id: archiveId,
          state: "reconciling",
          reconciliation_deadline_at: new Date("2026-01-01T00:05:00Z"),
        },
      ],
      [],
      [],
      [],
      [],
      [],
      [],
      [],
      [],
    ]);

    const result = await reconciliationSnapshot(
      fake.transaction as never,
      RESERVATION.consultationId,
      RESERVATION.generation,
      RESERVATION.generation,
    );

    expect(result?.archiveId).toBe(archiveId);
    expect(fake.calls[0]?.text).toContain("SELECT id,state,reconciliation_deadline_at");
    expect(fake.calls[0]?.text).toContain("FOR UPDATE SKIP LOCKED");
    expect(fake.calls[0]?.text).not.toContain("state='reconciling'");
    expect(fake.calls.filter((call) => call.text.includes("FROM archives"))).toHaveLength(1);
    expect(fake.calls[1]?.text).toContain(
      "JOIN worker_checkpoints checkpoint ON checkpoint.id=epoch.terminal_checkpoint_id",
    );
    expect(fake.calls[4]?.text).toContain(
      "JOIN worker_checkpoints checkpoint ON checkpoint.id=epoch.terminal_checkpoint_id",
    );
    expect(fake.calls[4]?.text).not.toContain("ORDER BY created_at");
  });

  it("returns terminal archives from the primary locked lookup without a fallback query", async () => {
    const terminal = fakeTransaction([
      [
        {
          id: "00000000-0000-4000-8000-000000000191",
          state: "complete",
          reconciliation_deadline_at: null,
        },
      ],
    ]);

    const result = await reconciliationSnapshot(
      terminal.transaction as never,
      RESERVATION.consultationId,
      RESERVATION.generation,
      RESERVATION.generation,
    );

    expect(result).toBeNull();
    expect(terminal.calls).toHaveLength(1);
    expect(terminal.calls[0]?.text).toContain("FOR UPDATE SKIP LOCKED");
  });

  it("uses only a minimal unlocked state lookup after a skipped archive lock", async () => {
    const terminal = fakeTransaction([[], [{ state: "complete" }]]);

    const result = await reconciliationSnapshot(
      terminal.transaction as never,
      RESERVATION.consultationId,
      RESERVATION.generation,
      RESERVATION.generation,
    );

    expect(result).toBeNull();
    expect(terminal.calls).toHaveLength(2);
    expect(terminal.calls[0]?.text).toContain("FOR UPDATE SKIP LOCKED");
    expect(terminal.calls[1]?.text).toContain("SELECT state FROM archives");
    expect(terminal.calls[1]?.text).not.toContain("SELECT id");
    expect(terminal.calls[1]?.text).not.toContain("reconciliation_deadline_at");

    const locked = fakeTransaction([[], [{ state: "reconciling" }]]);
    await expect(
      reconciliationSnapshot(
        locked.transaction as never,
        RESERVATION.consultationId,
        RESERVATION.generation,
        RESERVATION.generation,
      ),
    ).rejects.toThrow("reconciling archive is unavailable");
    expect(locked.calls).toHaveLength(2);
  });

  it("inserts final and derived reconciliation objects in one writer-epoch statement", async () => {
    const effect: Effect = {
      id: "00000000-0000-4000-8000-000000000201",
      consultationId: RESERVATION.consultationId,
      generation: RESERVATION.generation,
      kind: "ARCHIVE_RECONCILE",
      subjectId: "00000000-0000-4000-8000-000000000202",
      occurrenceKey: "reconcile",
      plan: {},
      state: "calling",
      requestBytes: null,
      requestSha256: null,
      remoteId: null,
      attempt: 1,
      leaseOwner: "00000000-0000-4000-8000-000000000203",
      leaseExpiresAt: new Date("2026-01-01T00:10:00Z"),
    };
    const { leaseOwner } = effect;
    if (!leaseOwner) {
      throw new Error("reconciliation fixture requires a lease owner");
    }
    const archiveId = "00000000-0000-4000-8000-000000000204";
    const finalObjectId = "00000000-0000-4000-8000-000000000205";
    const derivedObjectId = "00000000-0000-4000-8000-000000000206";
    const fake = fakeTransaction([
      [{ id: effect.id }],
      [{ generation: effect.generation }],
      [{ id: archiveId }],
      [
        {
          key: `v1/meetings/${effect.consultationId}/inventory/final.json`,
          version_id: "final-version",
        },
        { key: "v1/meetings/transcript/final.vtt", version_id: "derived-version" },
      ],
      [],
      [],
      [{ id: effect.id }],
      [],
    ]);
    const completedAt = new Date("2026-01-01T00:00:00Z");

    const completed = await completeReconciliation(
      fake.transaction as never,
      effect,
      leaseOwner,
      completedAt,
      {
        archiveId,
        reconciliationDeadlineAt: new Date("2026-01-01T00:05:00Z"),
        state: "reconciling",
        roomClose: {},
        workerTerminal: {},
        egressResults: [],
        providerAttempts: [],
        providerGaps: [],
        directions: [],
        expectations: [],
        objects: [],
      },
      {
        status: "complete",
        roomClose: {},
        workerTerminal: {},
        egressResults: [],
        missing: [],
        errors: [],
      },
      "final-sha256",
      { id: finalObjectId, versionId: "final-version", size: 10, checksum: "final-checksum" },
      [
        {
          id: derivedObjectId,
          objectClass: "transcript_vtt",
          key: "v1/meetings/transcript/final.vtt",
          versionId: "derived-version",
          size: 20,
          sha256: "derived-sha256",
          checksum: "derived-checksum",
          contentType: "text/vtt",
        },
      ],
    );

    expect(completed).toBe(true);
    const objectInsert = fake.calls[3];
    expect(objectInsert?.text.match(/INSERT INTO archive_objects/g)).toHaveLength(1);
    expect(objectInsert?.text).toContain("VALUES");
    expect(objectInsert?.text).toContain("requested.content_type,archive.write_epoch");
    expect(objectInsert?.text).toContain(
      "ON CONFLICT (key,version_id) DO UPDATE SET archive_id=archive_objects.archive_id",
    );
    expect(objectInsert?.text).toContain("archive_objects.archive_id=EXCLUDED.archive_id");
    expect(objectInsert?.text).toContain(
      "RETURNING archive_objects.key,archive_objects.version_id",
    );
    expect(objectInsert?.text).not.toContain("DO NOTHING");
    expect(objectInsert?.values).toContain(finalObjectId);
    expect(objectInsert?.values).toContain(derivedObjectId);
    const completion = fake.calls[6];
    expect(completion?.text).toContain("WITH eligible_effect AS");
    expect(completion?.text.match(/lease_expires_at>\?/g)).toHaveLength(2);
    expect(completion?.text.match(/lease_owner=\?/g)).toHaveLength(2);
    expect(completion?.values.filter((value) => value === completedAt)).toHaveLength(4);
    expect(completion?.text).toContain("consultation.generation=?");
    expect(completion?.text).toContain("archive.state='reconciling'");
  });

  it("chunks reconciliation object inserts and persists the final object exactly once", async () => {
    const effect: Effect = {
      id: "00000000-0000-4000-8000-000000000211",
      consultationId: RESERVATION.consultationId,
      generation: RESERVATION.generation,
      kind: "ARCHIVE_RECONCILE",
      subjectId: "00000000-0000-4000-8000-000000000212",
      occurrenceKey: "reconcile-large",
      plan: {},
      state: "calling",
      requestBytes: null,
      requestSha256: null,
      remoteId: null,
      attempt: 1,
      leaseOwner: "00000000-0000-4000-8000-000000000213",
      leaseExpiresAt: new Date("2026-01-01T00:10:00Z"),
    };
    const archiveId = "00000000-0000-4000-8000-000000000214";
    const finalObjectId = "00000000-0000-4000-8000-000000000215";
    const derivedObjects = Array.from({ length: 1_000 }, (_, index) => ({
      id: `00000000-0000-4001-${String(index + 1).padStart(12, "0")}`,
      objectClass: "caption_packet",
      key: `v1/meetings/${effect.consultationId}/captions/${String(index)}.json`,
      versionId: `version-${String(index)}`,
      size: index + 1,
      sha256: `sha-${String(index)}`,
      checksum: `checksum-${String(index)}`,
      contentType: "application/json",
    }));
    const firstBatchRows = [
      {
        key: `v1/meetings/${effect.consultationId}/inventory/final.json`,
        version_id: "final-version",
      },
      ...derivedObjects.slice(0, 999).map((object) => ({
        key: object.key,
        version_id: object.versionId,
      })),
    ];
    const finalDerived = derivedObjects[999];
    if (!finalDerived || !effect.leaseOwner) {
      throw new Error("large reconciliation fixture is incomplete");
    }
    const fake = fakeTransaction([
      [{ id: effect.id }],
      [{ generation: effect.generation }],
      [{ id: archiveId }],
      firstBatchRows,
      [{ key: finalDerived.key, version_id: finalDerived.versionId }],
      [],
      [],
      [{ id: effect.id }],
      [],
    ]);

    const completed = await completeReconciliation(
      fake.transaction as never,
      effect,
      effect.leaseOwner,
      new Date("2026-01-01T00:00:00Z"),
      {
        archiveId,
        reconciliationDeadlineAt: new Date("2026-01-01T00:05:00Z"),
        state: "reconciling",
        roomClose: {},
        workerTerminal: {},
        egressResults: [],
        providerAttempts: [],
        providerGaps: [],
        directions: [],
        expectations: [],
        objects: [],
      },
      {
        status: "complete",
        roomClose: {},
        workerTerminal: {},
        egressResults: [],
        missing: [],
        errors: [],
      },
      "final-sha256",
      { id: finalObjectId, versionId: "final-version", size: 10, checksum: "final-checksum" },
      derivedObjects,
    );

    expect(completed).toBe(true);
    const objectInserts = fake.calls.filter((call) =>
      call.text.includes("INSERT INTO archive_objects"),
    );
    expect(objectInserts).toHaveLength(2);
    expect(objectInserts[0]?.values).toContain(finalObjectId);
    expect(objectInserts[1]?.values).not.toContain(finalObjectId);
    expect(
      objectInserts.flatMap((call) => call.values).filter((value) => value === finalObjectId),
    ).toHaveLength(1);
    expect(objectInserts.every((call) => call.values.length < 65_535)).toBe(true);
  });

  it("rejects cross-archive reconciliation object identity conflicts", async () => {
    const effect: Effect = {
      id: "00000000-0000-4000-8000-000000000221",
      consultationId: RESERVATION.consultationId,
      generation: RESERVATION.generation,
      kind: "ARCHIVE_RECONCILE",
      subjectId: "00000000-0000-4000-8000-000000000222",
      occurrenceKey: "reconcile-conflict",
      plan: {},
      state: "calling",
      requestBytes: null,
      requestSha256: null,
      remoteId: null,
      attempt: 1,
      leaseOwner: "00000000-0000-4000-8000-000000000223",
      leaseExpiresAt: new Date("2026-01-01T00:10:00Z"),
    };
    if (!effect.leaseOwner) {
      throw new Error("conflict reconciliation fixture requires a lease owner");
    }
    const fake = fakeTransaction([
      [{ id: effect.id }],
      [{ generation: effect.generation }],
      [{ id: "00000000-0000-4000-8000-000000000224" }],
      [],
    ]);

    await expect(
      completeReconciliation(
        fake.transaction as never,
        effect,
        effect.leaseOwner,
        new Date("2026-01-01T00:00:00Z"),
        {
          archiveId: "00000000-0000-4000-8000-000000000224",
          reconciliationDeadlineAt: new Date("2026-01-01T00:05:00Z"),
          state: "reconciling",
          roomClose: {},
          workerTerminal: {},
          egressResults: [],
          providerAttempts: [],
          providerGaps: [],
          directions: [],
          expectations: [],
          objects: [],
        },
        {
          status: "complete",
          roomClose: {},
          workerTerminal: {},
          egressResults: [],
          missing: [],
          errors: [],
        },
        "final-sha256",
        {
          id: "00000000-0000-4000-8000-000000000225",
          versionId: "conflicting-version",
          size: 10,
          checksum: "final-checksum",
        },
        [],
      ),
    ).rejects.toThrow("archive object identity conflicts with another archive");
    expect(fake.calls).toHaveLength(4);
  });

  it("throws at the transaction seam when the final lease fence rejects completion", async () => {
    const effect: Effect = {
      id: "00000000-0000-4000-8000-000000000231",
      consultationId: RESERVATION.consultationId,
      generation: RESERVATION.generation,
      kind: "ARCHIVE_RECONCILE",
      subjectId: "00000000-0000-4000-8000-000000000232",
      occurrenceKey: "reconcile-expired",
      plan: {},
      state: "calling",
      requestBytes: null,
      requestSha256: null,
      remoteId: null,
      attempt: 1,
      leaseOwner: "00000000-0000-4000-8000-000000000233",
      leaseExpiresAt: new Date("2026-01-01T00:10:00Z"),
    };
    if (!effect.leaseOwner) {
      throw new Error("lease-fence fixture requires a lease owner");
    }
    const archiveId = "00000000-0000-4000-8000-000000000234";
    const fake = fakeTransaction([
      [{ id: effect.id }],
      [{ generation: effect.generation }],
      [{ id: archiveId }],
      [
        {
          key: `v1/meetings/${effect.consultationId}/inventory/final.json`,
          version_id: "final-version",
        },
      ],
      [],
      [],
      [],
    ]);

    await expect(
      completeReconciliation(
        fake.transaction as never,
        effect,
        effect.leaseOwner,
        new Date("2026-01-01T00:00:00Z"),
        {
          archiveId,
          reconciliationDeadlineAt: new Date("2026-01-01T00:05:00Z"),
          state: "reconciling",
          roomClose: {},
          workerTerminal: {},
          egressResults: [],
          providerAttempts: [],
          providerGaps: [],
          directions: [],
          expectations: [],
          objects: [],
        },
        {
          status: "complete",
          roomClose: {},
          workerTerminal: {},
          egressResults: [],
          missing: [],
          errors: [],
        },
        "final-sha256",
        {
          id: "00000000-0000-4000-8000-000000000235",
          versionId: "final-version",
          size: 10,
          checksum: "final-checksum",
        },
        [],
      ),
    ).rejects.toThrow("reconciliation completion fence rejected");
    expect(fake.calls).toHaveLength(7);
    expect(fake.calls[6]?.text).toContain("effect.lease_expires_at>?");
  });

  it("returns false after an owned transaction rolls back a rejected final fence", async () => {
    const effect: Effect = {
      id: "00000000-0000-4000-8000-000000000241",
      consultationId: RESERVATION.consultationId,
      generation: RESERVATION.generation,
      kind: "ARCHIVE_RECONCILE",
      subjectId: "00000000-0000-4000-8000-000000000242",
      occurrenceKey: "reconcile-rollback",
      plan: {},
      state: "calling",
      requestBytes: null,
      requestSha256: null,
      remoteId: null,
      attempt: 1,
      leaseOwner: "00000000-0000-4000-8000-000000000243",
      leaseExpiresAt: new Date("2026-01-01T00:10:00Z"),
    };
    if (!effect.leaseOwner) {
      throw new Error("rollback reconciliation fixture requires a lease owner");
    }
    const archiveId = "00000000-0000-4000-8000-000000000244";
    const fake = fakeTransaction([
      [{ id: effect.id }],
      [{ generation: effect.generation }],
      [{ id: archiveId }],
      [
        {
          key: `v1/meetings/${effect.consultationId}/inventory/final.json`,
          version_id: "final-version",
        },
      ],
      [],
      [],
      [],
    ]);
    let rolledBack = false;
    const client = {
      $transaction: async (
        operation: (transaction: typeof fake.transaction) => Promise<boolean>,
      ) => {
        try {
          return await operation(fake.transaction);
        } catch (error) {
          rolledBack = true;
          throw error;
        }
      },
    };

    const completed = await completeReconciliation(
      client as never,
      effect,
      effect.leaseOwner,
      new Date("2026-01-01T00:00:00Z"),
      {
        archiveId,
        reconciliationDeadlineAt: new Date("2026-01-01T00:05:00Z"),
        state: "reconciling",
        roomClose: {},
        workerTerminal: {},
        egressResults: [],
        providerAttempts: [],
        providerGaps: [],
        directions: [],
        expectations: [],
        objects: [],
      },
      {
        status: "complete",
        roomClose: {},
        workerTerminal: {},
        egressResults: [],
        missing: [],
        errors: [],
      },
      "final-sha256",
      {
        id: "00000000-0000-4000-8000-000000000245",
        versionId: "final-version",
        size: 10,
        checksum: "final-checksum",
      },
      [],
    );

    expect(completed).toBe(false);
    expect(rolledBack).toBe(true);
  });

  it("ages every active effect claim before applying recovery and STATUS_PACKET tie-breaks", async () => {
    const fake = fakeTransaction([[]]);

    await claimEffects(fake.transaction as never, {
      owner: "00000000-0000-4000-8000-000000000301",
      now: new Date("2026-01-01T00:00:00Z"),
      leaseMs: 30_000,
      limit: 10,
    });

    expect(fake.calls).toHaveLength(1);
    expect(fake.calls[0]?.text).toContain(
      "ORDER BY COALESCE(candidate.lease_expires_at,candidate.created_at),",
    );
    expect(fake.calls[0]?.text).toContain(
      "CASE candidate.state WHEN 'planned'::external_effect_state THEN 1 ELSE 0 END,",
    );
    expect(fake.calls[0]?.text).toContain(
      "CASE candidate.effect_kind WHEN 'STATUS_PACKET'::text THEN 0 ELSE 1 END,candidate.created_at,candidate.id",
    );
    expect(fake.calls[0]?.text).not.toContain("candidate.claim_priority");
    expect(fake.calls[0]?.text).toContain(
      "candidate.state IN ('planned','calling','applied','compensating')",
    );
  });

  it("keeps the released claim index immutable and replaces it additively", async () => {
    const [schema, releasedMigration, replacementMigration] = await Promise.all([
      readFile(`${import.meta.dir}/../../../packages/server-core/prisma/schema.prisma`, "utf8"),
      readFile(
        `${import.meta.dir}/../../../packages/server-core/prisma/migrations/` +
          "20260723070000_performance_indexes/migration.sql",
        "utf8",
      ),
      readFile(
        `${import.meta.dir}/../../../packages/server-core/prisma/migrations/` +
          "20260723120000_reorder_active_effect_claims/migration.sql",
        "utf8",
      ),
    ]);
    expect(schema).toContain(
      "external_effects_active_claim_priority_lease_created_id_idx ages eligible work by first/current eligibility, then prefers recovery and STATUS_PACKET work within equal-age ties",
    );
    expect(schema).not.toContain("claimPriority");
    expect(schema).not.toContain('map("claim_priority")');
    expect(new Bun.CryptoHasher("sha256").update(releasedMigration).digest("hex")).toBe(
      "a4cc7cf07091c747690246b14b88c9c14e419bbc61701ab224abca4c9155672f",
    );
    expect(replacementMigration).toContain(
      'DROP INDEX "external_effects_active_claim_priority_lease_created_id_idx"',
    );
    expect(replacementMigration).toContain(
      'CREATE INDEX "external_effects_active_claim_priority_lease_created_id_idx"',
    );
    expect(replacementMigration).toContain(
      `ON "external_effects" (COALESCE("lease_expires_at", "created_at"), (CASE "state" WHEN 'planned'::external_effect_state THEN 1 ELSE 0 END), (CASE "effect_kind" WHEN 'STATUS_PACKET'::text THEN 0 ELSE 1 END), "created_at", "id")`,
    );
    expect(replacementMigration).toContain("'planned'::external_effect_state");
    expect(replacementMigration).toContain("'compensating'::external_effect_state");
  });
});
