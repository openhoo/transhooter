import { chmod, rm, stat } from "node:fs/promises";
import { healthFailureAfter } from "../../harness-contracts.mjs";

export async function runStorageScenarios(ctx, proof) {
  const {
    faultFile,
    onlyContainer,
    inspect,
    stop,
    start,
    waitFor,
    shouldRunScenario,
    checkpointScenario,
    setFaults,
    runConsultation,
    assertServiceHealthy,
    resetAuthenticationThrottle,
    consultationEvidence,
    consultationStatus,
    effectEvidence,
    settlementObservation,
    isCleanSettlement,
    settlementSummary,
    queryJson,
    fetchWithDeadline,
    serviceBaseUrl,
  } = ctx;

  if (shouldRunScenario("minio-inflight-recovery")) {
    await resetAuthenticationThrottle();
    const minioBefore = await onlyContainer("minio");
    const minioStateBefore = await inspect(minioBefore.Id);
    const run = await runConsultation({
      faults: { holdAfterPersistCalling: ["ARCHIVE_RECONCILE"] },
    });
    const reconciliationHoldMarker = `${faultFile}.${run.consultationId}.ARCHIVE_RECONCILE.calling-owner`;
    await waitFor(
      "archive reconciliation held before its remote call",
      async () => {
        const status = await consultationStatus(run.consultationId);
        if (status.archive_state !== "reconciling") return null;
        try {
          await stat(reconciliationHoldMarker);
          return status;
        } catch {
          return null;
        }
      },
      120_000,
    );
    const reconciliationEffects = await effectEvidence(run.consultationId, "ARCHIVE_RECONCILE");
    const reconciliationAttemptsBeforeOutage = new Map(
      reconciliationEffects.map((effect) => [effect.id, effect.attempts]),
    );
    const minioId = await stop("minio");
    await setFaults(run.consultationId);
    await rm(reconciliationHoldMarker, { force: true });
    await waitFor(
      "web readiness to fail closed during in-flight archive outage",
      async (signal, deadline) => {
        try {
          const response = await fetchWithDeadline(
            `${serviceBaseUrl}/api/health/ready`,
            {},
            deadline,
            signal,
          );
          return !response.ok;
        } catch {
          return true;
        }
      },
      60_000,
    );
    const outageEvidence = await consultationStatus(run.consultationId);
    if (outageEvidence.archive_state !== "reconciling") {
      throw new Error("archive completed before the MinIO outage could exercise reconciliation");
    }
    const failedReconciliation = await waitFor(
      "archive reconciliation retries after the MinIO outage",
      async () => {
        const effects = await effectEvidence(run.consultationId, "ARCHIVE_RECONCILE");
        return (
          effects.find(
            (effect) => effect.attempts > (reconciliationAttemptsBeforeOutage.get(effect.id) ?? 0),
          ) ?? null
        );
      },
      120_000,
    );
    await start(minioId);
    await assertServiceHealthy("minio");
    const minioStateAfter = await inspect(minioId);
    if (
      minioStateAfter.State.StartedAt === minioStateBefore.State.StartedAt ||
      !minioStateAfter.State.Running
    ) {
      throw new Error("MinIO did not restart on the same Compose volume");
    }
    await waitFor(
      "web readiness after MinIO recovery",
      async (signal, deadline) =>
        (await fetchWithDeadline(`${serviceBaseUrl}/api/health/ready`, {}, deadline, signal)).ok,
      120_000,
    );
    const completed = await run.completed;
    if (completed.code !== 0) {
      throw new Error(
        `MinIO recovery consultation failed: ${completed.stderr}\n${completed.stdout}`,
      );
    }
    await waitFor(
      "MinIO recovery clean settlement",
      async () => {
        const observation = await settlementObservation(run.consultationId);
        return isCleanSettlement(observation) ? observation : null;
      },
      120_000,
    );
    const settled = await consultationEvidence(run.consultationId);
    const [objects, pendingMultipart] = await Promise.all([
      queryJson(`
          SELECT count(*)::int AS object_count,
            bool_and(version_id <> '' AND sha256 ~ '^[0-9a-f]{64}$' AND s3_checksum <> '')
              AS version_hash_complete
          FROM archive_objects WHERE archive_id='${settled.archive_id}'`),
      queryJson(`
          SELECT count(*)::int AS pending
          FROM multipart_uploads
          WHERE archive_id='${settled.archive_id}' AND state IN ('open','completing')`),
    ]);
    if (
      settled.archive_state !== "complete" ||
      !/^[0-9a-f]{64}$/u.test(settled.final_inventory_hash ?? "") ||
      objects[0]?.object_count < 1 ||
      objects[0]?.version_hash_complete !== true ||
      pendingMultipart[0]?.pending !== 0 ||
      settled.active_effects !== 0
    ) {
      throw new Error(
        `MinIO recovery lacks complete versioned inventory settlement: ${JSON.stringify({
          settlement: settlementSummary(settled),
          objects,
          pendingMultipart,
        })}`,
      );
    }
    const recoveredReconciliation = settled.effects.find(
      (effect) =>
        effect.id === failedReconciliation.id &&
        effect.kind === "ARCHIVE_RECONCILE" &&
        effect.state === "done" &&
        effect.attempts >= failedReconciliation.attempts,
    );
    if (!recoveredReconciliation) {
      throw new Error("MinIO-dependent reconciliation did not retry to completion after recovery");
    }
    proof.scenarios.push({
      name: "minio-inflight-recovery",
      consultationId: run.consultationId,
      consultationExitCode: completed.code,
      reconciliationEffectId: failedReconciliation.id,
      outageAttempts: failedReconciliation.attempts,
      recoveredAttempts: recoveredReconciliation.attempts,
      minioFailureObserved: true,
      sameVolumeRestart: true,
      inventorySha256: settled.final_inventory_hash,
      objectCount: objects[0].object_count,
      versionHashComplete: true,
      pendingMultipart: 0,
      pendingEffects: settled.active_effects,
      cleanSettlement: true,
    });
    await checkpointScenario("minio-inflight-recovery");
  }

  if (shouldRunScenario("unwritable-spool")) {
    // An unwritable encrypted spool must make both worker roles unavailable. Restore
    // permissions and require semantic health before the harness exits.
    const healthWatermarkMs = Date.now();
    const workerId = await stop("translation-worker");
    const drainerId = await stop("spool-drainer");
    await chmod("/var/lib/transhooter/spool", 0o000);
    await start(workerId);
    await start(drainerId);
    const unavailable = await waitFor(
      "worker and drainer unavailable on unwritable spool",
      async (signal, deadline) => {
        const [workerState, drainerState] = await Promise.all([
          inspect(workerId, { signal, deadline }).then((value) => value.State),
          inspect(drainerId, { signal, deadline }).then((value) => value.State),
        ]);
        const workerUnavailable =
          !workerState.Running ||
          workerState.Health?.Status === "unhealthy" ||
          workerState.Health?.Log?.some((entry) => healthFailureAfter(entry, healthWatermarkMs)) ===
            true;
        const drainerUnavailable =
          !drainerState.Running ||
          drainerState.Health?.Status === "unhealthy" ||
          drainerState.Health?.Log?.some((entry) =>
            healthFailureAfter(entry, healthWatermarkMs),
          ) === true;
        return workerUnavailable && drainerUnavailable
          ? { workerUnavailable, drainerUnavailable, healthWatermarkMs }
          : null;
      },
      90_000,
    );
    await stop("translation-worker");
    await stop("spool-drainer");
    await chmod("/var/lib/transhooter/spool", 0o700);
    await start(workerId);
    await start(drainerId);
    await assertServiceHealthy("translation-worker");
    await assertServiceHealthy("spool-drainer");
    proof.scenarios.push({ name: "unwritable-spool", ...unavailable, recovered: true });
    await checkpointScenario("unwritable-spool");
  }
}
