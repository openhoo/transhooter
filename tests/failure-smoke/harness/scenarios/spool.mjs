import { randomUUID } from "node:crypto";
import { restartCountIncremented, workerCrashMatches } from "../../harness-contracts.mjs";

export async function runSpoolScenarios(ctx, proof) {
  const {
    onlyContainer,
    inspect,
    stop,
    start,
    waitFor,
    shouldRunScenario,
    checkpointScenario,
    setWorkerScenario,
    runConsultation,
    assertServiceHealthy,
    resetAuthenticationThrottle,
    consultationStatus,
    workerSupervisionEvidence,
    settlementSummary,
    settleConsultation,
    queryJson,
    sql,
  } = ctx;
  const preservationFenceSelected = shouldRunScenario("preservation-fence");
  const spoolRecoverySelected = shouldRunScenario("spool-durable-recovery");
  if (!preservationFenceSelected && !spoolRecoverySelected) return;

  const workerBefore = await onlyContainer("translation-worker");
  const drainerBefore = await onlyContainer("spool-drainer");
  const [workerBeforeState, drainerBeforeState] = await Promise.all([
    inspect(workerBefore.Id),
    inspect(drainerBefore.Id),
  ]);
  const workerRestartBaseline = workerBeforeState.RestartCount;
  if (spoolRecoverySelected) await stop("spool-drainer");
  await resetAuthenticationThrottle();
  const preservationRun = await runConsultation({
    workerScenario: {
      spool: { walFailAfterAppends: 1, sqliteFailAfterAppends: 1 },
      failureReport: { deny: true },
    },
  });
  const preservationConsultationId = preservationRun.consultationId;
  const reservation = await waitFor(
    "worker reservation before preservation failure",
    async () => {
      const evidence = await workerSupervisionEvidence(preservationConsultationId);
      const candidate = evidence.reservations[0];
      return typeof candidate?.workerId === "string" &&
        candidate.workerId.length > 0 &&
        Number.isInteger(Number(candidate.generation)) &&
        Number.isInteger(Number(candidate.epoch))
        ? candidate
        : null;
    },
    60_000,
  );
  const workerRestart = waitFor(
    "exact worker container restart after preservation failure",
    async (signal, deadline) => {
      const restartCount = (await inspect(workerBefore.Id, { signal, deadline })).RestartCount;
      return restartCountIncremented(workerRestartBaseline, restartCount)
        ? { containerId: workerBefore.Id, restartCount }
        : null;
    },
    120_000,
  );
  const preservation = await preservationRun.completed;
  if (preservation.code === 0)
    throw new Error("WAL/SQLite fault unexpectedly produced a complete archive");
  await workerRestart;
  const beforeFence = await consultationStatus(preservationConsultationId);
  if (
    beforeFence.generation !== reservation.generation ||
    !["active", "finalizing"].includes(beforeFence.state)
  ) {
    throw new Error(
      `preservation fence preconditions changed before supervision: ${JSON.stringify({
        consultationState: beforeFence.state,
        consultationGeneration: beforeFence.generation,
        reservationGeneration: reservation.generation,
        reservationEpoch: reservation.epoch,
      })}`,
    );
  }
  if (beforeFence.expected_count < 1)
    throw new Error("preservation failure crossed no durable expected-artifact barrier");
  await sql(
    `UPDATE worker_reservations SET lease_expires_at=now()-interval '1 second', heartbeat_at=now()-interval '2 minutes' WHERE consultation_id='${preservationConsultationId}' AND epoch=${reservation.epoch}`,
  );
  const fencedEvidence = await waitFor(
    "heartbeat-expiry supervisor fence",
    async () => {
      const evidence = await workerSupervisionEvidence(preservationConsultationId);
      const epoch = evidence.worker_epochs.find(
        (candidate) => candidate.epoch === reservation.epoch,
      );
      const currentReservation = evidence.reservations.find(
        (candidate) => candidate.epoch === reservation.epoch,
      );
      const expectedCrash = {
        workerId: reservation.workerId,
        generation: Number(reservation.generation),
        epoch: Number(reservation.epoch),
      };
      return workerCrashMatches(expectedCrash, currentReservation, epoch) &&
        epoch.fencedAt &&
        epoch.terminalOutcome === "failed" &&
        currentReservation.fencedAt &&
        currentReservation.fenceReason
        ? evidence
        : null;
    },
    90_000,
  );
  const staleHeartbeatId = randomUUID();
  await sql(`
  INSERT INTO outbox(id,topic,aggregate_id,generation,payload,available_at)
  VALUES ('${staleHeartbeatId}','worker.heartbeat','${preservationConsultationId}',${fencedEvidence.generation},
    jsonb_build_object('workerId','${reservation.workerId ?? fencedEvidence.reservations[0]?.workerId}','epoch',${reservation.epoch},'leaseSeconds',30),now())`);
  const staleRejected = await waitFor(
    "stale epoch heartbeat rejection",
    async () => {
      const rows = await queryJson(
        `SELECT attempts,delivered_at FROM outbox WHERE id='${staleHeartbeatId}'`,
      );
      return rows[0]?.attempts > 0 && rows[0]?.delivered_at === null ? rows[0] : null;
    },
    30_000,
  );
  const fencedEpoch = fencedEvidence.worker_epochs.find(
    (candidate) => candidate.epoch === reservation.epoch,
  );
  if (!fencedEpoch?.terminalCheckpointId)
    throw new Error(`supervisor fence omitted checkpoint evidence: ${JSON.stringify(fencedEpoch)}`);
  const failedStatus = await waitFor(
    "durable archive-failure status",
    async () => {
      const rows = await queryJson(
        `SELECT effect_kind,state,result FROM external_effects WHERE consultation_id='${preservationConsultationId}' AND effect_kind='STATUS_PACKET' AND result->'plan'->>'reasonCode'='ARCHIVE_FAILED' AND request_bytes IS NOT NULL AND request_hash IS NOT NULL AND attempts > 0 ORDER BY created_at`,
      );
      return rows.length > 0 ? rows : null;
    },
    30_000,
  );
  await setWorkerScenario();
  await assertServiceHealthy("translation-worker");
  if (spoolRecoverySelected) {
    await start(drainerBefore.Id);
  } else {
    await stop("spool-drainer");
    await start(drainerBefore.Id);
  }
  await waitFor(
    "same-volume spool drainer restart",
    async (signal, deadline) => {
      const current = await inspect(drainerBefore.Id, { signal, deadline });
      return current.State.Running &&
        current.State.StartedAt !== drainerBeforeState.State.StartedAt &&
        (!current.State.Health || current.State.Health.Status === "healthy")
        ? current.State.StartedAt
        : null;
    },
    120_000,
  );
  await sql(`DELETE FROM outbox WHERE id='${staleHeartbeatId}'`);
  const settled = await settleConsultation(preservationConsultationId);
  const duplicateEffects = await queryJson(`
        SELECT occurrence_key,generation,count(*)::int AS copies
        FROM external_effects
        WHERE consultation_id='${preservationConsultationId}'
        GROUP BY occurrence_key,generation
        HAVING count(*) > 1`);
  if (duplicateEffects.length > 0 || settled.pending_outbox !== 0 || settled.active_effects !== 0) {
    throw new Error(
      `spool recovery did not converge exactly once: ${JSON.stringify({
        duplicateEffects,
        settlement: settlementSummary(settled),
      })}`,
    );
  }
  const sharedEvidence = {
    consultationId: preservationConsultationId,
    workerExited: true,
    workerId: reservation.workerId,
    workerGeneration: reservation.generation,
    workerEpoch: reservation.epoch,
    expectedArtifactsBeforeSend: beforeFence.expected_count,
    heartbeatExpiryFencedAt: fencedEpoch.fencedAt,
    terminalCheckpointId: fencedEpoch.terminalCheckpointId,
    fenceReason: fencedEvidence.reservations[0].fenceReason,
    staleHeartbeatAttempts: staleRejected.attempts,
    archiveFailureStatusCount: failedStatus.length,
    supervisorRestarted: true,
    drainerRestarted: true,
    duplicateEffects: 0,
    pendingDelivery: settled.pending_outbox,
    cleanSettlement: true,
  };
  proof.scenarios.push({
    name: spoolRecoverySelected ? "spool-durable-recovery" : "preservation-fence",
    ...sharedEvidence,
    replayedCheckpointIds: [],
  });
  if (preservationFenceSelected) await checkpointScenario("preservation-fence");
  if (spoolRecoverySelected) await checkpointScenario("spool-durable-recovery");
}
