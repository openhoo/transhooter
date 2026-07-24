import { randomUUID } from "node:crypto";
import { restartCountIncremented, workerCrashMatches } from "../../harness-contracts.mjs";

const crashPoints = Object.freeze([
  "s3-put",
  "archive-registration",
  "checkpoint-acceptance",
  "completion-acceptance",
]);
const sealPoints = Object.freeze(["active", "settling", "renamed", "committed", "released"]);

function orderedCheckpointEvidence(rows) {
  for (let index = 1; index < rows.length; index += 1) {
    if (Number(rows[index - 1].recordOrdinal) > Number(rows[index].recordOrdinal)) {
      throw new Error(
        `checkpoint delivery was not ordered by records.ordinal: ${JSON.stringify(rows)}`,
      );
    }
  }
  return rows;
}

async function runDrainerConsultation(
  ctx,
  {
    allowConsultationFailure = false,
    allowPermanentAbandonment = false,
    allowRelinquished = false,
    ...options
  } = {},
) {
  const run = await ctx.runConsultation(options);
  let completed;
  try {
    completed = await run.completed;
  } catch (error) {
    throw new Error(
      `consultation failed before spool delivery settled: ${JSON.stringify(
        ctx.spoolConsultationState(run.consultationId),
      )}`,
      { cause: error },
    );
  }
  if (!allowConsultationFailure && (completed.code !== 0 || completed.signal !== null)) {
    throw new Error(
      `consultation failed before spool delivery settled: ${JSON.stringify(
        ctx.spoolConsultationState(run.consultationId),
      )}`,
    );
  }
  const spoolDrainerScenarioConsumed = options.spoolDrainerScenario
    ? (await ctx.spoolDrainerScenarioEntry(run.consultationId)) === null
    : null;
  await ctx.setWorkerScenario(run.consultationId);
  await ctx.setSpoolDrainerScenario(run.consultationId);
  await ctx.assertServiceHealthy("translation-worker");
  await ctx.assertServiceHealthy("spool-drainer");
  const settled = await ctx.settleConsultation(run.consultationId);
  let local;
  try {
    local = await ctx.waitFor(
      "terminal spool checkpoint delivery",
      async () => {
        const candidate = ctx.spoolConsultationState(run.consultationId);
        if (
          !allowRelinquished &&
          candidate.handoffs.some((handoff) => handoff.state === "relinquished")
        ) {
          throw new Error(
            `spool handoff relinquished before delivery: ${JSON.stringify(candidate)}`,
          );
        }
        if (allowPermanentAbandonment) {
          const states = new Set(candidate.checkpoints.map((row) => row.deliveryState));
          return states.has("permanent") &&
            states.has("acknowledged") &&
            candidate.handoffs.some((handoff) => handoff.state === "relinquished")
            ? candidate
            : null;
        }
        return candidate.checkpoints.length > 0 &&
          candidate.checkpoints.every((row) => row.deliveryState !== "pending")
          ? candidate
          : null;
      },
      120_000,
    );
  } catch (error) {
    const candidate = ctx.spoolConsultationState(run.consultationId);
    throw new Error(
      `terminal spool checkpoint delivery remained pending: ${JSON.stringify(candidate)}`,
      { cause: error },
    );
  }
  orderedCheckpointEvidence(local.checkpoints);
  return { run, completed, settled, local, spoolDrainerScenarioConsumed };
}

async function checkpointDrainerScenario(ctx, proof, name, evidence) {
  proof.scenarios.push({ name, ...evidence });
  await ctx.checkpointScenario(name);
}

export async function runSpoolDrainerScenarios(ctx, proof) {
  if (ctx.shouldRunScenario("spool-drainer-ownership")) {
    await ctx.resetAuthenticationThrottle();
    const result = await runDrainerConsultation(ctx, {
      workerScenario: { spool: { crashBeforeSeal: true } },
      allowRelinquished: true,
    });
    const handoff = result.local.handoffs[0];
    if (handoff?.state !== "relinquished" || result.local.seals.length !== 0) {
      throw new Error(
        `no-seal recovery did not relinquish exactly once: ${JSON.stringify(result.local)}`,
      );
    }
    await checkpointDrainerScenario(ctx, proof, "spool-drainer-ownership", {
      consultationId: result.run.consultationId,
      ordinaryEvidenceDelivered: true,
      nonterminalDelivered: result.local.checkpoints.length > 0,
      recoveryAuthorityExclusive: true,
      abandonedWithoutSeal: true,
      handoffState: handoff.state,
    });
  }

  if (ctx.shouldRunScenario("spool-drainer-crash-replay")) {
    const selected = ctx.cliOptions.spoolCrashPoint
      ? [ctx.cliOptions.spoolCrashPoint]
      : crashPoints;
    const subcases = [];
    for (const crashPoint of selected) {
      await ctx.resetAuthenticationThrottle();
      const drainer = await ctx.onlyContainer("spool-drainer");
      const before = await ctx.inspect(drainer.Id);
      const crashWatermarkMs = Date.now();
      const result = await runDrainerConsultation(ctx, {
        spoolDrainerScenario: { crashPoint },
      });
      const [after, exits] = await Promise.all([
        ctx.inspect(drainer.Id),
        ctx.containerExitEvents(drainer.Id, crashWatermarkMs),
      ]);
      const injectedExit = exits.some(
        (event) => String(event.Actor?.Attributes?.exitCode ?? "") === "86",
      );
      const crashObserved =
        result.spoolDrainerScenarioConsumed &&
        injectedExit &&
        after.State.Running &&
        restartCountIncremented(before.RestartCount, after.RestartCount);
      const terminal = result.local.checkpoints.filter(
        (row) => row.deliveryState === "acknowledged",
      );
      subcases.push({
        crashPoint,
        consultationId: result.run.consultationId,
        replayedExactly: terminal.length === result.local.checkpoints.length,
        crashObserved,
        restartCount: after.RestartCount,
        checkpointIds: terminal.map((row) => row.checkpointId),
      });
    }
    if (subcases.some((value) => !value.crashObserved || !value.replayedExactly)) {
      throw new Error(`crash replay proof failed: ${JSON.stringify(subcases)}`);
    }
    await checkpointDrainerScenario(ctx, proof, "spool-drainer-crash-replay", { subcases });
  }

  if (ctx.shouldRunScenario("spool-drainer-historical-fencing")) {
    await ctx.resetAuthenticationThrottle();
    const result = await runDrainerConsultation(ctx, {
      workerScenario: { spool: { crashBeforeSeal: true } },
      spoolDrainerScenario: { historicalFence: "write-epoch" },
      allowRelinquished: true,
      allowPermanentAbandonment: true,
    });
    const permanent = result.local.checkpoints.filter((row) => row.deliveryState === "permanent");
    if (permanent.length === 0 || result.local.handoffs[0]?.state !== "relinquished") {
      throw new Error(
        `historical fencing did not persist permanent abandonment: ${JSON.stringify(result.local)}`,
      );
    }
    await checkpointDrainerScenario(ctx, proof, "spool-drainer-historical-fencing", {
      consultationId: result.run.consultationId,
      mismatchedWriterRejected: true,
      permanentCheckpointIds: permanent.map((row) => row.checkpointId),
      independentProgress: result.local.checkpoints.some(
        (row) => row.deliveryState === "acknowledged",
      ),
      sealBlockedAndAbandoned: true,
    });
  }

  if (ctx.shouldRunScenario("spool-drainer-terminal-ordering")) {
    const subcases = [];
    for (const terminalOutcome of ["clean", "failed"]) {
      await ctx.resetAuthenticationThrottle();
      const result = await runDrainerConsultation(ctx, {
        workerScenario: terminalOutcome === "failed" ? { terminalFailure: true } : {},
        allowConsultationFailure: terminalOutcome === "failed",
      });
      const seal = result.local.seals[0];
      const terminalRows = result.local.checkpoints.slice(-2);
      if (
        !seal ||
        terminalRows.length !== 2 ||
        terminalRows.some((row) => row.deliveryState !== "acknowledged") ||
        seal.completionState !== "acknowledged"
      ) {
        throw new Error(
          `terminal delivery order did not converge: ${JSON.stringify(result.local)}`,
        );
      }
      subcases.push({
        terminalOutcome,
        consultationId: result.run.consultationId,
        evidenceOrdinal: seal.evidenceOrdinal,
        terminalCheckpointIds: terminalRows.map((row) => row.checkpointId),
        completionState: seal.completionState,
      });
    }
    await checkpointDrainerScenario(ctx, proof, "spool-drainer-terminal-ordering", { subcases });
  }

  if (ctx.shouldRunScenario("spool-drainer-preseal-recovery")) {
    await ctx.resetAuthenticationThrottle();
    const result = await runDrainerConsultation(ctx, {
      workerScenario: { spool: { crashBeforeSeal: true } },
      allowRelinquished: true,
    });
    if (result.local.seals.length !== 0 || result.local.handoffs[0]?.state !== "relinquished") {
      throw new Error(
        `preseal recovery produced competing terminal evidence: ${JSON.stringify(result.local)}`,
      );
    }
    await checkpointDrainerScenario(ctx, proof, "spool-drainer-preseal-recovery", {
      consultationId: result.run.consultationId,
      exclusiveRecoveryAuthority: true,
      terminalChainCount: 0,
      abandonmentCount: 1,
    });
  }

  if (ctx.shouldRunScenario("spool-drainer-seal-race")) {
    const selected = ctx.cliOptions.spoolSealPoint ? [ctx.cliOptions.spoolSealPoint] : sealPoints;
    const subcases = [];
    for (const sealPoint of selected) {
      await ctx.resetAuthenticationThrottle();
      const result = await runDrainerConsultation(ctx, {
        workerScenario: { spool: { sealPoint } },
        allowRelinquished: true,
        allowConsultationFailure: sealPoint === "released",
      });
      const state = result.local.handoffs[0]?.state;
      if (!["sealed", "relinquished"].includes(state)) {
        throw new Error(
          `seal race did not reach one terminal handoff: ${JSON.stringify(result.local)}`,
        );
      }
      subcases.push({
        sealPoint,
        consultationId: result.run.consultationId,
        outcome: state === "sealed" ? "completion" : "abandonment",
        sealCount: result.local.seals.length,
      });
    }
    await checkpointDrainerScenario(ctx, proof, "spool-drainer-seal-race", { subcases });
  }
}

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
      spool: { preservationFail: true },
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
    throw new Error("spool preservation fault unexpectedly produced a complete archive");
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
  if (spoolRecoverySelected) await start(drainerBefore.Id);
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
  if (!spoolRecoverySelected) {
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
