import { rm, stat } from "node:fs/promises";
import { crashRecoveryMatches } from "../../harness-contracts.mjs";

export async function runControlScenarios(ctx, proof) {
  const {
    faultFile,
    onlyContainer,
    inspect,
    containerExitEvents,
    waitFor,
    shouldRunScenario,
    checkpointScenario,
    setFaults,
    setWorkerScenario,
    runConsultation,
    resetAuthenticationThrottle,
    raceArchiveHoldAndDelete,
    consultationEvidence,
    isCleanSettlement,
    controlWorkerBaselines,
    runEffectBoundaryCrash,
    sql,
  } = ctx;
  if (shouldRunScenario("recovery-hold")) {
    // A crash after the durable `calling` transition must be adopted by a restarted
    // replica; the same full consultation must still produce a complete inventory.
    const controls = await Promise.all([
      onlyContainer("control-worker-1"),
      onlyContainer("control-worker-2"),
    ]);
    const controlBaselines = new Map(
      await Promise.all(
        controls.map(async (container) => {
          const details = await inspect(container.Id);
          const workerId = details.Config.Env.find((entry) =>
            entry.startsWith("INSTANCE_ID="),
          )?.slice("INSTANCE_ID=".length);
          if (!workerId) {
            throw new Error(`control worker ${container.Id} has no INSTANCE_ID`);
          }
          return [
            container.Id,
            {
              containerId: container.Id,
              workerId,
              restartCount: details.RestartCount,
              startedAt: details.State.StartedAt,
            },
          ];
        }),
      ),
    );
    await resetAuthenticationThrottle();
    const crashWatermarkMs = Date.now();
    const crashRun = await runConsultation({
      faults: { crashAfterPersistCalling: ["ROOM_CREATE"] },
    });
    const crashedEffect = await waitFor(
      "persisted ROOM_CREATE crash owner",
      async () => {
        const evidence = await consultationEvidence(crashRun.consultationId);
        const effect = evidence.effects.find(
          (candidate) =>
            candidate.kind === "ROOM_CREATE" &&
            candidate.state === "calling" &&
            candidate.attempts === 1 &&
            typeof candidate.leaseOwner === "string",
        );
        return effect ?? null;
      },
      30_000,
    );
    const crashedWorker = [...controlBaselines.values()].find(
      (candidate) => candidate.workerId === crashedEffect.leaseOwner,
    );
    if (!crashedWorker) {
      throw new Error("persisted ROOM_CREATE owner does not match a control-worker instance");
    }
    const restarted = await waitFor(
      "exact control worker restart after persisted ROOM_CREATE",
      async (signal, deadline) => {
        if (crashRun.closed) {
          throw new Error(
            `crash injection consultation exited before worker restart: ${crashRun.stderr}`,
          );
        }
        const container = controls.find((candidate) => candidate.Id === crashedWorker.containerId);
        const [current, exits] = await Promise.all([
          inspect(crashedWorker.containerId, { signal, deadline }),
          containerExitEvents(crashedWorker.containerId, crashWatermarkMs, {
            signal,
            deadline,
          }),
        ]);
        const observation = {
          workerId: crashedWorker.workerId,
          containerId: container?.Id,
          restartCount: current.RestartCount,
          startedAt: current.State.StartedAt,
        };
        const injectedExit = exits.some(
          (event) => String(event.Actor?.Attributes?.exitCode ?? "") === "86",
        );
        if (!injectedExit || !crashRecoveryMatches(crashedWorker, observation)) return null;
        if (
          !current.State.Running ||
          (current.State.Health && current.State.Health.Status !== "healthy")
        ) {
          return null;
        }
        const evidence = await consultationEvidence(crashRun.consultationId);
        const scopedEffect = evidence.effects.find(
          (effect) =>
            effect.kind === "ROOM_CREATE" &&
            Number(effect.generation) === Number(evidence.generation) &&
            effect.attempts >= 1,
        );
        return scopedEffect ? { ...observation, generation: evidence.generation } : null;
      },
      30_000,
    );
    await setFaults();
    await setWorkerScenario();
    const recovered = await crashRun.completed;
    if (recovered.code !== 0)
      throw new Error(
        `crash recovery consultation failed: ${recovered.stderr}\n${recovered.stdout}`,
      );
    const recoveryProof = recovered.stdout
      .trim()
      .split("\n")
      .map((line) => {
        try {
          return JSON.parse(line);
        } catch {
          return null;
        }
      })
      .find((value) => value?.inventorySha256);
    if (!recoveryProof) throw new Error("crash recovery emitted no inventory proof");
    if (recoveryProof.consultationId !== crashRun.consultationId) {
      throw new Error(
        `crash proof consultation mismatch: ${recoveryProof.consultationId} != ${crashRun.consultationId}`,
      );
    }
    const recoveredEvidence = await consultationEvidence(recoveryProof.consultationId);
    if (
      recoveredEvidence.state !== "ended" ||
      recoveredEvidence.archive_state !== "complete" ||
      recoveredEvidence.final_inventory_hash !== recoveryProof.inventorySha256 ||
      recoveredEvidence.inventory?.status !== "complete" ||
      Number(recoveredEvidence.expected_count) < 1 ||
      Number(recoveredEvidence.object_count) < Number(recoveredEvidence.expected_count) ||
      (recoveredEvidence.inventory?.missing ?? []).length !== 0 ||
      (recoveredEvidence.inventory?.errors ?? []).length !== 0
    )
      throw new Error(
        `crash recovery lacks gap-free durable inventory: ${JSON.stringify(recoveredEvidence)}`,
      );
    if (
      !recoveredEvidence.effects.some(
        (effect) =>
          effect.kind === "ROOM_CREATE" &&
          Number(effect.generation) === Number(restarted.generation) &&
          effect.state === "done" &&
          effect.attempts >= 2 &&
          typeof effect.requestHash === "string" &&
          /^[0-9a-f]{64}$/u.test(effect.requestHash) &&
          typeof effect.result?.remoteId === "string",
      )
    )
      throw new Error("ROOM_CREATE did not durably continue its exact persisted request");
    proof.scenarios.push({
      name: "outbox-crash-continuation",
      restartedContainer: restarted.containerId,
      restartCount: restarted.restartCount,
      crashedWorkerId: crashedWorker.workerId,
      crashedWorkerEpoch: crashedWorker.startedAt,
      successorWorkerEpoch: restarted.startedAt,
      generation: restarted.generation,
      consultationId: recoveryProof.consultationId,
      inventorySha256: recoveryProof.inventorySha256,
      gapFreeInventory: true,
      expectedArtifactCount: recoveredEvidence.expected_count,
      archiveObjectCount: recoveredEvidence.object_count,
      durableContinuation: true,
    });

    // Race the authenticated archive operations themselves. SQL is inspection-only:
    // exactly one production operation may cross the archive-row exclusion boundary.
    await resetAuthenticationThrottle();
    const holdRace = await raceArchiveHoldAndDelete(
      recoveryProof.archiveId,
      recoveryProof.consultationId,
    );
    proof.scenarios.push({
      name: "hold-delete-race",
      archiveId: recoveryProof.archiveId,
      archiveState: holdRace.archiveState,
      winner: holdRace.winner,
      holdStatus: holdRace.holdStatus,
      deleteStatus: holdRace.deleteStatus,
      mutuallyExclusive: true,
    });
    await checkpointScenario("recovery-hold");
  }
  if (shouldRunScenario("remote-success-crash")) {
    proof.scenarios.push(
      await runEffectBoundaryCrash({
        scenario: "remote-success-crash",
        fault: "crashAfterRemoteSuccess",
        expectedState: "calling",
        exitCode: 87,
      }),
    );
    await checkpointScenario("remote-success-crash");
  }
  if (shouldRunScenario("applied-state-crash")) {
    proof.scenarios.push(
      await runEffectBoundaryCrash({
        scenario: "applied-state-crash",
        fault: "crashAfterMarkApplied",
        expectedState: "applied",
        exitCode: 88,
      }),
    );
    await checkpointScenario("applied-state-crash");
  }
  if (shouldRunScenario("stale-owner-fencing")) {
    const { baselines } = await controlWorkerBaselines();
    await resetAuthenticationThrottle();
    const run = await runConsultation({
      faults: { holdAfterRemoteSuccess: ["ROOM_CREATE"] },
    });
    const held = await waitFor(
      "first replica held after remote ROOM_CREATE success",
      async () => {
        const evidence = await consultationEvidence(run.consultationId);
        const candidate = evidence.effects.find(
          (effect) =>
            effect.kind === "ROOM_CREATE" &&
            effect.state === "calling" &&
            typeof effect.leaseOwner === "string",
        );
        if (!candidate) return null;
        try {
          await stat(`${faultFile}.${run.consultationId}.ROOM_CREATE.remote-success-owner`);
          return candidate;
        } catch {
          return null;
        }
      },
      60_000,
    );
    const staleOwner = baselines.get(held.leaseOwner);
    const successor = [...baselines.values()].find(
      (candidate) => candidate.workerId !== held.leaseOwner,
    );
    if (!staleOwner || !successor) {
      throw new Error("stale-owner fencing did not identify two distinct live replicas");
    }
    const [staleState, successorState] = await Promise.all([
      inspect(staleOwner.containerId),
      inspect(successor.containerId),
    ]);
    if (!staleState.State.Running || !successorState.State.Running) {
      throw new Error("expired-lease overlap requires both control replicas to remain live");
    }
    await sql(`
        UPDATE external_effects
        SET lease_owner='${successor.workerId}',lease_expires_at=now()-interval '1 second'
        WHERE id='${held.id}' AND lease_owner='${staleOwner.workerId}' AND state='calling'`);
    const stolen = await waitFor(
      "designated successor replica ownership of expired effect lease",
      async () => {
        const evidence = await consultationEvidence(run.consultationId);
        const candidate = evidence.effects.find((effect) => effect.id === held.id);
        return candidate?.attempts >= 2 &&
          (candidate.leaseOwner === successor.workerId ||
            (candidate.state === "done" && typeof candidate.result?.remoteId === "string"))
          ? candidate
          : null;
      },
      60_000,
    );
    await setFaults();
    const completed = await run.completed;
    if (completed.code !== 0) {
      throw new Error(
        `stale-owner fencing consultation failed: ${completed.stderr}\n${completed.stdout}`,
      );
    }
    const settled = await waitFor(
      "stale-owner fencing clean settlement",
      async () => {
        const evidence = await consultationEvidence(run.consultationId);
        return isCleanSettlement(evidence) ? evidence : null;
      },
      120_000,
    );
    const outcome = settled.effects.find(
      (effect) =>
        effect.id === held.id &&
        effect.state === "done" &&
        typeof effect.result?.remoteId === "string",
    );
    if (!outcome || outcome.attempts !== stolen.attempts) {
      throw new Error("stale owner changed the successor's single durable remote outcome");
    }
    await rm(`${faultFile}.${run.consultationId}.ROOM_CREATE.remote-success-owner`, {
      force: true,
    });
    proof.scenarios.push({
      name: "stale-owner-fencing",
      consultationId: run.consultationId,
      staleOwner: staleOwner.workerId,
      successorOwner: successor.workerId,
      effectAttempts: outcome.attempts,
      remoteId: outcome.result.remoteId,
      overlappingLiveReplicas: true,
      successorOwnershipObserved: true,
      staleOwnerFenced: true,
      cleanSettlement: true,
    });
    await checkpointScenario("stale-owner-fencing");
  }
}
