import { chmod, readFile, rm } from "node:fs/promises";

export async function emitProof(ctx, proof, startedAt) {
  const { scenarioRegistry, scenarioDurations, writeJsonFile } = ctx;
  ctx.announcePhase("proof-emission");
  proof.totalDurationMs = Date.now() - startedAt;
  proof.scenarioDurationsMs = Object.fromEntries(
    scenarioRegistry
      .filter((name) => scenarioDurations.has(name))
      .map((name) => [name, scenarioDurations.get(name)]),
  );
  const serializedProof = JSON.stringify(proof);
  if (process.env.FAILURE_SMOKE_PROOF_FILE) {
    await writeJsonFile(process.env.FAILURE_SMOKE_PROOF_FILE, proof);
  }
  console.log(serializedProof);
}

export async function finalizeHarness(ctx, primaryError, cleanupFailures) {
  const {
    consultationRuns,
    trackedConsultations,
    faultFile,
    workerScenarioFile,
    database,
    ownerFile,
  } = ctx;
  const {
    announcePhase,
    terminateProcessTree,
    setFaults,
    setWorkerScenario,
    onlyContainer,
    inspect,
    start,
    assertServiceHealthy,
    settleConsultations,
    persistOwnerLease,
  } = ctx;
  announcePhase("cleanup");
  const cleanup = async (label, operation) => {
    try {
      await operation();
      return true;
    } catch (error) {
      cleanupFailures.push(
        new Error(`${label}: ${error?.message ?? String(error)}`, { cause: error }),
      );
      return false;
    }
  };
  let cleanupSucceeded = await cleanup("terminate consultation process trees", async () => {
    await Promise.all([...consultationRuns].map((run) => terminateProcessTree(run)));
  });
  cleanupSucceeded =
    (await cleanup("remove consultation release files", async () => {
      await Promise.all([...consultationRuns].map((run) => rm(run.releaseFile, { force: true })));
    })) && cleanupSucceeded;
  cleanupSucceeded =
    (await cleanup("reset fault controls", async () => {
      await setFaults();
      await setWorkerScenario();
      const [faults, scenarios] = await Promise.all([
        readFile(faultFile, "utf8").then(JSON.parse),
        readFile(workerScenarioFile, "utf8").then(JSON.parse),
      ]);
      const ownedFaultRemains = [...trackedConsultations].some(
        (consultationId) => consultationId in (faults.consultations ?? {}),
      );
      const ownedScenarioRemains = [...trackedConsultations].some(
        (consultationId) => consultationId in (scenarios.consultations ?? {}),
      );
      if (
        Object.keys(faults).length !== 1 ||
        !faults.consultations ||
        Object.keys(scenarios).length !== 1 ||
        !scenarios.consultations ||
        ownedFaultRemains ||
        ownedScenarioRemains
      ) {
        throw new Error(
          `fault reset retained harness consultations or invalid shape: ${JSON.stringify({
            faults,
            scenarios,
          })}`,
        );
      }
    })) && cleanupSucceeded;
  cleanupSucceeded =
    (await cleanup("restore spool permissions", () =>
      chmod("/var/lib/transhooter/spool", 0o700),
    )) && cleanupSucceeded;
  if (primaryError === null) {
    const servicesRestored = await cleanup("restore required services", async () => {
      await Promise.all(
        [
          "minio",
          "translation-worker",
          "spool-drainer",
          "control-worker-1",
          "control-worker-2",
        ].map(async (service) => {
          const container = await onlyContainer(service);
          if (!(await inspect(container.Id)).State.Running) await start(container.Id);
          await assertServiceHealthy(service, 30_000);
        }),
      );
    });
    cleanupSucceeded = servicesRestored && cleanupSucceeded;
    if (servicesRestored) {
      cleanupSucceeded =
        (await cleanup("settle all tracked consultations", () =>
          settleConsultations(trackedConsultations),
        )) && cleanupSucceeded;
    }
  } else {
    console.error(
      "[failure-smoke] primary failure: skipping service restoration and consultation settlement; the wrapper will remove the isolated stack",
    );
  }
  cleanupSucceeded =
    (await cleanup("close failure-smoke database", () => database.end({ timeout: 5 }))) &&
    cleanupSucceeded;
  if (cleanupSucceeded && primaryError === null && cleanupFailures.length === 0) {
    await cleanup("remove failure-smoke owner lease", () => rm(ownerFile, { force: true }));
  } else {
    await cleanup("persist recoverable failure-smoke owner lease", () => persistOwnerLease(0));
  }
}
