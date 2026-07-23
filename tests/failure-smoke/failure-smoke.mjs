import { createHarnessContext } from "./harness/config.mjs";
import { installConsultation } from "./harness/consultation.mjs";
import { emitProof, finalizeHarness } from "./harness/finalize.mjs";
import { installRuntime } from "./harness/runtime.mjs";
import { runControlScenarios } from "./harness/scenarios/control.mjs";
import { runProviderScenarios } from "./harness/scenarios/provider.mjs";
import { runSpoolScenarios } from "./harness/scenarios/spool.mjs";
import { runStorageScenarios } from "./harness/scenarios/storage.mjs";
import { installSettlement } from "./harness/settlement.mjs";
import {
  resumedSelectionFullyComplete,
  shouldEmitFailureSmokeProof,
} from "./harness-contracts.mjs";

const ctx = await createHarnessContext();
installRuntime(ctx);
installSettlement(ctx);
installConsultation(ctx);
const {
  cliOptions,
  initializeCheckpoint,
  resumeRequested,
  selectedScenarios,
  completedScenarios,
  announcePhase,
  persistOwnerLease,
  reapExpiredOwners,
  setFaults,
  setWorkerScenario,
  refreshFixtureCapabilities,
} = ctx;

async function main() {
  const startedAt = Date.now();
  const proof = { shard: cliOptions.shard ?? null, scenarios: [] };
  let primaryError = null;
  const cleanupFailures = [];
  try {
    announcePhase("initialization");
    await initializeCheckpoint();
    const resumedSelectionAlreadyComplete = resumedSelectionFullyComplete({
      resumeRequested,
      selectedScenarios,
      completedScenarios,
    });
    if (!resumedSelectionAlreadyComplete) await refreshFixtureCapabilities();
    await reapExpiredOwners();
    await persistOwnerLease();
    await setFaults();
    await setWorkerScenario();

    await runControlScenarios(ctx, proof);
    await runProviderScenarios(ctx, proof);
    await runSpoolScenarios(ctx, proof);
    await runStorageScenarios(ctx, proof);

    if (
      shouldEmitFailureSmokeProof({
        resumedSelectionAlreadyComplete,
        scenarioCount: proof.scenarios.length,
      })
    ) {
      await emitProof(ctx, proof, startedAt);
    } else {
      console.error(
        "[failure-smoke] resumed selection already complete; retaining the existing proof artifact",
      );
    }
  } catch (error) {
    primaryError = new Error(
      `[failure-smoke] phase ${ctx.activePhase} failed: ${error?.message ?? String(error)}`,
      { cause: error },
    );
    console.error(primaryError.message);
  } finally {
    await finalizeHarness(ctx, primaryError, cleanupFailures);
  }
  if (primaryError) {
    if (cleanupFailures.length > 0) primaryError.cleanupFailures = cleanupFailures;
    throw primaryError;
  }
  if (cleanupFailures.length > 0) {
    throw new AggregateError(cleanupFailures, "failure-smoke cleanup failed");
  }
}

try {
  await main();
} catch (error) {
  console.error(error);
  process.exitCode = 1;
}
