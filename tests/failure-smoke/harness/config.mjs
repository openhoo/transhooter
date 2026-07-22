import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import postgres from "postgres";

export async function createHarnessContext() {
  const consultationHarness = fileURLToPath(
    new URL("../../e2e/smoke-consultation.mjs", import.meta.url),
  );

  const baseUrl = process.env.BASE_URL ?? "http://web:3000";
  const serviceBaseUrl = (() => {
    const url = new URL(baseUrl);
    if (url.hostname === "app.localhost") {
      url.hostname = "web";
    }
    return url.toString().replace(/\/$/u, "");
  })();
  const livekitUrl = process.env.LIVEKIT_URL ?? "ws://livekit:7880";
  const mailpitUrl = process.env.MAILPIT_URL ?? "http://mailpit:8025";
  const adminEmail = process.env.E2E_EMPLOYEE_EMAIL;
  if (!adminEmail) throw new Error("E2E_EMPLOYEE_EMAIL is required");
  const faultFile = process.env.FAULT_CONTROL_FILE ?? "/shared/faults.json";
  const workerScenarioFile = process.env.WORKER_SCENARIO_FILE ?? "/shared/worker-scenarios.json";
  const expectedProfile = process.env.EXPECTED_PROFILE ?? "fixture";
  const databaseUrlFile = process.env.DATABASE_URL_FILE ?? "/run/secrets/database-web-url";
  const releaseDirectory = process.env.FAILURE_RELEASE_DIR ?? "/shared/releases";
  const dockerResponseLimit = 2 * 1024 * 1024;
  const configLockFile = "/shared/.failure-smoke-config.lock";
  const ownerDirectory = "/shared/failure-smoke-owners";
  const ownerId = randomUUID();
  const ownerFile = `${ownerDirectory}/${ownerId}.json`;
  const ownerLeaseMs = 30 * 60_000;
  const harnessVersion = 2;
  const checkpointFile =
    process.env.FAILURE_SMOKE_CHECKPOINT_FILE ?? "/shared/failure-smoke-checkpoint.json";
  const capabilityMinimumRemainingMs = 10 * 60_000;
  const scenarioRegistry = Object.freeze([
    "recovery-hold",
    "participant-egress-denied",
    "remote-success-crash",
    "applied-state-crash",
    "stale-owner-fencing",
    "translation-rate_limit",
    "translation-quota",
    "translation-transport",
    "tts-partial-finalization",
    "preservation-fence",
    "minio-inflight-recovery",
    "spool-durable-recovery",
    "unwritable-spool",
  ]);
  const scenarioShards = Object.freeze({
    control: Object.freeze([
      "recovery-hold",
      "remote-success-crash",
      "applied-state-crash",
      "stale-owner-fencing",
    ]),
    provider: Object.freeze([
      "participant-egress-denied",
      "translation-rate_limit",
      "translation-quota",
      "translation-transport",
      "tts-partial-finalization",
    ]),
    spool: Object.freeze(["preservation-fence", "spool-durable-recovery"]),
    storage: Object.freeze(["minio-inflight-recovery", "unwritable-spool"]),
  });
  const translationFailureCases = Object.freeze([
    Object.freeze({
      name: "translation-rate_limit",
      failure: "rate_limit",
      advice: "retry_after",
      action: "retry",
      expectRetry: true,
      watermarks: Object.freeze({ accepted: 1, received: 0, emitted: 0 }),
    }),
    Object.freeze({
      name: "translation-quota",
      failure: "quota",
      advice: "never",
      action: "degrade",
      expectRetry: false,
      watermarks: Object.freeze({ accepted: 1, received: 0, emitted: 0 }),
    }),
    Object.freeze({
      name: "translation-transport",
      failure: "transport",
      advice: "retry_after",
      action: "retry",
      expectRetry: true,
      watermarks: Object.freeze({ accepted: 1, received: 0, emitted: 0 }),
    }),
  ]);
  const cliOptions = {
    scenarios: undefined,
    shard: undefined,
    resume: false,
    deadlineEpochMs: undefined,
  };
  for (let index = 2; index < process.argv.length; index += 1) {
    const argument = process.argv[index];
    if (argument === "--resume") {
      cliOptions.resume = true;
      continue;
    }
    if (!["--scenarios", "--shard", "--deadline-epoch-ms"].includes(argument)) {
      throw new Error(`unknown failure-smoke argument: ${argument}`);
    }
    const value = process.argv[index + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new Error(`${argument} requires a value`);
    }
    if (argument === "--scenarios" && cliOptions.scenarios !== undefined) {
      throw new Error("--scenarios may only be specified once");
    }
    if (argument === "--shard" && cliOptions.shard !== undefined) {
      throw new Error("--shard may only be specified once");
    }
    if (argument === "--deadline-epoch-ms" && cliOptions.deadlineEpochMs !== undefined) {
      throw new Error("--deadline-epoch-ms may only be specified once");
    }
    index += 1;
    if (argument === "--scenarios") cliOptions.scenarios = value;
    else if (argument === "--shard") cliOptions.shard = value;
    else cliOptions.deadlineEpochMs = value;
  }
  const requestedScenarioText = cliOptions.scenarios ?? process.env.FAILURE_SMOKE_SCENARIOS;
  if (cliOptions.shard !== undefined && requestedScenarioText !== undefined) {
    throw new Error("--shard and --scenarios are mutually exclusive");
  }
  if (cliOptions.shard !== undefined && !(cliOptions.shard in scenarioShards)) {
    throw new Error(
      `unknown failure-smoke shard: ${cliOptions.shard}; expected one of ${Object.keys(scenarioShards).join(", ")}`,
    );
  }
  const configuredDeadline =
    cliOptions.deadlineEpochMs ?? process.env.FAILURE_SMOKE_DEADLINE_EPOCH_MS;
  const harnessDeadlineMs =
    configuredDeadline === undefined ? Number.POSITIVE_INFINITY : Number(configuredDeadline);
  if (
    (configuredDeadline !== undefined && !Number.isSafeInteger(harnessDeadlineMs)) ||
    harnessDeadlineMs <= Date.now()
  ) {
    throw new Error("--deadline-epoch-ms must be a future Unix epoch in milliseconds");
  }
  const selectedScenarios =
    cliOptions.shard !== undefined
      ? new Set(scenarioShards[cliOptions.shard])
      : requestedScenarioText === undefined
        ? new Set(scenarioRegistry)
        : new Set(
            requestedScenarioText
              .split(",")
              .map((value) => value.trim())
              .filter(Boolean),
          );
  if (requestedScenarioText !== undefined && selectedScenarios.size === 0) {
    throw new Error("explicit failure-smoke scenario selection must not be empty");
  }
  const unknownScenarios = [...selectedScenarios].filter(
    (name) => !scenarioRegistry.includes(name),
  );
  if (unknownScenarios.length > 0) {
    throw new Error(`unknown failure-smoke scenarios: ${unknownScenarios.join(", ")}`);
  }
  const resumeRequested = cliOptions.resume || process.env.FAILURE_SMOKE_RESUME === "true";
  const checkpointBinding = Object.freeze({
    harnessVersion,
    project: process.env.TARGET_COMPOSE_PROJECT ?? "",
    profile: expectedProfile,
  });
  const completedScenarios = new Set();
  const capabilityLease = null;
  const scenarioStartedAt = new Map();
  const scenarioDurations = new Map();
  const checkpointedConsultations = new Set();
  const checkpointedReleaseFiles = new Set();
  const database = postgres((await readFile(databaseUrlFile, "utf8")).trim(), {
    max: 4,
    prepare: false,
    connect_timeout: 10,
    idle_timeout: 20,
    connection: { statement_timeout: "15000" },
  });

  return {
    consultationHarness,
    baseUrl,
    serviceBaseUrl,
    livekitUrl,
    mailpitUrl,
    adminEmail,
    faultFile,
    workerScenarioFile,
    expectedProfile,
    databaseUrlFile,
    releaseDirectory,
    dockerResponseLimit,
    configLockFile,
    ownerDirectory,
    ownerId,
    ownerFile,
    ownerLeaseMs,
    harnessVersion,
    checkpointFile,
    capabilityMinimumRemainingMs,
    scenarioRegistry,
    scenarioShards,
    translationFailureCases,
    cliOptions,
    harnessDeadlineMs,
    selectedScenarios,
    resumeRequested,
    checkpointBinding,
    completedScenarios,
    capabilityLease,
    scenarioStartedAt,
    scenarioDurations,
    checkpointedConsultations,
    checkpointedReleaseFiles,
    database,
    consultationRuns: new Set(),
    trackedConsultations: new Set(),
    activePhase: "initialization",
  };
}
