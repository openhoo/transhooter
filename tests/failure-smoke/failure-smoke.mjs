import { Database } from "bun:sqlite";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import {
  chmod,
  mkdir,
  open,
  readdir,
  readFile,
  rename,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { dirname } from "node:path";
import http from "node:http";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import postgres from "postgres";
import {
  archiveLockHierarchyBlocked,
  archiveRaceWinner,
  crashRecoveryMatches,
  healthFailureAfter,
  pollUntil,
  restartCountIncremented,
  runWithDeadline,
  settlementProblems,
  workerCrashMatches,
} from "./harness-contracts.mjs";

const consultationHarness = fileURLToPath(
  new URL("../e2e/smoke-consultation.mjs", import.meta.url),
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
const unknownScenarios = [...selectedScenarios].filter((name) => !scenarioRegistry.includes(name));
if (unknownScenarios.length > 0) {
  throw new Error(`unknown failure-smoke scenarios: ${unknownScenarios.join(", ")}`);
}
const resumeRequested = cliOptions.resume || process.env.FAILURE_SMOKE_RESUME === "true";
const checkpointBinding = Object.freeze({
  harnessVersion,
  project: process.env.TARGET_COMPOSE_PROJECT ?? "",
  profile: expectedProfile,
});
let completedScenarios = new Set();
let capabilityLease = null;
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

function checkpointDeliveries(meetingId) {
  const spool = new Database("/var/lib/transhooter/spool/journal.sqlite3", { readonly: true });
  try {
    return spool
      .query(
        `SELECT checkpoint_id AS checkpointId, control_event_id AS controlEventId,
          acknowledged
        FROM checkpoint_deliveries
        WHERE meeting_id = ?
        ORDER BY checkpoint_id`,
      )
      .all(meetingId);
  } finally {
    spool.close();
  }
}

function operationDeadline(requestedDeadline = Date.now() + 30_000) {
  return Math.min(requestedDeadline, harnessDeadlineMs);
}

async function withAbsoluteDeadline(label, deadline, operation, parentSignal) {
  return await runWithDeadline(label, operationDeadline(deadline), operation, parentSignal);
}

function docker(method, path, body = null, options = {}) {
  const deadline = operationDeadline(options.deadline);
  return new Promise((resolve, reject) => {
    let settled = false;
    let responseBytes = 0;
    const finish = (operation, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(absoluteTimer);
      options.signal?.removeEventListener("abort", abort);
      operation(value);
    };
    const request = http.request(
      {
        socketPath: "/var/run/docker.sock",
        path,
        method,
        headers: body ? { "content-type": "application/json" } : {},
      },
      (response) => {
        const chunks = [];
        response.on("data", (chunk) => {
          responseBytes += chunk.length;
          if (responseBytes > dockerResponseLimit) {
            request.destroy(
              new Error(`Docker ${method} ${path} response exceeded ${dockerResponseLimit} bytes`),
            );
            return;
          }
          chunks.push(chunk);
        });
        response.on("end", () => {
          if (settled) return;
          const text = Buffer.concat(chunks).toString("utf8");
          if (response.statusCode >= 400) {
            finish(reject, new Error(`Docker ${method} ${path}: ${response.statusCode} ${text}`));
            return;
          }
          const contentType = response.headers["content-type"] ?? "";
          try {
            finish(
              resolve,
              text && contentType.includes("json") && !options.raw
                ? JSON.parse(text)
                : text || null,
            );
          } catch (error) {
            finish(reject, error);
          }
        });
      },
    );
    const abort = () => request.destroy(options.signal?.reason ?? new Error("Docker cancelled"));
    const absoluteTimer = setTimeout(
      () => request.destroy(deadlineError(`Docker ${method} ${path}`, deadline)),
      Math.max(0, deadline - Date.now()),
    );
    options.signal?.addEventListener("abort", abort, { once: true });
    request.on("error", (error) => finish(reject, error));
    if (options.signal?.aborted) abort();
    else if (body) request.end(JSON.stringify(body));
    else request.end();
  });
}
async function containers(service, options = {}) {
  const project = process.env.TARGET_COMPOSE_PROJECT;
  if (!project) {
    throw new Error("TARGET_COMPOSE_PROJECT is required");
  }
  const filters = encodeURIComponent(
    JSON.stringify({
      label: [
        `com.docker.compose.project=${project}`,
        `com.docker.compose.service=${service}`,
        "com.docker.compose.oneoff=False",
      ],
    }),
  );
  return await docker("GET", `/containers/json?all=1&filters=${filters}`, null, options);
}
async function onlyContainer(service, options = {}) {
  const matches = await containers(service, options);
  if (matches.length !== 1)
    throw new Error(`expected one ${service} container, found ${matches.length}`);
  return matches[0];
}
async function inspect(id, options = {}) {
  return await docker("GET", `/containers/${id}/json`, null, options);
}
async function containerExitEvents(id, sinceMs, options = {}) {
  const untilSeconds = Math.floor(Date.now() / 1_000);
  const sinceSeconds = Math.floor(sinceMs / 1_000);
  if (untilSeconds <= sinceSeconds) return [];
  const filters = encodeURIComponent(
    JSON.stringify({ container: [id], event: ["die"], type: ["container"] }),
  );
  const text = await docker(
    "GET",
    `/events?since=${sinceSeconds}&until=${untilSeconds}&filters=${filters}`,
    null,
    { ...options, raw: true },
  );
  return String(text ?? "")
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line))
    .filter((event) => Number(event.timeNano) / 1_000_000 >= sinceMs);
}
async function stop(service) {
  const container = await onlyContainer(service);
  await docker("POST", `/containers/${container.Id}/stop?t=20`);
  return container.Id;
}
async function start(id) {
  await docker("POST", `/containers/${id}/start`);
}
async function rerunOneShot(service) {
  const container = await onlyContainer(service);
  const initial = (await inspect(container.Id)).State;
  if (initial.Running) {
    await waitFor(`${service} existing run`, async (signal, deadline) => {
      const state = (await inspect(container.Id, { signal, deadline })).State;
      return state.Running ? null : state;
    });
  }
  await start(container.Id);
  const finished = await waitFor(
    `${service} refresh completion`,
    async (signal, deadline) => {
      const state = (await inspect(container.Id, { signal, deadline })).State;
      return state.Running ? null : state;
    },
    180_000,
  );
  if (finished.ExitCode !== 0) {
    throw new Error(`${service} refresh exited ${finished.ExitCode}: ${finished.Error ?? ""}`);
  }
}
async function waitFor(label, check, timeoutMs = 120_000, parentSignal) {
  return await pollUntil(label, check, {
    deadline: operationDeadline(Date.now() + timeoutMs),
    signal: parentSignal,
  });
}
async function writeJsonFile(path, value) {
  await mkdir(dirname(path), { recursive: true });
  const temporary = `${path}.${process.pid}.${randomUUID()}.tmp`;
  await writeFile(temporary, `${JSON.stringify(value)}\n`, { mode: 0o666 });
  await chmod(temporary, 0o666);
  await rename(temporary, path);
}

async function initializeCheckpoint() {
  if (!checkpointBinding.project) {
    throw new Error("TARGET_COMPOSE_PROJECT is required");
  }
  if (!resumeRequested) return;
  let checkpoint;
  try {
    checkpoint = JSON.parse(await readFile(checkpointFile, "utf8"));
  } catch (error) {
    if (error?.code === "ENOENT") return;
    throw error;
  }
  for (const key of ["harnessVersion", "project", "profile"]) {
    if (checkpoint[key] !== checkpointBinding[key]) {
      throw new Error(
        `failure-smoke checkpoint ${key} mismatch: ${JSON.stringify(checkpoint[key])} != ` +
          JSON.stringify(checkpointBinding[key]),
      );
    }
  }
  const completed = Array.isArray(checkpoint.completed) ? checkpoint.completed : [];
  const unknown = completed.filter((name) => !scenarioRegistry.includes(name));
  if (unknown.length > 0) {
    throw new Error(`failure-smoke checkpoint contains unknown scenarios: ${unknown.join(", ")}`);
  }
  completedScenarios = new Set(completed);
}

let activePhase = "initialization";
function announcePhase(name) {
  activePhase = name;
  console.error(`[failure-smoke] phase: ${name}`);
}

function shouldRunScenario(name) {
  const selected =
    selectedScenarios.has(name) && !(resumeRequested && completedScenarios.has(name));
  if (selected) {
    announcePhase(name);
    scenarioStartedAt.set(name, Date.now());
  }
  return selected;
}

async function checkpointScenario(name) {
  await setFaults();
  await setWorkerScenario();
  const openRuns = [...consultationRuns].filter((run) => !run.closed);
  if (openRuns.length > 0) {
    throw new Error(
      `refusing to checkpoint ${name} with open consultation IDs: ${openRuns
        .map((run) => run.consultationId ?? "pending")
        .join(", ")}`,
    );
  }
  const scenarioConsultations = [...trackedConsultations].filter(
    (id) => !checkpointedConsultations.has(id),
  );
  await settleConsultations(scenarioConsultations);
  const unsettledIds = (
    await Promise.all(
      scenarioConsultations.map(async (id) => ({
        id,
        clean: isCleanSettlement(await consultationEvidence(id)),
      })),
    )
  )
    .filter(({ clean }) => !clean)
    .map(({ id }) => id);
  if (unsettledIds.length > 0) {
    throw new Error(
      `refusing to checkpoint ${name} before consultation cleanup: ${unsettledIds.join(", ")}`,
    );
  }
  const scenarioReleaseFiles = [...consultationRuns]
    .map((run) => run.releaseFile)
    .filter((path) => !checkpointedReleaseFiles.has(path));
  await Promise.all(scenarioReleaseFiles.map((path) => rm(path, { force: true })));
  for (const id of scenarioConsultations) checkpointedConsultations.add(id);
  for (const path of scenarioReleaseFiles) checkpointedReleaseFiles.add(path);
  const startedAt = scenarioStartedAt.get(name);
  if (startedAt === undefined) throw new Error(`scenario ${name} has no timing start`);
  scenarioDurations.set(name, Date.now() - startedAt);
  completedScenarios.add(name);
  await writeJsonFile(checkpointFile, {
    ...checkpointBinding,
    completed: scenarioRegistry.filter((candidate) => completedScenarios.has(candidate)),
  });
  console.error(`[failure-smoke] completed: ${name}`);
}
async function withConfigLock(operation) {
  const deadline = Date.now() + 75_000;
  let handle;
  while (!handle && Date.now() < deadline) {
    try {
      handle = await open(configLockFile, "wx", 0o600);
    } catch (error) {
      if (error?.code !== "EEXIST") throw error;
      let lockStat;
      try {
        lockStat = await stat(configLockFile);
      } catch (statError) {
        if (statError?.code === "ENOENT") continue;
        throw statError;
      }
      const age = Date.now() - lockStat.mtimeMs;
      if (age > 60_000) {
        await rm(configLockFile, { force: true });
      } else {
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }
  }
  if (!handle) throw new Error("timed out acquiring failure-smoke config lock");
  try {
    return await operation();
  } finally {
    await handle.close();
    await rm(configLockFile, { force: true });
  }
}

async function updateConsultationMap(path, consultationId, value) {
  await withConfigLock(async () => {
    let current;
    try {
      current = JSON.parse(await readFile(path, "utf8"));
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
      current = { consultations: {} };
    }
    const consultations = { ...(current.consultations ?? {}) };
    if (consultationId) {
      consultations[consultationId] = value;
    } else {
      for (const ownedId of trackedConsultations) delete consultations[ownedId];
    }
    await writeJsonFile(path, { consultations });
  });
}

async function deleteConsultationsFromMap(path, consultationIds) {
  await withConfigLock(async () => {
    let current;
    try {
      current = JSON.parse(await readFile(path, "utf8"));
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
      current = { consultations: {} };
    }
    const consultations = { ...(current.consultations ?? {}) };
    for (const consultationId of consultationIds) delete consultations[consultationId];
    await writeJsonFile(path, { consultations });
  });
}

async function persistOwnerLease(leaseMs = ownerLeaseMs) {
  await mkdir(ownerDirectory, { recursive: true });
  await writeJsonFile(ownerFile, {
    ownerId,
    expiresAtMs: Date.now() + leaseMs,
    consultations: [...trackedConsultations],
  });
}

async function reapExpiredOwners() {
  await mkdir(ownerDirectory, { recursive: true });
  const expiredIds = new Set();
  const expiredPaths = [];
  for (const entry of await readdir(ownerDirectory)) {
    if (!entry.endsWith(".json") || entry === `${ownerId}.json`) continue;
    const path = `${ownerDirectory}/${entry}`;
    try {
      const lease = JSON.parse(await readFile(path, "utf8"));
      if (Number(lease.expiresAtMs) > Date.now()) continue;
      for (const consultationId of lease.consultations ?? []) {
        if (
          /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(
            consultationId,
          )
        ) {
          expiredIds.add(consultationId);
          trackedConsultations.add(consultationId);
        }
      }
      expiredPaths.push(path);
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
    }
  }
  if (expiredIds.size > 0) {
    await deleteConsultationsFromMap(faultFile, expiredIds);
    await deleteConsultationsFromMap(workerScenarioFile, expiredIds);
    await persistOwnerLease();
    await settleConsultations(expiredIds);
    await Promise.all(expiredPaths.map((path) => rm(path, { force: true })));
  }
}
async function setFaults(consultationId = null, faults = {}) {
  await updateConsultationMap(
    faultFile,
    consultationId,
    consultationId
      ? {
          failEffects: faults.failEffects ?? [],
          crashAfterPersistCalling: faults.crashAfterPersistCalling ?? [],
          holdAfterPersistCalling: faults.holdAfterPersistCalling ?? [],
          crashAfterRemoteSuccess: faults.crashAfterRemoteSuccess ?? [],
          crashAfterMarkApplied: faults.crashAfterMarkApplied ?? [],
          holdAfterRemoteSuccess: faults.holdAfterRemoteSuccess ?? [],
        }
      : null,
  );
}

async function setWorkerScenario(consultationId = null, scenario = {}) {
  await updateConsultationMap(workerScenarioFile, consultationId, consultationId ? scenario : null);
}
const consultationRuns = new Set();
const trackedConsultations = new Set();

async function refreshFixtureCapabilities() {
  if (expectedProfile !== "fixture" || process.env.APP_ENV !== "test") {
    throw new Error("failure smoke requires the hermetic fixture profile in APP_ENV=test");
  }
  await rerunOneShot("language-refresh");
  capabilityLease = await waitFor(
    "fresh fixture language capability publication",
    async () => {
      const rows = await queryJson(`
        SELECT p.name,p.current_revision,count(l.id)::int AS fresh_rows,min(l.fresh_until) AS fresh_until
        FROM provider_profiles p
        JOIN language_capabilities l
          ON l.profile_id=p.id AND l.revision=p.current_revision
        WHERE p.name='fixture' AND p.enabled AND l.enabled
        GROUP BY p.name,p.current_revision`);
      const lease = rows[0];
      const freshUntilMs = Date.parse(lease?.fresh_until);
      return Number(lease?.fresh_rows ?? 0) > 0 &&
        Number.isFinite(freshUntilMs) &&
        freshUntilMs - Date.now() >= capabilityMinimumRemainingMs
        ? lease
        : null;
    },
    30_000,
  );
}

async function assertFixtureCapabilityLease() {
  if (!capabilityLease) throw new Error("fixture capability lease was not initialized");
  const rows = await queryJson(`
    SELECT p.current_revision,count(l.id)::int AS fresh_rows,min(l.fresh_until) AS fresh_until
    FROM provider_profiles p
    JOIN language_capabilities l
      ON l.profile_id=p.id AND l.revision=p.current_revision
    WHERE p.name='fixture' AND p.enabled AND l.enabled
    GROUP BY p.current_revision`);
  const current = rows[0];
  const freshUntilMs = Date.parse(current?.fresh_until);
  if (
    current?.current_revision !== capabilityLease.current_revision ||
    Number(current?.fresh_rows ?? 0) !== Number(capabilityLease.fresh_rows) ||
    !Number.isFinite(freshUntilMs) ||
    freshUntilMs - Date.now() < capabilityMinimumRemainingMs
  ) {
    throw new Error(
      `fixture capability revision lease changed or expires too soon: ${JSON.stringify({
        leased: capabilityLease,
        current,
      })}`,
    );
  }
}

function customerEmailForRun(runId) {
  return `customer-${runId}@example.test`;
}

async function trackConsultationsForRun(runId) {
  const customerEmail = customerEmailForRun(runId);
  const candidates = (
    await queryJson(`
      SELECT DISTINCT c.id
      FROM consultations c
      JOIN consultation_participants p ON p.consultation_id=c.id AND p.role='customer'
      JOIN users u ON u.id=p.user_id
      JOIN magic_links m ON m.consultation_id=c.id AND m.user_id=u.id
        AND m.purpose='consultation_invite'
      WHERE u.email='${customerEmail}'
      ORDER BY c.id`)
  ).map((row) => row.id);
  for (const consultationId of candidates) {
    trackedConsultations.add(consultationId);
  }
  if (candidates.length > 0) await persistOwnerLease();
  return candidates;
}

function signalProcessGroup(child, signal) {
  try {
    process.kill(-child.pid, signal);
  } catch (error) {
    if (error?.code !== "ESRCH") throw error;
  }
}

async function settleBefore(promise, deadline) {
  let timer;
  try {
    return await Promise.race([
      promise.then((result) => ({ closed: true, result })),
      new Promise((resolve) => {
        timer = setTimeout(
          () => resolve({ closed: false }),
          Math.max(0, operationDeadline(deadline) - Date.now()),
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function terminateProcessTree(run) {
  if (run.treeTerminated) return await run.completed;
  signalProcessGroup(run.child, "SIGTERM");
  const graceful = run.closed
    ? { closed: true, result: await run.completed }
    : await settleBefore(run.completed, Date.now() + 10_000);
  signalProcessGroup(run.child, "SIGKILL");
  run.treeTerminated = true;
  if (graceful.closed) return graceful.result;
  const forced = await settleBefore(run.completed, Date.now() + 10_000);
  if (!forced.closed) {
    run.closed = true;
    const result = {
      code: null,
      signal: "SIGKILL",
      stdout: run.stdout,
      stderr: `${run.stderr}\nprocess group did not close after SIGKILL`,
    };
    run.child.stdout?.destroy();
    run.child.stderr?.destroy();
    run.child.stdin?.destroy();
    run.child.unref();
    run.completion.resolve(result);
    return result;
  }
  return forced.result;
}

async function runConsultation({
  faults = {},
  workerScenario = {},
  captureBarrierTimeoutMs = null,
} = {}) {
  await assertFixtureCapabilityLease();
  const runId = randomUUID();
  await mkdir(releaseDirectory, { recursive: true });
  const releaseFile = `${releaseDirectory}/${randomUUID()}.release`;
  await rm(releaseFile, { force: true });
  const runDeadline = operationDeadline(Date.now() + 8 * 60_000);
  const child = spawn(
    "bun",
    [
      consultationHarness,
      "--base-url",
      baseUrl,
      "--livekit-url",
      livekitUrl,
      "--mailpit-url",
      process.env.MAILPIT_URL ?? "http://mailpit:8025",
      "--expected-profile",
      expectedProfile,
      "--expected-profile-revision",
      String(capabilityLease.current_revision),
      "--deadline-epoch-ms",
      String(runDeadline),
      "--emit-proof-json",
      "--skip-media-output-proof",
      "--failure-harness-release-file",
      releaseFile,
      "--failure-harness-release-timeout-ms",
      "30000",
      ...(captureBarrierTimeoutMs === null
        ? []
        : ["--capture-barrier-timeout-ms", String(captureBarrierTimeoutMs)]),
    ],
    {
      detached: true,
      env: { ...process.env, E2E_RUN_ID: runId },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  const progress = Promise.withResolvers();
  const completion = Promise.withResolvers();
  const run = {
    child,
    consultationId: null,
    releaseFile,
    stdout: "",
    stderr: "",
    closed: false,
    treeTerminated: false,
    completion,
    completed: null,
  };
  consultationRuns.add(run);
  let stdoutLines = "";
  child.stdout.on("data", (chunk) => {
    const text = chunk.toString();
    run.stdout += text;
    stdoutLines += text;
    for (;;) {
      const newline = stdoutLines.indexOf("\n");
      if (newline < 0) break;
      const line = stdoutLines.slice(0, newline);
      stdoutLines = stdoutLines.slice(newline + 1);
      try {
        const record = JSON.parse(line);
        if (
          record?.phase === "consultation-created" &&
          record.runId === runId &&
          /^[0-9a-f-]{36}$/u.test(record.consultationId)
        ) {
          progress.resolve(record);
        }
      } catch {
        // Non-JSON Playwright output remains part of the child result.
      }
    }
  });
  child.stderr.on("data", (chunk) => {
    const text = chunk.toString();
    run.stderr += text;
    process.stderr.write(text);
  });
  run.completed = completion.promise;
  child.on("close", (code, signal) => {
    run.closed = true;
    clearTimeout(run.absoluteTimer);
    progress.reject(
      new Error(`consultation exited before creation progress: ${run.stderr}\n${run.stdout}`),
    );
    completion.resolve({ code, signal, stdout: run.stdout, stderr: run.stderr });
  });
  run.absoluteTimer = setTimeout(
    () => {
      terminateProcessTree(run).catch(() => undefined);
    },
    Math.max(0, runDeadline - Date.now()),
  );
  let created;
  try {
    created = await progress.promise;
  } catch (error) {
    const candidates = await trackConsultationsForRun(runId);
    throw new Error(
      `consultation exited before progress; recovered run-correlated candidate IDs: ` +
        `${candidates.join(", ") || "none"}`,
      { cause: error },
    );
  }
  const candidates = await trackConsultationsForRun(runId);
  if (candidates.length !== 1 || candidates[0] !== created.consultationId) {
    throw new Error(
      `created consultation ${created.consultationId} did not exactly match durable invite identity ` +
        `${customerEmailForRun(runId)}: ${candidates.join(", ") || "none"}`,
    );
  }
  run.consultationId = created.consultationId;
  trackedConsultations.add(created.consultationId);
  await persistOwnerLease();
  try {
    await assertFixtureCapabilityLease();
    await setFaults(created.consultationId, faults);
    await setWorkerScenario(created.consultationId, workerScenario);
    await writeFile(releaseFile, `${created.runId}\n`, { mode: 0o600 });
  } catch (error) {
    await terminateProcessTree(run).catch(() => undefined);
    throw error;
  }
  return run;
}
async function assertServiceHealthy(service, timeoutMs = 120_000) {
  const container = await onlyContainer(service);
  return await waitFor(
    `${service} healthy`,
    async (signal, deadline) => {
      const state = (await inspect(container.Id, { signal, deadline })).State;
      return state.Running && (!state.Health || state.Health.Status === "healthy");
    },
    timeoutMs,
  );
}

async function sql(statement) {
  const rows = await database.unsafe(statement);
  return rows
    .map((row) => {
      const values = Object.values(row);
      return values.length === 1
        ? String(values[0] ?? "")
        : values.map((value) => String(value ?? "")).join("|");
    })
    .join("\n");
}

async function resetAuthenticationThrottle() {
  await sql("DELETE FROM magic_link_requests");
}

async function fetchWithDeadline(url, init = {}, deadline = Date.now() + 30_000, parentSignal) {
  return await withAbsoluteDeadline(
    `fetch ${url}`,
    deadline,
    (signal) => fetch(url, { ...init, signal }),
    parentSignal ?? init.signal,
  );
}

async function latestLink(recipient, signal, deadline = Date.now() + 15_000) {
  const listResponse = await fetchWithDeadline(
    `${mailpitUrl}/api/v1/messages?limit=100`,
    {},
    deadline,
    signal,
  );
  if (!listResponse.ok) throw new Error(`Mailpit list failed: ${listResponse.status}`);
  const payload = await listResponse.json();
  const messages = (payload.messages ?? payload.Messages ?? [])
    .filter((message) => {
      const recipients = message.To ?? message.to ?? [];
      return recipients.some((entry) => (entry.Address ?? entry.address) === recipient);
    })
    .sort(
      (left, right) =>
        Date.parse(right.Created ?? right.created ?? 0) -
        Date.parse(left.Created ?? left.created ?? 0),
    );
  const message = messages[0];
  if (!message) return null;
  const id = message.ID ?? message.Id ?? message.id;
  const detailResponse = await fetchWithDeadline(
    `${mailpitUrl}/api/v1/message/${encodeURIComponent(id)}`,
    {},
    deadline,
    signal,
  );
  if (!detailResponse.ok) throw new Error(`Mailpit message failed: ${detailResponse.status}`);
  const detail = await detailResponse.json();
  const content = `${detail.HTML ?? detail.Html ?? ""}\n${detail.Text ?? detail.text ?? ""}`;
  const match = content.match(/https?:\/\/[^\s"'<>]+\/auth\/exchange\?[^\s"'<>]+/u);
  return match?.[0]?.replaceAll("&amp;", "&") ?? null;
}

function internalizeLink(link) {
  const url = new URL(link);
  const internal = new URL(baseUrl);
  url.protocol = internal.protocol;
  url.host = internal.host;
  return url.toString();
}

async function authenticateAdmin(context) {
  const page = await context.newPage();
  await page.goto(`${baseUrl}/sign-in`, { waitUntil: "domcontentloaded" });
  await page.getByLabel("Email address").fill(adminEmail);
  const previousLink = await latestLink(adminEmail);
  await page.getByRole("button", { name: "Email me a sign-in link" }).click();
  const link = await waitFor(
    "admin magic link",
    async (signal, deadline) => {
      const candidate = await latestLink(adminEmail, signal, deadline);
      return candidate && candidate !== previousLink ? candidate : null;
    },
    30_000,
  );
  await page.goto(internalizeLink(link), { waitUntil: "domcontentloaded" });
  await page.getByRole("button", { name: "Continue securely" }).click();
  await page.waitForURL(/\/consultations(?:\?|$)/u);
  return page;
}

async function pagePost(page, path, body, deadline = operationDeadline(Date.now() + 30_000)) {
  try {
    return await page.evaluate(
      async ({ path, body, deadline }) => {
        const csrf = document.cookie
          .split("; ")
          .find((part) => part.startsWith("csrf="))
          ?.slice(5);
        if (!csrf) throw new Error("CSRF cookie unavailable");
        const response = await fetch(path, {
          method: "POST",
          credentials: "same-origin",
          cache: "no-store",
          headers: { "content-type": "application/json", "x-csrf-token": csrf },
          body: JSON.stringify(body),
          signal: AbortSignal.timeout(Math.max(1, deadline - Date.now())),
        });
        return {
          ok: response.ok,
          status: response.status,
          body: await response.text(),
        };
      },
      { path, body, deadline },
    );
  } catch (error) {
    throw new Error(
      `${path} request failed: ${error instanceof Error ? error.message : String(error)}`,
      { cause: error },
    );
  }
}

async function reauthenticateForArchive(page, consultationId) {
  const previousLink = await latestLink(adminEmail);
  const requested = await pagePost(page, "/api/auth/archive-delete-reauth", { consultationId });
  if (!requested.ok) {
    throw new Error(`archive reauthentication request failed: ${JSON.stringify(requested)}`);
  }
  const link = await waitFor(
    "archive-bound reauthentication link",
    async (signal, deadline) => {
      const candidate = await latestLink(adminEmail, signal, deadline);
      return candidate && candidate !== previousLink ? candidate : null;
    },
    30_000,
  );
  await page.goto(internalizeLink(link), { waitUntil: "domcontentloaded" });
  await page.getByRole("button", { name: "Continue securely" }).click();
  await page.waitForURL((url) => !url.pathname.startsWith("/auth/"), { timeout: 30_000 });
}

async function raceArchiveHoldAndDelete(archiveId, consultationId) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--host-resolver-rules=MAP app.localhost web, MAP rtc.localhost livekit"],
  });
  const holdContext = await browser.newContext();
  const deleteContext = await browser.newContext();
  try {
    const holdPage = await authenticateAdmin(holdContext);
    const deletePage = await authenticateAdmin(deleteContext);
    await reauthenticateForArchive(holdPage, consultationId);
    await reauthenticateForArchive(deletePage, consultationId);
    let responses;
    const settledResponses = {};
    const observeResponse = (name, response) =>
      response.then(
        (value) => {
          settledResponses[name] = value;
          return value;
        },
        (error) => {
          settledResponses[name] = {
            error: error instanceof Error ? error.message : String(error),
          };
          throw error;
        },
      );
    const raceDeadline = operationDeadline(Date.now() + 120_000);
    await database.begin(async (transaction) => {
      await transaction`SELECT id FROM archives WHERE id=${archiveId} FOR UPDATE`;
      responses = Promise.all([
        observeResponse(
          "hold",
          pagePost(
            holdPage,
            `/api/archives/${archiveId}/hold`,
            {
              archiveId,
              consultationId,
              reason: "failure-smoke-race",
              enabled: true,
            },
            raceDeadline,
          ),
        ),
        observeResponse(
          "delete",
          pagePost(
            deletePage,
            `/api/archives/${archiveId}/delete`,
            {
              archiveId,
              consultationId,
              reason: "failure-smoke-race",
            },
            raceDeadline,
          ),
        ),
      ]);
      responses.catch(() => undefined);
      await waitFor(
        "both archive race requests blocked on the durable consultation/archive lock hierarchy",
        async (signal, deadline) => {
          const observations = await withAbsoluteDeadline(
            "archive lock observation",
            deadline,
            async () => {
              const rows = await database.unsafe(`
                SELECT 'Lock' AS "waitEventType",relation.relname AS query
                FROM pg_locks waiting
                JOIN pg_locks held_by_waiter
                  ON held_by_waiter.pid=waiting.pid
                  AND held_by_waiter.locktype='relation'
                  AND held_by_waiter.granted
                JOIN pg_class relation ON relation.oid=held_by_waiter.relation
                WHERE NOT waiting.granted
                  AND waiting.pid <> pg_backend_pid()
                  AND relation.relname IN ('archives','consultations')`);
              return rows;
            },
            signal,
          );
          if (Object.keys(settledResponses).length > 0) {
            throw new Error(
              `archive race request bypassed the durable lock hierarchy: ${JSON.stringify(settledResponses)}`,
            );
          }
          return archiveLockHierarchyBlocked(observations) ? observations : null;
        },
        30_000,
      );
    });
    let hold;
    let deletion;
    try {
      [hold, deletion] = await responses;
    } catch (error) {
      const [activity, archiveState] = await Promise.all([
        database.unsafe(`
          SELECT lock.locktype,lock.mode,lock.granted,relation.relname AS relation
          FROM pg_locks lock
          LEFT JOIN pg_class relation ON relation.oid=lock.relation
          WHERE lock.pid <> pg_backend_pid()
            AND (NOT lock.granted OR relation.relname IN ('archives','consultations'))`),
        queryJson(`
          SELECT a.state,a.hold_operation_id,a.hold_operation_kind,a.hold_operation_owner,
            a.hold_operation_lease_expires_at,
            COALESCE(jsonb_agg(jsonb_build_object('id',h.id,'state',h.state))
              FILTER (WHERE h.id IS NOT NULL AND h.released_at IS NULL),'[]'::jsonb) AS active_holds
          FROM archives a
          LEFT JOIN legal_holds h ON h.archive_id=a.id
          WHERE a.id='${archiveId}'
          GROUP BY a.id`),
      ]);
      throw new Error(
        `archive race requests did not settle: ${JSON.stringify({
          settledResponses,
          activity,
          archiveState,
        })}`,
        { cause: error },
      );
    }
    const outcome = await queryJson(`
      SELECT a.state,
        COALESCE(jsonb_agg(jsonb_build_object('id',h.id,'reason',h.reason))
          FILTER (WHERE h.id IS NOT NULL AND h.released_at IS NULL),'[]'::jsonb) AS active_holds
      FROM archives a
      LEFT JOIN legal_holds h ON h.archive_id=a.id
      WHERE a.id='${archiveId}'
      GROUP BY a.state`);
    const row = outcome[0];
    const activeHolds = row?.active_holds ?? [];
    const winner = archiveRaceWinner({
      holdOk: hold.ok,
      deleteOk: deletion.ok,
      archiveState: row?.state,
      activeHoldCount: activeHolds.length,
    });
    if (!winner) {
      throw new Error(
        `production hold/delete operations did not serialize: ${JSON.stringify({
          hold,
          deletion,
          outcome,
        })}`,
      );
    }
    if (winner === "hold") {
      await reauthenticateForArchive(holdPage, consultationId);
      const release = await pagePost(
        holdPage,
        `/api/archives/${archiveId}/hold`,
        {
          archiveId,
          consultationId,
          holdId: activeHolds[0].id,
          enabled: false,
        },
        operationDeadline(Date.now() + 120_000),
      );
      if (!release.ok) {
        throw new Error(`production hold cleanup failed: ${JSON.stringify(release)}`);
      }
    }
    return {
      archiveState: row.state,
      winner,
      holdStatus: hold.status,
      deleteStatus: deletion.status,
    };
  } finally {
    await Promise.allSettled([holdContext.close(), deleteContext.close()]);
    await browser.close();
  }
}

async function queryJson(statement) {
  const output = await sql(
    `SELECT COALESCE(json_agg(row_to_json(q)), '[]'::json)::text FROM (${statement}) q`,
  );
  return JSON.parse(output || "[]");
}

async function consultationEvidence(consultationId) {
  const rows = await queryJson(`
    SELECT c.id,c.generation,c.state,c.admission_fenced_at,
      a.id AS archive_id,a.state AS archive_state,a.final_inventory_hash,
      (SELECT row_to_json(f) FROM final_inventories f WHERE f.archive_id=a.id) AS inventory,
      COALESCE((SELECT json_agg(json_build_object(
        'id',e.id,'kind',e.effect_kind,'generation',e.generation,'subjectId',e.subject_id,
        'state',e.state,'attempts',e.attempts,'requestHash',e.request_hash,
        'leaseOwner',e.lease_owner,'leaseExpiresAt',e.lease_expires_at,
        'result',e.result,'compensationResult',e.compensation_result
      ) ORDER BY e.created_at) FROM external_effects e WHERE e.consultation_id=c.id),'[]'::json) AS effects,
      COALESCE((SELECT json_agg(json_build_object(
        'kind',j.kind,'state',j.state,'egressId',j.egress_id,'terminalAt',j.terminal_at,
        'terminalResult',j.terminal_result,'expectedArtifactId',j.expected_artifact_id
      ) ORDER BY j.created_at) FROM egress_jobs j WHERE j.consultation_id=c.id),'[]'::json) AS egress,
      COALESCE((SELECT json_agg(json_build_object(
        'id',p.id,'stage',p.stage,'operationId',p.operation_id,
        'attemptNumber',p.attempt_number,'outcome',p.outcome,'terminalAt',p.terminal_at,
        'errorKind',p.error_kind,'errorScope',p.error_scope,
        'providerRetryAdvice',p.provider_retry_advice,
        'retryOf',p.retry_of,'retryDecision',p.retry_decision,
        'accepted',p.accepted_input_watermark,'received',p.received_output_watermark,
        'emitted',p.emitted_output_watermark,'terminalHash',p.terminal_hash,
        'rawRefs',num_nonnulls(p.raw_http,p.raw_websocket,p.raw_grpc)
      ) ORDER BY p.started_at) FROM provider_attempts p
        WHERE p.consultation_id=c.id),'[]'::json) AS attempts,
      COALESCE((SELECT json_agg(json_build_object(
        'workerId',w.worker_id,'generation',w.generation,'epoch',w.epoch,
        'fencedAt',w.fenced_at,'terminalOutcome',w.terminal_outcome,
        'terminalAt',w.terminal_at,'terminalCheckpointId',w.terminal_checkpoint_id
      ) ORDER BY w.epoch) FROM worker_job_epochs w WHERE w.consultation_id=c.id),'[]'::json) AS worker_epochs,
      COALESCE((SELECT json_agg(json_build_object(
        'workerId',r.worker_id,'generation',r.generation,'epoch',r.epoch,
        'heartbeatAt',r.heartbeat_at,'leaseExpiresAt',r.lease_expires_at,
        'fencedAt',r.fenced_at,'fenceReason',r.fence_reason
      ) ORDER BY r.epoch) FROM worker_reservations r WHERE r.consultation_id=c.id),'[]'::json) AS reservations,
      COALESCE((SELECT count(*) FROM expected_archive_artifacts x WHERE x.archive_id=a.id),0)::int AS expected_count,
      COALESCE((SELECT count(*) FROM archive_objects o WHERE o.archive_id=a.id),0)::int AS object_count,
      COALESCE((SELECT count(*) FROM consultation_participants p
        WHERE p.consultation_id=c.id AND p.publication_granted),0)::int AS publication_grants,
      COALESCE((SELECT count(*) FROM external_effects e
        WHERE e.consultation_id=c.id AND e.effect_kind='PARTICIPANT_GRANT'),0)::int
        AS participant_grant_effects,
      COALESCE((SELECT count(*) FROM external_effects e
        WHERE e.consultation_id=c.id AND e.effect_kind='STATUS_PACKET'
          AND e.result->'plan'->>'reasonCode'='CAPTURE_READY'
          AND e.request_bytes IS NOT NULL),0)::int
        AS capture_ready_packets,
      COALESCE((SELECT count(*) FROM outbox o
        WHERE o.aggregate_id=c.id AND o.delivered_at IS NULL),0)::int AS pending_outbox,
      COALESCE((SELECT count(*) FROM outbox o
        WHERE o.aggregate_id=c.id AND o.topic='consultation.cancelled'
          AND o.generation=c.generation AND o.delivered_at IS NULL),0)::int
        AS pending_cancellation_outbox,
      COALESCE((SELECT count(*) FROM external_effects e
        WHERE e.consultation_id=c.id
          AND e.state IN ('planned','calling','applied','compensating')),0)::int
        AS active_effects,
      COALESCE((SELECT count(*) FROM expected_archive_artifacts x
        WHERE x.archive_id=a.id AND x.disposition='expected'
          AND x.fulfilled_object_id IS NULL),0)::int AS unresolved_expectations,
      COALESCE((SELECT count(*) FROM orchestration_deadlines d
        WHERE d.consultation_id=c.id AND d.completed_at IS NULL),0)::int
        AS unfinished_deadlines,
      COALESCE((SELECT count(*) FROM external_effects created
        WHERE created.consultation_id=c.id AND created.effect_kind='WORKER_DISPATCH'
          AND created.result->>'remoteId' IS NOT NULL
          AND created.compensation_result IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM external_effects removed
            WHERE removed.consultation_id=c.id AND removed.effect_kind='DISPATCH_DELETE'
              AND removed.state='done'
              AND removed.result->'plan'->>'dispatchId'=created.result->>'remoteId'
          )),0)::int AS unclean_dispatches,
      COALESCE((SELECT count(*) FROM external_effects created
        WHERE created.consultation_id=c.id AND created.effect_kind='ROOM_CREATE'
          AND created.result->>'remoteId' IS NOT NULL
          AND created.compensation_result IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM external_effects removed
            WHERE removed.consultation_id=c.id AND removed.effect_kind='ROOM_DELETE'
              AND removed.state='done'
              AND (removed.result->'plan'->>'resourceGeneration')::integer=created.generation
          )),0)::int AS unclean_rooms,
      COALESCE((SELECT count(*) FROM egress_jobs j
        WHERE j.consultation_id=c.id
          AND (
            j.terminal_at IS NULL OR j.terminal_result IS NULL
            OR j.state NOT IN ('EGRESS_COMPLETE','EGRESS_FAILED','EGRESS_ABORTED','EGRESS_LIMIT_REACHED')
          )),0)::int AS active_egress,
      COALESCE((SELECT count(*) FROM worker_reservations r
        WHERE r.consultation_id=c.id AND r.fenced_at IS NULL AND r.released_at IS NULL),0)::int
        AS unfenced_reservations,
      COALESCE((SELECT count(*) FROM worker_job_epochs w
        WHERE w.consultation_id=c.id AND w.terminal_at IS NULL),0)::int
        AS unterminated_worker_epochs,
      COALESCE((SELECT json_agg(json_build_object(
        'kind',d.kind,'generation',d.generation,'dueAt',d.due_at,'completedAt',d.completed_at,
        'leaseOwner',d.lease_owner,'leaseExpiresAt',d.lease_expires_at
      ) ORDER BY d.kind) FROM orchestration_deadlines d
        WHERE d.consultation_id=c.id),'[]'::json) AS deadlines,
      NOT EXISTS (
        SELECT 1 FROM external_effects created
        WHERE created.consultation_id=c.id AND created.effect_kind='ROOM_CREATE'
          AND created.result->>'remoteId' IS NOT NULL
          AND created.compensation_result IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM external_effects removed
            WHERE removed.consultation_id=c.id AND removed.effect_kind='ROOM_DELETE'
              AND removed.state='done'
              AND (removed.result->'plan'->>'resourceGeneration')::integer=created.generation
          )
      ) AS room_cleanup_confirmed
    FROM consultations c JOIN archives a ON a.consultation_id=c.id WHERE c.id='${consultationId}'`);
  if (rows.length !== 1) throw new Error(`missing durable consultation ${consultationId}`);
  return rows[0];
}

function isCleanSettlement(evidence) {
  return settlementProblems(evidence).length === 0;
}

function settlementSummary(evidence) {
  return {
    problems: settlementProblems(evidence),
    state: evidence.state,
    generation: evidence.generation,
    archiveState: evidence.archive_state,
    pendingOutbox: evidence.pending_outbox,
    activeEffects: evidence.active_effects,
    unresolvedExpectations: evidence.unresolved_expectations,
    unfinishedDeadlines: evidence.unfinished_deadlines,
    uncleanDispatches: evidence.unclean_dispatches,
    uncleanRooms: evidence.unclean_rooms,
    activeEgress: evidence.active_egress,
    unfencedReservations: evidence.unfenced_reservations,
    unterminatedWorkerEpochs: evidence.unterminated_worker_epochs,
    roomCleanupConfirmed: evidence.room_cleanup_confirmed,
    effects: evidence.effects.slice(0, 10).map((effect) => ({
      kind: effect.kind,
      generation: effect.generation,
      state: effect.state,
      attempts: effect.attempts,
    })),
    egress: evidence.egress.slice(0, 10).map((job) => ({
      kind: job.kind,
      state: job.state,
      terminal: job.terminalAt != null && job.terminalResult != null,
    })),
    workerEpochs: evidence.worker_epochs.slice(0, 10).map((epoch) => ({
      generation: epoch.generation,
      epoch: epoch.epoch,
      terminal: epoch.terminalAt != null,
    })),
    reservations: evidence.reservations.slice(0, 10).map((reservation) => ({
      generation: reservation.generation,
      epoch: reservation.epoch,
      fenced: reservation.fencedAt != null,
    })),
    deadlines: evidence.deadlines.slice(0, 10).map((deadline) => ({
      generation: deadline.generation,
      kind: deadline.kind,
      completed: deadline.completedAt != null,
    })),
    truncated:
      evidence.effects.length > 10 ||
      evidence.egress.length > 10 ||
      evidence.worker_epochs.length > 10 ||
      evidence.reservations.length > 10 ||
      evidence.deadlines.length > 10,
  };
}

async function cancelBeforeStartForCleanup(consultationId) {
  await database.begin(async (transaction) => {
    const rows = await transaction`
      SELECT state,generation FROM consultations WHERE id=${consultationId} FOR UPDATE`;
    const current = rows[0];
    if (!current || current.state === "cancelled") return;
    if (current.state !== "invited" && current.state !== "ready") {
      throw new Error(
        `cleanup cancellation requires invited or ready, received ${String(current.state)}`,
      );
    }
    const generation = Number(current.generation) + 1;
    await transaction`
      UPDATE consultations
      SET state='cancelled',generation=${generation},
        admission_fenced_at=COALESCE(admission_fenced_at,now()),
        updated_at=GREATEST(now(),updated_at+interval '1 microsecond')
      WHERE id=${consultationId}`;
    await transaction`
      UPDATE magic_links SET revoked_at=now()
      WHERE consultation_id=${consultationId}
        AND consumed_at IS NULL AND revoked_at IS NULL`;
    await transaction`
      INSERT INTO outbox(id,topic,aggregate_id,generation,payload,available_at)
      VALUES (${randomUUID()},'consultation.cancelled',${consultationId},${generation},
        jsonb_build_object(
          'resourceGeneration',${current.generation}::integer,
          'consultationId',${consultationId}::uuid,
          'generation',${generation}::integer
        ),now())`;
  });
}

async function forceArchiveReconciliationDeadline(consultationId, generation) {
  await sql(`
    UPDATE archives
    SET reconciliation_deadline_at=now()-interval '1 second'
    WHERE consultation_id='${consultationId}' AND state='reconciling';
    INSERT INTO orchestration_deadlines(
      consultation_id,generation,kind,due_at,completed_at,lease_owner,lease_expires_at
    )
    VALUES (
      '${consultationId}',${generation},'archive-reconcile',
      now()-interval '1 second',NULL,NULL,NULL
    )
    ON CONFLICT (consultation_id,generation,kind) DO UPDATE
    SET due_at=excluded.due_at,completed_at=NULL,lease_owner=NULL,lease_expires_at=NULL
  `);
}

async function settleConsultation(consultationId, { stopAtReconciliation = false } = {}) {
  for (let transition = 0; transition < 6; transition += 1) {
    const evidence = await consultationEvidence(consultationId);
    if (["ended", "cancelled", "deleted"].includes(evidence.state)) {
      if (stopAtReconciliation && evidence.archive_state === "reconciling") {
        return evidence;
      }
      let initialError;
      try {
        return await waitFor(
          `${consultationId} terminal resource settlement`,
          async () => {
            const current = await consultationEvidence(consultationId);
            return isCleanSettlement(current) ? current : null;
          },
          30_000,
        );
      } catch (error) {
        initialError = error;
      }
      let current = await consultationEvidence(consultationId);
      if (current.archive_state === "reconciling") {
        await forceArchiveReconciliationDeadline(consultationId, current.generation);
        try {
          return await waitFor(
            `${consultationId} forced terminal resource settlement`,
            async () => {
              const forced = await consultationEvidence(consultationId);
              return isCleanSettlement(forced) ? forced : null;
            },
            90_000,
          );
        } catch (error) {
          initialError = error;
          current = await consultationEvidence(consultationId);
        }
      }
      throw new Error(
        `terminal resources did not settle: ${JSON.stringify(settlementSummary(current))}`,
        { cause: initialError },
      );
    }
    const generation = evidence.generation;
    if (evidence.state === "invited" || evidence.state === "ready") {
      await cancelBeforeStartForCleanup(consultationId);
    } else if (evidence.state === "active") {
      await sql(`
        UPDATE consultations SET both_absent_since=now()-interval '1 minute'
        WHERE id='${consultationId}' AND state='active';
        INSERT INTO orchestration_deadlines(consultation_id,generation,kind,due_at)
        VALUES ('${consultationId}',${generation},'absence',now()-interval '1 second')
        ON CONFLICT(consultation_id,generation,kind)
        DO UPDATE SET due_at=excluded.due_at,completed_at=NULL,lease_owner=NULL,lease_expires_at=NULL`);
    } else if (evidence.state === "finalizing") {
      await sql(`
        UPDATE consultations SET finalize_deadline_at=now()-interval '1 second'
        WHERE id='${consultationId}' AND state='finalizing';
        INSERT INTO orchestration_deadlines(consultation_id,generation,kind,due_at)
        VALUES ('${consultationId}',${generation},'finalize',now()-interval '1 second')
        ON CONFLICT(consultation_id,generation,kind)
        DO UPDATE SET due_at=excluded.due_at,completed_at=NULL,lease_owner=NULL,lease_expires_at=NULL`);
    } else {
      throw new Error(`cannot settle consultation ${consultationId} from state ${evidence.state}`);
    }
    const previousState = evidence.state;
    try {
      await waitFor(
        `${consultationId} cleanup from ${previousState}`,
        async () => {
          const current = await consultationEvidence(consultationId);
          return current.state !== previousState ? current : null;
        },
        90_000,
      );
    } catch (error) {
      const current = await consultationEvidence(consultationId);
      throw new Error(
        `cleanup did not advance from ${previousState}: ${JSON.stringify(settlementSummary(current))}`,
        { cause: error },
      );
    }
  }
  throw new Error(`consultation ${consultationId} did not release its scenario resources`);
}

async function settleConsultations(consultationIds, concurrency = 3) {
  const ids = [...new Set(consultationIds)];
  if (ids.length === 0) return;
  const failures = [];
  let nextIndex = 0;
  let leaseWrite = Promise.resolve();
  const renewLease = (leaseMs = ownerLeaseMs) => {
    leaseWrite = leaseWrite.then(() => persistOwnerLease(leaseMs));
    return leaseWrite;
  };
  const heartbeat = setInterval(
    () => {
      renewLease().catch((error) => failures.push(error));
    },
    Math.max(1_000, Math.floor(ownerLeaseMs / 3)),
  );
  try {
    const workers = Array.from({ length: Math.min(concurrency, ids.length) }, async () => {
      for (;;) {
        const index = nextIndex;
        nextIndex += 1;
        if (index >= ids.length) return;
        const consultationId = ids[index];
        try {
          await settleConsultation(consultationId);
          await renewLease();
        } catch (error) {
          failures.push(
            new Error(`${consultationId}: ${error?.message ?? String(error)}`, { cause: error }),
          );
        }
      }
    });
    await Promise.all(workers);
    await leaseWrite;
    if (failures.length > 0) {
      await renewLease(0);
      throw new AggregateError(
        failures,
        `failed to settle owned consultations: ${failures
          .map((failure) => failure.message)
          .join("; ")}`,
      );
    }
  } catch (error) {
    await renewLease(0).catch(() => undefined);
    throw error;
  } finally {
    clearInterval(heartbeat);
  }
}

function assertTerminalAttempt(attempt, expectedKind) {
  if (
    !/^[0-9a-f]{64}$/u.test(attempt?.terminalHash ?? "") ||
    !attempt.outcome ||
    !attempt.terminalAt ||
    attempt.rawRefs !== 1 ||
    typeof attempt.retryDecision?.action !== "string"
  ) {
    throw new Error(`provider terminal lacks exact evidence: ${JSON.stringify(attempt)}`);
  }
  if (expectedKind && attempt.errorKind !== expectedKind)
    throw new Error(`expected ${expectedKind}, received ${attempt.errorKind}`);
}

function assertTranslationFailureEvidence(evidence, expected) {
  const attempts = evidence.attempts.filter(
    (attempt) => attempt.stage === "translation" && attempt.errorKind === expected.failure,
  );
  if (attempts.length === 0) {
    throw new Error(`missing ${expected.failure} translation terminal`);
  }
  const attempt = attempts[0];
  assertTerminalAttempt(attempt, expected.failure);
  if (
    attempt.providerRetryAdvice !== expected.advice ||
    attempt.retryDecision?.action !== expected.action ||
    Number(attempt.accepted) !== expected.watermarks.accepted ||
    Number(attempt.received) !== expected.watermarks.received ||
    Number(attempt.emitted) !== expected.watermarks.emitted
  ) {
    throw new Error(
      `${expected.failure} terminal policy/watermark mismatch: ${JSON.stringify(attempt)}`,
    );
  }
  if (attempt.retryDecision?.previousAttemptId !== attempt.id) {
    throw new Error(
      `${expected.failure} retry decision is not bound to its terminal attempt: ` +
        JSON.stringify(attempt),
    );
  }
  const operationAttempts = evidence.attempts.filter(
    (candidate) =>
      candidate.stage === "translation" && candidate.operationId === attempt.operationId,
  );
  const linkedAttempts = operationAttempts.filter((candidate) => candidate.retryOf === attempt.id);
  if (expected.expectRetry) {
    if (operationAttempts.length < 2 || linkedAttempts.length !== 1) {
      throw new Error(
        `${expected.failure} retry policy/relation mismatch: ${JSON.stringify(operationAttempts)}`,
      );
    }
    const orderedAttempts = operationAttempts.toSorted(
      (left, right) => Number(left.attemptNumber) - Number(right.attemptNumber),
    );
    for (let index = 1; index < orderedAttempts.length; index += 1) {
      const previous = orderedAttempts[index - 1];
      const current = orderedAttempts[index];
      if (
        !previous ||
        !current ||
        previous.retryDecision?.action !== "retry" ||
        Number(current.attemptNumber) !== Number(previous.attemptNumber) + 1 ||
        current.operationId !== previous.operationId ||
        current.retryOf !== previous.id
      ) {
        throw new Error(
          `${expected.failure} retry chain is not contiguous: ${JSON.stringify(operationAttempts)}`,
        );
      }
    }
    if (orderedAttempts.at(-1)?.retryDecision?.action === "retry") {
      throw new Error(
        `${expected.failure} retry chain lacks a terminal decision: ${JSON.stringify(operationAttempts)}`,
      );
    }
  } else if (
    operationAttempts.length !== 1 ||
    operationAttempts.some((candidate) => candidate.retryOf !== null)
  ) {
    throw new Error(`${expected.failure} must never retry: ${JSON.stringify(operationAttempts)}`);
  }
  return attempt;
}

async function controlWorkerBaselines() {
  const controls = await Promise.all([
    onlyContainer("control-worker-1"),
    onlyContainer("control-worker-2"),
  ]);
  const baselines = new Map(
    await Promise.all(
      controls.map(async (container) => {
        const details = await inspect(container.Id);
        const workerId = details.Config.Env.find((entry) =>
          entry.startsWith("INSTANCE_ID="),
        )?.slice("INSTANCE_ID=".length);
        if (!workerId) throw new Error(`control worker ${container.Id} has no INSTANCE_ID`);
        return [
          workerId,
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
  return { controls, baselines };
}

async function runEffectBoundaryCrash({ scenario, fault, expectedState, exitCode }) {
  const { baselines } = await controlWorkerBaselines();
  await resetAuthenticationThrottle();
  const crashWatermarkMs = Date.now();
  const run = await runConsultation({ faults: { [fault]: ["ROOM_CREATE"] } });
  const crashedEffect = await waitFor(
    `${scenario} durable boundary`,
    async () => {
      const evidence = await consultationEvidence(run.consultationId);
      return (
        evidence.effects.find(
          (candidate) =>
            candidate.kind === "ROOM_CREATE" &&
            candidate.state === expectedState &&
            typeof candidate.leaseOwner === "string",
        ) ?? null
      );
    },
    60_000,
  );
  const crashedWorker = baselines.get(crashedEffect.leaseOwner);
  if (!crashedWorker) throw new Error(`${scenario} owner was not a live control replica`);
  const restarted = await waitFor(
    `${scenario} exact owner restart`,
    async (signal, deadline) => {
      const [current, exits] = await Promise.all([
        inspect(crashedWorker.containerId, { signal, deadline }),
        containerExitEvents(crashedWorker.containerId, crashWatermarkMs, { signal, deadline }),
      ]);
      const injectedExit = exits.some(
        (event) => String(event.Actor?.Attributes?.exitCode ?? "") === String(exitCode),
      );
      return injectedExit &&
        current.State.Running &&
        (!current.State.Health || current.State.Health.Status === "healthy") &&
        restartCountIncremented(crashedWorker.restartCount, current.RestartCount)
        ? { restartCount: current.RestartCount, startedAt: current.State.StartedAt }
        : null;
    },
    90_000,
  );
  await setFaults();
  const completed = await run.completed;
  if (completed.code !== 0) {
    throw new Error(`${scenario} consultation failed: ${completed.stderr}\n${completed.stdout}`);
  }
  const evidence = await waitFor(
    `${scenario} clean durable settlement`,
    async () => {
      const current = await consultationEvidence(run.consultationId);
      return isCleanSettlement(current) ? current : null;
    },
    120_000,
  );
  const recoveredEffect = evidence.effects.find(
    (candidate) =>
      candidate.kind === "ROOM_CREATE" &&
      candidate.state === "done" &&
      typeof candidate.result?.remoteId === "string",
  );
  if (!recoveredEffect) {
    throw new Error(`${scenario} lacks one durable remote ROOM_CREATE outcome`);
  }
  if (expectedState === "calling" && recoveredEffect.attempts < 2) {
    throw new Error(`${scenario} did not recover by adopting the ambiguous remote success`);
  }
  if (expectedState === "applied" && recoveredEffect.attempts !== crashedEffect.attempts) {
    throw new Error(`${scenario} replayed a remote effect after durable applied evidence`);
  }
  return {
    name: scenario,
    consultationId: run.consultationId,
    remoteId: recoveredEffect.result.remoteId,
    effectAttempts: recoveredEffect.attempts,
    crashedWorkerId: crashedWorker.workerId,
    restartedEpoch: restarted.startedAt,
    restartCount: restarted.restartCount,
    cleanSettlement: true,
  };
}

async function main() {
  const startedAt = Date.now();
  const proof = { shard: cliOptions.shard ?? null, scenarios: [] };
  let primaryError = null;
  const cleanupFailures = [];
  try {
    announcePhase("initialization");
    await initializeCheckpoint();
    await refreshFixtureCapabilities();
    await reapExpiredOwners();
    await persistOwnerLease();
    await setFaults();
    await setWorkerScenario();

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
          const container = controls.find(
            (candidate) => candidate.Id === crashedWorker.containerId,
          );
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
    if (shouldRunScenario("participant-egress-denied")) {
      // Denying Participant Egress after the durable effect row must keep publication
      // blocked and surface a durable retry/failure rather than silently joining.
      await resetAuthenticationThrottle();
      const deniedRun = await runConsultation({
        faults: { failEffects: ["PARTICIPANT_EGRESS"] },
        captureBarrierTimeoutMs: 10_000,
      });
      const denial = await deniedRun.completed;
      const deniedConsultationId = deniedRun.consultationId;
      await setFaults();
      if (denial.code === 0) {
        throw new Error("Participant Egress denial unexpectedly allowed a complete consultation");
      }
      await cancelBeforeStartForCleanup(deniedConsultationId);
      const deniedEvidence = await waitFor(
        "fenced durable Participant Egress failure",
        async () => {
          const evidence = await consultationEvidence(deniedConsultationId);
          return evidence.admission_fenced_at != null &&
            evidence.effects.some(
              (effect) =>
                effect.kind === "PARTICIPANT_EGRESS" &&
                effect.attempts >= 1 &&
                String(effect.result?.error ?? "").includes("test fault denied PARTICIPANT_EGRESS"),
            )
            ? evidence
            : null;
        },
        120_000,
      );
      if (
        deniedEvidence.publication_grants !== 0 ||
        deniedEvidence.participant_grant_effects !== 0 ||
        deniedEvidence.capture_ready_packets !== 0
      ) {
        throw new Error(
          `Participant Egress denial crossed publication barrier: ${JSON.stringify({
            publicationGrants: deniedEvidence.publication_grants,
            participantGrantEffects: deniedEvidence.participant_grant_effects,
            captureReadyPackets: deniedEvidence.capture_ready_packets,
          })}`,
        );
      }
      const deniedEffect = deniedEvidence.effects.find(
        (effect) =>
          effect.kind === "PARTICIPANT_EGRESS" &&
          String(effect.result?.error ?? "").includes("test fault denied PARTICIPANT_EGRESS"),
      );
      if (!deniedEffect?.result || deniedEffect.attempts < 1)
        throw new Error(
          `Participant Egress failure lacks durable terminal detail: ${JSON.stringify(deniedEffect)}`,
        );
      proof.scenarios.push({
        name: "participant-egress-denied",
        consultationId: deniedConsultationId,
        publicationBlocked: true,
        effectAttempts: deniedEffect.attempts,
        durableTerminal: deniedEffect.result,
      });
      await settleConsultation(deniedConsultationId);

      await checkpointScenario("participant-egress-denied");
    }
    // Exercise normalized provider errors and partial synthesis through the actual
    // fixture runtime. Each must degrade/fail rather than produce false complete proof.
    for (const expected of translationFailureCases) {
      const { name, failure } = expected;
      if (!shouldRunScenario(name)) continue;
      await resetAuthenticationThrottle();
      const run = await runConsultation({
        workerScenario: { translation: { failure } },
      });
      const consultationId = run.consultationId;
      const evidence = await waitFor(
        `${failure} provider terminal`,
        async () => {
          const current = await consultationEvidence(consultationId);
          const attempts = current.attempts
            .filter((attempt) => attempt.stage === "translation" && attempt.errorKind === failure)
            .toSorted((left, right) => Number(left.attemptNumber) - Number(right.attemptNumber));
          if (attempts.length === 0) return null;
          const lastAttempt = attempts.at(-1);
          return !expected.expectRetry ||
            (attempts.length >= 2 && lastAttempt?.retryDecision?.action !== "retry")
            ? current
            : null;
        },
        90_000,
      );
      const result = await terminateProcessTree(run);
      if (result.code === 0) throw new Error(`${failure} unexpectedly produced a complete archive`);
      const attempt = assertTranslationFailureEvidence(evidence, expected);
      proof.scenarios.push({
        name,
        consultationId,
        outcome: attempt.outcome,
        retryDecision: attempt.retryDecision,
        terminalHash: attempt.terminalHash,
      });
      await settleConsultation(consultationId);
      await checkpointScenario(name);
    }
    if (shouldRunScenario("tts-partial-finalization")) {
      await resetAuthenticationThrottle();
      const partialRun = await runConsultation({
        workerScenario: { tts: { partialSamples: 960, failAfterPartial: true } },
      });
      const partialConsultationId = partialRun.consultationId;
      const partial = await partialRun.completed;
      if (partial.code === 0)
        throw new Error("partial TTS unexpectedly produced a complete archive");
      const partialEvidence = await waitFor(
        "partial TTS terminal evidence",
        async () => {
          const evidence = await consultationEvidence(partialConsultationId);
          return evidence.attempts.some(
            (attempt) =>
              attempt.stage === "tts" &&
              attempt.outcome === "failed" &&
              Number(attempt.received ?? 0) > 0,
          )
            ? evidence
            : null;
        },
        90_000,
      );
      const partialAttempt = partialEvidence.attempts.find(
        (attempt) =>
          attempt.stage === "tts" &&
          attempt.outcome === "failed" &&
          Number(attempt.received ?? 0) > 0,
      );
      assertTerminalAttempt(partialAttempt, "transport");
      if (partialAttempt.retryDecision?.action === "retry")
        throw new Error(
          `partial output was incorrectly retryable: ${JSON.stringify(partialAttempt)}`,
        );
      proof.scenarios.push({
        name: "tts-partial-output",
        consultationId: partialConsultationId,
        receivedOutputWatermark: partialAttempt.received,
        emittedOutputWatermark: partialAttempt.emitted,
        retryDecision: partialAttempt.retryDecision,
      });
      const reconcilingSettlement = await settleConsultation(partialConsultationId, {
        stopAtReconciliation: true,
      });
      if (
        reconcilingSettlement.state !== "ended" ||
        reconcilingSettlement.archive_state !== "reconciling"
      ) {
        throw new Error(
          `partial TTS did not reach archive reconciliation: ${JSON.stringify(
            settlementSummary(reconcilingSettlement),
          )}`,
        );
      }
      await forceArchiveReconciliationDeadline(
        partialConsultationId,
        reconcilingSettlement.generation,
      );
      const deadlineEvidence = await settleConsultation(partialConsultationId);
      if (
        deadlineEvidence.state !== "ended" ||
        deadlineEvidence.archive_state !== "incomplete" ||
        deadlineEvidence.inventory?.status !== "incomplete" ||
        (deadlineEvidence.inventory?.missing ?? []).length === 0 ||
        deadlineEvidence.egress.length === 0 ||
        !deadlineEvidence.egress.every((job) => job.terminalAt && job.terminalResult)
      ) {
        throw new Error(
          `forced finalization omitted terminal evidence: ${JSON.stringify(
            settlementSummary(deadlineEvidence),
          )}`,
        );
      }
      proof.scenarios.push({
        name: "finalization-deadline",
        consultationId: partialConsultationId,
        consultationState: deadlineEvidence.state,
        archiveState: deadlineEvidence.archive_state,
        explicitGaps: deadlineEvidence.inventory.missing,
        egressTerminals: deadlineEvidence.egress.map((job) => ({
          egressId: job.egressId,
          state: job.state,
          terminalAt: job.terminalAt,
          terminalResult: job.terminalResult,
        })),
      });
      await setWorkerScenario();
      await checkpointScenario("tts-partial-finalization");
    }
    const preservationFenceSelected = shouldRunScenario("preservation-fence");
    const spoolRecoverySelected = shouldRunScenario("spool-durable-recovery");
    if (preservationFenceSelected || spoolRecoverySelected) {
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
          const evidence = await consultationEvidence(preservationConsultationId);
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
      const preservation = await preservationRun.completed;
      if (preservation.code === 0)
        throw new Error("WAL/SQLite fault unexpectedly produced a complete archive");
      await waitFor(
        "exact worker container restart after preservation failure",
        async (signal, deadline) => {
          const restartCount = (await inspect(workerBefore.Id, { signal, deadline })).RestartCount;
          return restartCountIncremented(workerRestartBaseline, restartCount)
            ? { containerId: workerBefore.Id, restartCount }
            : null;
        },
        120_000,
      );
      const beforeFence = await consultationEvidence(preservationConsultationId);
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
          const evidence = await consultationEvidence(preservationConsultationId);
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
        throw new Error(
          `supervisor fence omitted checkpoint evidence: ${JSON.stringify(fencedEpoch)}`,
        );
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
      const pendingCheckpointDeliveries = spoolRecoverySelected
        ? await waitFor(
            "durable checkpoint delivery before drainer restart",
            async () => {
              const deliveries = checkpointDeliveries(preservationConsultationId);
              return deliveries.some((delivery) => delivery.acknowledged === 0)
                ? deliveries.filter((delivery) => delivery.acknowledged === 0)
                : null;
            },
            30_000,
          )
        : [];
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
      if (spoolRecoverySelected) {
        const pendingIds = new Set(
          pendingCheckpointDeliveries.map((delivery) => delivery.checkpointId),
        );
        await waitFor(
          "exact checkpoint delivery replay after drainer restart",
          async () => {
            const deliveries = checkpointDeliveries(preservationConsultationId);
            const replayed = deliveries.filter((delivery) => pendingIds.has(delivery.checkpointId));
            return pendingIds.size > 0 &&
              replayed.length === pendingIds.size &&
              replayed.every((delivery) => delivery.acknowledged === 1)
              ? deliveries
              : null;
          },
          60_000,
        );
      }
      await sql(`DELETE FROM outbox WHERE id='${staleHeartbeatId}'`);
      const settled = await settleConsultation(preservationConsultationId);
      const duplicateEffects = await queryJson(`
        SELECT occurrence_key,generation,count(*)::int AS copies
        FROM external_effects
        WHERE consultation_id='${preservationConsultationId}'
        GROUP BY occurrence_key,generation
        HAVING count(*) > 1`);
      if (
        duplicateEffects.length > 0 ||
        settled.pending_outbox !== 0 ||
        settled.active_effects !== 0
      ) {
        throw new Error(
          `spool recovery did not converge exactly once: ${JSON.stringify({
            duplicateEffects,
            settlement: settlementSummary(settled),
          })}`,
        );
      }
      proof.scenarios.push({
        name: spoolRecoverySelected ? "spool-durable-recovery" : "preservation-fence",
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
        replayedCheckpointIds: pendingCheckpointDeliveries.map((delivery) => delivery.checkpointId),
        supervisorRestarted: true,
        drainerRestarted: true,
        duplicateEffects: 0,
        pendingDelivery: settled.pending_outbox,
        cleanSettlement: true,
      });
      if (preservationFenceSelected) await checkpointScenario("preservation-fence");
      if (spoolRecoverySelected) await checkpointScenario("spool-durable-recovery");
    }
    if (shouldRunScenario("minio-inflight-recovery")) {
      await resetAuthenticationThrottle();
      const minioBefore = await onlyContainer("minio");
      const minioStateBefore = await inspect(minioBefore.Id);
      const run = await runConsultation({
        faults: { holdAfterPersistCalling: ["ARCHIVE_RECONCILE"] },
      });
      const reconciliationHoldMarker = `${faultFile}.${run.consultationId}.ARCHIVE_RECONCILE.calling-owner`;
      const reconciling = await waitFor(
        "archive reconciliation held before its remote call",
        async () => {
          const evidence = await consultationEvidence(run.consultationId);
          if (evidence.archive_state !== "reconciling") return null;
          try {
            await stat(reconciliationHoldMarker);
            return evidence;
          } catch {
            return null;
          }
        },
        120_000,
      );
      const reconciliationAttemptsBeforeOutage = new Map(
        reconciling.effects
          .filter((effect) => effect.kind === "ARCHIVE_RECONCILE")
          .map((effect) => [effect.id, effect.attempts]),
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
      const outageEvidence = await consultationEvidence(run.consultationId);
      if (outageEvidence.archive_state !== "reconciling") {
        throw new Error("archive completed before the MinIO outage could exercise reconciliation");
      }
      const failedReconciliation = await waitFor(
        "archive reconciliation retries after the MinIO outage",
        async () => {
          const evidence = await consultationEvidence(run.consultationId);
          return (
            evidence.effects.find(
              (effect) =>
                effect.kind === "ARCHIVE_RECONCILE" &&
                effect.attempts > (reconciliationAttemptsBeforeOutage.get(effect.id) ?? 0),
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
      const settled = await waitFor(
        "MinIO recovery clean settlement",
        async () => {
          const evidence = await consultationEvidence(run.consultationId);
          return isCleanSettlement(evidence) ? evidence : null;
        },
        120_000,
      );
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
        throw new Error(
          "MinIO-dependent reconciliation did not retry to completion after recovery",
        );
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
            workerState.Health?.Log?.some((entry) =>
              healthFailureAfter(entry, healthWatermarkMs),
            ) === true;
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

    if (proof.scenarios.length === 0) {
      throw new Error("failure-smoke produced no scenario evidence");
    }

    announcePhase("proof-emission");
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
  } catch (error) {
    primaryError = new Error(
      `[failure-smoke] phase ${activePhase} failed: ${error?.message ?? String(error)}`,
      { cause: error },
    );
    console.error(primaryError.message);
  } finally {
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
