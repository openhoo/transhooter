import { Database } from "bun:sqlite";
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
import http from "node:http";
import { dirname } from "node:path";
import { pollUntil, runWithDeadline } from "../harness-contracts.mjs";

export function installRuntime(ctx) {
  const {
    dockerResponseLimit,
    harnessDeadlineMs,
    checkpointBinding,
    resumeRequested,
    checkpointFile,
    scenarioRegistry,
    selectedScenarios,
    completedScenarios,
    scenarioStartedAt,
    scenarioDurations,
    checkpointedConsultations,
    checkpointedReleaseFiles,
    configLockFile,
    ownerDirectory,
    ownerId,
    ownerFile,
    ownerLeaseMs,
    faultFile,
    workerScenarioFile,
    spoolDrainerScenarioFile,
    consultationRuns,
    trackedConsultations,
  } = ctx;
  const settleConsultations = (...args) => ctx.settleConsultations(...args);
  const isCleanSettlement = (...args) => ctx.isCleanSettlement(...args);
  const consultationEvidence = (...args) => ctx.consultationEvidence(...args);
  function spoolDatabase() {
    return new Database("/var/lib/transhooter/spool/journal.sqlite3", { readonly: true });
  }
  function checkpointDeliveries(spool, meetingId) {
    return spool
      .query(
        `SELECT d.checkpoint_id AS checkpointId, d.control_event_id AS controlEventId,
          d.delivery_state AS deliveryState, d.error_kind AS errorKind,
          r.ordinal AS recordOrdinal
        FROM checkpoint_deliveries d
        JOIN records r ON r.object_id = d.record_id
        WHERE d.meeting_id = ?
        ORDER BY r.ordinal, d.checkpoint_id`,
      )
      .all(meetingId);
  }
  function spoolConsultationState(meetingId) {
    const spool = spoolDatabase();
    try {
      return spool.transaction(() => {
        const handoffs = spool
          .query(
            `SELECT meeting_id AS meetingId, generation, worker_id AS workerId,
              worker_epoch AS workerEpoch, write_epoch AS writeEpoch, state, reason
            FROM consultation_handoffs WHERE meeting_id = ?`,
          )
          .all(meetingId);
        const seals = spool
          .query(
            `SELECT seal_id AS sealId, terminal_outcome AS terminalOutcome,
              evidence_ordinal AS evidenceOrdinal, completion_event_id AS completionEventId,
              completion_state AS completionState
            FROM consultation_seals WHERE meeting_id = ?`,
          )
          .all(meetingId);
        const failedRecords = spool
          .query(
            `SELECT stage, state, error_kind AS errorKind, ordinal
            FROM records
            WHERE meeting_id = ? AND state IN ('permanent', 'quarantined')
            ORDER BY ordinal`,
          )
          .all(meetingId);
        return {
          handoffs,
          seals,
          failedRecords,
          checkpoints: checkpointDeliveries(spool, meetingId),
        };
      })();
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
                new Error(
                  `Docker ${method} ${path} response exceeded ${dockerResponseLimit} bytes`,
                ),
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
    completedScenarios.clear();
    for (const name of completed) completedScenarios.add(name);
  }

  function announcePhase(name) {
    ctx.activePhase = name;
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
        if (value === null) {
          delete consultations[consultationId];
        } else {
          consultations[consultationId] = value;
        }
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
      await deleteConsultationsFromMap(spoolDrainerScenarioFile, expiredIds);
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
  async function spoolDrainerScenarioEntry(consultationId) {
    const document = JSON.parse(await readFile(spoolDrainerScenarioFile, "utf8"));
    return document.consultations?.[consultationId] ?? null;
  }
  async function setSpoolDrainerScenario(consultationId = null, scenario = null) {
    await updateConsultationMap(
      spoolDrainerScenarioFile,
      consultationId,
      consultationId ? scenario : null,
    );
  }

  async function setWorkerScenario(consultationId = null, scenario = {}) {
    await updateConsultationMap(
      workerScenarioFile,
      consultationId,
      consultationId ? scenario : null,
    );
  }

  Object.assign(ctx, {
    checkpointDeliveries,
    operationDeadline,
    withAbsoluteDeadline,
    docker,
    containers,
    onlyContainer,
    inspect,
    containerExitEvents,
    stop,
    start,
    rerunOneShot,
    waitFor,
    writeJsonFile,
    initializeCheckpoint,
    announcePhase,
    shouldRunScenario,
    checkpointScenario,
    withConfigLock,
    updateConsultationMap,
    deleteConsultationsFromMap,
    persistOwnerLease,
    reapExpiredOwners,
    setFaults,
    setWorkerScenario,
    setSpoolDrainerScenario,
    spoolDrainerScenarioEntry,
    spoolConsultationState,
  });
}
