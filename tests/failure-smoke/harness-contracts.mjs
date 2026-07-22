function deadlineError(label, deadline) {
  return new Error(`${label} exceeded absolute deadline ${new Date(deadline).toISOString()}`);
}

export async function runWithDeadline(label, deadline, operation, parentSignal) {
  if (!Number.isFinite(deadline)) return await operation(parentSignal, deadline);
  const controller = new AbortController();
  const abortFromParent = () =>
    controller.abort(parentSignal?.reason ?? new Error(`${label} was cancelled`));
  if (parentSignal?.aborted) abortFromParent();
  else parentSignal?.addEventListener("abort", abortFromParent, { once: true });
  const timer = setTimeout(
    () => controller.abort(deadlineError(label, deadline)),
    Math.max(0, deadline - Date.now()),
  );
  const aborted = new Promise((_, reject) => {
    if (controller.signal.aborted) reject(controller.signal.reason);
    else {
      controller.signal.addEventListener("abort", () => reject(controller.signal.reason), {
        once: true,
      });
    }
  });
  try {
    return await Promise.race([
      Promise.resolve().then(() => operation(controller.signal, deadline)),
      aborted,
    ]);
  } finally {
    clearTimeout(timer);
    parentSignal?.removeEventListener("abort", abortFromParent);
  }
}

const transientPollErrorCodes = new Set([
  "EAI_AGAIN",
  "ECONNREFUSED",
  "ECONNRESET",
  "EHOSTUNREACH",
  "ENETDOWN",
  "ENETRESET",
  "ENETUNREACH",
  "EPIPE",
  "ETIMEDOUT",
  "UND_ERR_CONNECT_TIMEOUT",
  "UND_ERR_HEADERS_TIMEOUT",
  "UND_ERR_SOCKET",
]);

export function isTransientPollError(error) {
  const visited = new Set();
  for (let current = error; current && !visited.has(current); current = current.cause) {
    visited.add(current);
    if (transientPollErrorCodes.has(current.code)) return true;
    if (current.name === "TimeoutError") return true;
    if (/ check exceeded absolute deadline /u.test(` ${current.message ?? ""} `)) return true;
  }
  return false;
}

export async function pollUntil(
  label,
  check,
  { deadline, signal, intervalMs = 1_000, attemptMs = 15_000 },
) {
  let last;
  while (Date.now() < deadline) {
    if (signal?.aborted) throw signal.reason;
    const attemptDeadline = Math.min(deadline, Date.now() + attemptMs);
    try {
      last = await runWithDeadline(
        `${label} check`,
        attemptDeadline,
        (attemptSignal) => check(attemptSignal, attemptDeadline),
        signal,
      );
      if (last) return last;
    } catch (error) {
      if (signal?.aborted) throw signal.reason;
      if (!isTransientPollError(error)) throw error;
      last = error;
    }
    await runWithDeadline(
      `${label} polling delay`,
      deadline,
      (delaySignal) =>
        new Promise((resolve, reject) => {
          const timer = setTimeout(
            resolve,
            Math.min(intervalMs, Math.max(0, deadline - Date.now())),
          );
          delaySignal.addEventListener(
            "abort",
            () => {
              clearTimeout(timer);
              reject(delaySignal.reason);
            },
            { once: true },
          );
        }),
      signal,
    );
  }
  const detail = last instanceof Error ? last.message : String(last ?? "no observation");
  throw new Error(`timed out waiting for ${label}: ${detail}`);
}

export function crashRecoveryMatches(expected, observation) {
  return (
    typeof expected?.workerId === "string" &&
    expected.workerId.length > 0 &&
    typeof expected?.containerId === "string" &&
    expected.containerId.length > 0 &&
    typeof expected?.startedAt === "string" &&
    observation?.workerId === expected.workerId &&
    observation?.containerId === expected.containerId &&
    restartCountIncremented(expected.restartCount, observation?.restartCount) &&
    typeof observation?.startedAt === "string" &&
    observation.startedAt !== expected.startedAt
  );
}

export function archiveLockHierarchyBlocked(observations) {
  if (!Array.isArray(observations)) return false;
  const lockQueries = observations
    .filter((entry) => entry?.waitEventType === "Lock" && typeof entry?.query === "string")
    .map((entry) => entry.query);
  return (
    lockQueries.some((query) => /\barchives\b/iu.test(query)) &&
    lockQueries.some((query) => /\bconsultations\b/iu.test(query))
  );
}

const terminalConsultationStates = new Set(["ended", "cancelled", "deleted"]);

export function settlementProblems(evidence) {
  const problems = [];
  if (!terminalConsultationStates.has(evidence?.state)) problems.push("consultation");
  for (const [field, label] of [
    ["pending_outbox", "outbox"],
    ["active_effects", "effects"],
    ["unfinished_deadlines", "deadlines"],
    ["active_egress", "egress"],
    ["unclean_dispatches", "dispatches"],
    ["unclean_rooms", "rooms"],
    ["unfenced_reservations", "worker-reservations"],
    ["unterminated_worker_epochs", "worker-epochs"],
  ]) {
    if (!Number.isFinite(Number(evidence?.[field])) || Number(evidence[field]) !== 0) {
      problems.push(label);
    }
  }
  if (
    (!Number.isFinite(Number(evidence?.unresolved_expectations)) ||
      Number(evidence.unresolved_expectations) !== 0) &&
    evidence?.archive_state !== "incomplete"
  ) {
    problems.push("archive-expectations");
  }
  if (evidence?.room_cleanup_confirmed !== true) problems.push("room-cleanup");
  return problems;
}

export function restartCountIncremented(baseline, current) {
  return Number.isInteger(baseline) && Number.isInteger(current) && current > baseline;
}

export function workerCrashMatches(expected, reservation, terminalEpoch) {
  if (
    typeof expected?.workerId !== "string" ||
    expected.workerId.length === 0 ||
    !Number.isInteger(expected.generation) ||
    !Number.isInteger(expected.epoch)
  ) {
    return false;
  }
  return (
    reservation?.workerId === expected.workerId &&
    Number(reservation?.generation) === expected.generation &&
    Number(reservation?.epoch) === expected.epoch &&
    terminalEpoch?.workerId === expected.workerId &&
    Number(terminalEpoch?.generation) === expected.generation &&
    Number(terminalEpoch?.epoch) === expected.epoch &&
    terminalEpoch?.terminalAt != null
  );
}

export function healthFailureAfter(entry, watermarkMs) {
  const exitCode = Number(entry?.ExitCode);
  const endedAt = Date.parse(entry?.End ?? "");
  return (
    Number.isInteger(exitCode) &&
    exitCode !== 0 &&
    Number.isFinite(endedAt) &&
    Number.isFinite(watermarkMs) &&
    endedAt >= watermarkMs
  );
}

export function createBarrier(parties) {
  if (!Number.isInteger(parties) || parties < 1) {
    throw new Error("barrier parties must be a positive integer");
  }
  let arrivals = 0;
  const release = Promise.withResolvers();
  return Object.freeze({
    async arrive() {
      arrivals += 1;
      if (arrivals > parties) throw new Error("barrier received too many arrivals");
      if (arrivals === parties) release.resolve();
      await release.promise;
    },
  });
}

export function archiveRaceWinner({ holdOk, deleteOk, archiveState, activeHoldCount }) {
  if (holdOk && !deleteOk && archiveState === "complete" && activeHoldCount === 1) {
    return "hold";
  }
  if (
    deleteOk &&
    !holdOk &&
    (archiveState === "deleting" || archiveState === "deleted") &&
    activeHoldCount === 0
  ) {
    return "delete";
  }
  return null;
}
