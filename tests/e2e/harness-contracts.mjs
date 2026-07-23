export class ScenarioDeadlineError extends Error {
  constructor(label, deadlineEpochMs) {
    super(`${label} exceeded absolute scenario deadline ${deadlineEpochMs}`);
    this.name = "ScenarioDeadlineError";
    this.deadlineEpochMs = deadlineEpochMs;
  }
}

export function remainingDeadlineMs(deadlineEpochMs, now = Date.now()) {
  if (!Number.isSafeInteger(deadlineEpochMs) || deadlineEpochMs <= 0) {
    throw new TypeError("deadlineEpochMs must be a positive integer Unix epoch");
  }
  const remaining = deadlineEpochMs - now;
  if (remaining <= 0) throw new ScenarioDeadlineError("operation", deadlineEpochMs);
  return remaining;
}

export async function withinDeadline(
  deadlineEpochMs,
  label,
  operation,
  { cancel, now = Date.now, schedule = setTimeout, unschedule = clearTimeout } = {},
) {
  const timeoutMs = remainingDeadlineMs(deadlineEpochMs, now());
  const controller = new AbortController();
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = schedule(() => {
      const error = new ScenarioDeadlineError(label, deadlineEpochMs);
      controller.abort(error);
      try {
        const cancellation = cancel?.(error);
        Promise.resolve(cancellation).catch(() => {});
      } catch {
        // The deadline error remains authoritative when cancellation itself fails.
      }
      reject(error);
    }, timeoutMs);
  });
  const pending = Promise.resolve().then(() =>
    operation({ signal: controller.signal, timeoutMs, deadlineEpochMs }),
  );
  pending.catch(() => {});
  try {
    return await Promise.race([pending, timeout]);
  } finally {
    unschedule(timer);
  }
}

export async function pollWithinDeadline(
  deadlineEpochMs,
  label,
  operation,
  { intervalMs = 1_000, now = Date.now, schedule = setTimeout, unschedule = clearTimeout } = {},
) {
  if (!Number.isSafeInteger(intervalMs) || intervalMs < 0) {
    throw new TypeError("intervalMs must be a non-negative safe integer");
  }
  // Attempt errors can contain signed URLs, so terminal polling diagnostics exclude them.
  while (true) {
    try {
      const value = await withinDeadline(deadlineEpochMs, `${label} attempt`, operation, {
        now,
        schedule,
        unschedule,
      });
      if (value) return value;
    } catch (error) {
      if (error instanceof ScenarioDeadlineError) {
        throw new ScenarioDeadlineError(`Timed out waiting for ${label}`, deadlineEpochMs);
      }
      // Transient attempt failures are retried until the one absolute deadline.
    }

    let remainingMs;
    try {
      remainingMs = remainingDeadlineMs(deadlineEpochMs, now());
    } catch (error) {
      if (error instanceof ScenarioDeadlineError) {
        throw new ScenarioDeadlineError(`Timed out waiting for ${label}`, deadlineEpochMs);
      }
      throw error;
    }
    const delayMs = Math.min(intervalMs, remainingMs);
    try {
      await withinDeadline(
        deadlineEpochMs,
        `${label} retry delay`,
        ({ signal }) =>
          new Promise((resolve, reject) => {
            const timer = schedule(resolve, delayMs);
            signal.addEventListener(
              "abort",
              () => {
                unschedule(timer);
                reject(signal.reason);
              },
              { once: true },
            );
          }),
        { now, schedule, unschedule },
      );
    } catch (error) {
      if (error instanceof ScenarioDeadlineError) {
        throw new ScenarioDeadlineError(`Timed out waiting for ${label}`, deadlineEpochMs);
      }
      throw error;
    }
  }
}

export function hasCompleteArchiveEvidence(archive) {
  if (
    archive?.status !== "complete" ||
    !Array.isArray(archive.gaps) ||
    archive.gaps.length !== 0 ||
    typeof archive.inventoryVersion !== "string" ||
    archive.inventoryVersion.length === 0 ||
    typeof archive.inventorySha256 !== "string" ||
    !/^[0-9a-f]{64}$/u.test(archive.inventorySha256) ||
    !Array.isArray(archive.egressIds) ||
    archive.egressIds.length === 0 ||
    !Array.isArray(archive.providerAttemptIds) ||
    archive.providerAttemptIds.length === 0 ||
    !Array.isArray(archive.providerAttemptGroups) ||
    archive.providerAttemptGroups.length === 0
  ) {
    return false;
  }
  const nonemptyUniqueStrings = (values) =>
    values.every((value) => typeof value === "string" && value.length > 0) &&
    new Set(values).size === values.length;
  if (
    !nonemptyUniqueStrings(archive.egressIds) ||
    !nonemptyUniqueStrings(archive.providerAttemptIds)
  ) {
    return false;
  }
  return archive.providerAttemptGroups.every(
    (group) =>
      typeof group?.provider === "string" &&
      group.provider.length > 0 &&
      typeof group.direction === "string" &&
      group.direction.length > 0 &&
      ["stt", "translation", "tts"].includes(group.stage) &&
      Array.isArray(group.attemptIds) &&
      group.attemptIds.length > 0 &&
      nonemptyUniqueStrings(group.attemptIds),
  );
}

export const MODE_GAIN_PAIRS = Object.freeze([
  Object.freeze(["Interpreted", 0, 1]),
  Object.freeze(["Overlay", 0.18, 1]),
  Object.freeze(["Original", 1, 0]),
]);

export function acceptedCaptionMatchesRender({
  candidate,
  consultationId,
  destinationParticipantId,
  sourceParticipantId,
  sourceLanguage,
  targetLanguage,
  renderedTranslation,
  renderedSource,
  finalAnnouncement,
  otherDisplayName,
}) {
  if (
    candidate?.schemaVersion !== 1 ||
    candidate.finality !== "final" ||
    candidate.consultationId !== consultationId ||
    candidate.destinationParticipantId !== destinationParticipantId ||
    candidate.sourceParticipantId !== sourceParticipantId ||
    candidate.sourceLanguage !== sourceLanguage ||
    candidate.targetLanguage !== targetLanguage ||
    typeof candidate.sourceText !== "string" ||
    candidate.sourceText.trim() === "" ||
    typeof candidate.translatedText !== "string" ||
    candidate.translatedText.trim() === ""
  ) {
    return false;
  }
  return (
    renderedTranslation === candidate.translatedText &&
    renderedSource === candidate.sourceText &&
    finalAnnouncement ===
      `Final translation from ${otherDisplayName}, ${sourceLanguage} to ${targetLanguage}: ${candidate.translatedText}`
  );
}
