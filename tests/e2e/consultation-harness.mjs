import { randomUUID } from "node:crypto";
import { pollWithinDeadline, remainingDeadlineMs, withinDeadline } from "./harness-contracts.mjs";

const valueOptionNames = new Set([
  "--base-url",
  "--capture-barrier-timeout-ms",
  "--deadline-epoch-ms",
  "--expected-profile",
  "--expected-profile-revision",
  "--failure-harness-release-file",
  "--failure-harness-release-timeout-ms",
  "--livekit-url",
  "--mailpit-url",
]);
const booleanOptionNames = new Set([
  "--emit-proof-json",
  "--skip-audible-interpretation-proof",
  "--skip-media-output-proof",
]);

export function createConsultationHarness(argv = process.argv, environment = process.env) {
  const parsedOptions = new Map();
  const parsedFlags = new Set();
  for (let index = 2; index < argv.length; index += 1) {
    const argument = argv[index];
    if (booleanOptionNames.has(argument)) {
      if (parsedFlags.has(argument)) throw new Error(`duplicate option ${argument}`);
      parsedFlags.add(argument);
      continue;
    }
    if (!valueOptionNames.has(argument)) {
      throw new Error(`unknown or positional argument ${JSON.stringify(argument)}`);
    }
    if (parsedOptions.has(argument)) throw new Error(`duplicate option ${argument}`);
    const value = argv[index + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new Error(`${argument} requires a value`);
    }
    parsedOptions.set(argument, value);
    index += 1;
  }

  function option(name, fallback) {
    return parsedOptions.get(name) ?? fallback;
  }
  function requireValue(value, name) {
    if (!value) throw new Error(`${name} is required`);
    return value;
  }

  const deadlineEpochText = option("--deadline-epoch-ms", environment.SCENARIO_DEADLINE_EPOCH_MS);
  const deadlineEpochMs = Number(deadlineEpochText);
  if (!Number.isSafeInteger(deadlineEpochMs) || deadlineEpochMs <= Date.now()) {
    throw new Error("--deadline-epoch-ms must be a future Unix epoch in milliseconds");
  }
  function bounded(label, operation, cancel) {
    return withinDeadline(deadlineEpochMs, label, operation, { cancel });
  }
  function boundedPage(page, label, operation) {
    return bounded(label, operation, () => page.close({ runBeforeUnload: false }));
  }
  function boundedPages(pages, label, operation) {
    return bounded(label, operation, () =>
      Promise.allSettled(pages.map((page) => page.close({ runBeforeUnload: false }))),
    );
  }
  function boundedContext(context, label, operation) {
    return bounded(label, operation, () => context.close());
  }
  function boundedBrowser(browser, label, operation) {
    return bounded(label, operation, () => browser.close());
  }
  function closeIgnoringFailure(resource) {
    Promise.resolve(resource.close()).catch(() => {});
  }
  function deadlineTimeout(maximumMs = Number.POSITIVE_INFINITY) {
    return Math.min(maximumMs, remainingDeadlineMs(deadlineEpochMs));
  }
  function poll(label, operation, timeoutMs = 120_000, intervalMs = 1_000) {
    const localDeadline = Math.min(Date.now() + timeoutMs, deadlineEpochMs);
    return pollWithinDeadline(localDeadline, label, operation, { intervalMs });
  }

  const baseUrl = option("--base-url", environment.BASE_URL ?? "http://web:3000");
  const mailpitUrl = option("--mailpit-url", environment.MAILPIT_URL ?? "http://mailpit:8025");
  const expectedProfile = option("--expected-profile", environment.EXPECTED_PROFILE ?? "fixture");
  const expectedLiveKitUrl = option(
    "--livekit-url",
    environment.LIVEKIT_URL ?? "ws://livekit:7880",
  );
  const fixtureMinimumCompleteObjectCount = 5;
  const archivePageCeiling = 10_000;
  const archiveObjectCeiling = 1_000_000;
  const objectDownloadTimeoutMs = 30_000;
  const captureBarrierTimeoutMs = Number.parseInt(
    option("--capture-barrier-timeout-ms", "90000"),
    10,
  );
  if (!Number.isSafeInteger(captureBarrierTimeoutMs) || captureBarrierTimeoutMs <= 0) {
    throw new Error("--capture-barrier-timeout-ms must be a positive integer");
  }
  const expectedProfileRevisionText = option("--expected-profile-revision", null);
  const expectedProfileRevision =
    expectedProfileRevisionText === null ? null : Number(expectedProfileRevisionText);
  if (
    expectedProfileRevision !== null &&
    (!Number.isSafeInteger(expectedProfileRevision) || expectedProfileRevision < 1)
  ) {
    throw new Error("--expected-profile-revision must be a positive integer");
  }
  const allowedProvidersByProfile = {
    fixture: new Set(["fixture"]),
    "google-eu": new Set(["google"]),
    "google-speech-eu": new Set(["google"]),
    "deepgram-deepl-eu": new Set(["deepgram", "deepl"]),
  };
  const emitProof = parsedFlags.has("--emit-proof-json") || environment.EMIT_PROOF_JSON === "true";
  const skipAudibleInterpretationProof = parsedFlags.has("--skip-audible-interpretation-proof");
  const skipMediaOutputProof = parsedFlags.has("--skip-media-output-proof");
  const failureHarnessReleaseFile = option("--failure-harness-release-file", null);
  const failureHarnessReleaseTimeoutMs = Number.parseInt(
    option("--failure-harness-release-timeout-ms", "120000"),
    10,
  );
  if (
    failureHarnessReleaseFile &&
    (!Number.isSafeInteger(failureHarnessReleaseTimeoutMs) || failureHarnessReleaseTimeoutMs <= 0)
  ) {
    throw new Error("--failure-harness-release-timeout-ms must be a positive integer");
  }
  const employeeEmail = requireValue(environment.E2E_EMPLOYEE_EMAIL, "E2E_EMPLOYEE_EMAIL");
  const configuredRunId = environment.E2E_RUN_ID;
  const runId = configuredRunId === undefined ? randomUUID() : configuredRunId;
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(runId)) {
    throw new Error("E2E_RUN_ID must be a non-empty UUID");
  }

  return {
    allowedProvidersByProfile,
    archiveObjectCeiling,
    archivePageCeiling,
    baseUrl,
    bounded,
    boundedBrowser,
    boundedContext,
    boundedPage,
    boundedPages,
    captureBarrierTimeoutMs,
    closeIgnoringFailure,
    customerEmail: `customer-${runId}@example.test`,
    deadlineEpochMs,
    deadlineTimeout,
    emitProof,
    employeeEmail,
    expectedLiveKitUrl,
    expectedProfile,
    expectedProfileRevision,
    failureHarnessReleaseFile,
    failureHarnessReleaseTimeoutMs,
    fixtureMinimumCompleteObjectCount,
    mailpitUrl,
    objectDownloadTimeoutMs,
    poll,
    requireValue,
    runId,
    skipAudibleInterpretationProof,
    skipMediaOutputProof,
    startedAt: Date.now(),
  };
}
