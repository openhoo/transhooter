import { createHash, randomUUID } from "node:crypto";
import { lookup } from "node:dns";
import { access } from "node:fs/promises";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import { chromium } from "playwright";
import {
  acceptedCaptionMatchesRender,
  hasCompleteArchiveEvidence,
  MODE_GAIN_PAIRS,
  pollWithinDeadline,
  remainingDeadlineMs,
  withinDeadline,
} from "./harness-contracts.mjs";

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
const parsedOptions = new Map();
const parsedFlags = new Set();
for (let index = 2; index < process.argv.length; index += 1) {
  const argument = process.argv[index];
  if (booleanOptionNames.has(argument)) {
    if (parsedFlags.has(argument)) throw new Error(`duplicate option ${argument}`);
    parsedFlags.add(argument);
    continue;
  }
  if (!valueOptionNames.has(argument)) {
    throw new Error(`unknown or positional argument ${JSON.stringify(argument)}`);
  }
  if (parsedOptions.has(argument)) throw new Error(`duplicate option ${argument}`);
  const value = process.argv[index + 1];
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
const deadlineEpochText = option("--deadline-epoch-ms", process.env.SCENARIO_DEADLINE_EPOCH_MS);
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

const baseUrl = option("--base-url", process.env.BASE_URL ?? "http://web:3000");
const mailpitUrl = option("--mailpit-url", process.env.MAILPIT_URL ?? "http://mailpit:8025");
const expectedProfile = option("--expected-profile", process.env.EXPECTED_PROFILE ?? "fixture");
const expectedLiveKitUrl = option("--livekit-url", process.env.LIVEKIT_URL ?? "ws://livekit:7880");
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
  "deepgram-deepl-eu": new Set(["deepgram", "deepl"]),
};
const emitProof = parsedFlags.has("--emit-proof-json") || process.env.EMIT_PROOF_JSON === "true";
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
const employeeEmail = requireValue(process.env.E2E_EMPLOYEE_EMAIL, "E2E_EMPLOYEE_EMAIL");
const configuredRunId = process.env.E2E_RUN_ID;
const runId = configuredRunId === undefined ? randomUUID() : configuredRunId;
if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(runId)) {
  throw new Error("E2E_RUN_ID must be a non-empty UUID");
}
const customerEmail = `customer-${runId}@example.test`;
const startedAt = Date.now();

async function latestLink(recipient, { signal } = {}) {
  const response = await fetch(`${mailpitUrl}/api/v1/messages?limit=100`, { signal });
  if (!response.ok) throw new Error(`Mailpit list failed: ${response.status}`);
  const payload = await response.json();
  const messages = payload.messages ?? payload.Messages ?? [];
  const candidate = messages.find((message) => {
    const recipients = message.To ?? message.to ?? [];
    return (
      recipients.some((entry) => (entry.Address ?? entry.address) === recipient) &&
      Date.parse(message.Created ?? message.created ?? 0) >= startedAt - 1_000
    );
  });
  if (!candidate) return null;
  const id = candidate.ID ?? candidate.Id ?? candidate.id;
  const detailResponse = await fetch(`${mailpitUrl}/api/v1/message/${encodeURIComponent(id)}`, {
    signal,
  });
  if (!detailResponse.ok) throw new Error(`Mailpit message failed: ${detailResponse.status}`);
  const detail = await detailResponse.json();
  const content = `${detail.HTML ?? detail.Html ?? ""}\n${detail.Text ?? detail.text ?? ""}`;
  const match = content.match(/https?:\/\/[^\s"'<>]+\/auth\/exchange\?[^\s"'<>]+/);
  return match?.[0]?.replaceAll("&amp;", "&") ?? null;
}
function internalizeLink(link) {
  const parsed = new URL(link);
  const base = new URL(baseUrl);
  parsed.protocol = base.protocol;
  parsed.host = base.host;
  return parsed.toString();
}
async function authenticate(context, email, existingLink = null) {
  return await boundedContext(context, `authenticate ${email}`, async () => {
    const page = await context.newPage();
    if (!existingLink) {
      await page.goto(`${baseUrl}/sign-in`);
      await page.getByLabel("Email address").fill(email);
      await page.getByRole("button", { name: "Email me a sign-in link" }).click();
      await page.getByRole("status").filter({ hasText: "If this address can sign in" }).waitFor();
    }
    const link =
      existingLink ??
      (await poll("magic link delivery", ({ signal }) => latestLink(email, { signal })));
    await page.goto(internalizeLink(link), { waitUntil: "domcontentloaded" });
    await page.getByRole("button", { name: "Continue securely" }).click();
    await page.waitForURL(/\/consultations(?:\?|$)/);
    const cookies = await context.cookies();
    for (const required of ["session", "csrf"]) {
      if (!cookies.some((cookie) => cookie.name === required && cookie.value)) {
        throw new Error(`${required} cookie was not issued`);
      }
    }
    return page;
  });
}
async function createConsultation(page, customerName, customerAddress) {
  return await boundedPage(page, `create consultation for ${customerAddress}`, async () => {
    await page.goto(`${baseUrl}/consultations/new`);
    await page.getByLabel("Customer name").fill(customerName);
    await page.getByLabel("Customer email").fill(customerAddress);
    const profileValue = await page
      .getByLabel("Translation provider profile")
      .locator("option")
      .filter({ hasText: expectedProfile })
      .getAttribute("value");
    if (!profileValue) throw new Error(`provider profile ${expectedProfile} is unavailable`);
    await page.getByLabel("Translation provider profile").selectOption(profileValue);
    const creationResponsePromise = page.waitForResponse(
      (response) =>
        response.url().endsWith("/api/consultations") && response.request().method() === "POST",
    );
    await page.getByRole("button", { name: "Create and send invitation" }).click();
    const creationResponse = await creationResponsePromise;
    if (!creationResponse.ok()) {
      throw new Error(`consultation creation failed: ${creationResponse.status()}`);
    }
    const created = await creationResponse.json();
    return requireValue(created.id, "created consultation id");
  });
}
async function savePreferences(page, displayName, language) {
  await page.getByRole("button", { name: "Preview camera and microphone" }).click();
  await Promise.all([
    page.getByLabel("Microphone").locator("option").first().waitFor({ state: "attached" }),
    page.getByLabel("Camera").locator("option").first().waitFor({ state: "attached" }),
  ]);
  await page.getByLabel("Display name").fill(displayName);
  await page.getByLabel("Your spoken language").selectOption(language);
  const microphones = await page.getByLabel("Microphone").locator("option").count();
  const cameras = await page.getByLabel("Camera").locator("option").count();
  if (microphones < 1 || cameras < 1) throw new Error("fake media devices were not enumerated");
  const responsePromise = page.waitForResponse(
    (response) => response.url().includes("/preferences") && response.request().method() === "POST",
  );
  await page.getByRole("button", { name: "Save and continue" }).click();
  const response = await responsePromise;
  if (!response.ok()) {
    throw new Error(`preference save failed: ${response.status()} ${await response.text()}`);
  }
}
async function consentAndJoin(page, consultationId) {
  const consentCheckbox = page.getByRole("checkbox", { name: /I have read and agree/ });
  await consentCheckbox.waitFor({ timeout: 60_000 });
  const consentText = await page.locator("main").innerText();
  if (!consentText.toLowerCase().includes(expectedProfile.toLowerCase())) {
    throw new Error(
      `consent did not freeze expected profile ${expectedProfile}: ${JSON.stringify(consentText)}`,
    );
  }
  if (!/region|eu/i.test(consentText)) throw new Error("consent did not disclose provider region");
  await consentCheckbox.check();

  const responseTimeout = deadlineTimeout(90_000);
  const consentResponsePromise = page.waitForResponse(
    (response) =>
      new URL(response.url()).pathname.endsWith("/consent") &&
      response.request().method() === "POST",
    { timeout: responseTimeout },
  );
  const joinResponsePromise = page.waitForResponse(
    (response) =>
      new URL(response.url()).pathname.endsWith("/join") && response.request().method() === "POST",
    { timeout: responseTimeout },
  );
  // Keep pre-installed observers handled if clicking or an earlier request fails.
  void consentResponsePromise.catch(() => {});
  void joinResponsePromise.catch(() => {});

  await page.getByRole("button", { name: "Agree and join" }).click();

  const boundedString = (value, maximum) =>
    typeof value === "string" ? value.slice(0, maximum) : "<unavailable>";
  const boundedApiError = async (response) => {
    let body = {};
    try {
      body = await response.json();
    } catch {}
    return {
      code: boundedString(body?.code, 128),
      message: boundedString(body?.message, 512),
    };
  };

  const consentResponse = await consentResponsePromise;
  if (!consentResponse.ok()) {
    throw new Error(
      `consent POST failed: HTTP ${consentResponse.status()} ${JSON.stringify(
        await boundedApiError(consentResponse),
      )}`,
    );
  }

  const joinResponse = await joinResponsePromise;
  let joinDiagnostic = { status: joinResponse.status() };
  if (!joinResponse.ok()) {
    const error = await boundedApiError(joinResponse);
    joinDiagnostic = { status: joinResponse.status(), code: error.code };
    const expectedConsentRace = joinResponse.status() === 409 && error.code === "CONSENT_REQUIRED";
    if (!expectedConsentRace) {
      throw new Error(`join POST failed: HTTP ${joinResponse.status()} ${JSON.stringify(error)}`);
    }
  }

  try {
    await page.waitForURL(/\/consultations\/[0-9a-f-]+\/room/, {
      timeout: deadlineTimeout(90_000),
    });
  } catch (cause) {
    const lobby = await apiJson(page, `/api/consultations/${consultationId}`);
    const navigationDiagnostic = {
      join: joinDiagnostic,
      pathname: boundedString(new URL(page.url()).pathname, 512),
      lobby: {
        status: lobby.status,
        phase: boundedString(lobby.body?.phase, 128),
        redirectPresent: typeof lobby.body?.redirectTo === "string",
      },
    };
    throw new Error(`room navigation timed out: ${JSON.stringify(navigationDiagnostic)}`, {
      cause,
    });
  }
}
async function enterRoom(page) {
  await page.getByRole("button", { name: "Enter room" }).click();
  await page.getByLabel("Live consultation").waitFor({ timeout: 60_000 });
  await page
    .getByText("Media enabled", { exact: true })
    .waitFor({ timeout: captureBarrierTimeoutMs });
  await page.getByText("Recording and secure storage active", { exact: true }).waitFor();
}
async function audioGains(page) {
  return await page.locator("audio").evaluateAll((elements) =>
    elements.map((element) => ({
      volume: element.volume,
      attached:
        element.srcObject instanceof MediaStream &&
        element.srcObject.getAudioTracks().some((track) => track.readyState === "live"),
    })),
  );
}

async function assertGainPair(page, label, expectedOriginal, expectedInterpretation) {
  await poll(
    `${label} audio gains`,
    async () => {
      const media = await audioGains(page);
      if (media.length !== 2) return null;
      const [original, interpretation] = media;
      return Math.abs(original.volume - expectedOriginal) < 0.001 &&
        Math.abs(interpretation.volume - expectedInterpretation) < 0.001
        ? media
        : null;
    },
    5_000,
    50,
  );
}

async function assertModes(page) {
  await page
    .getByText("Interpretation reconnecting — original audio remains available.", { exact: true })
    .waitFor({ state: "hidden", timeout: 60_000 });
  for (const [mode, originalGain, interpretationGain] of MODE_GAIN_PAIRS) {
    const button = page.getByRole("button", { name: mode });
    await button.click();
    if ((await button.getAttribute("aria-pressed")) !== "true") {
      throw new Error(`${mode} mode did not become selected`);
    }
    await assertGainPair(page, mode, originalGain, interpretationGain);
  }
  const media = await audioGains(page);
  if (media.some((element) => !element.attached)) {
    throw new Error("original and interpretation audio were not attached");
  }

  await page.getByRole("button", { name: "Interpreted" }).click();
  await assertGainPair(page, "interpreted before fallback", 0, 1);
  await page.locator("audio").nth(1).dispatchEvent("stalled");
  await page
    .getByText("Interpretation reconnecting — original audio remains available.", { exact: true })
    .waitFor({ state: "visible" });
  await assertGainPair(page, "interpretation fallback", 1, 0);
}
async function assertAudibleInterpretation(page) {
  return await boundedPage(page, "audible interpretation proof", ({ timeoutMs }) =>
    page.evaluate(
      async ({ timeoutMs }) => {
        const element = document.querySelectorAll("audio")[1];
        if (!(element instanceof HTMLAudioElement)) {
          throw new Error("interpretation audio element is absent");
        }
        const deadline = Date.now() + Math.min(90_000, timeoutMs);
        const context = new AudioContext();
        const analyser = context.createAnalyser();
        analyser.fftSize = 1024;
        const samples = new Float32Array(analyser.fftSize);
        let observedStream = null;
        let source = null;
        try {
          if (context.state === "suspended") await context.resume();
          while (Date.now() < deadline) {
            const stream = element.srcObject;
            const liveAudio =
              stream instanceof MediaStream &&
              stream
                .getAudioTracks()
                .some((track) => track.readyState === "live" && track.enabled && !track.muted);
            if (liveAudio && stream !== observedStream) {
              source?.disconnect();
              source = context.createMediaStreamSource(stream);
              source.connect(analyser);
              observedStream = stream;
            }
            if (liveAudio && source && !element.paused && !element.muted && element.volume > 0) {
              analyser.getFloatTimeDomainData(samples);
              if (samples.some((sample) => Math.abs(sample) > 0.002)) return true;
            }
            await new Promise((resolve) =>
              setTimeout(resolve, Math.min(50, Math.max(0, deadline - Date.now()))),
            );
          }
          if (!(element.srcObject instanceof MediaStream)) {
            throw new Error("interpretation MediaStream was never attached");
          }
          throw new Error("interpretation track remained silent");
        } finally {
          source?.disconnect();
          await context.close();
        }
      },
      { timeoutMs: Math.min(timeoutMs, 90_000) },
    ),
  );
}
async function apiJson(page, path) {
  return await boundedPage(page, `browser GET ${path}`, ({ timeoutMs }) =>
    page.evaluate(
      async ({ path, timeoutMs }) => {
        const response = await fetch(path, {
          credentials: "same-origin",
          cache: "no-store",
          signal: AbortSignal.timeout(timeoutMs),
        });
        const text = await response.text();
        return { status: response.status, body: text ? JSON.parse(text) : null };
      },
      { path, timeoutMs },
    ),
  );
}

async function postApi(page, path, body = {}) {
  return await boundedPage(page, `browser POST ${path}`, ({ timeoutMs }) =>
    page.evaluate(
      async ({ path, body, timeoutMs }) => {
        const csrf = document.cookie
          .split("; ")
          .find((part) => part.startsWith("csrf="))
          ?.slice(5);
        if (!csrf) throw new Error("cleanup CSRF cookie is unavailable");
        const response = await fetch(path, {
          method: "POST",
          credentials: "same-origin",
          cache: "no-store",
          headers: {
            "content-type": "application/json",
            "x-csrf-token": csrf,
          },
          body: JSON.stringify(body),
          signal: AbortSignal.timeout(timeoutMs),
        });
        return { status: response.status, text: await response.text() };
      },
      { path, body, timeoutMs },
    ),
  );
}

async function settleCreatedConsultation(page, consultationId) {
  await page.goto(`${baseUrl}/consultations`, { waitUntil: "domcontentloaded" });
  await poll(
    `cleanup settlement for ${consultationId}`,
    async () => {
      const current = await apiJson(page, `/api/consultations/${consultationId}`);
      if (current.status === 404) return true;
      if (current.status !== 200) {
        throw new Error(`cleanup consultation lookup returned ${current.status}`);
      }
      const state = current.body?.state;
      if (["ended", "cancelled", "deleted"].includes(state)) return true;
      if (state === "invited" || state === "ready") {
        const response = await postApi(page, `/api/consultations/${consultationId}/cancel`, {
          consultationId,
        });
        if (![200, 202, 204, 409].includes(response.status)) {
          throw new Error(`cleanup cancel returned ${response.status}: ${response.text}`);
        }
      } else if (state === "active") {
        const response = await postApi(page, `/api/consultations/${consultationId}/end`, {
          consultationId,
        });
        if (![200, 202, 204, 409].includes(response.status)) {
          throw new Error(`cleanup end returned ${response.status}: ${response.text}`);
        }
      } else if (state !== "finalizing") {
        throw new Error(`cleanup cannot settle consultation in unexpected state ${String(state)}`);
      }
      return null;
    },
    120_000,
    1_000,
  );
}

async function assertFrozenProfile(page, consultationId) {
  const result = await poll("frozen provider profile", async () => {
    const response = await apiJson(page, `/api/consultations/${consultationId}`);
    return response.status === 200 && response.body?.directions?.length === 2
      ? response.body
      : null;
  });
  if (result.profileName !== expectedProfile) {
    throw new Error(
      `consultation froze profile ${String(result.profileName)} instead of ${expectedProfile}`,
    );
  }
  if (!Number.isInteger(result.profileRevision) || result.profileRevision < 1) {
    throw new Error(`frozen profile revision is invalid: ${String(result.profileRevision)}`);
  }
  if (expectedProfileRevision !== null && result.profileRevision !== expectedProfileRevision) {
    throw new Error(
      `frozen profile revision ${String(result.profileRevision)} did not match leased revision ` +
        String(expectedProfileRevision),
    );
  }
  const directionalKeys = new Set();
  for (const direction of result.directions) {
    for (const field of [
      "sourceLabel",
      "destinationLabel",
      "speech",
      "translation",
      "voice",
      "region",
    ]) {
      if (typeof direction[field] !== "string" || !direction[field].trim()) {
        throw new Error(`frozen direction omitted ${field}: ${JSON.stringify(direction)}`);
      }
    }
    if (!direction.speech.includes(" · ") || !direction.translation.includes(" · ")) {
      throw new Error(
        `frozen direction omitted provider/model evidence: ${JSON.stringify(direction)}`,
      );
    }
    if (direction.voice === "Original audio") {
      throw new Error(`translated language pair unexpectedly froze a TTS bypass`);
    }
    directionalKeys.add(`${direction.sourceLabel}->${direction.destinationLabel}`);
  }
  if (directionalKeys.size !== 2) {
    throw new Error(`frozen profile did not expose two inverse directions`);
  }
  return result;
}

async function installCaptionProbe(context) {
  await context.addInitScript(() => {
    const originalParse = JSON.parse;
    Object.defineProperty(globalThis, "__transhooterCaptionPackets", {
      configurable: false,
      value: [],
      writable: false,
    });
    JSON.parse = function parseWithCaptionProbe(...arguments_) {
      const value = Reflect.apply(originalParse, this, arguments_);
      if (
        value &&
        typeof value === "object" &&
        value.schemaVersion === 1 &&
        (value.finality === "provisional" || value.finality === "final") &&
        typeof value.consultationId === "string" &&
        typeof value.destinationParticipantId === "string"
      ) {
        globalThis.__transhooterCaptionPackets.push(structuredClone(value));
      }
      return value;
    };
  });
}

function canonicalLiveKitUrl(value) {
  const url = new URL(value);
  if (url.hostname === "rtc.localhost") url.hostname = "livekit";
  return url.toString().replace(/\/$/u, "");
}

async function assertFinalTargetedCaption(
  page,
  consultationId,
  expectedSourceLanguage,
  expectedTargetLanguage,
) {
  const room = await poll("room participant contract", async () => {
    const result = await apiJson(page, `/api/consultations/${consultationId}/room`);
    return result.status === 200 ? result.body : null;
  });
  if (canonicalLiveKitUrl(room.liveKitUrl) !== canonicalLiveKitUrl(expectedLiveKitUrl)) {
    throw new Error(
      `room advertised ${String(room.liveKitUrl)}, which does not route to ${expectedLiveKitUrl}`,
    );
  }
  const packet = await poll(
    "rendered final targeted caption",
    async () => {
      const candidate = await page.evaluate(
        ({ expectedConsultationId, destinationParticipantId, sourceParticipantId }) => {
          const packets = globalThis.__transhooterCaptionPackets ?? [];
          return (
            packets.findLast(
              (value) =>
                value.schemaVersion === 1 &&
                value.finality === "final" &&
                value.consultationId === expectedConsultationId &&
                value.destinationParticipantId === destinationParticipantId &&
                value.sourceParticipantId === sourceParticipantId,
            ) ?? null
          );
        },
        {
          expectedConsultationId: consultationId,
          destinationParticipantId: room.participantId,
          sourceParticipantId: room.otherParticipantId,
        },
      );
      const ribbon = page.getByLabel("Current translated and source caption");
      const lines = await ribbon.locator("p").allTextContents();
      const expectedAnnouncement = `Final translation from ${room.otherDisplayName}, ${expectedSourceLanguage} to ${expectedTargetLanguage}: ${candidate?.translatedText}`;
      const finalAnnouncement = await page
        .getByText(expectedAnnouncement, { exact: true })
        .textContent()
        .catch(() => null);
      return acceptedCaptionMatchesRender({
        candidate,
        consultationId,
        destinationParticipantId: room.participantId,
        sourceParticipantId: room.otherParticipantId,
        sourceLanguage: expectedSourceLanguage,
        targetLanguage: expectedTargetLanguage,
        renderedTranslation: lines[0],
        renderedSource: lines[1],
        finalAnnouncement,
        otherDisplayName: room.otherDisplayName,
      })
        ? candidate
        : null;
    },
    90_000,
    250,
  );
  if (
    !Number.isInteger(packet.revision) ||
    packet.revision < 1 ||
    !Number.isInteger(packet.sourceSampleStart) ||
    !Number.isInteger(packet.sourceSampleEnd) ||
    packet.sourceSampleEnd <= packet.sourceSampleStart
  ) {
    throw new Error(
      `rendered final caption lacks monotonic revision/sample evidence: ${JSON.stringify(packet)}`,
    );
  }
  return packet;
}

function archiveObjectGroup(objectClass) {
  if (objectClass.includes("composite")) return "composite";
  if (objectClass.includes("participant") || objectClass.includes("original")) return "original";
  if (objectClass.includes("interpret") || objectClass.includes("tts")) return "interpretation";
  if (objectClass.includes("caption") || objectClass.includes("vtt")) return "captions";
  if (objectClass.includes("inventory") || objectClass.includes("checkpoint")) return "inventory";
  return "pipeline";
}

async function allArchiveObjects(page, archiveId) {
  const objects = [];
  const seenCursors = new Set();
  let cursor = null;
  let pageCount = 0;
  do {
    if (pageCount >= archivePageCeiling) {
      throw new Error(`archive pagination exceeded ${archivePageCeiling} pages`);
    }
    pageCount += 1;
    const suffix = cursor ? `?cursor=${encodeURIComponent(cursor)}` : "";
    const result = await apiJson(page, `/api/archives/${archiveId}/objects${suffix}`);
    if (result.status !== 200 || !Array.isArray(result.body?.objects)) {
      throw new Error(`archive object listing failed: ${result.status}`);
    }
    if (objects.length + result.body.objects.length > archiveObjectCeiling) {
      throw new Error(`archive object listing exceeded ${archiveObjectCeiling} objects`);
    }
    for (const object of result.body.objects) {
      const objectClass = object.objectClass ?? object.object_class;
      objects.push({
        id: object.id,
        key: object.key,
        group: archiveObjectGroup(objectClass),
        label: objectClass,
        contentType: object.contentType ?? object.content_type,
        size: Number(object.size),
        sha256: object.sha256,
        s3Checksum: object.s3Checksum ?? object.s3_checksum,
        versionId: object.versionId ?? object.version_id,
      });
    }
    cursor = result.body.cursor;
    if (cursor !== null && cursor !== undefined) {
      if (typeof cursor !== "string" || cursor.length === 0) {
        throw new Error("archive pagination returned an invalid cursor");
      }
      if (seenCursors.has(cursor)) {
        throw new Error(`archive pagination repeated cursor ${cursor}`);
      }
      seenCursors.add(cursor);
    }
  } while (cursor);
  if (new Set(objects.map(({ id }) => id)).size !== objects.length) {
    throw new Error("archive object pagination returned duplicate object IDs");
  }
  return objects;
}

async function presignedObjectUrl(page, archiveId, objectId) {
  return await boundedPage(page, `authorize archive object ${objectId}`, ({ timeoutMs }) =>
    page.evaluate(
      async ({ id, archiveObjectId, timeoutMs }) => {
        const csrf = document.cookie
          .split("; ")
          .find((part) => part.startsWith("csrf="))
          ?.slice(5);
        if (!csrf) throw new Error("CSRF cookie is unavailable for archive verification");
        const response = await fetch(`/api/archives/${id}/download`, {
          method: "POST",
          credentials: "same-origin",
          cache: "no-store",
          headers: {
            "content-type": "application/json",
            "x-csrf-token": decodeURIComponent(csrf),
          },
          body: JSON.stringify({ archiveId: id, objectId: archiveObjectId }),
          signal: AbortSignal.timeout(timeoutMs),
        });
        const body = await response.json();
        if (!response.ok || typeof body.url !== "string") {
          throw new Error(`archive download authorization failed (${response.status})`);
        }
        return body.url;
      },
      { id: archiveId, archiveObjectId: objectId, timeoutMs },
    ),
  );
}

const crc64NvmeTable = Object.freeze(
  Array.from({ length: 256 }, (_, index) => {
    let value = BigInt(index);
    for (let bit = 0; bit < 8; bit += 1) {
      value = (value & 1n) === 1n ? (value >> 1n) ^ 0x9a6c9329ac4bc9b5n : value >> 1n;
    }
    return value;
  }),
);

function updateCrc64Nvme(crc, chunk) {
  let current = crc;
  for (const byte of chunk) {
    current = (current >> 8n) ^ crc64NvmeTable[Number((current ^ BigInt(byte)) & 0xffn)];
  }
  return current;
}

function encodeCrc64Nvme(crc) {
  let value = crc ^ 0xffffffffffffffffn;
  const bytes = Buffer.allocUnsafe(8);
  for (let index = 7; index >= 0; index -= 1) {
    bytes[index] = Number(value & 0xffn);
    value >>= 8n;
  }
  return bytes.toString("base64");
}

function crc32Table(polynomial) {
  return Object.freeze(
    Array.from({ length: 256 }, (_, index) => {
      let value = index;
      for (let bit = 0; bit < 8; bit += 1) {
        value = (value & 1) === 1 ? (value >>> 1) ^ polynomial : value >>> 1;
      }
      return value >>> 0;
    }),
  );
}

const crc32IeeeTable = crc32Table(0xedb88320);
const crc32cTable = crc32Table(0x82f63b78);

function updateCrc32(crc, chunk, table) {
  let current = crc;
  for (const byte of chunk) {
    current = (table[(current ^ byte) & 0xff] ^ (current >>> 8)) >>> 0;
  }
  return current;
}

function encodeCrc32(crc) {
  const bytes = Buffer.allocUnsafe(4);
  bytes.writeUInt32BE((crc ^ 0xffffffff) >>> 0);
  return bytes.toString("base64");
}

function headerValue(headers, name) {
  const value = headers[name];
  return Array.isArray(value) ? value[0] : value;
}
function canonicalBase64(value, byteLength) {
  try {
    const decoded = Buffer.from(value, "base64");
    return decoded.length === byteLength && decoded.toString("base64") === value;
  } catch {
    return false;
  }
}

function validS3Checksum(value) {
  if (canonicalBase64(value, 8)) return true;
  const separator = value.indexOf(":");
  if (separator < 1 || separator !== value.lastIndexOf(":")) return false;
  const algorithm = value.slice(0, separator);
  const encoded = value.slice(separator + 1);
  const byteLength = {
    CRC32: 4,
    CRC32C: 4,
    SHA256: 32,
  }[algorithm];
  return byteLength !== undefined && canonicalBase64(encoded, byteLength);
}

function checksumEvidence(declared, downloaded) {
  if (!declared.includes(":")) {
    return {
      computed: downloaded.checksums.crc64nvme,
      response: headerValue(downloaded.headers, "x-amz-checksum-crc64nvme"),
    };
  }
  const separator = declared.indexOf(":");
  const algorithm = declared.slice(0, separator);
  const headers = {
    CRC32: "x-amz-checksum-crc32",
    CRC32C: "x-amz-checksum-crc32c",
    SHA256: "x-amz-checksum-sha256",
  };
  const computed = {
    CRC32: downloaded.checksums.crc32,
    CRC32C: downloaded.checksums.crc32c,
    SHA256: downloaded.checksums.sha256,
  };
  const header = headers[algorithm];
  const digest = computed[algorithm];
  if (header === undefined || digest === undefined) {
    throw new Error(`unsupported S3 checksum algorithm: ${algorithm}`);
  }
  const responseDigest = headerValue(downloaded.headers, header);
  return {
    computed: `${algorithm}:${digest}`,
    response: responseDigest === undefined ? undefined : `${algorithm}:${responseDigest}`,
  };
}

function download(url, declaredSize, mapLocalhostToMinio = false, captureBody = false) {
  let outgoing;
  return bounded(
    "presigned archive object GET",
    ({ signal, timeoutMs }) =>
      new Promise((resolve, reject) => {
        const parsed = new URL(url);
        const request = parsed.protocol === "https:" ? httpsRequest : httpRequest;
        const operationTimeoutMs = Math.min(objectDownloadTimeoutMs, timeoutMs);
        const options = {
          headers: { "x-amz-checksum-mode": "ENABLED" },
          signal,
          ...(mapLocalhostToMinio
            ? {
                lookup(hostname, lookupOptions, callback) {
                  lookup(hostname === "localhost" ? "minio" : hostname, lookupOptions, callback);
                },
              }
            : {}),
        };
        let settled = false;
        const finish = (operation, value) => {
          if (settled) return;
          settled = true;
          operation(value);
        };
        outgoing = request(parsed, options, (response) => {
          let size = 0;
          const sha256 = createHash("sha256");
          let crc64Nvme = 0xffffffffffffffffn;
          let crc32 = 0xffffffff;
          let crc32c = 0xffffffff;
          const body = captureBody ? [] : null;
          response.on("data", (chunk) => {
            size += chunk.length;
            if (size > declaredSize) {
              outgoing.destroy(
                new Error(`presigned object GET exceeded declared size ${String(declaredSize)}`),
              );
              return;
            }
            sha256.update(chunk);
            crc64Nvme = updateCrc64Nvme(crc64Nvme, chunk);
            crc32 = updateCrc32(crc32, chunk, crc32IeeeTable);
            crc32c = updateCrc32(crc32c, chunk, crc32cTable);
            body?.push(chunk);
          });
          response.on("aborted", () =>
            finish(reject, new Error("presigned object GET response was aborted")),
          );
          response.on("error", (error) => finish(reject, error));
          response.on("end", () => {
            if (response.statusCode !== 200) {
              finish(
                reject,
                new Error(`presigned object GET failed (${String(response.statusCode)})`),
              );
              return;
            }
            if (size === 0) {
              finish(reject, new Error("presigned archive object body is empty"));
              return;
            }
            const sha256Hex = sha256.digest("hex");
            finish(resolve, {
              size,
              sha256: sha256Hex,
              checksums: {
                crc64nvme: encodeCrc64Nvme(crc64Nvme),
                crc32: encodeCrc32(crc32),
                crc32c: encodeCrc32(crc32c),
                sha256: Buffer.from(sha256Hex, "hex").toString("base64"),
              },
              headers: response.headers,
              body: body === null ? null : Buffer.concat(body, size),
            });
          });
        });
        outgoing.setTimeout(operationTimeoutMs, () => {
          outgoing.destroy(
            new Error(`presigned object GET timed out after ${operationTimeoutMs}ms`),
          );
        });
        outgoing.on("error", (error) => finish(reject, error));
        outgoing.end();
      }),
    (error) => outgoing?.destroy(error),
  );
}

async function independentlyVerifyObject(page, archiveId, object, captureBody = false) {
  const url = await presignedObjectUrl(page, archiveId, object.id);
  let downloaded;
  try {
    downloaded = await download(url, object.size, false, captureBody);
  } catch (error) {
    if (new URL(url).hostname !== "localhost") throw error;
    downloaded = await download(url, object.size, true, captureBody);
  }
  const checksum = checksumEvidence(object.s3Checksum, downloaded);
  const metadataHash = headerValue(downloaded.headers, "x-amz-meta-sha256");
  const responseVersion = headerValue(downloaded.headers, "x-amz-version-id");
  const responseChecksum = checksum.response;
  const contentLength = headerValue(downloaded.headers, "content-length");
  const contentType = headerValue(downloaded.headers, "content-type");
  if (
    downloaded.size !== object.size ||
    Number(contentLength) !== object.size ||
    downloaded.sha256 !== object.sha256 ||
    checksum.computed !== object.s3Checksum ||
    responseChecksum !== object.s3Checksum ||
    (metadataHash !== undefined && metadataHash !== object.sha256) ||
    contentType !== object.contentType ||
    responseVersion !== object.versionId
  ) {
    throw new Error(
      `independent object verification failed for ${object.id}: ` +
        JSON.stringify({
          expectedSize: object.size,
          downloadedSize: downloaded.size,
          contentLength,
          expectedContentType: object.contentType,
          responseContentType: contentType,
          expectedHash: object.sha256,
          actualHash: downloaded.sha256,
          metadataHash,
          expectedChecksum: object.s3Checksum,
          actualChecksum: checksum.computed,
          responseChecksum,
          expectedVersion: object.versionId,
          responseVersion,
        }),
    );
  }
  return downloaded;
}

async function independentlyVerifyObjects(page, archiveId, objects, concurrency = 4) {
  const failures = new Array(objects.length);
  let nextIndex = 0;
  const workers = Array.from({ length: Math.min(concurrency, objects.length) }, async () => {
    for (;;) {
      const index = nextIndex;
      nextIndex += 1;
      if (index >= objects.length) return;
      const object = objects[index];
      try {
        await independentlyVerifyObject(page, archiveId, object);
      } catch (error) {
        failures[index] = new Error(`${object.id}: ${error?.message ?? String(error)}`, {
          cause: error,
        });
      }
    }
  });
  await Promise.all(workers);
  const objectFailures = failures.filter(Boolean);
  if (objectFailures.length > 0) {
    throw new AggregateError(
      objectFailures,
      `independent archive verification failed for object IDs: ${objectFailures
        .map((failure) => failure.message.split(":", 1)[0])
        .join(", ")}`,
    );
  }
  return objects.length;
}

function archiveObjectProof(object) {
  return {
    id: object.id ?? object.objectId,
    key: object.key,
    label: object.label ?? object.objectClass ?? object.class,
    versionId: object.versionId,
    size: Number(object.size),
    sha256: object.sha256,
    s3Checksum: object.s3Checksum ?? object.checksum,
    contentType: object.contentType,
  };
}

function assertFinalInventoryBinding(
  archive,
  consultationId,
  listedObjects,
  finalObject,
  inventory,
) {
  if (
    inventory?.status !== "complete" ||
    !Array.isArray(inventory.objects) ||
    inventory.objects.length === 0 ||
    !Array.isArray(inventory.missing) ||
    !Array.isArray(inventory.errors)
  ) {
    throw new Error("downloaded final inventory has an invalid terminal shape");
  }
  if (inventory.missing.length !== 0 || inventory.errors.length !== 0) {
    throw new Error(
      `complete final inventory contains missing/errors: ${JSON.stringify({
        missing: inventory.missing,
        errors: inventory.errors,
      })}`,
    );
  }
  if ((archive.gaps ?? []).length !== inventory.missing.length) {
    throw new Error("archive detail gaps diverge from downloaded final inventory");
  }
  if (inventory.consultationId !== consultationId) {
    throw new Error("downloaded final inventory belongs to another consultation");
  }
  const listedMembers = listedObjects.filter((object) => object.id !== finalObject.id);
  const listedById = new Map(
    listedMembers.map((object) => {
      const proof = archiveObjectProof(object);
      return [proof.id, proof];
    }),
  );
  const inventoryProofs = inventory.objects.map(archiveObjectProof);
  if (
    listedById.size !== listedMembers.length ||
    new Set(inventoryProofs.map((object) => object.id)).size !== inventoryProofs.length ||
    inventoryProofs.length !== listedMembers.length
  ) {
    throw new Error(
      "final inventory and downloaded listing do not have the same unique object IDs",
    );
  }
  for (const proof of inventoryProofs) {
    const listed = listedById.get(proof.id);
    if (!listed || JSON.stringify(listed) !== JSON.stringify(proof)) {
      throw new Error(
        `final inventory object does not exactly bind listing object ${String(proof.id)}: ` +
          JSON.stringify({ inventory: proof, listed }),
      );
    }
  }
  return inventoryProofs;
}

function assertAttemptArchiveEvidence(providerAttemptGroups, inventoryObjects, consultationId) {
  const meetingPrefix = `v1/meetings/${consultationId}/`;
  for (const group of providerAttemptGroups) {
    for (const attemptId of group.attemptIds ?? []) {
      const terminalPath = `/pipeline/terminal/raw/${attemptId}/`;
      const exchangePath = `/pipeline/${group.stage}/raw/${attemptId}/`;
      const terminal = inventoryObjects.find(
        (object) => object.key.startsWith(meetingPrefix) && object.key.includes(terminalPath),
      );
      const exchange = inventoryObjects.find(
        (object) => object.key.startsWith(meetingPrefix) && object.key.includes(exchangePath),
      );
      if (!terminal || !exchange || terminal.id === exchange.id) {
        throw new Error(
          `attempt ${attemptId} lacks distinct archived terminal/raw ${group.stage} evidence`,
        );
      }
    }
  }
}
let currentPhase = "browser-startup";
function beginPhase(name) {
  currentPhase = name;
  console.error(`[consultation-smoke] phase: ${name}`);
}

beginPhase(currentPhase);
const commonMediaArgs = [
  "--use-fake-ui-for-media-stream",
  "--use-fake-device-for-media-stream",
  "--host-resolver-rules=MAP app.localhost web, MAP rtc.localhost livekit",
  "--use-file-for-fake-video-capture=/workspace/tests/fixtures/consultation.y4m",
];
const employeeBrowser = await bounded("launch employee browser", ({ timeoutMs }) =>
  chromium.launch({
    headless: true,
    timeout: timeoutMs,
    args: [
      ...commonMediaArgs,
      "--use-file-for-fake-audio-capture=/workspace/tests/fixtures/en-good-morning.wav",
    ],
  }),
);
const customerBrowser = await bounded("launch customer browser", ({ timeoutMs }) =>
  chromium.launch({
    headless: true,
    timeout: timeoutMs,
    args: [
      ...commonMediaArgs,
      "--use-file-for-fake-audio-capture=/workspace/tests/fixtures/de-guten-morgen.wav",
    ],
  }),
);
const employeeContext = await boundedBrowser(employeeBrowser, "create employee context", () =>
  employeeBrowser.newContext({ permissions: ["camera", "microphone"] }),
);
const customerContext = await boundedBrowser(customerBrowser, "create customer context", () =>
  customerBrowser.newContext({ permissions: ["camera", "microphone"] }),
);
const thirdContext = await boundedBrowser(employeeBrowser, "create third-user context", () =>
  employeeBrowser.newContext(),
);
const browserDeadlineCancellation = setTimeout(() => {
  void Promise.allSettled([employeeBrowser.close(), customerBrowser.close()]);
}, remainingDeadlineMs(deadlineEpochMs));
await bounded("install caption probes", () =>
  Promise.all([installCaptionProbe(employeeContext), installCaptionProbe(customerContext)]),
);
let employee;
let admissionFixtureConsultationId = null;
let consultationId = null;
let completed = false;
let employeeFinalCaption = null;
let customerFinalCaption = null;
try {
  beginPhase("employee-authentication-and-consultation-creation");
  employee = await authenticate(employeeContext, employeeEmail);
  consultationId = await createConsultation(
    employee,
    `Customer ${runId.slice(0, 8)}`,
    customerEmail,
  );
  beginPhase("failure-injection-release");
  if (failureHarnessReleaseFile) {
    console.log(
      JSON.stringify({
        phase: "consultation-created",
        runId,
        consultationId,
      }),
    );
    await poll(
      "failure harness release",
      async () => {
        try {
          await access(failureHarnessReleaseFile);
          return true;
        } catch {
          return null;
        }
      },
      failureHarnessReleaseTimeoutMs,
      100,
    );
  }
  beginPhase("customer-and-third-user-invitation-authentication");
  const invite = await poll("customer invitation", ({ signal }) =>
    latestLink(customerEmail, { signal }),
  );
  const customer = await authenticate(customerContext, customerEmail, invite);
  const thirdEmail = `admission-${runId}@example.test`;
  admissionFixtureConsultationId = await createConsultation(
    employee,
    `Admission probe ${runId.slice(0, 8)}`,
    thirdEmail,
  );
  const thirdInvite = await poll("third-user invitation", ({ signal }) =>
    latestLink(thirdEmail, { signal }),
  );
  const third = await authenticate(thirdContext, thirdEmail, thirdInvite);

  beginPhase("preferences-and-frozen-provider-consent");
  await boundedPages([employee, customer], "open participant lobbies", () =>
    Promise.all([
      employee.goto(`${baseUrl}/consultations/${consultationId}/lobby`),
      customer.goto(`${baseUrl}/consultations/${consultationId}/lobby`),
    ]),
  );
  await boundedPage(employee, "save employee preferences", () =>
    savePreferences(employee, `Employee ${runId.slice(0, 8)}`, "en-US"),
  );
  await boundedPage(customer, "save customer preferences", () =>
    savePreferences(customer, `Customer ${runId.slice(0, 8)}`, "de-DE"),
  );
  const [employeeProfile, customerProfile] = await boundedPages(
    [employee, customer],
    "verify frozen participant profiles",
    () =>
      Promise.all([
        assertFrozenProfile(employee, consultationId),
        assertFrozenProfile(customer, consultationId),
      ]),
  );
  if (
    employeeProfile.profileRevision !== customerProfile.profileRevision ||
    JSON.stringify(employeeProfile.directions) !== JSON.stringify(customerProfile.directions)
  ) {
    throw new Error("participants did not receive the same frozen provider profile");
  }
  beginPhase("room-admission-and-capture-barrier");
  await boundedPages([employee, customer], "participant consent and join", () =>
    Promise.all([
      consentAndJoin(employee, consultationId),
      consentAndJoin(customer, consultationId),
    ]),
  );
  await boundedPages([employee, customer], "participant room entry", () =>
    Promise.all([enterRoom(employee), enterRoom(customer)]),
  );

  beginPhase("authenticated-third-user-admission-rejection");
  const [forbiddenRoom, forbiddenJoin] = await boundedPage(
    third,
    "third-user room API rejection",
    () =>
      Promise.all([
        apiJson(third, `/api/consultations/${consultationId}/room`),
        postApi(third, `/api/consultations/${consultationId}/join`, { consultationId }),
      ]),
  );
  await boundedPage(third, "third-user room navigation", () =>
    third.goto(`${baseUrl}/consultations/${consultationId}/room`),
  );
  const exposedRoomSurface = await boundedPage(third, "third-user room surface check", () =>
    third.getByLabel("Live consultation").count(),
  );
  if (forbiddenRoom.status !== 404 || forbiddenJoin.status !== 404 || exposedRoomSurface !== 0) {
    throw new Error(
      "authenticated non-member reached a consultation room " +
        `(read=${String(forbiddenRoom.status)}, join=${String(forbiddenJoin.status)}, ` +
        `surface=${String(exposedRoomSurface)})`,
    );
  }

  if (!skipMediaOutputProof) {
    beginPhase("captions-interpretation-and-audio-modes");
    [employeeFinalCaption, customerFinalCaption] = await boundedPages(
      [employee, customer],
      "caption interpretation and audible routing proof",
      () =>
        Promise.all([
          assertFinalTargetedCaption(employee, consultationId, "de-DE", "en-US"),
          assertFinalTargetedCaption(customer, consultationId, "en-US", "de-DE"),
          ...(!skipAudibleInterpretationProof
            ? [assertAudibleInterpretation(employee), assertAudibleInterpretation(customer)]
            : []),
        ]),
    );
    await boundedPages([employee, customer], "exact audio mode routing proof", () =>
      Promise.all([assertModes(employee), assertModes(customer)]),
    );
  }

  beginPhase("consultation-finalization");
  await boundedPage(employee, "request consultation end", () =>
    employee.getByRole("button", { name: "End consultation" }).click(),
  );
  await boundedPages([employee, customer], "observe consultation ending", () =>
    Promise.all([
      employee
        .getByRole("timer")
        .filter({ hasText: /Consultation ending in [1-5] seconds?/ })
        .waitFor(),
      customer
        .getByRole("timer")
        .filter({ hasText: /Consultation ending in [1-5] seconds?/ })
        .waitFor(),
    ]),
  );
  await boundedPage(employee, "open completed archive", () =>
    employee.waitForURL(/\/archives\/[0-9a-f-]+/, {
      timeout: deadlineTimeout(120_000),
    }),
  );
  const archiveId = employee.url().match(/\/archives\/([0-9a-f-]+)/)?.[1];
  requireValue(archiveId, "archive id");
  beginPhase("complete-archive-reconciliation");
  const archive = await poll(
    "complete archive inventory",
    async () => {
      const result = await apiJson(employee, `/api/archives/${archiveId}`);
      return result.status === 200 && hasCompleteArchiveEvidence(result.body) ? result.body : null;
    },
    180_000,
    2_000,
  );
  beginPhase("archive-object-pagination-and-shape");
  const objects = await allArchiveObjects(employee, archiveId);
  const requiredGroups = [
    "composite",
    "original",
    "interpretation",
    "captions",
    "pipeline",
    "inventory",
  ];
  for (const group of requiredGroups) {
    if (!objects.some((object) => object.group === group))
      throw new Error(`archive missing ${group} evidence`);
  }
  for (const [pathClass, objectClass] of [
    ["tts-output", "tts_output_pcm"],
    ["livekit-output", "livekit_output_pcm"],
  ]) {
    if (
      !objects.some(
        (object) => object.label === objectClass && object.key.includes(`/audio/${pathClass}/`),
      )
    ) {
      throw new Error(`archive missing preserved ${pathClass} interpretation audio`);
    }
  }
  if (!objects.some((object) => object.label.includes("checkpoint"))) {
    throw new Error("archive omitted the terminal worker checkpoint");
  }
  if (expectedProfile === "fixture" && objects.length < fixtureMinimumCompleteObjectCount) {
    throw new Error(
      `fixture archive has ${objects.length} objects; expected at least ` +
        `${fixtureMinimumCompleteObjectCount}`,
    );
  }
  for (const object of objects) {
    if (
      typeof object.id !== "string" ||
      typeof object.key !== "string" ||
      object.key.length === 0 ||
      typeof object.label !== "string" ||
      object.label.length === 0 ||
      typeof object.versionId !== "string" ||
      object.versionId.length === 0 ||
      typeof object.contentType !== "string" ||
      object.contentType.length === 0 ||
      !/^[0-9a-f]{64}$/u.test(object.sha256) ||
      typeof object.s3Checksum !== "string" ||
      !validS3Checksum(object.s3Checksum) ||
      !Number.isSafeInteger(object.size) ||
      object.size <= 0
    ) {
      throw new Error(`archive object lacks integrity evidence: ${String(object.id)}`);
    }
  }
  beginPhase("archive-object-download-and-checksum-verification");
  const checkedObjectCount = await independentlyVerifyObjects(employee, archiveId, objects);
  if (checkedObjectCount !== objects.length) {
    throw new Error(
      `archive verification count mismatch: checked ${checkedObjectCount} of ${objects.length}`,
    );
  }
  const finalObjects = objects.filter(
    (object) => object.key === `v1/meetings/${consultationId}/inventory/final.json`,
  );
  if (finalObjects.length !== 1) {
    throw new Error(
      `archive must contain exactly one final inventory object, got ${finalObjects.length}`,
    );
  }
  const finalObject = finalObjects[0];
  if (
    finalObject.versionId !== archive.inventoryVersion ||
    finalObject.sha256 !== archive.inventorySha256
  ) {
    throw new Error("archive detail inventory version/hash diverges from the final object listing");
  }
  beginPhase("final-inventory-binding");
  const finalDownload = await independentlyVerifyObject(employee, archiveId, finalObject, true);
  let finalInventory;
  try {
    finalInventory = JSON.parse(finalDownload.body.toString("utf8"));
  } catch (error) {
    throw new Error("downloaded inventory/final.json is not valid JSON", { cause: error });
  }
  const inventoryObjects = assertFinalInventoryBinding(
    archive,
    consultationId,
    objects,
    finalObject,
    finalInventory,
  );
  beginPhase("provider-attempt-and-egress-evidence");
  const allowedProviders = allowedProvidersByProfile[expectedProfile];
  if (!allowedProviders) {
    throw new Error(`smoke has no provider evidence policy for ${expectedProfile}`);
  }
  const providerAttemptIds = archive.providerAttemptIds ?? [];
  const providerAttemptGroups = archive.providerAttemptGroups ?? [];
  if (providerAttemptIds.length === 0 || providerAttemptGroups.length === 0) {
    throw new Error("complete archive omitted provider-attempt evidence");
  }
  const groupedAttemptIdList = providerAttemptGroups.flatMap((group) => group.attemptIds ?? []);
  const groupedAttemptIds = new Set(groupedAttemptIdList);
  const directions = new Map();
  for (const group of providerAttemptGroups) {
    if (!allowedProviders.has(group.provider)) {
      throw new Error(
        `foreign provider attempt ${group.provider} found for profile ${expectedProfile}`,
      );
    }
    if (
      typeof group.direction !== "string" ||
      !["stt", "translation", "tts"].includes(group.stage) ||
      !group.attemptIds?.length
    ) {
      throw new Error(`provider attempt group is incomplete: ${JSON.stringify(group)}`);
    }
    const stages = directions.get(group.direction) ?? new Set();
    if (stages.has(group.stage)) {
      throw new Error(`provider attempt partition repeated ${group.direction}/${group.stage}`);
    }
    stages.add(group.stage);
    directions.set(group.direction, stages);
  }
  if (
    directions.size !== 2 ||
    [...directions.values()].some(
      (stages) =>
        stages.size !== 3 || !["stt", "translation", "tts"].every((stage) => stages.has(stage)),
    )
  ) {
    throw new Error("provider attempts do not cover STT/Translation/TTS in both directions");
  }
  if (
    new Set(providerAttemptIds).size !== providerAttemptIds.length ||
    groupedAttemptIds.size !== groupedAttemptIdList.length ||
    groupedAttemptIds.size !== providerAttemptIds.length ||
    providerAttemptIds.some((id) => !groupedAttemptIds.has(id))
  ) {
    throw new Error("provider attempt groups are not an exact unique attempt partition");
  }
  assertAttemptArchiveEvidence(providerAttemptGroups, inventoryObjects, consultationId);
  const egressIds = archive.egressIds ?? [];
  if (!egressIds.length || new Set(egressIds).size !== egressIds.length) {
    throw new Error("complete archive omitted unique terminal Egress IDs");
  }
  const inventoryEgressIds = (finalInventory.egressResults ?? []).map(
    (result) => result.egressId ?? result.egress_id ?? result.id,
  );
  if (
    new Set(inventoryEgressIds).size !== inventoryEgressIds.length ||
    inventoryEgressIds.length !== egressIds.length ||
    egressIds.some((id) => !inventoryEgressIds.includes(id))
  ) {
    throw new Error("archive Egress IDs diverge from downloaded final inventory");
  }
  beginPhase("authenticated-admission-fixture-cleanup");
  await boundedPage(employee, "settle authenticated admission fixture", () =>
    settleCreatedConsultation(employee, admissionFixtureConsultationId),
  );
  admissionFixtureConsultationId = null;
  beginPhase("proof-emission");
  const proof = {
    runId,
    consultationId,
    archiveId,
    providerProfile: expectedProfile,
    profileRevision: employeeProfile.profileRevision,
    directions: employeeProfile.directions,
    objectCount: objects.length,
    checkedObjectCount,
    inventoryBoundObjectCount: inventoryObjects.length,
    inventoryVersion: archive.inventoryVersion ?? null,
    inventorySha256: archive.inventorySha256 ?? null,
    egressIds,
    providerAttemptIds,
    finalCaptionRevisions:
      employeeFinalCaption && customerFinalCaption
        ? [employeeFinalCaption.revision, customerFinalCaption.revision]
        : [],
    targetedCaptions: employeeFinalCaption && customerFinalCaption ? 2 : 0,
    audioModes: skipMediaOutputProof ? 0 : 3,
    thirdAuthenticatedHumanRejected: true,
  };
  if (!proof.inventoryVersion || !/^[0-9a-f]{64}$/u.test(proof.inventorySha256 ?? "")) {
    throw new Error("archive proof omitted inventory version/hash");
  }
  if (emitProof) console.log(JSON.stringify(proof));
  completed = true;
} catch (error) {
  throw new Error(
    `[consultation-smoke] phase ${currentPhase} failed: ${error?.message ?? String(error)}`,
    { cause: error },
  );
} finally {
  clearTimeout(browserDeadlineCancellation);
  const cleanupIds = [
    ...(admissionFixtureConsultationId ? [admissionFixtureConsultationId] : []),
    ...(!completed && consultationId && failureHarnessReleaseFile === null ? [consultationId] : []),
  ];
  for (const cleanupId of cleanupIds) {
    if (!employee || Date.now() >= deadlineEpochMs) break;
    await boundedPage(employee, `settle cleanup consultation ${cleanupId}`, () =>
      settleCreatedConsultation(employee, cleanupId),
    ).catch((cleanupError) => {
      console.error(
        `[consultation-smoke] cleanup could not settle ${cleanupId}: ${
          cleanupError?.message ?? String(cleanupError)
        }`,
      );
    });
  }
  const contexts = [employeeContext, customerContext, thirdContext];
  if (Date.now() < deadlineEpochMs) {
    await bounded(
      "close browser contexts",
      () => Promise.allSettled(contexts.map((context) => context.close())),
      () => contexts.forEach(closeIgnoringFailure),
    ).catch(() => {});
  } else {
    contexts.forEach(closeIgnoringFailure);
  }
  const browsers = [employeeBrowser, customerBrowser];
  if (Date.now() < deadlineEpochMs) {
    await bounded(
      "close browsers",
      () => Promise.allSettled(browsers.map((browser) => browser.close())),
      () => browsers.forEach(closeIgnoringFailure),
    ).catch(() => {});
  } else {
    browsers.forEach(closeIgnoringFailure);
  }
}
