import { acceptedCaptionMatchesRender, MODE_GAIN_PAIRS } from "./harness-contracts.mjs";

export function createConsultationScenario(context) {
  const {
    baseUrl,
    boundedContext,
    boundedPage,
    captureBarrierTimeoutMs,
    deadlineTimeout,
    expectedLiveKitUrl,
    expectedProfile,
    expectedProfileRevision,
    mailpitUrl,
    poll,
    requireValue,
    startedAt,
  } = context;

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
      (response) =>
        response.url().includes("/preferences") && response.request().method() === "POST",
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
    if (!/region|eu/i.test(consentText))
      throw new Error("consent did not disclose provider region");
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
        new URL(response.url()).pathname.endsWith("/join") &&
        response.request().method() === "POST",
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
      const expectedConsentRace =
        joinResponse.status() === 409 && error.code === "CONSENT_REQUIRED";
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
          throw new Error(
            `cleanup cannot settle consultation in unexpected state ${String(state)}`,
          );
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

  async function runConsultationScenario(workflow) {
    const {
      accessReleaseFile,
      beginPhase,
      boundedPages,
      customerContext,
      customerEmail,
      employeeContext,
      employeeEmail,
      failureHarnessReleaseFile,
      failureHarnessReleaseTimeoutMs,
      runId,
      skipAudibleInterpretationProof,
      skipMediaOutputProof,
      state,
      thirdContext,
    } = workflow;

    beginPhase("employee-authentication-and-consultation-creation");
    state.employee = await authenticate(employeeContext, employeeEmail);
    const employee = state.employee;
    state.consultationId = await createConsultation(
      employee,
      `Customer ${runId.slice(0, 8)}`,
      customerEmail,
    );
    const consultationId = state.consultationId;
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
            await accessReleaseFile(failureHarnessReleaseFile);
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
    state.admissionFixtureConsultationId = await createConsultation(
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
      [state.employeeFinalCaption, state.customerFinalCaption] = await boundedPages(
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
    return { archiveId, customerProfile, employeeProfile };
  }

  return {
    apiJson,
    assertAudibleInterpretation,
    assertFinalTargetedCaption,
    assertFrozenProfile,
    assertModes,
    authenticate,
    consentAndJoin,
    createConsultation,
    enterRoom,
    installCaptionProbe,
    latestLink,
    postApi,
    savePreferences,
    settleCreatedConsultation,
    runConsultationScenario,
  };
}
