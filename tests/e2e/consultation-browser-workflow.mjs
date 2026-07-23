import { access } from "node:fs/promises";
import { chromium } from "playwright";
import { createConsultationArchiveProof } from "./consultation-archive-proof.mjs";
import { createConsultationScenario } from "./consultation-scenario.mjs";
import { hasCompleteArchiveEvidence, remainingDeadlineMs } from "./harness-contracts.mjs";

const CHROMIUM_INSTALL_DIAGNOSTIC = "Run: bunx playwright install chromium";

async function requireChromiumExecutable() {
  const executable = chromium.executablePath();
  try {
    await access(executable);
  } catch (error) {
    throw new Error(CHROMIUM_INSTALL_DIAGNOSTIC, { cause: error });
  }
  return executable;
}

function displayLaunchOptions(browserMode) {
  if (browserMode !== "headed-demo") return { args: [], env: undefined };
  if (process.env.DISPLAY) return { args: [], env: process.env };
  if (!process.env.WAYLAND_DISPLAY || !process.env.XDG_RUNTIME_DIR) {
    throw new Error("Headed demo requires DISPLAY or both WAYLAND_DISPLAY and XDG_RUNTIME_DIR");
  }
  return { args: ["--ozone-platform=wayland"], env: process.env };
}

async function setWindowBounds(browser, page, bounds) {
  const session = await browser.newBrowserCDPSession();
  try {
    const { targetInfos } = await session.send("Target.getTargets");
    const target = targetInfos.find(
      (candidate) => candidate.type === "page" && candidate.url === page.url(),
    );
    if (!target) throw new Error(`could not resolve Chromium window for ${page.url()}`);
    const { windowId } = await session.send("Browser.getWindowForTarget", {
      targetId: target.targetId,
    });
    await session.send("Browser.setWindowBounds", { windowId, bounds });
  } finally {
    await session.detach();
  }
}

async function placeParticipantWindows(
  employeeBrowser,
  customerBrowser,
  employeePage,
  customerPage,
) {
  const available = await employeePage.evaluate(() => ({
    width: screen.availWidth,
    height: screen.availHeight,
  }));
  const halfWidth = Math.max(1, Math.floor(available.width / 2));
  await Promise.all([
    setWindowBounds(employeeBrowser, employeePage, {
      left: 0,
      top: 0,
      width: halfWidth,
      height: available.height,
      windowState: "normal",
    }),
    setWindowBounds(customerBrowser, customerPage, {
      left: halfWidth,
      top: 0,
      width: Math.max(1, available.width - halfWidth),
      height: available.height,
      windowState: "normal",
    }),
  ]);
}

export async function runConsultationBrowserWorkflow({
  harness,
  browserMode,
  employeeMedia,
  customerMedia,
}) {
  if (browserMode !== "headless-smoke" && browserMode !== "headed-demo") {
    throw new Error(`unsupported browser mode: ${String(browserMode)}`);
  }
  const {
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
    customerEmail,
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
    startedAt,
  } = harness;

  await requireChromiumExecutable();
  for (const [participant, media] of [
    ["employee", employeeMedia],
    ["customer", customerMedia],
  ]) {
    if (!media?.video || !media?.audio) {
      throw new Error(`${participant} media requires video and audio files`);
    }
  }

  const scenario = createConsultationScenario({
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
  });
  const { apiJson, installCaptionProbe, runConsultationScenario, settleCreatedConsultation } =
    scenario;
  const archiveProof = createConsultationArchiveProof({
    apiJson,
    archiveObjectCeiling,
    archivePageCeiling,
    bounded,
    boundedPage,
    objectDownloadTimeoutMs,
  });
  const {
    allArchiveObjects,
    assertAttemptArchiveEvidence,
    assertFinalInventoryBinding,
    independentlyVerifyObject,
    independentlyVerifyObjects,
    validS3Checksum,
  } = archiveProof;

  let currentPhase = "browser-startup";
  const beginPhase = (name) => {
    currentPhase = name;
    console.error(`[consultation-smoke] phase: ${name}`);
  };
  beginPhase(currentPhase);

  const headed = browserMode === "headed-demo";
  const display = displayLaunchOptions(browserMode);
  const baseArgs = [
    "--use-fake-ui-for-media-stream",
    "--use-fake-device-for-media-stream",
    ...(headed
      ? ["--window-size=960,900", ...display.args]
      : ["--host-resolver-rules=MAP app.localhost web, MAP rtc.localhost livekit"]),
  ];
  const launchBrowser = (participant, media, visible = headed) =>
    bounded(`launch ${participant} browser`, ({ timeoutMs }) =>
      chromium.launch({
        executablePath: chromium.executablePath(),
        headless: !visible,
        slowMo: 0,
        timeout: timeoutMs,
        env: visible ? display.env : undefined,
        args: [
          ...baseArgs,
          ...(media
            ? [
                `--use-file-for-fake-video-capture=${media.video}`,
                `--use-file-for-fake-audio-capture=${media.audio}`,
              ]
            : []),
        ],
      }),
    );

  const browserResults = await Promise.allSettled([
    launchBrowser("employee", employeeMedia),
    launchBrowser("customer", customerMedia),
    headed ? launchBrowser("admission probe", null, false) : Promise.resolve(undefined),
  ]);
  const browserFailure = browserResults.find((entry) => entry.status === "rejected");
  if (browserFailure) {
    await Promise.allSettled(
      browserResults
        .filter((entry) => entry.status === "fulfilled" && entry.value)
        .map((entry) => entry.value.close()),
    );
    throw browserFailure.reason;
  }
  const [employeeBrowser, customerBrowser, thirdBrowser] = browserResults.map(
    (entry) => entry.value,
  );
  const contextOptions = {
    permissions: ["camera", "microphone"],
    viewport: headed ? null : undefined,
  };
  const contextResults = await Promise.allSettled([
    boundedBrowser(employeeBrowser, "create employee context", () =>
      employeeBrowser.newContext(contextOptions),
    ),
    boundedBrowser(customerBrowser, "create customer context", () =>
      customerBrowser.newContext(contextOptions),
    ),
    boundedBrowser(thirdBrowser ?? employeeBrowser, "create third-user context", () =>
      (thirdBrowser ?? employeeBrowser).newContext(),
    ),
  ]);
  const contextFailure = contextResults.find((entry) => entry.status === "rejected");
  if (contextFailure) {
    await Promise.allSettled([
      ...contextResults
        .filter((entry) => entry.status === "fulfilled")
        .map((entry) => entry.value.close()),
      ...[employeeBrowser, customerBrowser, thirdBrowser]
        .filter(Boolean)
        .map((browser) => browser.close()),
    ]);
    throw contextFailure.reason;
  }
  const [employeeContext, customerContext, thirdContext] = contextResults.map(
    (entry) => entry.value,
  );
  let browserDeadlineCancellation = setTimeout(() => {
    void Promise.allSettled(
      [employeeBrowser, customerBrowser, thirdBrowser]
        .filter(Boolean)
        .map((browser) => browser.close()),
    );
  }, remainingDeadlineMs(deadlineEpochMs));
  await bounded("install caption probes", () =>
    Promise.all([installCaptionProbe(employeeContext), installCaptionProbe(customerContext)]),
  );

  const scenarioState = {
    employee: undefined,
    admissionFixtureConsultationId: null,
    consultationId: null,
    employeeFinalCaption: null,
    customerFinalCaption: null,
  };
  let completed = false;
  let closed = false;
  const waitCleanups = new Set();
  let result;
  let primaryError;
  try {
    const { archiveId, employeeProfile, employeePage, customerPage } =
      await runConsultationScenario({
        accessReleaseFile: access,
        beginPhase,
        boundedPages,
        customerContext,
        customerEmail,
        employeeContext,
        employeeEmail,
        failureHarnessReleaseFile,
        failureHarnessReleaseTimeoutMs,
        runId,
        onParticipantPagesReady:
          browserMode === "headed-demo"
            ? (employeePage, customerPage) =>
                boundedPages([employeePage, customerPage], "position participant windows", () =>
                  placeParticipantWindows(
                    employeeBrowser,
                    customerBrowser,
                    employeePage,
                    customerPage,
                  ),
                )
            : undefined,
        skipAudibleInterpretationProof,
        skipMediaOutputProof,
        state: scenarioState,
        thirdContext,
      });
    const { employee, consultationId } = scenarioState;
    beginPhase("complete-archive-reconciliation");
    const archive = await poll(
      "complete archive inventory",
      async () => {
        const result = await apiJson(employee, `/api/archives/${archiveId}`);
        return result.status === 200 && hasCompleteArchiveEvidence(result.body)
          ? result.body
          : null;
      },
      180_000,
      2_000,
    );
    beginPhase("archive-object-pagination-and-shape");
    const objects = await allArchiveObjects(employee, archiveId);
    const requiredGroups = ["composite", "original", "captions", "pipeline", "inventory"];
    if (!skipMediaOutputProof) requiredGroups.push("interpretation");
    for (const group of requiredGroups) {
      if (!objects.some((object) => object.group === group))
        throw new Error(`archive missing ${group} evidence`);
    }
    if (!skipMediaOutputProof) {
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
      throw new Error(
        "archive detail inventory version/hash diverges from the final object listing",
      );
    }
    beginPhase("final-inventory-binding");
    const binding = await poll(
      "final inventory binding",
      async () => {
        const currentObjects = await allArchiveObjects(employee, archiveId);
        const currentFinalObject = currentObjects.find(
          (object) => object.key === `v1/meetings/${consultationId}/inventory/final.json`,
        );
        if (!currentFinalObject) return null;
        const currentDownload = await independentlyVerifyObject(
          employee,
          archiveId,
          currentFinalObject,
          true,
        );
        try {
          const currentInventory = JSON.parse(currentDownload.body.toString("utf8"));
          return {
            finalInventory: currentInventory,
            inventoryObjects: assertFinalInventoryBinding(
              archive,
              consultationId,
              currentObjects,
              currentFinalObject,
              currentInventory,
            ),
          };
        } catch {
          return null;
        }
      },
      30_000,
      500,
    );
    const inventoryObjects = binding.inventoryObjects;
    const finalInventory = binding.finalInventory;
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
      settleCreatedConsultation(employee, scenarioState.admissionFixtureConsultationId),
    );
    scenarioState.admissionFixtureConsultationId = null;
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
        scenarioState.employeeFinalCaption && scenarioState.customerFinalCaption
          ? [
              scenarioState.employeeFinalCaption.revision,
              scenarioState.customerFinalCaption.revision,
            ]
          : [],
      targetedCaptions:
        scenarioState.employeeFinalCaption && scenarioState.customerFinalCaption ? 2 : 0,
      audioModes: skipMediaOutputProof ? 0 : 3,
      thirdAuthenticatedHumanRejected: true,
    };
    if (!proof.inventoryVersion || !/^[0-9a-f]{64}$/u.test(proof.inventorySha256 ?? "")) {
      throw new Error("archive proof omitted inventory version/hash");
    }
    if (expectedProfile === "fixture" && browserMode === "headed-demo") {
      await boundedPages([employeePage, customerPage], "label deterministic fixture demo", () =>
        Promise.all([
          employeePage.evaluate(() => {
            document.title = "Deterministic fixture — video transport demo";
          }),
          customerPage.evaluate(() => {
            document.title = "Deterministic fixture — video transport demo";
          }),
        ]),
      );
    }
    if (emitProof) console.log(JSON.stringify(proof));
    clearTimeout(browserDeadlineCancellation);
    browserDeadlineCancellation = null;
    completed = true;
    result = {
      archiveUrl: new URL(`/archives/${archiveId}`, baseUrl).toString(),
      proof,
      employeePage,
      customerPage,
    };
  } catch (error) {
    primaryError = new Error(
      `[consultation-smoke] phase ${currentPhase} failed: ${error?.message ?? String(error)}`,
      { cause: error },
    );
    throw primaryError;
  } finally {
    if (!completed) await close();
  }

  async function close() {
    if (closed) return;
    closed = true;
    if (browserDeadlineCancellation !== null) clearTimeout(browserDeadlineCancellation);
    browserDeadlineCancellation = null;
    for (const cleanup of waitCleanups) cleanup();
    waitCleanups.clear();
    const cleanupIds = [
      ...(scenarioState.admissionFixtureConsultationId
        ? [scenarioState.admissionFixtureConsultationId]
        : []),
      ...(!completed && scenarioState.consultationId && failureHarnessReleaseFile === null
        ? [scenarioState.consultationId]
        : []),
    ];
    for (const cleanupId of cleanupIds) {
      if (!scenarioState.employee || Date.now() >= deadlineEpochMs) break;
      await boundedPage(scenarioState.employee, `settle cleanup consultation ${cleanupId}`, () =>
        settleCreatedConsultation(scenarioState.employee, cleanupId),
      ).catch((cleanupError) => {
        console.error(
          `[consultation-smoke] cleanup could not settle ${cleanupId}: ${cleanupError?.message ?? String(cleanupError)}`,
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
    } else contexts.forEach(closeIgnoringFailure);
    const browsers = [employeeBrowser, customerBrowser, thirdBrowser].filter(Boolean);
    if (Date.now() < deadlineEpochMs) {
      await bounded(
        "close browsers",
        () => Promise.allSettled(browsers.map((browser) => browser.close())),
        () => browsers.forEach(closeIgnoringFailure),
      ).catch(() => {});
    } else browsers.forEach(closeIgnoringFailure);
  }

  function waitForWindowClose(signal) {
    return new Promise((resolve) => {
      let finished = false;
      const finish = () => {
        if (finished) return;
        finished = true;
        cleanup();
        resolve();
      };
      const cleanup = () => {
        result.employeePage.off("close", finish);
        result.customerPage.off("close", finish);
        employeeBrowser.off("disconnected", finish);
        customerBrowser.off("disconnected", finish);
        signal?.removeEventListener("abort", finish);
        waitCleanups.delete(cleanup);
      };
      waitCleanups.add(cleanup);
      result.employeePage.once("close", finish);
      result.customerPage.once("close", finish);
      employeeBrowser.once("disconnected", finish);
      customerBrowser.once("disconnected", finish);
      signal?.addEventListener("abort", finish, { once: true });
      if (
        closed ||
        result.employeePage.isClosed() ||
        result.customerPage.isClosed() ||
        !employeeBrowser.isConnected() ||
        !customerBrowser.isConnected() ||
        signal?.aborted
      ) {
        finish();
      }
    });
  }

  result = { ...result, waitForWindowClose, close };
  return result;
}
