import { access } from "node:fs/promises";
import { chromium } from "playwright";
import { createConsultationArchiveProof } from "./consultation-archive-proof.mjs";
import { createConsultationHarness } from "./consultation-harness.mjs";
import { createConsultationScenario } from "./consultation-scenario.mjs";
import { hasCompleteArchiveEvidence, remainingDeadlineMs } from "./harness-contracts.mjs";

const harness = createConsultationHarness();
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
const scenarioState = {
  employee: undefined,
  admissionFixtureConsultationId: null,
  consultationId: null,
  employeeFinalCaption: null,
  customerFinalCaption: null,
};
let completed = false;
try {
  const { archiveId, employeeProfile } = await runConsultationScenario({
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
        ? [scenarioState.employeeFinalCaption.revision, scenarioState.customerFinalCaption.revision]
        : [],
    targetedCaptions:
      scenarioState.employeeFinalCaption && scenarioState.customerFinalCaption ? 2 : 0,
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
