import { describe, expect, it } from "bun:test";
import { readFile } from "node:fs/promises";
import { createConsultationArchiveProof } from "./consultation-archive-proof.mjs";
import { createConsultationHarness } from "./consultation-harness.mjs";
import { createConsultationScenario } from "./consultation-scenario.mjs";
import {
  acceptedCaptionMatchesRender,
  hasCompleteArchiveEvidence,
  MODE_GAIN_PAIRS,
  pollWithinDeadline,
  remainingDeadlineMs,
  ScenarioDeadlineError,
  withinDeadline,
} from "./harness-contracts.mjs";

const caption = Object.freeze({
  schemaVersion: 1,
  finality: "final",
  consultationId: "consultation",
  destinationParticipantId: "employee",
  sourceParticipantId: "customer",
  sourceLanguage: "de-DE",
  targetLanguage: "en-US",
  sourceText: "Guten Morgen",
  translatedText: "Good morning",
});

function rendered(overrides = {}) {
  return {
    candidate: caption,
    consultationId: "consultation",
    destinationParticipantId: "employee",
    sourceParticipantId: "customer",
    sourceLanguage: "de-DE",
    targetLanguage: "en-US",
    renderedTranslation: "Good morning",
    renderedSource: "Guten Morgen",
    finalAnnouncement: "Final translation from Customer, de-DE to en-US: Good morning",
    otherDisplayName: "Customer",
    ...overrides,
  };
}

function consultationHarness(profile) {
  return createConsultationHarness(
    [
      "bun",
      "consultation",
      "--deadline-epoch-ms",
      String(Date.now() + 60_000),
      "--expected-profile",
      profile,
    ],
    {
      E2E_EMPLOYEE_EMAIL: "employee@example.test",
      E2E_RUN_ID: "00000000-0000-4000-8000-000000000001",
    },
  );
}

describe("consultation harness public contracts", () => {
  it("allows Google evidence for both Google profiles and fails closed for unknown profiles", () => {
    for (const profile of ["google-eu", "google-speech-eu"]) {
      expect(consultationHarness(profile).allowedProvidersByProfile[profile]).toEqual(
        new Set(["google"]),
      );
    }
    expect(
      consultationHarness("unregistered-profile").allowedProvidersByProfile["unregistered-profile"],
    ).toBeUndefined();
  });

  it("exposes only scenario operations used by the browser workflow", () => {
    expect(Object.keys(createConsultationScenario({})).sort()).toEqual([
      "apiJson",
      "installCaptionProbe",
      "runConsultationScenario",
      "settleCreatedConsultation",
    ]);
  });
  it("does not externally return the validated customer profile or closed admission page", async () => {
    const source = await readFile(new URL("./consultation-scenario.mjs", import.meta.url), "utf8");
    const resultStart = source.indexOf("    return {\n      archiveId,");
    const resultEnd = source.indexOf("\n    };", resultStart);
    const resultSurface = source.slice(resultStart, resultEnd);
    expect(resultStart).toBeGreaterThan(-1);
    expect(resultSurface).not.toContain("customerProfile");
    expect(resultSurface).not.toContain("thirdPage");
    expect(resultSurface).toContain("employeeProfile");
    expect(resultSurface).toContain("employeePage: employee");
    expect(resultSurface).toContain("customerPage: customer");
  });

  it("keeps browser workflow results free of unreachable window-close listener state", async () => {
    const source = await readFile(
      new URL("./consultation-browser-workflow.mjs", import.meta.url),
      "utf8",
    );
    expect(source).not.toContain("waitForWindowClose");
    expect(source).not.toContain("waitCleanups");
    expect(source).toContain("archiveUrl: new URL");
    expect(source).toContain("proof,");
    expect(source).toContain("close,");
  });
});

describe("consultation smoke proof contracts", () => {
  it("requires every exact audio mode gain pair", () => {
    expect(MODE_GAIN_PAIRS).toEqual([
      ["Interpreted", 0, 1],
      ["Overlay", 0.18, 1],
      ["Original", 1, 0],
    ]);
  });

  it("accepts only a targeted final caption that is the rendered UI state", () => {
    expect(acceptedCaptionMatchesRender(rendered())).toBe(true);
    for (const mismatch of [
      { candidate: { ...caption, schemaVersion: 2 } },
      { candidate: { ...caption, finality: "provisional" } },
      { sourceParticipantId: "other" },
      { sourceLanguage: "en-US" },
      { renderedTranslation: "unrelated text" },
      { finalAnnouncement: "" },
    ]) {
      expect(acceptedCaptionMatchesRender(rendered(mismatch))).toBe(false);
    }
  });
});

describe("absolute scenario deadline", () => {
  it("passes the exact remaining budget to an operation", async () => {
    let scheduledFor;
    let cleared;
    const value = await withinDeadline(
      1_500,
      "bounded operation",
      ({ timeoutMs, signal }) => {
        expect(timeoutMs).toBe(500);
        expect(signal.aborted).toBe(false);
        return "done";
      },
      {
        now: () => 1_000,
        schedule: (_callback, delay) => {
          scheduledFor = delay;
          return 7;
        },
        unschedule: (timer) => {
          cleared = timer;
        },
      },
    );
    expect(value).toBe("done");
    expect(scheduledFor).toBe(500);
    expect(cleared).toBe(7);
  });

  it("aborts and cancels an in-flight operation at the absolute deadline", async () => {
    let expire;
    let operationSignal;
    let cancelled = false;
    const pending = withinDeadline(
      2_000,
      "hung operation",
      ({ signal }) => {
        operationSignal = signal;
        return new Promise(() => {});
      },
      {
        cancel: () => {
          cancelled = true;
        },
        now: () => 1_000,
        schedule: (callback) => {
          expire = callback;
          return 9;
        },
        unschedule: () => {},
      },
    );
    await Promise.resolve();
    expire();
    await expect(pending).rejects.toBeInstanceOf(ScenarioDeadlineError);
    expect(operationSignal.aborted).toBe(true);
    expect(cancelled).toBe(true);
  });

  it("rejects an already exhausted deadline without starting work", async () => {
    let started = false;
    expect(() => remainingDeadlineMs(999, 1_000)).toThrow(ScenarioDeadlineError);
    await expect(
      withinDeadline(
        999,
        "late",
        () => {
          started = true;
        },
        { now: () => 1_000 },
      ),
    ).rejects.toBeInstanceOf(ScenarioDeadlineError);
    expect(started).toBe(false);
  });
});

describe("absolute polling deadline", () => {
  it("aborts a hung attempt at the poll's single overall deadline", async () => {
    let operationSignal;
    const startedAt = Date.now();
    await expect(
      pollWithinDeadline(
        startedAt + 30,
        "hung poll",
        ({ signal }) => {
          operationSignal = signal;
          return new Promise(() => {});
        },
        { intervalMs: 5 },
      ),
    ).rejects.toBeInstanceOf(ScenarioDeadlineError);
    expect(operationSignal.aborted).toBe(true);
    expect(Date.now() - startedAt).toBeLessThan(150);
  });

  it("shares one deadline between attempt body latency and retry delay", async () => {
    let attempts = 0;
    const startedAt = Date.now();
    await expect(
      pollWithinDeadline(
        startedAt + 35,
        "body and delay",
        async ({ signal }) => {
          attempts += 1;
          await new Promise((resolve, reject) => {
            const timer = setTimeout(resolve, 20);
            signal.addEventListener(
              "abort",
              () => {
                clearTimeout(timer);
                reject(signal.reason);
              },
              { once: true },
            );
          });
          return null;
        },
        { intervalMs: 50 },
      ),
    ).rejects.toBeInstanceOf(ScenarioDeadlineError);
    expect(attempts).toBe(1);
    expect(Date.now() - startedAt).toBeLessThan(150);
  });

  it("does not expose transient error details in timeout diagnostics", async () => {
    const secret = "signed-url-secret-value";
    let message = "";
    try {
      await pollWithinDeadline(
        Date.now() + 20,
        "safe poll",
        () => {
          throw new Error(secret);
        },
        { intervalMs: 5 },
      );
    } catch (error) {
      message = error.message;
    }
    expect(message).toContain("safe poll");
    expect(message).not.toContain(secret);
  });
});

describe("archive object proof presentation", () => {
  it("enumerates every nextCursor page and preserves presented object fields", async () => {
    const paths = [];
    const pages = [
      {
        objects: [
          {
            id: "object-1",
            group: "original",
            label: "stt_input_pcm",
            key: "v1/meetings/consultation/audio/original.pcm",
            contentType: "audio/L16",
            size: 128,
            sha256: "a".repeat(64),
            s3Checksum: "checksum-1",
            versionId: "version-1",
          },
        ],
        nextCursor: "object-1",
      },
      {
        objects: [
          {
            id: "object-2",
            group: "captions",
            label: "caption_vtt",
            key: "v1/meetings/consultation/captions/final.vtt",
            contentType: "text/vtt",
            size: 256,
            sha256: "b".repeat(64),
            s3Checksum: "checksum-2",
            versionId: "version-2",
          },
        ],
        nextCursor: null,
      },
    ];
    const { allArchiveObjects } = createConsultationArchiveProof({
      apiJson: async (_page, path) => {
        paths.push(path);
        return { status: 200, body: pages.shift() };
      },
      archiveObjectCeiling: 10,
      archivePageCeiling: 3,
    });

    await expect(allArchiveObjects({}, "archive-1")).resolves.toEqual([
      {
        id: "object-1",
        group: "original",
        label: "stt_input_pcm",
        key: "v1/meetings/consultation/audio/original.pcm",
        contentType: "audio/L16",
        size: 128,
        sha256: "a".repeat(64),
        s3Checksum: "checksum-1",
        versionId: "version-1",
      },
      {
        id: "object-2",
        group: "captions",
        label: "caption_vtt",
        key: "v1/meetings/consultation/captions/final.vtt",
        contentType: "text/vtt",
        size: 256,
        sha256: "b".repeat(64),
        s3Checksum: "checksum-2",
        versionId: "version-2",
      },
    ]);
    expect(paths).toEqual([
      "/api/archives/archive-1/objects",
      "/api/archives/archive-1/objects?cursor=object-1",
    ]);
  });

  it("rejects archive objects without a label or legacy class", async () => {
    const { allArchiveObjects } = createConsultationArchiveProof({
      apiJson: async () => ({
        status: 200,
        body: {
          objects: [
            {
              id: "object-1",
              group: "pipeline",
              key: "v1/meetings/consultation/pipeline/exchange.json",
              contentType: "application/json",
              size: 32,
              sha256: "a".repeat(64),
              s3Checksum: "checksum-1",
              versionId: "version-1",
            },
          ],
          nextCursor: null,
        },
      }),
      archiveObjectCeiling: 10,
      archivePageCeiling: 1,
    });

    await expect(allArchiveObjects({}, "archive-1")).rejects.toThrow(
      "archive object is missing a non-empty label/class",
    );
  });
});

describe("complete archive evidence", () => {
  const complete = {
    status: "complete",
    gaps: [],
    inventoryVersion: "inventory-version",
    inventorySha256: "a".repeat(64),
    egressIds: ["egress-1"],
    providerAttemptIds: ["attempt-1"],
    providerAttemptGroups: [
      {
        provider: "fixture",
        direction: "de-DE:en-US",
        stage: "translation",
        attemptIds: ["attempt-1"],
      },
    ],
  };

  it("requires nonempty structurally valid terminal evidence", () => {
    expect(hasCompleteArchiveEvidence(complete)).toBe(true);
    for (const incomplete of [
      { ...complete, inventoryVersion: null },
      { ...complete, inventorySha256: "" },
      { ...complete, egressIds: [] },
      { ...complete, providerAttemptIds: [] },
      { ...complete, providerAttemptGroups: [] },
      {
        ...complete,
        providerAttemptGroups: [{ ...complete.providerAttemptGroups[0], attemptIds: [] }],
      },
      { ...complete, gaps: [{}] },
    ]) {
      expect(hasCompleteArchiveEvidence(incomplete)).toBe(false);
    }
  });
});
