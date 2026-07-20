import { describe, expect, it } from "bun:test";
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
