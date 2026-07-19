import { describe, expect, it } from "bun:test";
import { acceptedCaptionMatchesRender, MODE_GAIN_PAIRS } from "./harness-contracts.mjs";

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
