import { describe, expect, test } from "bun:test";
import { describeLobbyPhase, type LobbyPhase } from "./lobby-phase";

describe("lobby phase descriptors", () => {
  const cases: ReadonlyArray<
    readonly [
      LobbyPhase,
      1 | 2 | null,
      boolean,
      string,
      "preferences" | "waiting" | "provider-consent" | "consent-waiting" | "terminal",
    ]
  > = [
    ["preferences", 1, false, "Step 1: Choose devices and language.", "preferences"],
    [
      "waiting",
      1,
      true,
      "Step 1: Preferences saved. Waiting for the other participant’s preferences.",
      "waiting",
    ],
    ["consent", 2, true, "Step 2: Provider details ready. Review and consent.", "provider-consent"],
    [
      "consent-waiting",
      2,
      true,
      "Step 2: Your consent is recorded. Waiting for the other participant’s consent.",
      "consent-waiting",
    ],
    ["ready", 2, true, "Consent recorded. Preparing the consultation.", "provider-consent"],
    ["terminal", null, false, "This consultation is closed.", "terminal"],
  ];

  test.each(cases)(
    "maps %s to one complete view descriptor",
    (phase, stage, polls, announcement, contentKind) => {
      expect(describeLobbyPhase(phase)).toEqual({ stage, polls, announcement, contentKind });
    },
  );
});
