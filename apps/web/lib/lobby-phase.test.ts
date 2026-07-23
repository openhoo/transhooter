import { describe, expect, test } from "bun:test";
import {
  describeLobbyPhase,
  type LobbyPhase,
  LobbyPreviewFence,
  lobbyPreferencesPayload,
  lobbyPreviewConstraints,
  type PreviewStream,
} from "./browser/lobby-phase";

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

describe("lobby device selection", () => {
  test("requests the explicitly selected microphone and camera", () => {
    expect(lobbyPreviewConstraints({ microphoneId: "microphone-2", cameraId: "camera-3" })).toEqual(
      {
        audio: { deviceId: { exact: "microphone-2" } },
        video: { deviceId: { exact: "camera-3" } },
      },
    );
  });

  test("uses browser defaults only for unselected device kinds", () => {
    expect(lobbyPreviewConstraints({ microphoneId: "", cameraId: "camera-3" })).toEqual({
      audio: true,
      video: { deviceId: { exact: "camera-3" } },
    });
  });

  test("preferences payload excludes local device identities", () => {
    const values = new FormData();
    values.set("displayName", "Customer");
    values.set("language", "de-DE");
    values.set("microphone", "microphone-2");
    values.set("camera", "camera-3");

    expect(lobbyPreferencesPayload(values)).toEqual({
      displayName: "Customer",
      language: "de-DE",
    });
  });
});

describe("lobby preview request fence", () => {
  test("only the newest concurrent acquisition can own its stream", () => {
    const firstStops = [0, 0];
    const secondStops = [0, 0];
    const firstStream: PreviewStream = {
      getTracks: () =>
        firstStops.map((_, index) => ({
          stop: () => {
            firstStops[index] = (firstStops[index] ?? 0) + 1;
          },
        })),
    };
    const secondStream: PreviewStream = {
      getTracks: () =>
        secondStops.map((_, index) => ({
          stop: () => {
            secondStops[index] = (secondStops[index] ?? 0) + 1;
          },
        })),
    };
    const fence = new LobbyPreviewFence();

    const firstRequest = fence.begin();
    const secondRequest = fence.begin();

    expect(firstRequest.signal.aborted).toBe(true);
    expect(secondRequest.generation).toBeGreaterThan(firstRequest.generation);
    expect(fence.adopt(firstRequest, firstStream)).toBe(false);
    expect(firstStops).toEqual([1, 1]);
    expect(fence.adopt(secondRequest, secondStream)).toBe(true);
    expect(fence.owns(firstRequest, firstStream)).toBe(false);
    expect(fence.owns(secondRequest, secondStream)).toBe(true);
    expect(secondStops).toEqual([0, 0]);
  });

  test("superseding, settling, and canceling release every owned track", () => {
    const stops = [0, 0];
    const stream: PreviewStream = {
      getTracks: () =>
        stops.map((_, index) => ({
          stop: () => {
            stops[index] = (stops[index] ?? 0) + 1;
          },
        })),
    };
    const fence = new LobbyPreviewFence();
    const request = fence.begin();

    expect(fence.adopt(request, stream)).toBe(true);
    const supersedingRequest = fence.begin();
    expect(stops).toEqual([1, 1]);
    expect(request.signal.aborted).toBe(true);

    expect(fence.settle(request)).toBe(false);
    expect(fence.settle(supersedingRequest)).toBe(true);
    expect(supersedingRequest.signal.aborted).toBe(true);
  });

  test("a canceled acquisition cannot adopt or retain a late stream", () => {
    let stops = 0;
    const stream: PreviewStream = {
      getTracks: () => [
        {
          stop: () => {
            stops += 1;
          },
        },
      ],
    };
    const fence = new LobbyPreviewFence();
    const request = fence.begin();

    fence.cancel();

    expect(request.signal.aborted).toBe(true);
    expect(fence.adopt(request, stream)).toBe(false);
    expect(fence.owns(request, stream)).toBe(false);
    expect(fence.settle(request)).toBe(false);
    expect(stops).toBe(1);
  });

  test("a throwing track cannot prevent the remaining tracks from stopping", () => {
    let secondTrackStops = 0;
    const stream: PreviewStream = {
      getTracks: () => [
        {
          stop: () => {
            throw new Error("track already detached");
          },
        },
        {
          stop: () => {
            secondTrackStops += 1;
          },
        },
      ],
    };
    const fence = new LobbyPreviewFence();
    const request = fence.begin();
    fence.adopt(request, stream);

    expect(() => {
      fence.cancel();
    }).not.toThrow();
    expect(secondTrackStops).toBe(1);
  });
});
