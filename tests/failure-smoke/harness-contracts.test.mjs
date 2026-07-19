import { describe, expect, it } from "bun:test";
import { archiveRaceWinner, restartCountIncremented } from "./harness-contracts.mjs";

describe("failure-smoke proof contracts", () => {
  it("requires the Docker RestartCount itself to increase", () => {
    expect(restartCountIncremented(2, 3)).toBe(true);
    expect(restartCountIncremented(2, 2)).toBe(false);
    expect(restartCountIncremented(2, 1)).toBe(false);
  });

  it("accepts exactly one admitted archive operation with matching durable outcome", () => {
    expect(
      archiveRaceWinner({
        holdOk: true,
        deleteOk: false,
        archiveState: "complete",
        activeHoldCount: 1,
      }),
    ).toBe("hold");
    expect(
      archiveRaceWinner({
        holdOk: false,
        deleteOk: true,
        archiveState: "deleting",
        activeHoldCount: 0,
      }),
    ).toBe("delete");
    for (const falsePositive of [
      { holdOk: true, deleteOk: true, archiveState: "complete", activeHoldCount: 1 },
      { holdOk: false, deleteOk: false, archiveState: "complete", activeHoldCount: 0 },
      { holdOk: true, deleteOk: false, archiveState: "complete", activeHoldCount: 0 },
      { holdOk: false, deleteOk: true, archiveState: "complete", activeHoldCount: 0 },
    ]) {
      expect(archiveRaceWinner(falsePositive)).toBeNull();
    }
  });
});
