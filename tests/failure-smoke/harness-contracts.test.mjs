import { describe, expect, it } from "bun:test";
import { readFile } from "node:fs/promises";
import {
  archiveLockHierarchyBlocked,
  archiveRaceWinner,
  crashRecoveryMatches,
  createBarrier,
  healthFailureAfter,
  isTransientPollError,
  pollUntil,
  restartCountIncremented,
  runWithDeadline,
  settlementProblems,
  workerCrashMatches,
} from "./harness-contracts.mjs";

describe("failure-smoke proof contracts", () => {
  it("requires the Docker RestartCount itself to increase", () => {
    expect(restartCountIncremented(2, 3)).toBe(true);
    expect(restartCountIncremented(2, 2)).toBe(false);
    expect(restartCountIncremented(2, 1)).toBe(false);
  });

  it("correlates a crash to one worker identity, generation, and epoch", () => {
    const expected = { workerId: "worker-a", generation: 4, epoch: 7 };
    const reservation = { workerId: "worker-a", generation: 4, epoch: 7 };
    const terminal = {
      workerId: "worker-a",
      generation: 4,
      epoch: 7,
      terminalAt: "2026-07-19T12:00:00.000Z",
    };
    expect(workerCrashMatches(expected, reservation, terminal)).toBe(true);
    expect(workerCrashMatches(expected, { ...reservation, workerId: "worker-b" }, terminal)).toBe(
      false,
    );
    expect(workerCrashMatches(expected, reservation, { ...terminal, generation: 5 })).toBe(false);
    expect(workerCrashMatches(expected, reservation, { ...terminal, epoch: 8 })).toBe(false);
    expect(workerCrashMatches(expected, reservation, { ...terminal, terminalAt: null })).toBe(
      false,
    );
  });

  it("times out even when a wait body ignores its AbortSignal", async () => {
    const started = Date.now();
    await expect(
      pollUntil("stuck body", () => new Promise(() => {}), {
        deadline: Date.now() + 25,
        intervalMs: 1,
        attemptMs: 10,
      }),
    ).rejects.toThrow(/absolute deadline|timed out/u);
    expect(Date.now() - started).toBeLessThan(500);
  });

  it("fails polling immediately on permanent or unknown errors", async () => {
    let attempts = 0;
    const fatal = new Error("invalid durable state");
    await expect(
      pollUntil(
        "fatal check",
        () => {
          attempts += 1;
          throw fatal;
        },
        { deadline: Date.now() + 5_000, intervalMs: 1 },
      ),
    ).rejects.toBe(fatal);
    expect(attempts).toBe(1);
    expect(isTransientPollError(fatal)).toBe(false);
  });

  it("retries recognized transient transport errors", async () => {
    let attempts = 0;
    const result = await pollUntil(
      "transient check",
      () => {
        attempts += 1;
        if (attempts === 1) {
          throw new TypeError("fetch failed", { cause: { code: "ECONNRESET" } });
        }
        return "ready";
      },
      { deadline: Date.now() + 5_000, intervalMs: 1 },
    );
    expect(result).toBe("ready");
    expect(attempts).toBe(2);
  });

  it("propagates parent cancellation into a body operation", async () => {
    const controller = new AbortController();
    const reason = new Error("scenario cancelled");
    const operation = runWithDeadline(
      "body",
      Date.now() + 5_000,
      () => new Promise(() => {}),
      controller.signal,
    );
    controller.abort(reason);
    await expect(operation).rejects.toBe(reason);
  });

  it("rejects a healthy or restarted worker that is not the crashed worker successor", () => {
    const expected = {
      workerId: "control-a",
      containerId: "container-a",
      restartCount: 3,
      startedAt: "2026-07-19T12:00:00.000Z",
    };
    expect(
      crashRecoveryMatches(expected, {
        workerId: "control-a",
        containerId: "container-a",
        restartCount: 4,
        startedAt: "2026-07-19T12:00:01.000Z",
      }),
    ).toBe(true);
    expect(
      crashRecoveryMatches(expected, {
        workerId: "control-b",
        containerId: "container-b",
        restartCount: 9,
        startedAt: "2026-07-19T12:00:01.000Z",
      }),
    ).toBe(false);
    expect(
      crashRecoveryMatches(expected, {
        workerId: "control-a",
        containerId: "container-a",
        restartCount: 4,
        startedAt: expected.startedAt,
      }),
    ).toBe(false);
  });

  it("does not release the hold/delete race before both lock levels visibly block", () => {
    expect(
      archiveLockHierarchyBlocked([
        { waitEventType: "Lock", query: "update archives set state = $1" },
      ]),
    ).toBe(false);
    expect(
      archiveLockHierarchyBlocked([
        { waitEventType: "Client", query: "select id from consultations for update" },
        { waitEventType: "Lock", query: "select state from archives where id = $1 for update" },
      ]),
    ).toBe(false);
    expect(
      archiveLockHierarchyBlocked([
        { waitEventType: "Lock", query: "update archives set state = $1" },
        { waitEventType: "Lock", query: "select id from consultations for update" },
      ]),
    ).toBe(true);
  });

  it("rejects settlement with any unfinished durable state", () => {
    const clean = {
      state: "ended",
      archive_state: "complete",
      pending_outbox: 0,
      active_effects: 0,
      unresolved_expectations: 0,
      unfinished_deadlines: 0,
      active_egress: 0,
      unclean_dispatches: 0,
      unclean_rooms: 0,
      unfenced_reservations: 0,
      unterminated_worker_epochs: 0,
      room_cleanup_confirmed: true,
    };
    expect(settlementProblems(clean)).toEqual([]);
    for (const field of [
      "pending_outbox",
      "active_effects",
      "active_egress",
      "unfenced_reservations",
      "unterminated_worker_epochs",
    ]) {
      expect(settlementProblems({ ...clean, [field]: 1 })).not.toEqual([]);
    }
    expect(
      settlementProblems({
        ...clean,
        archive_state: "incomplete",
        unresolved_expectations: 1,
      }),
    ).toEqual([]);
    expect(settlementProblems({ ...clean, unresolved_expectations: 1 })).toContain(
      "archive-expectations",
    );
  });

  it("admits only health failures at or after the scenario watermark", () => {
    const watermark = Date.parse("2026-07-19T12:00:00.000Z");
    expect(healthFailureAfter({ ExitCode: 1, End: "2026-07-19T12:00:00.000Z" }, watermark)).toBe(
      true,
    );
    expect(healthFailureAfter({ ExitCode: 1, End: "2026-07-19T11:59:59.999Z" }, watermark)).toBe(
      false,
    );
    expect(healthFailureAfter({ ExitCode: 0, End: "2026-07-19T12:00:01.000Z" }, watermark)).toBe(
      false,
    );
  });

  it("releases a deterministic race only after every participant arrives", async () => {
    const barrier = createBarrier(2);
    const events = [];
    const first = (async () => {
      events.push("first-ready");
      await barrier.arrive();
      events.push("first-released");
    })();
    await Promise.resolve();
    expect(events).toEqual(["first-ready"]);
    const second = (async () => {
      events.push("second-ready");
      await barrier.arrive();
      events.push("second-released");
    })();
    await Promise.all([first, second]);
    expect(events.slice(0, 2)).toEqual(["first-ready", "second-ready"]);
    expect(events.slice(2).sort()).toEqual(["first-released", "second-released"]);
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
  it("captures ordered database diagnostics in one bounded disposable container", async () => {
    const wrapper = await readFile(new URL("./internal/failure-smoke", import.meta.url), "utf8");
    const functionStart = wrapper.indexOf("capture_database_diagnostics() {");
    const cleanupStart = wrapper.indexOf("\ncleanup() {", functionStart);
    const diagnosticFunction = wrapper.slice(functionStart, cleanupStart);

    expect(functionStart).toBeGreaterThan(-1);
    expect(cleanupStart).toBeGreaterThan(functionStart);
    expect(diagnosticFunction.match(/failure-smoke bun --eval/g)).toHaveLength(1);
    expect(diagnosticFunction).toContain(
      'compose_bounded "Capturing database diagnostics" "$COMPOSE_DIAGNOSTIC_TIMEOUT_SECONDS"',
    );
    expect(diagnosticFunction).toContain("run --rm --no-deps");
    expect(diagnosticFunction).toContain("for (const [label, statement] of diagnostics)");
    expect(diagnosticFunction).toContain("catch (error)");
    expect(diagnosticFunction).toContain("failed = true");
    expect(diagnosticFunction).toContain("if (failed) process.exitCode = 1");

    const labels = [
      "Capturing consultation diagnostics",
      "Capturing external-effect diagnostics",
      "Capturing archive recovery diagnostics",
      "Capturing outbox diagnostics",
    ];
    let previousLabel = -1;
    for (const label of labels) {
      const labelIndex = diagnosticFunction.indexOf(label);
      expect(labelIndex).toBeGreaterThan(previousLabel);
      previousLabel = labelIndex;
    }
    for (const queryFragment of [
      "FROM consultations ORDER BY created_at DESC LIMIT 3",
      "FROM external_effects GROUP BY generation, effect_kind, state, attempts",
      "FROM archives a LEFT JOIN archive_objects o ON o.archive_id=a.id",
      "FROM outbox WHERE delivered_at IS NULL ORDER BY available_at, id",
    ]) {
      expect(diagnosticFunction).toContain(queryFragment);
    }

    const statusIndex = wrapper.indexOf('"Capturing failure-smoke service status"');
    const logsIndex = wrapper.indexOf('"Capturing failure-smoke logs"', statusIndex);
    const databaseIndex = wrapper.indexOf("capture_database_diagnostics || true", logsIndex);
    expect(statusIndex).toBeGreaterThan(-1);
    expect(logsIndex).toBeGreaterThan(statusIndex);
    expect(databaseIndex).toBeGreaterThan(logsIndex);
  });
});
