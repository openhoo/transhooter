import { describe, expect, it } from "bun:test";
import { persistSupervisorTerminalCheckpoints } from "../src/adapters/postgres-store";
import type { WorkerReservation } from "../src/orchestration/model";

const SOURCE = "00000000-0000-4000-8000-000000000011";
const DESTINATION = "00000000-0000-4000-8000-000000000012";

const RESERVATION: WorkerReservation = {
  consultationId: "00000000-0000-4000-8000-000000000021",
  generation: 3,
  workerId: "00000000-0000-4000-8000-000000000022",
  epoch: 2,
  heartbeatAt: new Date("2026-01-01T00:00:00Z"),
  leaseExpiresAt: new Date("2026-01-01T00:00:30Z"),
  acceptingLoad: true,
};

function fakeTransaction(results: unknown[]) {
  const calls: Array<{ readonly text: string; readonly values: readonly unknown[] }> = [];
  const execute = (text: string, values: readonly unknown[]) => {
    calls.push({ text, values });
    return Promise.resolve(results.shift() ?? []);
  };
  const transaction = Object.assign(
    (strings: TemplateStringsArray, ...values: unknown[]) => execute(strings.join("?"), values),
    {
      options: {
        parsers: {} as Record<string, unknown>,
        serializers: {} as Record<string, unknown>,
      },
      unsafe: (text: string, values: readonly unknown[] = []) => {
        const pending = execute(text, values);
        return Object.assign(pending, {
          values: async () => {
            const rows = await pending;
            return Array.isArray(rows)
              ? rows.map((row) =>
                  typeof row === "object" && row !== null ? Object.values(row) : [row],
                )
              : [];
          },
        });
      },
    },
  );
  return { calls, transaction };
}

describe("PostgresStore supervisor checkpoint settlement", () => {
  it("persists and verifies one terminal for each frozen direction", async () => {
    const fake = fakeTransaction([
      [
        { source_participant_id: SOURCE, destination_participant_id: DESTINATION },
        { source_participant_id: DESTINATION, destination_participant_id: SOURCE },
      ],
      [],
      [],
      [],
      [
        { id: "00000000-0000-4000-8000-000000000031" },
        { id: "00000000-0000-4000-8000-000000000032" },
      ],
    ]);

    const terminalCheckpointId = await persistSupervisorTerminalCheckpoints(
      fake.transaction as never,
      RESERVATION,
      "heartbeat expired",
    );
    expect(terminalCheckpointId).toBe("00000000-0000-4000-8000-000000000031");

    expect(fake.calls).toHaveLength(5);
    expect(fake.calls[1]?.text).toContain('from "worker_job_epochs"');
    expect(fake.calls[1]?.text).toContain("for update");
    expect(fake.calls[2]?.values).toContain(SOURCE);
    expect(fake.calls[2]?.values).toContain(DESTINATION);
    expect(fake.calls[3]?.values).toContain(DESTINATION);
    expect(fake.calls[3]?.values).toContain(SOURCE);
    expect(fake.calls[2]?.text).toContain(
      "accepted_input_sequence,high_watermark,received_output,emitted_output",
    );
    expect(fake.calls[2]?.text).toContain("COALESCE(previous.accepted_input_sequence+1,0)");
  });

  it("refuses to terminalize without exactly two frozen directions", async () => {
    const fake = fakeTransaction([
      [{ source_participant_id: SOURCE, destination_participant_id: DESTINATION }],
    ]);

    await expect(
      persistSupervisorTerminalCheckpoints(fake.transaction as never, RESERVATION, "cancelled"),
    ).rejects.toThrow("requires two frozen directions");
    expect(fake.calls).toHaveLength(1);
  });
});
