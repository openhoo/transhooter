import { test } from "bun:test";
import assert from "node:assert/strict";
import { Coordinator } from "../src/orchestration/coordinator";
import type { DurableStore, PlannedEffect } from "../src/orchestration/model";

const consultationId = "30000000-0000-4000-8000-000000000001";
const owner = "30000000-0000-4000-8000-000000000002";
const employeeIdentity = "30000000-0000-4000-8000-000000000003";
const customerIdentity = "30000000-0000-4000-8000-000000000004";

test("absence deadline queries LiveKit and does not finalize while a human is present", async () => {
  // Arrange
  const planned: PlannedEffect[] = [];
  let queried = false;
  let completed = false;
  const absenceDeadline = {
    consultationId,
    generation: 2,
    kind: "absence" as const,
    dueAt: new Date(0),
  };
  const store = {
    claimOutbox: async () => [],
    claimStaleReservations: async () => [],
    claimDeadlines: async () => [absenceDeadline],
    preparePendingArchiveDeletes: async () => undefined,
    currentGeneration: async () => 2,
    humanIdentities: async () => [employeeIdentity, customerIdentity] as const,
    completeDeadline: async () => {
      completed = true;
    },
    scheduleEffect: async (effect: PlannedEffect) => {
      planned.push(effect);
    },
  } as unknown as DurableStore;
  const coordinator = new Coordinator(
    store,
    { now: () => new Date(5_000) },
    {
      owner,
      leaseMs: 1_000,
      batchSize: 2,
    },
    {
      areHumansAbsent: async () => {
        queried = true;
        return false;
      },
    },
    {
      reserve: async () => true,
      release: async () => undefined,
    },
  );

  // Act
  await coordinator.tick();

  // Assert
  assert.equal(queried, true);
  assert.equal(completed, true);
  assert.deepEqual(planned, []);
});
