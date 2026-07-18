import { expect, mock, test } from "bun:test";

mock.module("server-only", () => ({}));

// Import after replacing Next's server-only guard so this pure parsing boundary can run in Bun.
const { providerAttemptCommand } = await import("./server-application");

const report = {
  directionId: "00000000-0000-4000-8000-000000000001",
  stage: "translation" as const,
  terminalId: "00000000-0000-4000-8000-000000000002",
  operationId: "00000000-0000-4000-8000-000000000003",
  attemptId: "00000000-0000-4000-8000-000000000004",
  attemptNumber: 1,
  retryOfAttemptId: null,
  outcome: "succeeded" as const,
  error: null,
  retryDecision: {
    action: "do_not_retry" as const,
    reason: "accepted",
    retryAtMs: null,
    previousAttemptId: null,
  },
  watermarks: {
    acceptedInputSequence: 0,
    acceptedInputSampleEnd: 4_000,
    receivedOutputSequence: 0,
    receivedOutputSampleEnd: null,
    emittedOutputSequence: null,
    emittedOutputSampleEnd: null,
  },
  credentialVersion: "7",
  credentialFingerprint: "opaque-fingerprint",
  transport: "http" as const,
  rawReferences: [
    {
      objectId: "00000000-0000-4000-8000-000000000005",
      ordinal: 0,
      sha256: "a".repeat(64),
      size: 128,
      mediaType: "application/json",
    },
  ],
  terminalHash: "b".repeat(64),
  startedAtMs: 1_700_000_000_000,
  occurredAtMs: 1_700_000_000_100,
};

const envelope = {
  consultationId: "00000000-0000-4000-8000-000000000006",
  generation: 3,
  workerId: "00000000-0000-4000-8000-000000000007",
  epoch: 2,
  eventId: "00000000-0000-4000-8000-000000000008",
  report,
};

test("provider attempt command parses the exact worker envelope", () => {
  expect(providerAttemptCommand(envelope)).toEqual({
    kind: "internal.providerAttempt",
    ...envelope,
  });
});

test("provider attempt command rejects envelope extras and inconsistent reports", () => {
  expect(() => providerAttemptCommand({ ...envelope, provider: "untrusted" })).toThrow();
  expect(() =>
    providerAttemptCommand({
      ...envelope,
      report: { ...report, outcome: "failed", error: null },
    }),
  ).toThrow();
});
