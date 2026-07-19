import { expect, mock, test } from "bun:test";

mock.module("server-only", () => ({}));
mock.module("next/headers", () => ({
  cookies: async () => ({
    get: () => undefined,
    set: () => undefined,
    delete: () => undefined,
  }),
  headers: async () => new Headers(),
}));

// Import after replacing Next's request guards so the server boundary can run in Bun.
const {
  parseRequestPayload,
  presentConsultationEnd,
  providerAttemptCommand,
  statusFor,
  workerFailureCommand,
} = await import("./server-application");
const { DomainError } = await import("@transhooter/server-core");
const { trustedClientIp } = await import("./composition");
const { POST } = await import("../app/api/[...path]/route");

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

test("consultation end presents the service wrapper without inventing a deadline", () => {
  const participant = (id: string, role: "employee" | "customer") => ({
    id,
    role,
    userId: id,
    livekitIdentity: id,
    displayName: role,
    language: "en-US",
    consent: null,
  });
  const consultation = {
    id: "00000000-0000-4000-8000-000000000010",
    state: "finalizing",
    archiveState: "reconciling",
    providerProfileId: "google-eu",
    providerProfileRevision: 1,
    participants: [
      participant("00000000-0000-4000-8000-000000000011", "employee"),
      participant("00000000-0000-4000-8000-000000000012", "customer"),
    ],
    providerSelection: null,
    snapshotHash: null,
    generation: 4,
    roomName: "room",
    roomSid: "RM_room",
    dispatchId: "dispatch",
    compositeEgressId: "egress",
    createdAt: new Date("2026-01-01T00:00:00Z"),
    updatedAt: new Date("2026-01-01T00:00:01Z"),
  };
  expect(
    presentConsultationEnd({
      consultation,
      generation: 4,
      shutdownAtMs: 1_700_000_005_000,
    }),
  ).toEqual({ generation: 4, shutdownAtMs: 1_700_000_005_000 });
});

test("request payload parsing rejects declared and streamed bodies above the cap", async () => {
  const declared = new Request("http://localhost/api/internal/heartbeat", {
    method: "POST",
    headers: { "content-length": "1048577" },
    body: "{}",
  });
  await expect(parseRequestPayload("internal.worker.heartbeat", declared)).rejects.toMatchObject({
    code: "PAYLOAD_TOO_LARGE",
  });

  const streamed = new Request("http://localhost/api/webhooks/livekit", {
    method: "POST",
    body: new Uint8Array(1_048_577),
  });
  await expect(parseRequestPayload("webhooks.livekit.receive", streamed)).rejects.toMatchObject({
    code: "PAYLOAD_TOO_LARGE",
  });
});

test("request boundary classifies invalid UTF-8 JSON bodies as HTTP 400", async () => {
  const response = await POST(
    new Request("http://localhost/api/internal/heartbeat", {
      method: "POST",
      body: new Uint8Array([0x7b, 0x22, 0x78, 0x22, 0x3a, 0xff, 0x7d]),
    }),
    { params: Promise.resolve({ path: ["internal", "heartbeat"] }) },
  );

  expect(response.status).toBe(400);
  expect(await response.json()).toEqual({
    code: "REQUEST_FAILED",
    message: "Request body is not valid JSON",
  });
});

test("unauthorized internal principals map to HTTP 401", () => {
  expect(statusFor(new DomainError("UNAUTHORIZED_INTERNAL"))).toBe(401);
});

test("worker failure parser preserves the authenticated fencing envelope", () => {
  const failure = {
    consultationId: envelope.consultationId,
    generation: 3,
    workerId: envelope.workerId,
    epoch: 2,
    eventId: envelope.eventId,
    kind: "SpoolUnavailable",
    message: "fsync failed",
    lastCheckpointHashes: {
      "00000000-0000-4000-8000-000000000009": "a".repeat(64),
    },
  };
  expect(workerFailureCommand(failure)).toEqual({
    kind: "internal.failure",
    kindName: "SpoolUnavailable",
    consultationId: failure.consultationId,
    generation: failure.generation,
    workerId: failure.workerId,
    epoch: failure.epoch,
    eventId: failure.eventId,
    message: failure.message,
    lastCheckpointHashes: failure.lastCheckpointHashes,
  });
});

test("rate limiter ignores caller forwarding headers in direct-local mode", () => {
  const request = new Request("http://localhost", {
    headers: {
      "x-forwarded-for": "198.51.100.25",
      "x-real-ip": "198.51.100.26",
    },
  });

  expect(trustedClientIp(request, { mode: "direct-local" })).toBe("direct-local");
});

test("rate limiter uses only the explicitly trusted boundary header", () => {
  const request = new Request("https://app.example", {
    headers: {
      "x-forwarded-for": "192.0.2.250",
      "x-real-ip": "192.0.2.251",
      "x-transhooter-client-ip": "2001:db8::25",
    },
  });

  expect(
    trustedClientIp(request, {
      mode: "trusted-header",
      headerName: "x-transhooter-client-ip",
    }),
  ).toBe("2001:db8::25");
  expect(
    trustedClientIp(request, {
      mode: "trusted-header",
      headerName: "x-another-boundary-client-ip",
    }),
  ).toBe("unknown");
});

test("rate limiter rejects malformed or multi-address trusted boundary values", () => {
  for (const address of ["not-an-ip", "198.51.100.25, 10.0.0.7", "[2001:db8::25]:443"]) {
    const request = new Request("https://app.example", {
      headers: { "x-transhooter-client-ip": address },
    });
    expect(
      trustedClientIp(request, {
        mode: "trusted-header",
        headerName: "x-transhooter-client-ip",
      }),
    ).toBe("unknown");
  }
});
