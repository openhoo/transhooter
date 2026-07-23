import { expect, mock, spyOn, test } from "bun:test";

mock.module("server-only", () => ({}));
mock.module("next/headers", () => ({
  cookies: async () => ({
    get: () => undefined,
    set: () => undefined,
    delete: () => undefined,
  }),
  headers: async () => new Headers(),
}));
mock.module("livekit-client", () => ({
  ConnectionState: {},
  DataPacket_Kind: { RELIABLE: 0 },
  Room: class {},
  RoomEvent: {},
  Track: {},
  TrackEvent: {},
  createLocalAudioTrack: async () => undefined,
  createLocalVideoTrack: async () => undefined,
}));
mock.module("../components/room.module.css", () => ({ default: {} }));

// Import after replacing Next's request guards so the server boundary can run in Bun.
const {
  parseRequestPayload,
  adminLanguageUpdateCommand,
  archiveDeleteCommand,
  consultationCreateCommand,
  normalizeAuthFlowError,
  presentArchiveObjects,
  presentConsultationEnd,
  presentConsultationList,
  presentLanguages,
  providerAttemptCommand,
  statusFor,
  workerFailureCommand,
} = await import("./server-application");
const { startFinalizationPolling, startShutdownCountdown } = await import(
  "../components/consultation-room"
);
const { DomainError } = await import("@transhooter/server-core");
const { trustedClientIp } = await import("./server/composition");
const { POST: heartbeatPOST } = await import("../app/api/internal/heartbeat/route");
const { createRoute } = await import("../app/api/_route");

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

test("archive deletion command requires and preserves a non-blank reason", () => {
  const consultationId = "00000000-0000-4000-8000-000000000009";
  expect(
    archiveDeleteCommand({
      consultationId,
      reason: "  retention period elapsed  ",
    }),
  ).toEqual({
    kind: "archive.delete",
    consultationId,
    reason: "retention period elapsed",
  });

  for (const body of [
    { consultationId },
    { consultationId, reason: "" },
    { consultationId, reason: "   " },
  ]) {
    expect(() => archiveDeleteCommand(body)).toThrow();
  }
});

test("consultation creation requires and preserves its UUID idempotency key", () => {
  const creationIdempotencyKey = "00000000-0000-4000-8000-000000000013";
  const payload = {
    customerEmail: "customer@example.com",
    customerName: "Customer",
    providerProfileId: "00000000-0000-4000-8000-000000000014",
    creationIdempotencyKey,
  };

  expect(consultationCreateCommand(payload)).toEqual({
    kind: "consultation.createInvitation",
    ...payload,
  });
  expect(() =>
    consultationCreateCommand({
      ...payload,
      creationIdempotencyKey: undefined,
    }),
  ).toThrow();
});

test("language update preserves the complete revision-fenced capability identity", () => {
  const payload = {
    directionId: "00000000-0000-4000-8000-000000000015",
    profileId: "00000000-0000-4000-8000-000000000016",
    profileRevision: 7,
    enabled: true,
  };

  expect(adminLanguageUpdateCommand(payload)).toEqual({
    kind: "language.enable",
    capabilityId: payload.directionId,
    profileId: payload.profileId,
    profileRevision: payload.profileRevision,
    enabled: true,
  });
  expect(() => adminLanguageUpdateCommand({ ...payload, profileRevision: 0 })).toThrow();
  expect(() => adminLanguageUpdateCommand({ ...payload, profileId: "google-eu" })).toThrow();
});

test("language list exposes the persisted provider profile identity for fenced updates", () => {
  const profileId = "00000000-0000-4000-8000-000000000016";
  expect(
    presentLanguages([
      {
        id: "00000000-0000-4000-8000-000000000015",
        profileId,
        profileName: "google-eu",
        revision: 7,
        sourceLocale: "en-US",
        targetLocale: "de-DE",
        snapshot: { region: "eu" },
        enabled: false,
        freshUntil: new Date("2026-07-19T00:00:00Z"),
      },
    ]),
  ).toMatchObject({
    directions: [
      {
        profileId,
        revision: 7,
        enabled: false,
      },
    ],
  });
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

test("consultation list carries the authenticated viewer role into presentation", () => {
  const customerId = "00000000-0000-4000-8000-000000000021";
  const employeeId = "00000000-0000-4000-8000-000000000024";
  const consultationId = "00000000-0000-4000-8000-000000000022";
  expect(
    presentConsultationList({
      viewer: { staffRole: "employee" },
      consultations: [
        {
          id: consultationId,
          state: "invited",
          archiveState: "pending",
          providerProfileId: "google-eu",
          providerProfileRevision: 1,
          participants: [
            {
              id: employeeId,
              role: "employee",
              userId: employeeId,
              livekitIdentity: employeeId,
              displayName: "Employee",
              language: null,
              consent: null,
            },
            {
              id: customerId,
              role: "customer",
              userId: customerId,
              livekitIdentity: customerId,
              displayName: "Customer",
              language: null,
              consent: null,
            },
          ],
          providerSelection: null,
          snapshotHash: null,
          generation: 0,
          roomName: null,
          roomSid: null,
          dispatchId: null,
          compositeEgressId: null,
          createdAt: new Date("2026-01-01T00:00:00Z"),
          updatedAt: new Date("2026-01-01T00:00:00Z"),
        },
      ],
    }),
  ).toMatchObject({
    viewer: { staffRole: "employee" },
    consultations: [{ id: consultationId, customerName: "Customer", canResend: true }],
  });
});

test("archive object presentation exposes only the public payload with response aliases", () => {
  const id = "00000000-0000-4000-8000-000000000023";
  expect(
    presentArchiveObjects({
      objects: [
        {
          id,
          object_class: "participant_original",
          key: "v1/meetings/consultation/audio/original.ogg",
          content_type: "audio/ogg",
          size: "128",
          sha256: "a".repeat(64),
          s3_checksum: "checksum-1",
          version_id: "version-1",
        },
      ],
      cursor: id,
    }),
  ).toEqual({
    objects: [
      {
        id,
        group: "original",
        label: "participant_original",
        key: "v1/meetings/consultation/audio/original.ogg",
        contentType: "audio/ogg",
        size: 128,
        sha256: "a".repeat(64),
        s3Checksum: "checksum-1",
        versionId: "version-1",
      },
    ],
    nextCursor: id,
  });
});

test("shutdown countdown updates immediately on deadline-aligned boundaries through exact zero", () => {
  let now = 7_250;
  let scheduled: (() => void) | undefined;
  let scheduledDelay: number | undefined;
  const updates: number[] = [];
  const scheduler = {
    now: () => now,
    setTimeout(callback: () => void, delayMs: number) {
      scheduled = callback;
      scheduledDelay = delayMs;
      return 1;
    },
    clearTimeout() {
      scheduled = undefined;
      scheduledDelay = undefined;
    },
  };

  const stop = startShutdownCountdown(10_000, (seconds) => updates.push(seconds), scheduler);
  expect(updates).toEqual([3]);
  expect(scheduledDelay).toBe(750);

  for (const expectedSeconds of [2, 1, 0]) {
    const callback = scheduled;
    const delay = scheduledDelay;
    expect(callback).toBeDefined();
    expect(delay).toBeDefined();
    scheduled = undefined;
    scheduledDelay = undefined;
    now += delay ?? 0;
    callback?.();
    expect(updates.at(-1)).toBe(expectedSeconds);
    if (expectedSeconds > 0) expect(Number(scheduledDelay)).toBe(1_000);
  }

  expect(updates).toEqual([3, 2, 1, 0]);
  expect(scheduled).toBeUndefined();
  stop();
});

function createFinalizationPollingScheduler() {
  let nextId = 1;
  const timers = new Map<number, { callback: () => void; delayMs: number }>();
  const scheduler = {
    setTimeout(callback: () => void, delayMs: number) {
      const id = nextId++;
      timers.set(id, { callback, delayMs });
      return id;
    },
    clearTimeout(id: number) {
      timers.delete(id);
    },
  };
  return {
    scheduler,
    pendingDelays: () => [...timers.values()].map(({ delayMs }) => delayMs).sort((a, b) => a - b),
    run(delayMs: number) {
      const timer = [...timers].find(([, pending]) => pending.delayMs === delayMs);
      expect(timer).toBeDefined();
      if (!timer) return;
      timers.delete(timer[0]);
      timer[1].callback();
    },
  };
}
test("finalization polling times out a stalled single-flight attempt before polling again", async () => {
  const timers = createFinalizationPollingScheduler();
  const firstAttempt = Promise.withResolvers<{ redirectTo?: string }>();
  const secondAttempt = Promise.withResolvers<{ redirectTo?: string }>();
  let firstSignal: AbortSignal | undefined;
  let secondSignal: AbortSignal | undefined;
  let requestNumber = 0;
  const request = mock((_consultationId: string, signal: AbortSignal) => {
    requestNumber += 1;
    if (requestNumber === 1) {
      firstSignal = signal;
      return firstAttempt.promise;
    }
    secondSignal = signal;
    return secondAttempt.promise;
  });

  const stop = startFinalizationPolling("00000000-0000-4000-8000-000000000025", () => undefined, {
    scheduler: timers.scheduler,
    request,
    pollIntervalMs: 25,
    attemptTimeoutMs: 100,
  });
  expect(request).toHaveBeenCalledTimes(1);
  expect(timers.pendingDelays()).toEqual([100]);

  timers.run(100);
  await Promise.resolve();
  await Promise.resolve();
  expect(firstSignal?.aborted).toBe(true);
  expect(timers.pendingDelays()).toEqual([25]);

  timers.run(25);
  expect(request).toHaveBeenCalledTimes(2);
  expect(secondSignal?.aborted).toBe(false);
  expect(timers.pendingDelays()).toEqual([100]);

  firstAttempt.resolve({ redirectTo: "/archives/stale" });
  await Promise.resolve();
  expect(request).toHaveBeenCalledTimes(2);

  stop();
  expect(secondSignal?.aborted).toBe(true);
  expect(timers.pendingDelays()).toEqual([]);
});

test("finalization polling cleanup aborts the active attempt and clears every timer", async () => {
  const timers = createFinalizationPollingScheduler();
  let attemptSignal: AbortSignal | undefined;
  const request = mock((_consultationId: string, signal: AbortSignal) => {
    attemptSignal = signal;
    const { promise, reject } = Promise.withResolvers<{ redirectTo?: string }>();
    signal.addEventListener("abort", () => reject(new Error("cleanup aborted")), { once: true });
    return promise;
  });

  const stop = startFinalizationPolling("00000000-0000-4000-8000-000000000026", () => undefined, {
    scheduler: timers.scheduler,
    request,
    pollIntervalMs: 25,
    attemptTimeoutMs: 100,
  });
  expect(request).toHaveBeenCalledTimes(1);
  expect(timers.pendingDelays()).toEqual([100]);

  stop();
  await Promise.resolve();
  await Promise.resolve();
  expect(attemptSignal?.aborted).toBe(true);
  expect(timers.pendingDelays()).toEqual([]);
  expect(request).toHaveBeenCalledTimes(1);
});

test("finalization polling cleanup clears a scheduled retry", async () => {
  const timers = createFinalizationPollingScheduler();
  const request = mock(async () => ({}));
  const stop = startFinalizationPolling("00000000-0000-4000-8000-000000000027", () => undefined, {
    scheduler: timers.scheduler,
    request,
    pollIntervalMs: 25,
    attemptTimeoutMs: 100,
  });

  await Promise.resolve();
  await Promise.resolve();
  expect(request).toHaveBeenCalledTimes(1);
  expect(timers.pendingDelays()).toEqual([25]);
  stop();
  expect(timers.pendingDelays()).toEqual([]);
});

test("finalization polling redirects once and stops permanently", async () => {
  const timers = createFinalizationPollingScheduler();
  const { promise: redirected, resolve: resolveRedirect } = Promise.withResolvers<string>();
  const onRedirect = mock((redirectTo: string) => resolveRedirect(redirectTo));
  const request = mock(async () => ({ redirectTo: "/archives/final" }));

  const stop = startFinalizationPolling("00000000-0000-4000-8000-000000000028", onRedirect, {
    scheduler: timers.scheduler,
    request,
    pollIntervalMs: 25,
    attemptTimeoutMs: 100,
  });
  expect(await redirected).toBe("/archives/final");
  expect(onRedirect).toHaveBeenCalledTimes(1);
  expect(request).toHaveBeenCalledTimes(1);
  expect(timers.pendingDelays()).toEqual([]);

  stop();
  expect(timers.pendingDelays()).toEqual([]);
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

test("native heartbeat route classifies invalid UTF-8 JSON bodies as HTTP 400", async () => {
  const response = await heartbeatPOST(
    new Request("http://localhost/api/internal/heartbeat", {
      method: "POST",
      body: new Uint8Array([0x7b, 0x22, 0x78, 0x22, 0x3a, 0xff, 0x7d]),
    }),
  );

  expect(response.status).toBe(400);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toEqual({
    code: "REQUEST_FAILED",
    message: "Request body is not valid JSON",
  });
});

test("application failure logs identify the operation without exposing error or request data", async () => {
  const sensitiveRequestValue = "never-log-this-request-value";
  const sensitiveErrorText = "never-log-this-error-message";
  let readCount = 0;
  const body = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (readCount++ === 0) {
        controller.enqueue(new TextEncoder().encode(sensitiveRequestValue));
        return;
      }
      controller.error(new Error(sensitiveErrorText));
    },
  });
  const consoleError = spyOn(console, "error").mockImplementation(() => undefined);
  try {
    const response = await heartbeatPOST(
      new Request("http://localhost/api/internal/heartbeat?token=never-log-this-query", {
        method: "POST",
        body,
      }),
    );

    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({
      code: "REQUEST_FAILED",
      message: "The request could not be completed",
    });
    expect(consoleError).toHaveBeenCalledTimes(1);
    expect(consoleError.mock.calls[0]).toEqual([
      "Unhandled API request failure",
      {
        operation: "internal.worker.heartbeat",
        name: "Error",
      },
    ]);
    const logged = JSON.stringify(consoleError.mock.calls);
    expect(logged).not.toContain(sensitiveRequestValue);
    expect(logged).not.toContain(sensitiveErrorText);
    expect(logged).not.toContain("never-log-this-query");
    expect(logged).not.toContain("http://localhost");
  } finally {
    consoleError.mockRestore();
  }
});

test("native route boundary converts unhandled failures to HTTP 503", async () => {
  const route = createRoute("internal.worker.heartbeat");
  const response = await route(new Request("http://localhost/api/internal/heartbeat"), {
    params: Promise.reject(new Error("route params unavailable")),
  });

  expect(response.status).toBe(503);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toEqual({
    code: "REQUEST_FAILED",
    message: "The request could not be completed",
  });
});

test("domain errors preserve their normalized HTTP status at the request boundary", () => {
  expect(statusFor(new DomainError("UNAUTHORIZED_INTERNAL"))).toBe(401);
  expect(statusFor(new DomainError("FORBIDDEN"))).toBe(403);
  expect(statusFor(new DomainError("NOT_FOUND"))).toBe(404);
  expect(statusFor(new DomainError("INVALID_STATE"))).toBe(409);
  expect(statusFor(new DomainError("CONSULTATION_CREATION_CONFLICT"))).toBe(409);
  expect(statusFor(new DomainError("CAPABILITY_REVISION_CONFLICT"))).toBe(409);
});

test("exchange failures expose one invalid, expired, or used response shape", () => {
  for (const code of [
    "INVALID_OR_EXPIRED_LINK",
    "INVALID_EXCHANGE",
    "INVALID_EXCHANGE_CONTEXT",
  ] as const) {
    const normalized = normalizeAuthFlowError("auth.exchange.verify", new DomainError(code));
    expect({ code: normalized.code, message: normalized.message }).toEqual({
      code: "INVALID_OR_EXPIRED_LINK",
      message: "This sign-in flow is unavailable",
    });
  }

  const unrelated = new DomainError("CSRF_REJECTED");
  expect(normalizeAuthFlowError("consultations.list", unrelated)).toBe(unrelated);
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
