import { describe, expect, it, mock } from "bun:test";
import type {
  InternalPrincipal,
  InternalPrincipalVerifier,
  SessionRecord,
  UserRecord,
} from "../src/ports/index";
import { createWebApplication } from "../src/web-application";

const CONSULTATION = "00000000-0000-4000-8000-000000000003";
const PUBLIC_BASE_URL = "https://app.example";
const SESSION: SessionRecord = {
  id: "00000000-0000-4000-8000-000000000001",
  userId: "00000000-0000-4000-8000-000000000002",
  tokenHash: "t",
  csrfHash: "c",
  expiresAt: new Date("2026-01-02T00:00:00Z"),
  reauthenticatedAt: new Date("2026-01-01T00:00:00Z"),
  reauthConsultationId: CONSULTATION,
};
const USER: UserRecord = {
  id: SESSION.userId,
  email: "admin@example.com",
  displayName: "Admin",
  staffRole: "admin",
};

const CONTROL_PRINCIPAL: InternalPrincipal = {
  service: "control-worker",
  subject: "control",
  permissions: ["delete:drain"],
};
const PROVIDER_REPORT = {
  directionId: "00000000-0000-4000-8000-000000000010",
  stage: "stt" as const,
  terminalId: "00000000-0000-4000-8000-000000000011",
  operationId: "00000000-0000-4000-8000-000000000012",
  attemptId: "00000000-0000-4000-8000-000000000013",
  attemptNumber: 1,
  retryOfAttemptId: null,
  outcome: "succeeded" as const,
  error: null,
  retryDecision: {
    action: "do_not_retry" as const,
    reason: "complete",
    retryAtMs: null,
    previousAttemptId: null,
  },
  watermarks: {
    acceptedInputSequence: 1,
    acceptedInputSampleEnd: 4_000,
    receivedOutputSequence: 1,
    receivedOutputSampleEnd: 4_000,
    emittedOutputSequence: 1,
    emittedOutputSampleEnd: 4_000,
  },
  credentialVersion: "7",
  credentialFingerprint: "fingerprint",
  transport: "grpc" as const,
  rawReferences: [],
  terminalHash: "a".repeat(64),
  startedAtMs: 1_700_000_000_000,
  occurredAtMs: 1_700_000_000_100,
};
const PROVIDER_COMMAND = {
  kind: "internal.providerAttempt" as const,
  consultationId: CONSULTATION,
  generation: 3,
  workerId: "00000000-0000-4000-8000-000000000014",
  epoch: 2,
  eventId: "00000000-0000-4000-8000-000000000015",
  report: PROVIDER_REPORT,
};

function fixture(principal: InternalPrincipal = CONTROL_PRINCIPAL) {
  const beginDelete = mock(async () => undefined);
  const requestMagicLink = mock(async () => undefined);
  const providerAttempt = mock(async () => true);
  const workerFailure = mock(async () => true);
  const verify = mock(async () => principal);
  const authenticate = mock(async () => ({ session: SESSION, user: USER }));
  const consultations = {
    get: mock(async () => ({
      id: CONSULTATION,
      providerProfileId: "google-eu",
    })),
    setPreferences: mock(async () => ({
      id: CONSULTATION,
      providerProfileId: "google-eu",
    })),
  };
  const operations = {
    archivePresentation: mock(async () => ({ archive: { id: CONSULTATION }, objectPage: {} })),
    consultationOptions: mock(async () => []),
    providerProfileMetadata: mock(async () => []),
    providerAttempt,
    workerFailure,
  };
  const app = createWebApplication({
    auth: {
      requestMagicLink,
      authenticateMutation: authenticate,
      authenticate,
    } as never,
    consultations: consultations as never,
    archives: {
      beginDelete,
      drainDeletion: async () => true,
    } as never,
    languages: {} as never,
    operations: operations as never,
    internalPrincipalVerifier: { verify } as InternalPrincipalVerifier,
    publicBaseUrl: PUBLIC_BASE_URL,
    clientIp: () => "127.0.0.1",
    ready: async () => true,
  });

  return {
    app,
    authenticate,
    beginDelete,
    consultations,
    operations,
    providerAttempt,
    requestMagicLink,
    verify,
    workerFailure,
  };
}

describe("WebApplication authentication boundary", () => {
  it("passes only the server-loaded session to destructive archive operations", async () => {
    const { app, beginDelete } = fixture();

    await app.execute(
      {
        kind: "archive.delete",
        consultationId: CONSULTATION,
        reason: "retention elapsed",
      },
      {
        sessionToken: "opaque",
        csrfToken: "csrf",
      },
    );

    expect(beginDelete).toHaveBeenCalledWith(CONSULTATION, SESSION, "retention elapsed");
  });

  it("authenticates once while loading an authorized lobby aggregate", async () => {
    const { app, authenticate, consultations, operations } = fixture();

    const result = await app.execute(
      { kind: "consultation.lobby", consultationId: CONSULTATION },
      { sessionToken: "opaque" },
    );

    expect(authenticate).toHaveBeenCalledTimes(1);
    expect(consultations.get).toHaveBeenCalledWith(CONSULTATION, USER.id);
    expect(operations.consultationOptions).toHaveBeenCalledWith("google-eu");
    expect(result).toMatchObject({
      consultation: { id: CONSULTATION },
      viewer: { userId: USER.id, staffRole: "admin" },
    });
  });

  it("authenticates once while loading authorized archive detail and its first object page", async () => {
    const { app, authenticate, operations } = fixture();

    const result = await app.execute(
      { kind: "archive.get", archiveId: CONSULTATION },
      { sessionToken: "opaque" },
    );

    expect(authenticate).toHaveBeenCalledTimes(1);
    expect(operations.archivePresentation).toHaveBeenCalledWith(
      { userId: USER.id, role: "admin" },
      CONSULTATION,
      null,
      100,
    );
    expect(result).toMatchObject({ viewer: { staffRole: "admin" } });
  });

  it("loads only provider profile metadata for the staff new-consultation page", async () => {
    const { app, authenticate, operations } = fixture();

    await app.execute({ kind: "consultation.profileMetadata" }, { sessionToken: "opaque" });

    expect(authenticate).toHaveBeenCalledTimes(1);
    expect(operations.providerProfileMetadata).toHaveBeenCalledTimes(1);
    expect(operations.consultationOptions).not.toHaveBeenCalled();
  });

  it("derives internal principal exclusively from verified headers", async () => {
    const { app, verify } = fixture();
    const internalHeaders = { authorization: "Bearer signed" };

    await app.execute(
      {
        kind: "internal.deleteDrain",
        consultationId: CONSULTATION,
        writeEpoch: 4,
        reason: "retention elapsed",
      },
      { internalHeaders },
    );

    expect(verify).toHaveBeenCalledWith(internalHeaders);
  });

  it("allows provider evidence only from a specifically authorized translation worker", async () => {
    const translationWorker: InternalPrincipal = {
      service: "translation-worker",
      subject: "translation",
      permissions: ["checkpoint:write"],
    };
    const allowed = fixture(translationWorker);
    await expect(
      allowed.app.execute(PROVIDER_COMMAND, {
        internalHeaders: { authorization: "Bearer translation" },
      }),
    ).resolves.toBe(true);
    expect(allowed.providerAttempt).toHaveBeenCalledWith(PROVIDER_COMMAND);

    const wrongService = fixture({
      service: "control-worker",
      subject: "control",
      permissions: ["checkpoint:write"],
    });
    await expect(
      wrongService.app.execute(PROVIDER_COMMAND, {
        internalHeaders: { authorization: "Bearer control" },
      }),
    ).rejects.toThrow("FORBIDDEN_INTERNAL");

    const missingPermission = fixture({
      service: "translation-worker",
      subject: "translation",
      permissions: [],
    });
    await expect(
      missingPermission.app.execute(PROVIDER_COMMAND, {
        internalHeaders: { authorization: "Bearer translation" },
      }),
    ).rejects.toThrow("FORBIDDEN_INTERNAL");
  });

  it("dispatches authenticated worker failure reports to durable fencing", async () => {
    const translationWorker: InternalPrincipal = {
      service: "translation-worker",
      subject: "translation",
      permissions: ["failure:write"],
    };
    const configured = fixture(translationWorker);
    const command = {
      kind: "internal.failure" as const,
      consultationId: CONSULTATION,
      generation: 3,
      workerId: PROVIDER_COMMAND.workerId,
      epoch: 2,
      eventId: PROVIDER_COMMAND.eventId,
      kindName: "SpoolUnavailable",
      message: "fsync failed",
      lastCheckpointHashes: {},
    };

    await expect(
      configured.app.execute(command, {
        internalHeaders: { authorization: "Bearer translation" },
      }),
    ).resolves.toBe(true);
    expect(configured.workerFailure).toHaveBeenCalledWith(command);
  });

  it("derives public magic-link purpose, origin and client IP from trusted request context", async () => {
    const { app, requestMagicLink } = fixture();
    const evilRequest = new Request(`${PUBLIC_BASE_URL}/api/auth/magic-link`, {
      method: "POST",
      headers: { origin: "https://evil.example" },
    });

    await expect(
      app.execute(
        {
          kind: "auth.requestMagicLink",
          email: "person@example.com",
        },
        { request: evilRequest },
      ),
    ).rejects.toThrowError(/ORIGIN_INVALID/);

    const trustedRequest = new Request(`${PUBLIC_BASE_URL}/api/auth/magic-link`, {
      method: "POST",
      headers: { origin: PUBLIC_BASE_URL },
    });

    await app.execute(
      {
        kind: "auth.requestMagicLink",
        email: "person@example.com",
      },
      { request: trustedRequest },
    );

    expect(requestMagicLink).toHaveBeenCalledWith({
      email: "person@example.com",
      ip: "127.0.0.1",
      purpose: "sign_in",
      publicBaseUrl: PUBLIC_BASE_URL,
    });
  });

  it("rejects cross-origin exchange verification before creating credentials", async () => {
    const { app } = fixture();
    const request = new Request(`${PUBLIC_BASE_URL}/api/auth/verify`, {
      method: "POST",
      headers: { origin: "https://evil.example" },
    });

    await expect(
      app.execute(
        {
          kind: "auth.verifyExchange",
          nonce: "opaque",
        },
        {
          csrfToken: "opaque",
          request,
        },
      ),
    ).rejects.toThrowError(/ORIGIN_INVALID/);
  });
});
