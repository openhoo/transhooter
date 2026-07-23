import { describe, expect, it, mock } from "bun:test";
import { AuthService, type MagicLinkTokenSealer } from "../src/auth/service";
import type {
  ActiveMagicLink,
  AuthRepository,
  MagicLinkCandidate,
  MagicLinkIdentity,
  MagicLinkRecord,
  PendingExchangeRecord,
  SessionRecord,
  Transaction,
  UserRecord,
} from "../src/ports/index";

const USER_ID = "00000000-0000-4000-8000-000000000001";
const CONSULTATION_ID = "00000000-0000-4000-8000-000000000002";
const SESSION_ID = "00000000-0000-4000-8000-000000000003";
const TRANSACTION = { opaque: Symbol("auth-test") } satisfies Transaction;
const PUBLIC_BASE_URL = "https://app";

type AuthState = {
  usersByEmail: Map<string, UserRecord>;
  magicLinksByTokenHash: Map<string, MagicLinkRecord>;
  sealedLinksById: Map<string, Omit<MagicLinkCandidate, "record">>;
  exchangesByNonceHash: Map<string, PendingExchangeRecord>;
  sessionsById: Map<string, SessionRecord>;
  admissionRequests: Array<{ emailHash: string | null; ipHash: string; requestedAt: Date }>;
};

function hash(value: Uint8Array | string): string {
  return Buffer.from(typeof value === "string" ? value : value).toString("hex");
}

function magicLinkIdentity(record: MagicLinkRecord): string {
  return JSON.stringify([record.userId, record.purpose, record.consultationId, record.sessionId]);
}

function magicLinkAad(record: MagicLinkRecord): string {
  return JSON.stringify([
    record.id,
    record.userId,
    record.purpose,
    record.consultationId,
    record.sessionId,
  ]);
}

class TestTokenSealer implements MagicLinkTokenSealer {
  readonly sealedWith: string[] = [];

  constructor(
    readonly currentKeyId: string,
    readonly keys: Readonly<Record<string, true>>,
  ) {}

  seal(rawToken: Uint8Array, record: MagicLinkRecord) {
    if (!this.keys[this.currentKeyId]) {
      throw new Error("missing current key");
    }
    this.sealedWith.push(this.currentKeyId);
    return {
      keyId: this.currentKeyId,
      sealedRawToken: Buffer.from(
        JSON.stringify({
          keyId: this.currentKeyId,
          aad: magicLinkAad(record),
          token: Buffer.from(rawToken).toString("base64url"),
        }),
      ).toString("base64url"),
    };
  }

  open(link: ActiveMagicLink): Uint8Array {
    if (!this.keys[link.keyId]) {
      throw new Error("missing retained key");
    }
    const value = JSON.parse(Buffer.from(link.sealedRawToken, "base64url").toString("utf8")) as {
      keyId: string;
      aad: string;
      token: string;
    };
    if (value.keyId !== link.keyId || value.aad !== magicLinkAad(link.record)) {
      throw new Error("authentication failed");
    }
    return new Uint8Array(Buffer.from(value.token, "base64url"));
  }
}

function createRepository(state: AuthState): AuthRepository {
  return {
    transaction: async <T>(work: (value: Transaction) => Promise<T>) => work(TRANSACTION),
    findUserByEmail: async (email: string) => state.usersByEmail.get(email) ?? null,
    findUserById: async (id: string) =>
      [...state.usersByEmail.values()].find((user) => user.id === id) ?? null,
    findOrCreateCustomer: async () => {
      throw new Error("unused");
    },
    findAuthenticatedSessionByTokenHash: async (tokenHash: string, now: Date) => {
      const session = [...state.sessionsById.values()].find(
        (candidate) => candidate.tokenHash === tokenHash && candidate.expiresAt > now,
      );
      if (!session) {
        return null;
      }
      const user = [...state.usersByEmail.values()].find(
        (candidate) => candidate.id === session.userId,
      );
      return user ? { session, user } : null;
    },
    getOrCreateActiveMagicLink: async (
      identity: MagicLinkIdentity,
      candidate: MagicLinkCandidate,
      now: Date,
    ) => {
      const existing = [...state.magicLinksByTokenHash.values()].find(
        (link) =>
          magicLinkIdentity(link) ===
            JSON.stringify([
              identity.userId,
              identity.purpose,
              identity.consultationId,
              identity.sessionId,
            ]) &&
          !link.consumedAt &&
          !link.revokedAt &&
          link.expiresAt > now,
      );
      if (existing) {
        const sealed = state.sealedLinksById.get(existing.id);
        if (!sealed) {
          throw new Error("MAGIC_LINK_DELIVERY_UNRECOVERABLE");
        }
        return { record: existing, ...sealed, created: false };
      }
      state.magicLinksByTokenHash.set(candidate.record.tokenHash, candidate.record);
      state.sealedLinksById.set(candidate.record.id, {
        sealedRawToken: candidate.sealedRawToken,
        keyId: candidate.keyId,
      });
      return { ...candidate, created: true };
    },
    lockMagicLinkByTokenHash: async (tokenHash: string) =>
      state.magicLinksByTokenHash.get(tokenHash) ?? null,
    lockMagicLinkById: async (id: string) =>
      [...state.magicLinksByTokenHash.values()].find((link) => link.id === id) ?? null,
    createPendingExchange: async (exchange: PendingExchangeRecord) => {
      const preparedAt = new Date(exchange.expiresAt.getTime() - 5 * 60_000);
      for (const [nonceHash, existing] of state.exchangesByNonceHash) {
        if (
          existing.magicLinkId === exchange.magicLinkId &&
          (existing.consumedAt !== null || existing.expiresAt <= preparedAt)
        ) {
          state.exchangesByNonceHash.delete(nonceHash);
        }
      }
      const live = [...state.exchangesByNonceHash.values()].some(
        (existing) => existing.magicLinkId === exchange.magicLinkId && existing.consumedAt === null,
      );
      if (live) {
        throw new Error("INVALID_OR_EXPIRED_LINK");
      }
      state.exchangesByNonceHash.set(exchange.nonceHash, exchange);
    },
    lockPendingExchangeByNonceHash: async (nonceHash: string) =>
      state.exchangesByNonceHash.get(nonceHash) ?? null,
    consumeExchangeAndLink: async (exchangeId: string, linkId: string, consumedAt: Date) => {
      const exchange = [...state.exchangesByNonceHash.values()].find(
        (candidate) => candidate.id === exchangeId,
      );
      const link = [...state.magicLinksByTokenHash.values()].find(
        (candidate) => candidate.id === linkId,
      );
      if (!exchange || !link || exchange.consumedAt || link.consumedAt) {
        return false;
      }
      exchange.consumedAt = consumedAt;
      link.consumedAt = consumedAt;
      return true;
    },
    createSession: async (session: SessionRecord) => {
      state.sessionsById.set(session.id, session);
    },
    rotateSession: async (session: SessionRecord, replacedSessionId: string) => {
      if (!state.sessionsById.has(replacedSessionId)) {
        throw new Error("INVALID_REAUTH_BINDING");
      }
      state.sessionsById.delete(replacedSessionId);
      state.sessionsById.set(session.id, session);
    },
    admitMagicLinkRequest: async (
      emailHash: string | null,
      ipHash: string,
      since: Date,
      at: Date,
      emailLimit: number,
      ipLimit: number,
    ) => {
      state.admissionRequests = state.admissionRequests.filter(
        (request) => request.requestedAt > since,
      );
      const emailCount = state.admissionRequests.filter(
        (request) => request.emailHash === emailHash,
      ).length;
      const ipCount = state.admissionRequests.filter((request) => request.ipHash === ipHash).length;
      const admitted = ipCount < ipLimit && (emailHash === null || emailCount < emailLimit);
      if (admitted) {
        state.admissionRequests.push({ emailHash, ipHash, requestedAt: at });
      }
      return admitted;
    },
    revokeConsultationLinks: async (consultationId: string, revokedAt: Date) => {
      for (const link of state.magicLinksByTokenHash.values()) {
        if (link.consultationId === consultationId && !link.consumedAt) {
          link.revokedAt = revokedAt;
        }
      }
    },
  } as unknown as AuthRepository;
}

function makeSession(overrides: Partial<SessionRecord> = {}): SessionRecord {
  return {
    id: SESSION_ID,
    userId: USER_ID,
    tokenHash: "token-hash",
    csrfHash: "old",
    expiresAt: new Date("2026-01-01T12:00:00Z"),
    reauthenticatedAt: null,
    reauthConsultationId: null,
    ...overrides,
  };
}

function createAuthFixture(
  options: { state?: AuthState; sealer?: MagicLinkTokenSealer; sequenceStart?: number } = {},
) {
  let now = new Date("2026-01-01T00:00:00Z");
  let sequence = options.sequenceStart ?? 10;
  const state: AuthState = options.state ?? {
    usersByEmail: new Map([
      [
        "known@example.com",
        {
          id: USER_ID,
          email: "known@example.com",
          displayName: "Admin",
          staffRole: "admin",
        },
      ],
    ]),
    magicLinksByTokenHash: new Map(),
    sealedLinksById: new Map(),
    exchangesByNonceHash: new Map(),
    sessionsById: new Map(),
    admissionRequests: [],
  };
  const repository = createRepository(state);
  const mail = {
    sendMagicLink: mock(
      async (_input: {
        to: string;
        purpose: "sign_in" | "consultation_invite" | "archive_delete_reauth";
        url: string;
        expiresAt: Date;
      }) => undefined,
    ),
  };
  const service = new AuthService(
    repository,
    mail,
    { now: () => now },
    {
      uuid: () => `00000000-0000-4000-8000-${String(sequence++).padStart(12, "0")}`,
    },
    { bytes: () => new Uint8Array(32).fill(sequence++) },
    { sha256: hash },
    { rateLimitKey: "rate" },
    options.sealer ?? new TestTokenSealer("current", { current: true }),
  );

  return {
    service,
    mail,
    state,
    repository,
    advance: (milliseconds: number) => {
      now = new Date(now.getTime() + milliseconds);
    },
  };
}

function requireLastMagicLinkUrl(calls: readonly (readonly [{ readonly url: string }])[]): string {
  const call = calls.at(-1);
  if (call === undefined) {
    throw new Error("Expected a sent magic link");
  }
  return call[0].url;
}

function requireFirstMagicLink(state: AuthState): MagicLinkRecord {
  const link = state.magicLinksByTokenHash.values().next().value;
  if (link === undefined) {
    throw new Error("Expected a stored magic link");
  }
  return link;
}

function tokenFromMagicLinkUrl(url: string): string {
  const token = new URL(url).searchParams.get("token");
  if (token === null) {
    throw new Error("Expected magic-link token");
  }
  return token;
}

async function requestKnownSignIn(
  fixture: { service: AuthService },
  input: { email?: string; ip: string; publicBaseUrl: string },
): Promise<void> {
  await fixture.service.requestMagicLink({
    email: input.email ?? "known@example.com",
    ip: input.ip,
    purpose: "sign_in",
    publicBaseUrl: input.publicBaseUrl,
  });
}

describe("AuthService", () => {
  it("returns the related user for a live session and keeps invalid sessions unauthenticated", async () => {
    const fixture = createAuthFixture();
    const rawToken = new Uint8Array(32).fill(4);
    const session = makeSession({ tokenHash: hash(rawToken) });
    fixture.state.sessionsById.set(session.id, session);
    const encoded = Buffer.from(rawToken).toString("base64url");

    await expect(fixture.service.authenticate(encoded)).resolves.toMatchObject({
      session: { id: SESSION_ID, userId: USER_ID },
      user: { id: USER_ID, email: "known@example.com", staffRole: "admin" },
    });

    fixture.advance(12 * 60 * 60_000);
    await expect(fixture.service.authenticate(encoded)).rejects.toMatchObject({
      code: "UNAUTHENTICATED",
    });
    fixture.state.sessionsById.set(
      session.id,
      makeSession({ tokenHash: hash(rawToken), expiresAt: new Date("2026-01-02T00:00:00Z") }),
    );
    fixture.state.usersByEmail.clear();
    await expect(fixture.service.authenticate(encoded)).rejects.toMatchObject({
      code: "UNAUTHENTICATED",
    });
  });

  it("keeps scanner GET non-consuming and consumes only an origin-bound CSRF-checked user POST once", async () => {
    const fixture = createAuthFixture();
    await requestKnownSignIn(fixture, {
      email: " Known@Example.com ",
      ip: "1",
      publicBaseUrl: PUBLIC_BASE_URL,
    });
    const token = tokenFromMagicLinkUrl(
      requireLastMagicLinkUrl(fixture.mail.sendMagicLink.mock.calls),
    );
    const link = requireFirstMagicLink(fixture.state);

    const pending = await fixture.service.beginExchange(token);

    expect(link.consumedAt).toBeNull();
    const verificationContext = {
      csrfToken: pending.verificationCsrfToken,
      origin: PUBLIC_BASE_URL,
      publicBaseUrl: PUBLIC_BASE_URL,
      requestIp: "1",
    };
    await fixture.service.verifyExchange(pending.exchangeNonce, verificationContext);
    expect(link.consumedAt).not.toBeNull();
    await expect(
      fixture.service.verifyExchange(pending.exchangeNonce, verificationContext),
    ).rejects.toThrow(/INVALID_EXCHANGE/);
  });

  it("enforces atomic email and IP admissions under concurrent requests", async () => {
    const fixture = createAuthFixture();

    await Promise.all(
      Array.from({ length: 25 }, () =>
        requestKnownSignIn(fixture, {
          ip: "one-ip",
          publicBaseUrl: PUBLIC_BASE_URL,
        }),
      ),
    );

    expect(fixture.mail.sendMagicLink).toHaveBeenCalledTimes(5);
  });

  it("retains only admitted requests after the limit and expires admission rows", async () => {
    const fixture = createAuthFixture();

    for (let request = 0; request < 100; request += 1) {
      await requestKnownSignIn(fixture, {
        ip: "bounded-ip",
        publicBaseUrl: PUBLIC_BASE_URL,
      });
    }

    expect(fixture.mail.sendMagicLink).toHaveBeenCalledTimes(5);
    expect(fixture.state.admissionRequests).toHaveLength(5);

    fixture.advance(15 * 60_000 + 1);
    await requestKnownSignIn(fixture, {
      ip: "bounded-ip",
      publicBaseUrl: PUBLIC_BASE_URL,
    });

    expect(fixture.mail.sendMagicLink).toHaveBeenCalledTimes(6);
    expect(fixture.state.admissionRequests).toHaveLength(1);
  });

  it("allows only one concurrent live preparation for a magic link", async () => {
    const fixture = createAuthFixture();
    await requestKnownSignIn(fixture, { ip: "1", publicBaseUrl: PUBLIC_BASE_URL });
    const token = tokenFromMagicLinkUrl(
      requireLastMagicLinkUrl(fixture.mail.sendMagicLink.mock.calls),
    );

    const preparations = await Promise.allSettled(
      Array.from({ length: 20 }, () => fixture.service.beginExchange(token)),
    );
    const prepared = preparations.filter(
      (
        result,
      ): result is PromiseFulfilledResult<{
        exchangeNonce: string;
        verificationCsrfToken: string;
      }> => result.status === "fulfilled",
    );

    expect(prepared).toHaveLength(1);
    expect(fixture.state.exchangesByNonceHash.size).toBe(1);
    const successfulPreparation = prepared[0];
    if (!successfulPreparation) {
      throw new Error("Expected one successful exchange preparation");
    }
    await fixture.service.verifyExchange(successfulPreparation.value.exchangeNonce, {
      csrfToken: successfulPreparation.value.verificationCsrfToken,
      origin: PUBLIC_BASE_URL,
      publicBaseUrl: PUBLIC_BASE_URL,
      requestIp: "1",
    });
  });

  it("replaces an expired preparation without retaining or accepting its nonce", async () => {
    const fixture = createAuthFixture();
    await requestKnownSignIn(fixture, { ip: "1", publicBaseUrl: PUBLIC_BASE_URL });
    const token = tokenFromMagicLinkUrl(
      requireLastMagicLinkUrl(fixture.mail.sendMagicLink.mock.calls),
    );
    const expired = await fixture.service.beginExchange(token);

    fixture.advance(5 * 60_000 + 1);
    const current = await fixture.service.beginExchange(token);

    expect(fixture.state.exchangesByNonceHash.size).toBe(1);
    await expect(
      fixture.service.verifyExchange(expired.exchangeNonce, {
        csrfToken: expired.verificationCsrfToken,
        origin: PUBLIC_BASE_URL,
        publicBaseUrl: PUBLIC_BASE_URL,
        requestIp: "1",
      }),
    ).rejects.toThrow(/INVALID_EXCHANGE/);
    await fixture.service.verifyExchange(current.exchangeNonce, {
      csrfToken: current.verificationCsrfToken,
      origin: PUBLIC_BASE_URL,
      publicBaseUrl: PUBLIC_BASE_URL,
      requestIp: "1",
    });
  });

  it("reuses one usable token after ambiguous delivery and duplicate submissions", async () => {
    const fixture = createAuthFixture();
    fixture.mail.sendMagicLink.mockImplementationOnce(async () => {
      throw new Error("provider response lost after acceptance");
    });

    await requestKnownSignIn(fixture, {
      ip: "1",
      publicBaseUrl: PUBLIC_BASE_URL,
    });
    await requestKnownSignIn(fixture, {
      ip: "1",
      publicBaseUrl: PUBLIC_BASE_URL,
    });

    expect(fixture.state.magicLinksByTokenHash.size).toBe(1);
    expect(fixture.mail.sendMagicLink).toHaveBeenCalledTimes(2);
    const firstUrl = fixture.mail.sendMagicLink.mock.calls[0]?.[0].url;
    const retryUrl = fixture.mail.sendMagicLink.mock.calls[1]?.[0].url;
    if (!firstUrl || !retryUrl) {
      throw new Error("Expected both delivery attempts");
    }
    expect(retryUrl).toBe(firstUrl);

    const pending = await fixture.service.beginExchange(tokenFromMagicLinkUrl(retryUrl));
    await fixture.service.verifyExchange(pending.exchangeNonce, {
      csrfToken: pending.verificationCsrfToken,
      origin: PUBLIC_BASE_URL,
      publicBaseUrl: PUBLIC_BASE_URL,
      requestIp: "1",
    });
    expect(requireFirstMagicLink(fixture.state).consumedAt).not.toBeNull();
  });

  it("converges concurrent issuers on one durable URL and expiry", async () => {
    const fixture = createAuthFixture();

    await Promise.all(
      Array.from({ length: 5 }, () =>
        requestKnownSignIn(fixture, {
          ip: "concurrent",
          publicBaseUrl: PUBLIC_BASE_URL,
        }),
      ),
    );

    expect(fixture.state.magicLinksByTokenHash.size).toBe(1);
    expect(
      new Set(fixture.mail.sendMagicLink.mock.calls.map(([message]) => message.url)).size,
    ).toBe(1);
    expect(
      new Set(
        fixture.mail.sendMagicLink.mock.calls.map(([message]) => message.expiresAt.toISOString()),
      ).size,
    ).toBe(1);
  });

  it("reuses the sealed durable token after restart and ambiguous SMTP acceptance", async () => {
    const first = createAuthFixture();
    first.mail.sendMagicLink.mockImplementationOnce(async () => {
      throw new Error("provider response lost after acceptance");
    });
    await requestKnownSignIn(first, { ip: "1", publicBaseUrl: PUBLIC_BASE_URL });
    const acceptedUrl = first.mail.sendMagicLink.mock.calls[0]?.[0].url;
    const acceptedExpiry = first.mail.sendMagicLink.mock.calls[0]?.[0].expiresAt;

    const restarted = createAuthFixture({ state: first.state, sequenceStart: 100 });
    await requestKnownSignIn(restarted, { ip: "1", publicBaseUrl: PUBLIC_BASE_URL });

    expect(restarted.state.magicLinksByTokenHash.size).toBe(1);
    expect(restarted.mail.sendMagicLink.mock.calls[0]?.[0].url).toBe(acceptedUrl);
    expect(restarted.mail.sendMagicLink.mock.calls[0]?.[0].expiresAt).toEqual(acceptedExpiry);
  });

  it("opens retained-key links across rotation and seals the next link with the current key", async () => {
    const oldSealer = new TestTokenSealer("old", { old: true });
    const first = createAuthFixture({ sealer: oldSealer });
    await requestKnownSignIn(first, { ip: "1", publicBaseUrl: PUBLIC_BASE_URL });
    const oldUrl = requireLastMagicLinkUrl(first.mail.sendMagicLink.mock.calls);

    const rotatedSealer = new TestTokenSealer("new", { old: true, new: true });
    const restarted = createAuthFixture({
      state: first.state,
      sealer: rotatedSealer,
      sequenceStart: 100,
    });
    await requestKnownSignIn(restarted, { ip: "1", publicBaseUrl: PUBLIC_BASE_URL });
    expect(requireLastMagicLinkUrl(restarted.mail.sendMagicLink.mock.calls)).toBe(oldUrl);
    expect(rotatedSealer.sealedWith).toEqual(["new"]);

    const pending = await restarted.service.beginExchange(tokenFromMagicLinkUrl(oldUrl));
    await restarted.service.verifyExchange(pending.exchangeNonce, {
      csrfToken: pending.verificationCsrfToken,
      origin: PUBLIC_BASE_URL,
      publicBaseUrl: PUBLIC_BASE_URL,
      requestIp: "1",
    });
    await requestKnownSignIn(restarted, { ip: "1", publicBaseUrl: PUBLIC_BASE_URL });

    const current = [...restarted.state.magicLinksByTokenHash.values()].find(
      (record) => !record.consumedAt,
    );
    expect(current).toBeDefined();
    expect(current && restarted.state.sealedLinksById.get(current.id)?.keyId).toBe("new");
  });

  it("fails closed for missing retained keys and corrupt ciphertext", async () => {
    const first = createAuthFixture({
      sealer: new TestTokenSealer("old", { old: true }),
    });
    await requestKnownSignIn(first, { ip: "1", publicBaseUrl: PUBLIC_BASE_URL });
    const link = requireFirstMagicLink(first.state);

    const missingKey = createAuthFixture({
      state: first.state,
      sealer: new TestTokenSealer("new", { new: true }),
      sequenceStart: 100,
    });
    await expect(
      requestKnownSignIn(missingKey, { ip: "1", publicBaseUrl: PUBLIC_BASE_URL }),
    ).rejects.toThrow(/MAGIC_LINK_DELIVERY_UNRECOVERABLE/);
    expect(missingKey.mail.sendMagicLink).not.toHaveBeenCalled();
    expect(missingKey.state.magicLinksByTokenHash.size).toBe(1);

    first.state.sealedLinksById.set(link.id, {
      keyId: "old",
      sealedRawToken: "corrupt",
    });
    const corrupt = createAuthFixture({
      state: first.state,
      sealer: new TestTokenSealer("new", { old: true, new: true }),
      sequenceStart: 300,
    });
    await expect(
      requestKnownSignIn(corrupt, { ip: "1", publicBaseUrl: PUBLIC_BASE_URL }),
    ).rejects.toThrow(/MAGIC_LINK_DELIVERY_UNRECOVERABLE/);
    expect(corrupt.mail.sendMagicLink).not.toHaveBeenCalled();
    expect(corrupt.state.magicLinksByTokenHash.size).toBe(1);
  });

  it("never persists plaintext tokens and rejects ciphertext swapped between record identities", async () => {
    const fixture = createAuthFixture();
    await requestKnownSignIn(fixture, { ip: "1", publicBaseUrl: PUBLIC_BASE_URL });
    await fixture.service.requestMagicLink({
      email: "known@example.com",
      ip: "2",
      purpose: "consultation_invite",
      consultationId: CONSULTATION_ID,
      publicBaseUrl: PUBLIC_BASE_URL,
    });
    const [firstRecord, secondRecord] = [...fixture.state.magicLinksByTokenHash.values()];
    if (!firstRecord || !secondRecord) {
      throw new Error("Expected two stored link identities");
    }
    const firstUrl = fixture.mail.sendMagicLink.mock.calls[0]?.[0].url;
    if (!firstUrl) {
      throw new Error("Expected plaintext delivery URL");
    }
    const plaintextToken = tokenFromMagicLinkUrl(firstUrl);
    for (const persisted of fixture.state.sealedLinksById.values()) {
      expect(persisted.sealedRawToken).not.toBe(plaintextToken);
      expect(persisted.sealedRawToken).not.toContain(plaintextToken);
    }

    const firstSealed = fixture.state.sealedLinksById.get(firstRecord.id);
    const secondSealed = fixture.state.sealedLinksById.get(secondRecord.id);
    if (!firstSealed || !secondSealed) {
      throw new Error("Expected sealed link material");
    }
    fixture.state.sealedLinksById.set(firstRecord.id, secondSealed);
    fixture.state.sealedLinksById.set(secondRecord.id, firstSealed);
    const restarted = createAuthFixture({ state: fixture.state, sequenceStart: 100 });
    await expect(
      requestKnownSignIn(restarted, { ip: "3", publicBaseUrl: PUBLIC_BASE_URL }),
    ).rejects.toThrow(/MAGIC_LINK_DELIVERY_UNRECOVERABLE/);
    expect(restarted.mail.sendMagicLink).not.toHaveBeenCalled();
    expect(restarted.state.magicLinksByTokenHash.size).toBe(2);
  });

  it("rejects expired links and returns no observable mail for unknown users", async () => {
    const fixture = createAuthFixture();
    await fixture.service.requestMagicLink({
      email: "unknown@example.com",
      ip: "1",
      purpose: "sign_in",
      publicBaseUrl: PUBLIC_BASE_URL,
    });
    expect(fixture.mail.sendMagicLink).not.toHaveBeenCalled();

    await requestKnownSignIn(fixture, {
      ip: "2",
      publicBaseUrl: PUBLIC_BASE_URL,
    });
    const token = tokenFromMagicLinkUrl(
      requireLastMagicLinkUrl(fixture.mail.sendMagicLink.mock.calls),
    );
    fixture.advance(15 * 60_000 + 1);

    await expect(fixture.service.beginExchange(token)).rejects.toThrow(/INVALID_OR_EXPIRED_LINK/);
  });

  it("binds reauth to the authenticated admin session and rotates it", async () => {
    const fixture = createAuthFixture();
    const rawSessionToken = new Uint8Array(32).fill(7);
    const encodedSessionToken = Buffer.from(rawSessionToken).toString("base64url");
    const oldSession = makeSession({ tokenHash: hash(rawSessionToken) });
    fixture.state.sessionsById.set(oldSession.id, oldSession);

    await fixture.service.requestArchiveDeleteReauth(
      encodedSessionToken,
      CONSULTATION_ID,
      "1",
      PUBLIC_BASE_URL,
    );
    const token = tokenFromMagicLinkUrl(
      requireLastMagicLinkUrl(fixture.mail.sendMagicLink.mock.calls),
    );
    const pending = await fixture.service.beginExchange(token);
    const verified = await fixture.service.verifyExchange(pending.exchangeNonce, {
      csrfToken: pending.verificationCsrfToken,
      origin: PUBLIC_BASE_URL,
      publicBaseUrl: PUBLIC_BASE_URL,
      requestIp: "1",
      sessionToken: encodedSessionToken,
    });

    expect(verified.session.reauthConsultationId).toBe(CONSULTATION_ID);
    expect(fixture.state.sessionsById.has(SESSION_ID)).toBe(false);
  });

  it("does not let another authenticated session claim a reauthentication exchange", async () => {
    const fixture = createAuthFixture();
    const requestingRawToken = new Uint8Array(32).fill(7);
    const requestingToken = Buffer.from(requestingRawToken).toString("base64url");
    fixture.state.sessionsById.set(
      SESSION_ID,
      makeSession({ tokenHash: hash(requestingRawToken) }),
    );
    await fixture.service.requestArchiveDeleteReauth(
      requestingToken,
      CONSULTATION_ID,
      "1",
      PUBLIC_BASE_URL,
    );
    const token = tokenFromMagicLinkUrl(
      requireLastMagicLinkUrl(fixture.mail.sendMagicLink.mock.calls),
    );
    const pending = await fixture.service.beginExchange(token);

    const otherSessionId = "00000000-0000-4000-8000-000000000099";
    const otherRawToken = new Uint8Array(32).fill(8);
    fixture.state.sessionsById.set(
      otherSessionId,
      makeSession({ id: otherSessionId, tokenHash: hash(otherRawToken) }),
    );
    await expect(
      fixture.service.verifyExchange(pending.exchangeNonce, {
        csrfToken: pending.verificationCsrfToken,
        origin: PUBLIC_BASE_URL,
        publicBaseUrl: PUBLIC_BASE_URL,
        requestIp: "1",
        sessionToken: Buffer.from(otherRawToken).toString("base64url"),
      }),
    ).rejects.toThrow(/INVALID_EXCHANGE/);
    expect(requireFirstMagicLink(fixture.state).consumedAt).toBeNull();
  });

  it("rejects cross-origin or missing-CSRF verification without consuming the exchange", async () => {
    const fixture = createAuthFixture();
    await requestKnownSignIn(fixture, {
      ip: "1",
      publicBaseUrl: PUBLIC_BASE_URL,
    });
    const token = tokenFromMagicLinkUrl(
      requireLastMagicLinkUrl(fixture.mail.sendMagicLink.mock.calls),
    );
    const pending = await fixture.service.beginExchange(token);

    await expect(
      fixture.service.verifyExchange(pending.exchangeNonce, {
        csrfToken: pending.verificationCsrfToken,
        origin: "https://evil.example",
        publicBaseUrl: PUBLIC_BASE_URL,
        requestIp: "1",
      }),
    ).rejects.toThrow(/INVALID_EXCHANGE_CONTEXT/);
    await expect(
      fixture.service.verifyExchange(pending.exchangeNonce, {
        csrfToken: "wrong",
        origin: PUBLIC_BASE_URL,
        publicBaseUrl: PUBLIC_BASE_URL,
        requestIp: "1",
      }),
    ).rejects.toThrow(/INVALID_EXCHANGE_CONTEXT/);
    expect(requireFirstMagicLink(fixture.state).consumedAt).toBeNull();
  });
  it("normalizes absent and malformed Origin to INVALID_EXCHANGE_CONTEXT", async () => {
    const fixture = createAuthFixture();
    await requestKnownSignIn(fixture, {
      ip: "1",
      publicBaseUrl: PUBLIC_BASE_URL,
    });
    const token = tokenFromMagicLinkUrl(
      requireLastMagicLinkUrl(fixture.mail.sendMagicLink.mock.calls),
    );
    const pending = await fixture.service.beginExchange(token);

    for (const origin of ["", "not a url", "://"]) {
      await expect(
        fixture.service.verifyExchange(pending.exchangeNonce, {
          csrfToken: pending.verificationCsrfToken,
          origin,
          publicBaseUrl: PUBLIC_BASE_URL,
          requestIp: "1",
        }),
      ).rejects.toThrow(/INVALID_EXCHANGE_CONTEXT/);
    }
    expect(requireFirstMagicLink(fixture.state).consumedAt).toBeNull();
  });
});
