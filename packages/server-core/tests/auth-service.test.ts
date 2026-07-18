import { describe, expect, it, mock } from "bun:test";
import { AuthService } from "../src/auth/service";
import type {
  AuthRepository,
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
  exchangesByNonceHash: Map<string, PendingExchangeRecord>;
  sessionsById: Map<string, SessionRecord>;
  admissionCounts: Map<string, number>;
};

function hash(value: Uint8Array | string): string {
  return Buffer.from(typeof value === "string" ? value : value).toString("hex");
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
    findSessionByTokenHash: async (tokenHash: string) =>
      [...state.sessionsById.values()].find((session) => session.tokenHash === tokenHash) ?? null,
    createMagicLink: async (link: MagicLinkRecord) => {
      state.magicLinksByTokenHash.set(link.tokenHash, link);
    },
    lockMagicLinkByTokenHash: async (tokenHash: string) =>
      state.magicLinksByTokenHash.get(tokenHash) ?? null,
    lockMagicLinkById: async (id: string) =>
      [...state.magicLinksByTokenHash.values()].find((link) => link.id === id) ?? null,
    createPendingExchange: async (exchange: PendingExchangeRecord) => {
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
      _since: Date,
      _at: Date,
      emailLimit: number,
      ipLimit: number,
    ) => {
      const emailKey = `e:${emailHash}`;
      const ipKey = `i:${ipHash}`;
      const emailCount = state.admissionCounts.get(emailKey) ?? 0;
      const ipCount = state.admissionCounts.get(ipKey) ?? 0;
      state.admissionCounts.set(emailKey, emailCount + 1);
      state.admissionCounts.set(ipKey, ipCount + 1);
      return ipCount < ipLimit && (emailHash === null || emailCount < emailLimit);
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

function createAuthFixture() {
  let now = new Date("2026-01-01T00:00:00Z");
  let sequence = 10;
  const state: AuthState = {
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
    exchangesByNonceHash: new Map(),
    sessionsById: new Map(),
    admissionCounts: new Map(),
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
  );

  return {
    service,
    mail,
    state,
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
    });

    expect(verified.session.reauthConsultationId).toBe(CONSULTATION_ID);
    expect(fixture.state.sessionsById.has(SESSION_ID)).toBe(false);
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
});
