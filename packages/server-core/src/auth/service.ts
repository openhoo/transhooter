import {
  addMilliseconds,
  type Clock,
  DomainError,
  type IdGenerator,
  type MagicLinkPurpose,
  type TokenGenerator,
  type TokenHasher,
  type UUID,
} from "../domain/model";
import type {
  AuthRepository,
  MagicLinkRecord,
  MailPort,
  PendingExchangeRecord,
  SessionRecord,
  UserRecord,
} from "../ports/index";

const LINK_TTL_MS = 15 * 60_000;
const EXCHANGE_TTL_MS = 5 * 60_000;
const SESSION_TTL_MS = 12 * 60 * 60_000;

export interface MagicLinkRequest {
  email: string;
  ip: string;
  purpose: MagicLinkPurpose;
  consultationId?: UUID;
  sessionId?: UUID;
  publicBaseUrl: string;
}

export interface AuthSecrets {
  rateLimitKey: string;
}

interface VerifyExchangeContext {
  csrfToken: string;
  origin: string;
  publicBaseUrl: string;
  requestIp: string;
}

interface AuthenticatedSession {
  session: SessionRecord;
  user: UserRecord;
}

interface VerifiedExchange {
  sessionToken: string;
  csrfToken: string;
  session: SessionRecord;
  purpose: MagicLinkPurpose;
}

interface AdmittedMagicLink {
  email: string;
  userId: UUID;
  purpose: MagicLinkPurpose;
  consultationId: UUID | null;
  sessionId: UUID | null;
  publicBaseUrl: string;
  now: Date;
}

export class AuthService {
  constructor(
    private readonly repository: AuthRepository,
    private readonly mail: MailPort,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
    private readonly tokens: TokenGenerator,
    private readonly hasher: TokenHasher,
    private readonly secrets: AuthSecrets,
  ) {}

  async requestMagicLink(input: MagicLinkRequest): Promise<void> {
    if (input.purpose === "archive_delete_reauth") {
      return;
    }

    const now = this.clock.now();
    let email: string;
    try {
      email = normalizeEmail(input.email);
    } catch {
      await this.admitMagicLinkRequest(null, input.ip, now);
      return;
    }

    const admitted = await this.admitMagicLinkRequest(email, input.ip, now);
    if (!admitted) {
      return;
    }

    const user = await this.repository.findUserByEmail(email);
    if (!user) {
      return;
    }

    await this.issueMagicLink({
      email,
      userId: user.id,
      purpose: input.purpose,
      consultationId: input.consultationId ?? null,
      sessionId: input.sessionId ?? null,
      publicBaseUrl: input.publicBaseUrl,
      now,
    });
  }

  async requestArchiveDeleteReauth(
    sessionToken: string,
    consultationId: UUID,
    ip: string,
    publicBaseUrl: string,
  ): Promise<void> {
    const authenticated = await this.authenticate(sessionToken);
    if (authenticated.user.staffRole !== "admin") {
      throw new DomainError("FORBIDDEN");
    }

    const now = this.clock.now();
    const admitted = await this.admitMagicLinkRequest(authenticated.user.email, ip, now);
    if (!admitted) {
      return;
    }

    await this.issueMagicLink({
      email: authenticated.user.email,
      userId: authenticated.user.id,
      purpose: "archive_delete_reauth",
      consultationId,
      sessionId: authenticated.session.id,
      publicBaseUrl,
      now,
    });
  }

  async beginExchange(
    encodedToken: string,
  ): Promise<{ exchangeNonce: string; verificationCsrfToken: string }> {
    const token = decodeBase64url(encodedToken);
    if (token.length !== 32) {
      throw new DomainError("INVALID_OR_EXPIRED_LINK");
    }

    const now = this.clock.now();
    const nonce = this.tokens.bytes(32);
    const verificationCsrf = this.tokens.bytes(32);
    const record: PendingExchangeRecord = {
      id: this.ids.uuid(),
      magicLinkId: "",
      nonceHash: this.hasher.sha256(nonce),
      csrfHash: this.hasher.sha256(verificationCsrf),
      expiresAt: addMilliseconds(now, EXCHANGE_TTL_MS),
      consumedAt: null,
    };

    await this.repository.transaction(async (tx) => {
      const tokenHash = this.hasher.sha256(token);
      const link = await this.repository.lockMagicLinkByTokenHash(tokenHash, tx);
      if (!link || link.consumedAt || link.revokedAt || link.expiresAt <= now) {
        throw new DomainError("INVALID_OR_EXPIRED_LINK");
      }

      await this.repository.createPendingExchange({ ...record, magicLinkId: link.id }, tx);
    });

    return {
      exchangeNonce: base64url(nonce),
      verificationCsrfToken: base64url(verificationCsrf),
    };
  }

  async verifyExchange(
    exchangeNonce: string,
    context: VerifyExchangeContext,
  ): Promise<VerifiedExchange> {
    const nonce = decodeBase64url(exchangeNonce);
    const verificationCsrf = decodeBase64url(context.csrfToken);
    let sameOrigin = false;
    try {
      sameOrigin =
        new URL(context.origin).origin === new URL(context.publicBaseUrl).origin &&
        context.origin.length > 0;
    } catch {
      sameOrigin = false;
    }
    const invalidContext =
      nonce.length !== 32 || verificationCsrf.length !== 32 || !context.requestIp || !sameOrigin;
    if (invalidContext) {
      throw new DomainError("INVALID_EXCHANGE_CONTEXT");
    }

    const now = this.clock.now();
    const sessionToken = this.tokens.bytes(32);
    const csrfToken = this.tokens.bytes(32);
    let created!: SessionRecord;
    let purpose!: MagicLinkPurpose;

    await this.repository.transaction(async (tx) => {
      const nonceHash = this.hasher.sha256(nonce);
      const exchange = await this.repository.lockPendingExchangeByNonceHash(nonceHash, tx);
      if (!exchange || exchange.consumedAt || exchange.expiresAt <= now) {
        throw new DomainError("INVALID_EXCHANGE");
      }

      const verificationCsrfHash = this.hasher.sha256(verificationCsrf);
      if (exchange.csrfHash !== verificationCsrfHash) {
        throw new DomainError("INVALID_EXCHANGE_CONTEXT");
      }

      const link = await this.repository.lockMagicLinkById(exchange.magicLinkId, tx);
      if (!link?.userId || link.consumedAt || link.revokedAt || link.expiresAt <= now) {
        throw new DomainError("INVALID_EXCHANGE");
      }

      const consumed = await this.repository.consumeExchangeAndLink(exchange.id, link.id, now, tx);
      if (!consumed) {
        throw new DomainError("INVALID_EXCHANGE");
      }

      purpose = link.purpose;
      const isArchiveDeleteReauth = link.purpose === "archive_delete_reauth";
      created = {
        id: this.ids.uuid(),
        userId: link.userId,
        tokenHash: this.hasher.sha256(sessionToken),
        csrfHash: this.hasher.sha256(csrfToken),
        expiresAt: addMilliseconds(now, SESSION_TTL_MS),
        reauthenticatedAt: isArchiveDeleteReauth ? now : null,
        reauthConsultationId: isArchiveDeleteReauth ? link.consultationId : null,
      };

      if (isArchiveDeleteReauth) {
        if (!link.sessionId || !link.consultationId) {
          throw new DomainError("INVALID_REAUTH_BINDING");
        }
        await this.repository.rotateSession(created, link.sessionId, tx);
        return;
      }

      await this.repository.createSession(created, tx);
    });

    return {
      sessionToken: base64url(sessionToken),
      csrfToken: base64url(csrfToken),
      session: created,
      purpose,
    };
  }

  async authenticate(sessionToken: string): Promise<AuthenticatedSession> {
    const token = decodeBase64url(sessionToken);
    if (token.length !== 32) {
      throw new DomainError("UNAUTHENTICATED");
    }

    const tokenHash = this.hasher.sha256(token);
    const session = await this.repository.findSessionByTokenHash(tokenHash);
    if (!session || session.expiresAt <= this.clock.now()) {
      throw new DomainError("UNAUTHENTICATED");
    }

    const user = await this.repository.findUserById(session.userId);
    if (!user) {
      throw new DomainError("UNAUTHENTICATED");
    }

    return { session, user };
  }

  async authenticateMutation(
    sessionToken: string,
    csrfToken: string,
  ): Promise<AuthenticatedSession> {
    const authenticated = await this.authenticate(sessionToken);
    const csrf = decodeBase64url(csrfToken);
    if (csrf.length !== 32 || this.hasher.sha256(csrf) !== authenticated.session.csrfHash) {
      throw new DomainError("CSRF_INVALID");
    }

    return authenticated;
  }

  async provisionCustomer(email: string, displayName: string): Promise<UserRecord> {
    const normalized = normalizeEmail(email);
    const name = displayName.trim();
    if (!name || name.length > 120) {
      throw new DomainError("INVALID_DISPLAY_NAME");
    }

    return this.repository.findOrCreateCustomer(
      this.ids.uuid(),
      normalized,
      name,
      this.clock.now(),
    );
  }

  private async issueMagicLink(input: AdmittedMagicLink): Promise<void> {
    const rawToken = this.tokens.bytes(32);
    const expiresAt = addMilliseconds(input.now, LINK_TTL_MS);
    const link: MagicLinkRecord = {
      id: this.ids.uuid(),
      userId: input.userId,
      consultationId: input.consultationId,
      sessionId: input.sessionId,
      purpose: input.purpose,
      tokenHash: this.hasher.sha256(rawToken),
      expiresAt,
      consumedAt: null,
      revokedAt: null,
    };
    await this.repository.createMagicLink(link);
    await this.mail.sendMagicLink({
      to: input.email,
      purpose: input.purpose,
      url: `${input.publicBaseUrl}/auth/exchange?token=${encodeURIComponent(base64url(rawToken))}`,
      expiresAt,
    });
  }

  private admitMagicLinkRequest(email: string | null, ip: string, now: Date): Promise<boolean> {
    const emailHash = email
      ? this.hasher.sha256(`${this.secrets.rateLimitKey}:email:${email}`)
      : null;
    const ipHash = this.hasher.sha256(`${this.secrets.rateLimitKey}:ip:${ip}`);
    return this.repository.admitMagicLinkRequest(
      emailHash,
      ipHash,
      addMilliseconds(now, -LINK_TTL_MS),
      now,
      5,
      20,
    );
  }
}

export function normalizeEmail(value: string): string {
  const email = value.trim().normalize("NFKC").toLowerCase();
  if (email.length > 320 || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    throw new DomainError("INVALID_EMAIL");
  }
  return email;
}

function base64url(value: Uint8Array): string {
  return Buffer.from(value).toString("base64url");
}

function decodeBase64url(value: string): Uint8Array {
  if (!/^[A-Za-z0-9_-]+$/.test(value)) {
    return new Uint8Array();
  }

  try {
    return new Uint8Array(Buffer.from(value, "base64url"));
  } catch {
    return new Uint8Array();
  }
}
