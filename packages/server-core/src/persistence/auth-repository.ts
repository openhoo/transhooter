import { DomainError, type Instant, type UUID } from "../domain/model";
import type { MagicLink, PendingExchange, Session, User } from "../generated/prisma/client.js";
import type {
  ActiveMagicLink,
  AuthenticatedSessionRecord,
  AuthRepository,
  MagicLinkCandidate,
  MagicLinkIdentity,
  MagicLinkRecord,
  PendingExchangeRecord,
  SessionRecord,
  Transaction,
  UserRecord,
} from "../ports/index";
import { Prisma, type PrismaClient } from "./database";
import { TransactionHandle, unwrap } from "./transaction";

export class PrismaAuthRepository implements AuthRepository {
  constructor(private readonly database: PrismaClient) {}

  transaction<T>(work: (tx: Transaction) => Promise<T>): Promise<T> {
    return this.database.$transaction((database) => work(new TransactionHandle(database)));
  }

  async findUserByEmail(email: string, tx?: Transaction): Promise<UserRecord | null> {
    const database = tx ? unwrap(tx) : this.database;
    const row = await database.user.findUnique({ where: { email } });
    return row ? mapUser(row) : null;
  }

  async findUserById(id: UUID, tx?: Transaction): Promise<UserRecord | null> {
    const database = tx ? unwrap(tx) : this.database;
    const row = await database.user.findUnique({ where: { id } });
    return row ? mapUser(row) : null;
  }

  async findOrCreateCustomer(
    id: UUID,
    email: string,
    displayName: string,
    createdAt: Instant,
  ): Promise<UserRecord> {
    const row = await this.database.user.upsert({
      where: { email },
      create: { id, email, displayName, staffRole: null, createdAt },
      update: { email },
    });
    return mapUser(row);
  }

  async findAuthenticatedSessionByTokenHash(
    hash: string,
    now: Instant,
  ): Promise<AuthenticatedSessionRecord | null> {
    const row = await this.database.session.findFirst({
      where: { tokenHash: hash, revokedAt: null, expiresAt: { gt: now } },
      include: { user: true },
    });
    return row ? { session: mapSession(row), user: mapUser(row.user) } : null;
  }

  async getOrCreateActiveMagicLink(
    identity: MagicLinkIdentity,
    candidate: MagicLinkCandidate,
    now: Instant,
  ): Promise<ActiveMagicLink> {
    if (
      candidate.record.userId !== identity.userId ||
      candidate.record.purpose !== identity.purpose ||
      candidate.record.consultationId !== identity.consultationId ||
      candidate.record.sessionId !== identity.sessionId ||
      candidate.record.consumedAt !== null ||
      candidate.record.revokedAt !== null ||
      candidate.record.expiresAt <= now ||
      candidate.sealedRawToken.length === 0 ||
      candidate.keyId.length === 0
    ) {
      throw new DomainError("INVALID_MAGIC_LINK_CANDIDATE");
    }

    return this.database.$transaction(async (database) => {
      await database.$queryRaw<AdvisoryLockRow[]>(Prisma.sql`
        SELECT pg_advisory_xact_lock(
          hashtextextended(
            jsonb_build_array(
              ${identity.userId}::uuid,
              ${identity.purpose}::magic_link_purpose,
              ${identity.consultationId}::uuid,
              ${identity.sessionId}::uuid
            )::text,
            2
          )
        )::text AS locked
      `);

      const rows = await database.$queryRaw<MagicLink[]>(Prisma.sql`
        SELECT
          id,
          user_id AS "userId",
          consultation_id AS "consultationId",
          session_id AS "sessionId",
          purpose,
          token_hash AS "tokenHash",
          expires_at AS "expiresAt",
          consumed_at AS "consumedAt",
          revoked_at AS "revokedAt",
          created_at AS "createdAt",
          sealed_raw_token AS "sealedRawToken",
          sealed_token_key_id AS "sealedTokenKeyId"
        FROM magic_links
        WHERE user_id IS NOT DISTINCT FROM ${identity.userId}::uuid
          AND purpose = ${identity.purpose}::magic_link_purpose
          AND consultation_id IS NOT DISTINCT FROM ${identity.consultationId}::uuid
          AND session_id IS NOT DISTINCT FROM ${identity.sessionId}::uuid
          AND consumed_at IS NULL
          AND revoked_at IS NULL
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        FOR UPDATE
      `);
      const current = rows[0];
      if (current && current.expiresAt > now) {
        return {
          record: mapMagicLink(current),
          sealedRawToken: current.sealedRawToken,
          keyId: current.sealedTokenKeyId,
          created: false,
        };
      }

      if (current) {
        await database.magicLink.updateMany({
          where: {
            id: current.id,
            consumedAt: null,
            revokedAt: null,
            expiresAt: { lte: now },
          },
          data: { revokedAt: now },
        });
      }

      await database.magicLink.create({
        data: {
          id: candidate.record.id,
          userId: candidate.record.userId,
          consultationId: candidate.record.consultationId,
          sessionId: candidate.record.sessionId,
          purpose: candidate.record.purpose,
          tokenHash: candidate.record.tokenHash,
          expiresAt: candidate.record.expiresAt,
          consumedAt: null,
          revokedAt: null,
          createdAt: now,
          sealedRawToken: candidate.sealedRawToken,
          sealedTokenKeyId: candidate.keyId,
        },
      });
      return { ...candidate, created: true };
    });
  }

  async lockMagicLinkByTokenHash(hash: string, tx: Transaction): Promise<MagicLinkRecord | null> {
    const rows = await unwrap(tx).$queryRaw<MagicLink[]>(Prisma.sql`
      SELECT
        id,
        user_id AS "userId",
        consultation_id AS "consultationId",
        session_id AS "sessionId",
        purpose,
        token_hash AS "tokenHash",
        expires_at AS "expiresAt",
        consumed_at AS "consumedAt",
        revoked_at AS "revokedAt",
        created_at AS "createdAt",
        sealed_raw_token AS "sealedRawToken",
        sealed_token_key_id AS "sealedTokenKeyId"
      FROM magic_links
      WHERE token_hash = ${hash}
      LIMIT 1
      FOR UPDATE
    `);
    return rows[0] ? mapMagicLink(rows[0]) : null;
  }

  async lockMagicLinkById(id: UUID, tx: Transaction): Promise<MagicLinkRecord | null> {
    const rows = await unwrap(tx).$queryRaw<MagicLink[]>(Prisma.sql`
      SELECT
        id,
        user_id AS "userId",
        consultation_id AS "consultationId",
        session_id AS "sessionId",
        purpose,
        token_hash AS "tokenHash",
        expires_at AS "expiresAt",
        consumed_at AS "consumedAt",
        revoked_at AS "revokedAt",
        created_at AS "createdAt",
        sealed_raw_token AS "sealedRawToken",
        sealed_token_key_id AS "sealedTokenKeyId"
      FROM magic_links
      WHERE id = ${id}::uuid
      LIMIT 1
      FOR UPDATE
    `);
    return rows[0] ? mapMagicLink(rows[0]) : null;
  }

  async createPendingExchange(exchange: PendingExchangeRecord, tx: Transaction): Promise<void> {
    const database = unwrap(tx);
    await database.$executeRaw(Prisma.sql`
      DELETE FROM pending_exchanges
      WHERE magic_link_id = ${exchange.magicLinkId}::uuid
        AND (consumed_at IS NOT NULL OR expires_at <= CURRENT_TIMESTAMP)
    `);
    const inserted = await database.$queryRaw<{ id: UUID }[]>(Prisma.sql`
      INSERT INTO pending_exchanges (
        id,
        magic_link_id,
        nonce_hash,
        csrf_hash,
        expires_at,
        consumed_at,
        created_at
      ) VALUES (
        ${exchange.id}::uuid,
        ${exchange.magicLinkId}::uuid,
        ${exchange.nonceHash},
        ${exchange.csrfHash},
        ${exchange.expiresAt},
        ${exchange.consumedAt},
        CURRENT_TIMESTAMP
      )
      ON CONFLICT DO NOTHING
      RETURNING id
    `);
    if (inserted.length !== 1) {
      throw new DomainError("INVALID_OR_EXPIRED_LINK");
    }
  }

  async lockPendingExchangeByNonceHash(
    hash: string,
    tx: Transaction,
  ): Promise<PendingExchangeRecord | null> {
    const rows = await unwrap(tx).$queryRaw<PendingExchange[]>(Prisma.sql`
      SELECT
        id,
        magic_link_id AS "magicLinkId",
        nonce_hash AS "nonceHash",
        csrf_hash AS "csrfHash",
        expires_at AS "expiresAt",
        consumed_at AS "consumedAt",
        created_at AS "createdAt"
      FROM pending_exchanges
      WHERE nonce_hash = ${hash}
      LIMIT 1
      FOR UPDATE
    `);
    return rows[0] ? mapExchange(rows[0]) : null;
  }

  async consumeExchangeAndLink(
    exchangeId: UUID,
    linkId: UUID,
    at: Instant,
    tx: Transaction,
  ): Promise<boolean> {
    const result = await unwrap(tx).$queryRaw<ConsumeExchangeRow[]>(Prisma.sql`
      WITH consumed_exchange AS (
        UPDATE pending_exchanges
        SET consumed_at = ${at}
        WHERE id = ${exchangeId}::uuid
          AND consumed_at IS NULL
        RETURNING id
      ), consumed_link AS (
        UPDATE magic_links
        SET consumed_at = ${at}
        WHERE id = ${linkId}::uuid
          AND consumed_at IS NULL
          AND revoked_at IS NULL
        RETURNING id
      )
      SELECT
        (SELECT count(*) FROM consumed_exchange) = 1
        AND (SELECT count(*) FROM consumed_link) = 1 AS consumed
    `);
    return result[0]?.consumed === true;
  }

  async createSession(session: SessionRecord, tx: Transaction): Promise<void> {
    await unwrap(tx).$executeRaw(Prisma.sql`
      INSERT INTO sessions (
        id,
        user_id,
        token_hash,
        csrf_hash,
        expires_at,
        reauthenticated_at,
        reauth_consultation_id,
        created_at
      ) VALUES (
        ${session.id}::uuid,
        ${session.userId}::uuid,
        ${session.tokenHash},
        ${session.csrfHash},
        ${session.expiresAt},
        ${session.reauthenticatedAt},
        ${session.reauthConsultationId}::uuid,
        now()
      )
    `);
  }

  async rotateSession(
    session: SessionRecord,
    replacesSessionId: UUID,
    tx: Transaction,
  ): Promise<void> {
    const database = unwrap(tx);
    const eligible = await database.$queryRaw<SessionEligibilityRow[]>(Prisma.sql`
      SELECT sessions.id
      FROM sessions
      JOIN users ON users.id = sessions.user_id
      WHERE sessions.id = ${replacesSessionId}::uuid
        AND sessions.user_id = ${session.userId}::uuid
        AND sessions.revoked_at IS NULL
        AND sessions.expires_at > now()
        AND users.staff_role = 'admin'
      FOR UPDATE
    `);
    if (eligible.length !== 1) {
      throw new DomainError("INVALID_REAUTH_BINDING");
    }

    await this.createSession(session, tx);
    const revoked = await database.$queryRaw<{ id: UUID }[]>(Prisma.sql`
      UPDATE sessions
      SET revoked_at = now(), replaced_by = ${session.id}::uuid
      WHERE id = ${replacesSessionId}::uuid
        AND user_id = ${session.userId}::uuid
        AND revoked_at IS NULL
        AND expires_at > now()
      RETURNING id
    `);
    if (revoked.length !== 1) {
      throw new DomainError("INVALID_REAUTH_BINDING");
    }
  }

  async revokeConsultationLinks(consultationId: UUID, at: Instant, tx: Transaction): Promise<void> {
    await unwrap(tx).magicLink.updateMany({
      where: { consultationId, consumedAt: null, revokedAt: null },
      data: { revokedAt: at },
    });
  }

  async admitMagicLinkRequest(
    emailHash: string | null,
    ipHash: string,
    since: Instant,
    at: Instant,
    emailLimit: number,
    ipLimit: number,
  ): Promise<boolean> {
    return this.database.$transaction(async (database) => {
      await database.$queryRaw<AdvisoryLockRow[]>(Prisma.sql`
        SELECT pg_advisory_xact_lock(hashtextextended(${ipHash}, 0))::text AS locked
      `);
      if (emailHash) {
        await database.$queryRaw<AdvisoryLockRow[]>(Prisma.sql`
          SELECT pg_advisory_xact_lock(hashtextextended(${emailHash}, 1))::text AS locked
        `);
      }

      await database.magicLinkRequest.deleteMany({ where: { requestedAt: { lte: since } } });

      const result = await database.$queryRaw<RateLimitCountRow[]>(Prisma.sql`
        SELECT
          count(*) FILTER (WHERE email_hash = ${emailHash})::int AS email,
          count(*) FILTER (WHERE ip_hash = ${ipHash})::int AS ip
        FROM magic_link_requests
        WHERE requested_at > ${since}
      `);
      const row = result[0];
      if (!row) {
        throw new Error("magic-link admission count returned no row");
      }

      const admitted =
        Number(row.ip) < ipLimit && (emailHash === null || Number(row.email) < emailLimit);
      if (!admitted) {
        return false;
      }

      await database.magicLinkRequest.create({ data: { emailHash, ipHash, requestedAt: at } });
      return true;
    });
  }
}

type ConsumeExchangeRow = { consumed: boolean };
type SessionEligibilityRow = { id: UUID };
type AdvisoryLockRow = { locked: string };
type RateLimitCountRow = { email: number; ip: number };

function mapUser(row: User): UserRecord {
  return {
    id: row.id,
    email: row.email,
    displayName: row.displayName,
    staffRole: row.staffRole,
  };
}

function mapSession(row: Session): SessionRecord {
  return {
    id: row.id,
    userId: row.userId,
    tokenHash: row.tokenHash,
    csrfHash: row.csrfHash,
    expiresAt: row.expiresAt,
    reauthenticatedAt: row.reauthenticatedAt,
    reauthConsultationId: row.reauthConsultationId,
  };
}

function mapMagicLink(row: MagicLink): MagicLinkRecord {
  return {
    id: row.id,
    userId: row.userId,
    consultationId: row.consultationId,
    sessionId: row.sessionId,
    purpose: row.purpose,
    tokenHash: row.tokenHash,
    expiresAt: row.expiresAt,
    consumedAt: row.consumedAt,
    revokedAt: row.revokedAt,
  };
}

function mapExchange(row: PendingExchange): PendingExchangeRecord {
  return {
    id: row.id,
    magicLinkId: row.magicLinkId,
    nonceHash: row.nonceHash,
    csrfHash: row.csrfHash,
    expiresAt: row.expiresAt,
    consumedAt: row.consumedAt,
  };
}
