import { and, desc, eq, gt, isNull, lte, or, sql } from "drizzle-orm";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";
import { DomainError, type Instant, type UUID } from "../domain/model";
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
} from "../ports/index";
import { magicLinkRequests, magicLinks, pendingExchanges, sessions, users } from "./schema";
import { type DrizzleSchema, TransactionHandle, unwrap } from "./transaction";

export class DrizzleAuthRepository implements AuthRepository {
  constructor(private readonly database: NodePgDatabase<DrizzleSchema>) {}

  transaction<T>(work: (tx: Transaction) => Promise<T>): Promise<T> {
    return this.database.transaction((database) => work(new TransactionHandle(database)));
  }

  async findUserByEmail(email: string, tx?: Transaction): Promise<UserRecord | null> {
    const database = tx ? unwrap(tx) : this.database;
    const [row] = await database.select().from(users).where(eq(users.email, email)).limit(1);
    return row ? mapUser(row) : null;
  }

  async findUserById(id: UUID, tx?: Transaction): Promise<UserRecord | null> {
    const database = tx ? unwrap(tx) : this.database;
    const [row] = await database.select().from(users).where(eq(users.id, id)).limit(1);
    return row ? mapUser(row) : null;
  }

  async findOrCreateCustomer(
    id: UUID,
    email: string,
    displayName: string,
    createdAt: Instant,
  ): Promise<UserRecord> {
    const [row] = await this.database
      .insert(users)
      .values({ id, email, displayName, staffRole: null, createdAt })
      .onConflictDoUpdate({
        target: users.email,
        set: { email: sql`excluded.email` },
      })
      .returning();
    if (!row) {
      throw new Error("customer upsert returned no row");
    }
    return mapUser(row);
  }

  async findSessionByTokenHash(hash: string): Promise<SessionRecord | null> {
    const [row] = await this.database
      .select()
      .from(sessions)
      .where(and(eq(sessions.tokenHash, hash), isNull(sessions.revokedAt)))
      .limit(1);
    return row ? mapSession(row) : null;
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

    return this.database.transaction(async (database) => {
      await database.execute<AdvisoryLockRow>(sql`
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
        )
      `);

      const [current] = await database
        .select()
        .from(magicLinks)
        .where(
          and(
            sql`${magicLinks.userId} IS NOT DISTINCT FROM ${identity.userId}::uuid`,
            eq(magicLinks.purpose, identity.purpose),
            sql`${magicLinks.consultationId} IS NOT DISTINCT FROM ${identity.consultationId}::uuid`,
            sql`${magicLinks.sessionId} IS NOT DISTINCT FROM ${identity.sessionId}::uuid`,
            isNull(magicLinks.consumedAt),
            isNull(magicLinks.revokedAt),
          ),
        )
        .orderBy(desc(magicLinks.createdAt), desc(magicLinks.id))
        .limit(1)
        .for("update");
      if (current && current.expiresAt > now) {
        return {
          record: mapMagicLink(current),
          sealedRawToken: current.sealedRawToken,
          keyId: current.sealedTokenKeyId,
          created: false,
        };
      }

      if (current) {
        await database
          .update(magicLinks)
          .set({ revokedAt: now })
          .where(
            and(
              eq(magicLinks.id, current.id),
              isNull(magicLinks.consumedAt),
              isNull(magicLinks.revokedAt),
              lte(magicLinks.expiresAt, now),
            ),
          );
      }

      await database.insert(magicLinks).values({
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
      });
      return { ...candidate, created: true };
    });
  }

  async lockMagicLinkByTokenHash(hash: string, tx: Transaction): Promise<MagicLinkRecord | null> {
    const [row] = await unwrap(tx)
      .select()
      .from(magicLinks)
      .where(eq(magicLinks.tokenHash, hash))
      .limit(1)
      .for("update");
    return row ? mapMagicLink(row) : null;
  }

  async lockMagicLinkById(id: UUID, tx: Transaction): Promise<MagicLinkRecord | null> {
    const [row] = await unwrap(tx)
      .select()
      .from(magicLinks)
      .where(eq(magicLinks.id, id))
      .limit(1)
      .for("update");
    return row ? mapMagicLink(row) : null;
  }

  async createPendingExchange(exchange: PendingExchangeRecord, tx: Transaction): Promise<void> {
    const database = unwrap(tx);
    await database
      .delete(pendingExchanges)
      .where(
        and(
          eq(pendingExchanges.magicLinkId, exchange.magicLinkId),
          or(
            sql`${pendingExchanges.consumedAt} IS NOT NULL`,
            lte(pendingExchanges.expiresAt, sql`CURRENT_TIMESTAMP`),
          ),
        ),
      );
    const inserted = await database
      .insert(pendingExchanges)
      .values({
        id: exchange.id,
        magicLinkId: exchange.magicLinkId,
        nonceHash: exchange.nonceHash,
        csrfHash: exchange.csrfHash,
        expiresAt: exchange.expiresAt,
        consumedAt: exchange.consumedAt,
        createdAt: sql`CURRENT_TIMESTAMP`,
      })
      .onConflictDoNothing()
      .returning({ id: pendingExchanges.id });
    if (inserted.length !== 1) {
      throw new DomainError("INVALID_OR_EXPIRED_LINK");
    }
  }

  async lockPendingExchangeByNonceHash(
    hash: string,
    tx: Transaction,
  ): Promise<PendingExchangeRecord | null> {
    const [row] = await unwrap(tx)
      .select()
      .from(pendingExchanges)
      .where(eq(pendingExchanges.nonceHash, hash))
      .limit(1)
      .for("update");
    return row ? mapExchange(row) : null;
  }

  async consumeExchangeAndLink(
    exchangeId: UUID,
    linkId: UUID,
    at: Instant,
    tx: Transaction,
  ): Promise<boolean> {
    const result = await unwrap(tx).execute<ConsumeExchangeRow>(sql`
      WITH consumed_exchange AS (
        UPDATE pending_exchanges
        SET consumed_at = ${at}
        WHERE id = ${exchangeId}
          AND consumed_at IS NULL
        RETURNING id
      ), consumed_link AS (
        UPDATE magic_links
        SET consumed_at = ${at}
        WHERE id = ${linkId}
          AND consumed_at IS NULL
          AND revoked_at IS NULL
        RETURNING id
      )
      SELECT
        (SELECT count(*) FROM consumed_exchange) = 1
        AND (SELECT count(*) FROM consumed_link) = 1 AS consumed
    `);
    return result.rows[0]?.consumed === true;
  }

  async createSession(session: SessionRecord, tx: Transaction): Promise<void> {
    await unwrap(tx).insert(sessions).values({
      id: session.id,
      userId: session.userId,
      tokenHash: session.tokenHash,
      csrfHash: session.csrfHash,
      expiresAt: session.expiresAt,
      reauthenticatedAt: session.reauthenticatedAt,
      reauthConsultationId: session.reauthConsultationId,
      createdAt: sql`now()`,
    });
  }

  async rotateSession(
    session: SessionRecord,
    replacesSessionId: UUID,
    tx: Transaction,
  ): Promise<void> {
    const database = unwrap(tx);
    const eligible = await database.execute<SessionEligibilityRow>(sql`
      SELECT sessions.id
      FROM sessions
      JOIN users ON users.id = sessions.user_id
      WHERE sessions.id = ${replacesSessionId}
        AND sessions.user_id = ${session.userId}
        AND sessions.revoked_at IS NULL
        AND sessions.expires_at > now()
        AND users.staff_role = 'admin'
      FOR UPDATE
    `);
    if (eligible.rowCount !== 1) {
      throw new DomainError("INVALID_REAUTH_BINDING");
    }

    await this.createSession(session, tx);
    const revoked = await database
      .update(sessions)
      .set({ revokedAt: sql`now()`, replacedBy: session.id })
      .where(
        and(
          eq(sessions.id, replacesSessionId),
          eq(sessions.userId, session.userId),
          isNull(sessions.revokedAt),
          gt(sessions.expiresAt, sql`now()`),
        ),
      )
      .returning({ id: sessions.id });
    if (revoked.length !== 1) {
      throw new DomainError("INVALID_REAUTH_BINDING");
    }
  }

  async revokeConsultationLinks(consultationId: UUID, at: Instant, tx: Transaction): Promise<void> {
    await unwrap(tx)
      .update(magicLinks)
      .set({ revokedAt: at })
      .where(
        and(
          eq(magicLinks.consultationId, consultationId),
          isNull(magicLinks.consumedAt),
          isNull(magicLinks.revokedAt),
        ),
      );
  }

  async admitMagicLinkRequest(
    emailHash: string | null,
    ipHash: string,
    since: Instant,
    at: Instant,
    emailLimit: number,
    ipLimit: number,
  ): Promise<boolean> {
    return this.database.transaction(async (database) => {
      await database.execute<AdvisoryLockRow>(sql`
        SELECT pg_advisory_xact_lock(hashtextextended(${ipHash}, 0))
      `);
      if (emailHash) {
        await database.execute<AdvisoryLockRow>(sql`
          SELECT pg_advisory_xact_lock(hashtextextended(${emailHash}, 1))
        `);
      }

      await database.delete(magicLinkRequests).where(lte(magicLinkRequests.requestedAt, since));

      const result = await database.execute<RateLimitCountRow>(sql`
        SELECT
          count(*) FILTER (WHERE email_hash = ${emailHash})::int AS email,
          count(*) FILTER (WHERE ip_hash = ${ipHash})::int AS ip
        FROM magic_link_requests
        WHERE requested_at > ${since}
      `);
      const row = result.rows[0];
      if (!row) {
        throw new Error("magic-link admission count returned no row");
      }

      const admitted =
        Number(row.ip) < ipLimit && (emailHash === null || Number(row.email) < emailLimit);
      if (!admitted) {
        return false;
      }

      await database.insert(magicLinkRequests).values({
        emailHash,
        ipHash,
        requestedAt: at,
      });
      return true;
    });
  }
}

type ConsumeExchangeRow = { consumed: boolean };
type SessionEligibilityRow = { id: UUID };
type AdvisoryLockRow = { pg_advisory_xact_lock: string };
type RateLimitCountRow = { email: number; ip: number };

function mapUser(row: typeof users.$inferSelect): UserRecord {
  return {
    id: row.id,
    email: row.email,
    displayName: row.displayName,
    staffRole: row.staffRole,
  };
}

function mapSession(row: typeof sessions.$inferSelect): SessionRecord {
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

function mapMagicLink(row: typeof magicLinks.$inferSelect): MagicLinkRecord {
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

function mapExchange(row: typeof pendingExchanges.$inferSelect): PendingExchangeRecord {
  return {
    id: row.id,
    magicLinkId: row.magicLinkId,
    nonceHash: row.nonceHash,
    csrfHash: row.csrfHash,
    expiresAt: row.expiresAt,
    consumedAt: row.consumedAt,
  };
}
