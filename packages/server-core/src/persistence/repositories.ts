import type { ExtractTablesWithRelations } from "drizzle-orm";
import { and, eq, gt, isNull, lte, or, sql } from "drizzle-orm";
import type { NodePgDatabase, NodePgTransaction } from "drizzle-orm/node-postgres";
import { DomainError, type Instant, type UUID } from "../domain/model";
import type {
  AuthRepository,
  EffectRepository,
  ExternalEffect,
  MagicLinkRecord,
  OutboxMessage,
  PendingExchangeRecord,
  SessionRecord,
  Transaction,
  UserRecord,
} from "../ports/index";
import type * as schema from "./schema";
import {
  effectCompensationAttempts,
  externalEffects as effects,
  inbox,
  magicLinkRequests,
  magicLinks,
  outbox,
  pendingExchanges,
  sessions,
  users,
} from "./schema";

export type DrizzleSchema = typeof schema;
export type DrizzleTransaction = NodePgTransaction<
  DrizzleSchema,
  ExtractTablesWithRelations<DrizzleSchema>
>;

export class TransactionHandle implements Transaction {
  readonly opaque = Symbol("drizzle-transaction");

  constructor(readonly database: DrizzleTransaction) {}
}

export function unwrap(tx: Transaction): DrizzleTransaction {
  if (!(tx instanceof TransactionHandle)) {
    throw new DomainError("INVALID_TRANSACTION");
  }
  return tx.database;
}

export class DrizzleEffectRepository implements EffectRepository {
  constructor(private readonly database: NodePgDatabase<DrizzleSchema>) {}

  transaction<T>(work: (tx: Transaction) => Promise<T>): Promise<T> {
    return this.database.transaction((database) => work(new TransactionHandle(database)));
  }

  async plan(effect: ExternalEffect, tx: Transaction): Promise<ExternalEffect> {
    const database = unwrap(tx);
    const [row] = await database
      .insert(effects)
      .values({
        id: effect.id,
        consultationId: effect.consultationId,
        generation: effect.generation,
        effectKind: effect.kind,
        subjectId: effect.subjectId,
        occurrenceKey: "",
        state: effect.state,
        requestBytes: effect.requestBytes,
        requestHash: effect.requestHash,
        leaseOwner: effect.leaseOwner,
        leaseExpiresAt: effect.leaseExpiresAt,
        result: effect.result,
        attempts: effect.attempts,
        createdAt: new Date(),
        updatedAt: new Date(),
      })
      .onConflictDoNothing({
        target: [
          effects.consultationId,
          effects.generation,
          effects.effectKind,
          effects.subjectId,
          effects.occurrenceKey,
        ],
      })
      .returning();

    if (row) {
      return mapEffect(row);
    }

    const [existing] = await database
      .select()
      .from(effects)
      .where(
        and(
          eq(effects.consultationId, effect.consultationId),
          eq(effects.generation, effect.generation),
          eq(effects.effectKind, effect.kind),
          eq(effects.subjectId, effect.subjectId),
          eq(effects.occurrenceKey, ""),
        ),
      )
      .limit(1);

    if (!existing) {
      throw new DomainError("EFFECT_IDENTITY_CONFLICT");
    }
    return mapEffect(existing);
  }

  async lock(effectId: UUID, tx: Transaction): Promise<ExternalEffect | null> {
    const result = await unwrap(tx).execute<EffectLockRow>(sql`
      SELECT *
      FROM external_effects
      WHERE id = ${effectId}
      FOR UPDATE
    `);
    const row = result.rows[0];
    return row ? mapRawEffect(row) : null;
  }

  async beginCall(
    effectId: UUID,
    requestBytes: Uint8Array,
    requestHash: string,
    owner: UUID,
    leaseUntil: Instant,
    tx: Transaction,
  ): Promise<boolean> {
    const rows = await unwrap(tx)
      .update(effects)
      .set({
        state: "calling",
        requestBytes,
        requestHash,
        leaseOwner: owner,
        leaseExpiresAt: leaseUntil,
        attempts: sql`${effects.attempts} + 1`,
        updatedAt: new Date(),
      })
      .where(
        and(
          eq(effects.id, effectId),
          or(
            eq(effects.state, "planned"),
            and(eq(effects.state, "calling"), lte(effects.leaseExpiresAt, new Date())),
          ),
        ),
      )
      .returning({ id: effects.id });
    return rows.length === 1;
  }

  async beginCompensation(
    effectId: UUID,
    owner: UUID,
    leaseUntil: Instant,
    tx: Transaction,
  ): Promise<boolean> {
    const rows = await unwrap(tx)
      .update(effects)
      .set({
        leaseOwner: owner,
        leaseExpiresAt: leaseUntil,
        updatedAt: new Date(),
      })
      .where(
        and(
          eq(effects.id, effectId),
          eq(effects.state, "compensating"),
          or(sql`${effects.leaseExpiresAt} IS NULL`, lte(effects.leaseExpiresAt, new Date())),
        ),
      )
      .returning({ id: effects.id });
    return rows.length === 1;
  }

  async complete(
    effectId: UUID,
    owner: UUID,
    requestHash: string,
    state: "applied" | "done" | "failed" | "compensating",
    result: unknown,
    tx: Transaction,
  ): Promise<boolean> {
    const update =
      state === "compensating"
        ? { state, result, updatedAt: new Date() }
        : {
            state,
            result,
            leaseOwner: null,
            leaseExpiresAt: null,
            updatedAt: new Date(),
          };
    const rows = await unwrap(tx)
      .update(effects)
      .set(update)
      .where(
        and(
          eq(effects.id, effectId),
          eq(effects.state, "calling"),
          eq(effects.leaseOwner, owner),
          eq(effects.requestHash, requestHash),
        ),
      )
      .returning({ id: effects.id });
    return rows.length === 1;
  }

  async completeCompensation(
    effectId: UUID,
    owner: UUID,
    requestHash: string,
    result: unknown,
    tx: Transaction,
  ): Promise<boolean> {
    const rows = await unwrap(tx)
      .update(effects)
      .set({
        state: "done",
        compensationResult: result,
        leaseOwner: null,
        leaseExpiresAt: null,
        updatedAt: new Date(),
      })
      .where(
        and(
          eq(effects.id, effectId),
          eq(effects.state, "compensating"),
          eq(effects.leaseOwner, owner),
          eq(effects.requestHash, requestHash),
        ),
      )
      .returning({ id: effects.id });
    return rows.length === 1;
  }

  async recordCompensationAttempt(
    effectId: UUID,
    owner: UUID,
    requestHash: string,
    result: unknown,
    tx: Transaction,
  ): Promise<void> {
    await unwrap(tx)
      .insert(effectCompensationAttempts)
      .values({
        effectId,
        owner,
        requestHash,
        result,
        createdAt: new Date(),
      })
      .onConflictDoUpdate({
        target: [
          effectCompensationAttempts.effectId,
          effectCompensationAttempts.owner,
          effectCompensationAttempts.requestHash,
        ],
        set: { result: sql`excluded.result` },
      });
  }

  async claimOutbox(
    owner: UUID,
    now: Instant,
    leaseUntil: Instant,
    limit: number,
    tx: Transaction,
  ): Promise<readonly OutboxMessage[]> {
    const claimed = await unwrap(tx).execute<OutboxClaimRow>(sql`
      WITH candidates AS (
        SELECT id
        FROM outbox
        WHERE delivered_at IS NULL
          AND available_at <= ${now}
          AND (lease_expires_at IS NULL OR lease_expires_at <= ${now})
        ORDER BY available_at, id
        FOR UPDATE SKIP LOCKED
        LIMIT ${limit}
      )
      UPDATE outbox AS claimed
      SET lease_owner = ${owner},
          lease_expires_at = ${leaseUntil},
          attempts = claimed.attempts + 1
      FROM candidates
      WHERE claimed.id = candidates.id
      RETURNING claimed.*
    `);
    return claimed.rows.map(mapOutbox);
  }

  async enqueue(message: OutboxMessage, tx: Transaction): Promise<void> {
    await unwrap(tx).insert(outbox).values(message).onConflictDoNothing({ target: outbox.id });
  }

  async markOutboxDone(id: UUID, owner: UUID, at: Instant, tx: Transaction): Promise<boolean> {
    const rows = await unwrap(tx)
      .update(outbox)
      .set({ deliveredAt: at, leaseOwner: null, leaseExpiresAt: null })
      .where(and(eq(outbox.id, id), eq(outbox.leaseOwner, owner), gt(outbox.leaseExpiresAt, at)))
      .returning({ id: outbox.id });
    return rows.length === 1;
  }

  async acceptInbox(
    source: string,
    eventId: string,
    occurredAt: Instant,
    payloadHash: string,
    payload: unknown,
    tx: Transaction,
  ): Promise<boolean> {
    const rows = await unwrap(tx)
      .insert(inbox)
      .values({
        source,
        eventId,
        occurredAt,
        payloadHash,
        payload,
        receivedAt: new Date(),
      })
      .onConflictDoNothing()
      .returning({ eventId: inbox.eventId });
    return rows.length === 1;
  }
}

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

  async createMagicLink(link: MagicLinkRecord, tx?: Transaction): Promise<void> {
    const database = tx ? unwrap(tx) : this.database;
    await database.insert(magicLinks).values({
      id: link.id,
      userId: link.userId,
      consultationId: link.consultationId,
      sessionId: link.sessionId,
      purpose: link.purpose,
      tokenHash: link.tokenHash,
      expiresAt: link.expiresAt,
      consumedAt: link.consumedAt,
      revokedAt: link.revokedAt,
      createdAt: sql`now()`,
    });
  }

  async lockMagicLinkByTokenHash(hash: string, tx: Transaction): Promise<MagicLinkRecord | null> {
    const result = await unwrap(tx).execute<MagicLinkLockRow>(sql`
      SELECT *
      FROM magic_links
      WHERE token_hash = ${hash}
      FOR UPDATE
    `);
    return result.rows[0] ? mapRawMagicLink(result.rows[0]) : null;
  }

  async lockMagicLinkById(id: UUID, tx: Transaction): Promise<MagicLinkRecord | null> {
    const result = await unwrap(tx).execute<MagicLinkLockRow>(sql`
      SELECT *
      FROM magic_links
      WHERE id = ${id}
      FOR UPDATE
    `);
    return result.rows[0] ? mapRawMagicLink(result.rows[0]) : null;
  }

  async createPendingExchange(exchange: PendingExchangeRecord, tx: Transaction): Promise<void> {
    await unwrap(tx).insert(pendingExchanges).values({
      id: exchange.id,
      magicLinkId: exchange.magicLinkId,
      nonceHash: exchange.nonceHash,
      csrfHash: exchange.csrfHash,
      expiresAt: exchange.expiresAt,
      consumedAt: exchange.consumedAt,
      createdAt: sql`now()`,
    });
  }

  async lockPendingExchangeByNonceHash(
    hash: string,
    tx: Transaction,
  ): Promise<PendingExchangeRecord | null> {
    const result = await unwrap(tx).execute<PendingExchangeLockRow>(sql`
      SELECT *
      FROM pending_exchanges
      WHERE nonce_hash = ${hash}
      FOR UPDATE
    `);
    return result.rows[0] ? mapRawExchange(result.rows[0]) : null;
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

      const result = await database.execute<RateLimitCountRow>(sql`
        SELECT
          count(*) FILTER (WHERE email_hash = ${emailHash})::int AS email,
          count(*) FILTER (WHERE ip_hash = ${ipHash})::int AS ip
        FROM magic_link_requests
        WHERE requested_at >= ${since}
      `);
      const row = result.rows[0];
      if (!row) {
        throw new Error("magic-link admission count returned no row");
      }

      await database.insert(magicLinkRequests).values({
        emailHash,
        ipHash,
        requestedAt: at,
      });
      return Number(row.ip) < ipLimit && (emailHash === null || Number(row.email) < emailLimit);
    });
  }
}

type EffectLockRow = {
  id: UUID;
  consultation_id: UUID;
  generation: number;
  effect_kind: string;
  subject_id: UUID;
  state: ExternalEffect["state"];
  request_bytes: Uint8Array | null;
  request_hash: string | null;
  lease_owner: UUID | null;
  lease_expires_at: Date | null;
  result: unknown;
  attempts: number;
};

type MagicLinkLockRow = {
  id: UUID;
  user_id: UUID | null;
  consultation_id: UUID | null;
  session_id: UUID | null;
  purpose: MagicLinkRecord["purpose"];
  token_hash: string;
  expires_at: Date;
  consumed_at: Date | null;
  revoked_at: Date | null;
};

type PendingExchangeLockRow = {
  id: UUID;
  magic_link_id: UUID;
  nonce_hash: string;
  csrf_hash: string;
  expires_at: Date;
  consumed_at: Date | null;
};

type OutboxClaimRow = {
  id: UUID;
  topic: string;
  aggregate_id: UUID;
  generation: number;
  payload: unknown;
  available_at: Date;
  attempts: number;
};

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

function mapRawMagicLink(row: MagicLinkLockRow): MagicLinkRecord {
  return {
    id: row.id,
    userId: row.user_id,
    consultationId: row.consultation_id,
    sessionId: row.session_id,
    purpose: row.purpose,
    tokenHash: row.token_hash,
    expiresAt: row.expires_at,
    consumedAt: row.consumed_at,
    revokedAt: row.revoked_at,
  };
}

function mapRawExchange(row: PendingExchangeLockRow): PendingExchangeRecord {
  return {
    id: row.id,
    magicLinkId: row.magic_link_id,
    nonceHash: row.nonce_hash,
    csrfHash: row.csrf_hash,
    expiresAt: row.expires_at,
    consumedAt: row.consumed_at,
  };
}

function mapEffect(row: typeof effects.$inferSelect): ExternalEffect {
  return {
    id: row.id,
    consultationId: row.consultationId,
    generation: row.generation,
    kind: row.effectKind,
    subjectId: row.subjectId,
    state: row.state,
    requestBytes: row.requestBytes,
    requestHash: row.requestHash,
    leaseOwner: row.leaseOwner,
    leaseExpiresAt: row.leaseExpiresAt,
    result: row.result,
    attempts: row.attempts,
  };
}

function mapRawEffect(row: EffectLockRow): ExternalEffect {
  return {
    id: row.id,
    consultationId: row.consultation_id,
    generation: row.generation,
    kind: row.effect_kind,
    subjectId: row.subject_id,
    state: row.state,
    requestBytes: row.request_bytes,
    requestHash: row.request_hash,
    leaseOwner: row.lease_owner,
    leaseExpiresAt: row.lease_expires_at,
    result: row.result,
    attempts: row.attempts,
  };
}

function mapOutbox(row: OutboxClaimRow): OutboxMessage {
  return {
    id: row.id,
    topic: row.topic,
    aggregateId: row.aggregate_id,
    generation: row.generation,
    payload: row.payload,
    availableAt: row.available_at,
    attempts: row.attempts,
  };
}
