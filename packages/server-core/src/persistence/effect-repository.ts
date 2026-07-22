import { and, eq, gt, lte, or, sql } from "drizzle-orm";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";
import { DomainError, type Instant, type UUID } from "../domain/model";
import type { EffectRepository, ExternalEffect, OutboxMessage, Transaction } from "../ports/index";
import { effectCompensationAttempts, externalEffects as effects, inbox, outbox } from "./schema";
import { type DrizzleSchema, TransactionHandle, unwrap } from "./transaction";

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
    const [row] = await unwrap(tx)
      .select()
      .from(effects)
      .where(eq(effects.id, effectId))
      .limit(1)
      .for("update");
    return row ? mapEffect(row) : null;
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

type OutboxClaimRow = {
  id: UUID;
  topic: string;
  aggregate_id: UUID;
  generation: number;
  payload: unknown;
  available_at: Date;
  attempts: number;
};

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
