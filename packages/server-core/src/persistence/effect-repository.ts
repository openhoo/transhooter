import { DomainError, type Instant, type UUID } from "../domain/model";
import type { EffectRepository, ExternalEffect, OutboxMessage, Transaction } from "../ports/index";
import { Prisma, type PrismaClient } from "./database";
import { TransactionHandle, unwrap } from "./transaction";

const EFFECT_PROJECTION = Prisma.sql`
  id,
  consultation_id,
  generation,
  effect_kind,
  subject_id,
  state,
  request_bytes,
  request_hash,
  lease_owner,
  lease_expires_at,
  result,
  attempts
`;

export class PrismaEffectRepository implements EffectRepository {
  constructor(private readonly database: PrismaClient) {}

  transaction<T>(work: (tx: Transaction) => Promise<T>): Promise<T> {
    return this.database.$transaction((database) => work(new TransactionHandle(database)));
  }

  async plan(effect: ExternalEffect, tx: Transaction): Promise<ExternalEffect> {
    const database = unwrap(tx);
    const now = new Date();
    const inserted = await database.$queryRaw<EffectRow[]>(Prisma.sql`
      INSERT INTO external_effects(
        id,
        consultation_id,
        generation,
        effect_kind,
        subject_id,
        occurrence_key,
        state,
        request_bytes,
        request_hash,
        lease_owner,
        lease_expires_at,
        result,
        attempts,
        created_at,
        updated_at
      ) VALUES (
        ${effect.id},
        ${effect.consultationId},
        ${effect.generation},
        ${effect.kind},
        ${effect.subjectId},
        '',
        ${effect.state}::external_effect_state,
        ${effect.requestBytes},
        ${effect.requestHash},
        ${effect.leaseOwner},
        ${effect.leaseExpiresAt},
        ${JSON.stringify(effect.result)}::jsonb,
        ${effect.attempts},
        ${now},
        ${now}
      )
      ON CONFLICT(consultation_id, generation, effect_kind, subject_id, occurrence_key)
      DO NOTHING
      RETURNING ${EFFECT_PROJECTION}
    `);
    if (inserted[0]) {
      return mapEffect(inserted[0]);
    }

    const existing = await database.$queryRaw<EffectRow[]>(Prisma.sql`
      SELECT ${EFFECT_PROJECTION}
      FROM external_effects
      WHERE consultation_id = ${effect.consultationId}
        AND generation = ${effect.generation}
        AND effect_kind = ${effect.kind}
        AND subject_id = ${effect.subjectId}
        AND occurrence_key = ''
      LIMIT 1
    `);
    if (!existing[0]) {
      throw new DomainError("EFFECT_IDENTITY_CONFLICT");
    }
    return mapEffect(existing[0]);
  }

  async lock(effectId: UUID, tx: Transaction): Promise<ExternalEffect | null> {
    const rows = await unwrap(tx).$queryRaw<EffectRow[]>(Prisma.sql`
      SELECT ${EFFECT_PROJECTION}
      FROM external_effects
      WHERE id = ${effectId}
      LIMIT 1
      FOR UPDATE
    `);
    return rows[0] ? mapEffect(rows[0]) : null;
  }

  async beginCall(
    effectId: UUID,
    requestBytes: Uint8Array,
    requestHash: string,
    owner: UUID,
    leaseUntil: Instant,
    tx: Transaction,
  ): Promise<boolean> {
    const rows = await unwrap(tx).$queryRaw<IdRow[]>(Prisma.sql`
      UPDATE external_effects
      SET state = 'calling',
          request_bytes = ${requestBytes},
          request_hash = ${requestHash},
          lease_owner = ${owner},
          lease_expires_at = ${leaseUntil},
          attempts = attempts + 1,
          updated_at = ${new Date()}
      WHERE id = ${effectId}
        AND (
          state = 'planned'
          OR (state = 'calling' AND lease_expires_at <= ${new Date()})
        )
      RETURNING id
    `);
    return rows.length === 1;
  }

  async beginCompensation(
    effectId: UUID,
    owner: UUID,
    leaseUntil: Instant,
    tx: Transaction,
  ): Promise<boolean> {
    const rows = await unwrap(tx).$queryRaw<IdRow[]>(Prisma.sql`
      UPDATE external_effects
      SET lease_owner = ${owner},
          lease_expires_at = ${leaseUntil},
          updated_at = ${new Date()}
      WHERE id = ${effectId}
        AND state = 'compensating'
        AND (lease_expires_at IS NULL OR lease_expires_at <= ${new Date()})
      RETURNING id
    `);
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
    const leaseUpdate =
      state === "compensating"
        ? Prisma.empty
        : Prisma.sql`, lease_owner = NULL, lease_expires_at = NULL`;
    const rows = await unwrap(tx).$queryRaw<IdRow[]>(Prisma.sql`
      UPDATE external_effects
      SET state = ${state}::external_effect_state,
          result = ${JSON.stringify(result)}::jsonb,
          updated_at = ${new Date()}
          ${leaseUpdate}
      WHERE id = ${effectId}
        AND state = 'calling'
        AND lease_owner = ${owner}
        AND request_hash = ${requestHash}
      RETURNING id
    `);
    return rows.length === 1;
  }

  async completeCompensation(
    effectId: UUID,
    owner: UUID,
    requestHash: string,
    result: unknown,
    tx: Transaction,
  ): Promise<boolean> {
    const rows = await unwrap(tx).$queryRaw<IdRow[]>(Prisma.sql`
      UPDATE external_effects
      SET state = 'done',
          compensation_result = ${JSON.stringify(result)}::jsonb,
          lease_owner = NULL,
          lease_expires_at = NULL,
          updated_at = ${new Date()}
      WHERE id = ${effectId}
        AND state = 'compensating'
        AND lease_owner = ${owner}
        AND request_hash = ${requestHash}
      RETURNING id
    `);
    return rows.length === 1;
  }

  async recordCompensationAttempt(
    effectId: UUID,
    owner: UUID,
    requestHash: string,
    result: unknown,
    tx: Transaction,
  ): Promise<void> {
    await unwrap(tx).$executeRaw(Prisma.sql`
      INSERT INTO effect_compensation_attempts(effect_id, owner, request_hash, result, created_at)
      VALUES (${effectId}, ${owner}, ${requestHash}, ${JSON.stringify(result)}::jsonb, ${new Date()})
      ON CONFLICT(effect_id, owner, request_hash)
      DO UPDATE SET result = excluded.result
    `);
  }

  async claimOutbox(
    owner: UUID,
    now: Instant,
    leaseUntil: Instant,
    limit: number,
    tx: Transaction,
  ): Promise<readonly OutboxMessage[]> {
    const claimed = await unwrap(tx).$queryRaw<OutboxClaimRow[]>(Prisma.sql`
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
    return claimed.map(mapOutbox);
  }

  async enqueue(message: OutboxMessage, tx: Transaction): Promise<void> {
    await unwrap(tx).$executeRaw(Prisma.sql`
      INSERT INTO outbox(id, topic, aggregate_id, generation, payload, available_at, attempts)
      VALUES (
        ${message.id},
        ${message.topic},
        ${message.aggregateId},
        ${message.generation},
        ${JSON.stringify(message.payload)}::jsonb,
        ${message.availableAt},
        ${message.attempts}
      )
      ON CONFLICT(id) DO NOTHING
    `);
  }

  async acceptInbox(
    source: string,
    eventId: string,
    occurredAt: Instant,
    payloadHash: string,
    payload: unknown,
    tx: Transaction,
  ): Promise<boolean> {
    const rows = await unwrap(tx).$queryRaw<{ event_id: string }[]>(Prisma.sql`
      INSERT INTO inbox(source, event_id, occurred_at, payload_hash, payload, received_at)
      VALUES (
        ${source},
        ${eventId},
        ${occurredAt},
        ${payloadHash},
        ${JSON.stringify(payload)}::jsonb,
        ${new Date()}
      )
      ON CONFLICT DO NOTHING
      RETURNING event_id
    `);
    return rows.length === 1;
  }
}

type IdRow = { id: UUID };

type EffectRow = {
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

type OutboxClaimRow = {
  id: UUID;
  topic: string;
  aggregate_id: UUID;
  generation: number;
  payload: unknown;
  available_at: Date;
  attempts: number;
};

function mapEffect(row: EffectRow): ExternalEffect {
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
