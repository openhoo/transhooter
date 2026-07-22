import type { ExtractTablesWithRelations } from "drizzle-orm";
import type { NodePgTransaction } from "drizzle-orm/node-postgres";
import { DomainError } from "../domain/model";
import type { Transaction } from "../ports/index";
import type * as schema from "./schema";

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
