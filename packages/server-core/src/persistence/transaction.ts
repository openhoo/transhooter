import { DomainError } from "../domain/model";
import type { Prisma } from "../generated/prisma/client.js";
import type { Transaction } from "../ports/index";

export class TransactionHandle implements Transaction {
  readonly opaque = Symbol("prisma-transaction");

  constructor(readonly database: Prisma.TransactionClient) {}
}

export function unwrap(tx: Transaction): Prisma.TransactionClient {
  if (!(tx instanceof TransactionHandle)) {
    throw new DomainError("INVALID_TRANSACTION");
  }
  return tx.database;
}
