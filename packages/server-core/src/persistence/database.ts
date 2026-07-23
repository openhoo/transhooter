import { PrismaPg } from "@prisma/adapter-pg";
import type { PoolConfig } from "pg";
import { PrismaClient } from "../generated/prisma/client.js";

export type PrismaPoolOptions = Pick<
  PoolConfig,
  "max" | "idleTimeoutMillis" | "connectionTimeoutMillis"
>;
export type PrismaLogLevel = "error" | "warn";

export interface PrismaDatabaseOptions {
  connectionString: string;
  pool?: PrismaPoolOptions;
  log?: readonly PrismaLogLevel[];
}

export interface PrismaDatabaseDependencies {
  createAdapter(config: PoolConfig): PrismaPg;
  createClient(options: ConstructorParameters<typeof PrismaClient>[0]): PrismaClient;
}

const defaultDependencies: PrismaDatabaseDependencies = {
  createAdapter: (config) => new PrismaPg(config),
  createClient: (options) => new PrismaClient(options),
};

export function createPrismaDatabase(
  options: PrismaDatabaseOptions,
  dependencies: PrismaDatabaseDependencies = defaultDependencies,
): PrismaDatabase {
  return new PrismaDatabase(options, dependencies);
}
export class PrismaDatabase {
  readonly client: PrismaClient;

  constructor(
    options: PrismaDatabaseOptions,
    dependencies: PrismaDatabaseDependencies = defaultDependencies,
  ) {
    const adapter = dependencies.createAdapter({
      ...options.pool,
      connectionString: options.connectionString,
    });
    this.client = dependencies.createClient({
      adapter,
      ...(options.log === undefined ? {} : { log: [...options.log] }),
    });
  }

  async readiness(): Promise<void> {
    await this.client.$queryRaw`SELECT 1`;
  }

  async disconnect(): Promise<void> {
    await this.client.$disconnect();
  }
}

export { Prisma, PrismaClient } from "../generated/prisma/client.js";
