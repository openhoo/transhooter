import { describe, expect, it, mock } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { drizzle } from "drizzle-orm/node-postgres";
import { Client } from "pg";
import { LanguageService } from "../src/languages/service";
import { DrizzleLanguageRepository } from "../src/persistence/application-repositories";
import { DrizzleAuthRepository, TransactionHandle } from "../src/persistence/repositories";
import * as schema from "../src/persistence/schema";
import type { PendingExchangeRecord } from "../src/ports/index";

const NOW = new Date("2026-01-01T00:00:00Z");
const SINCE = new Date("2025-12-31T23:45:00Z");
const LINK_ID = "00000000-0000-4000-8000-000000000001";

function admissionDatabase(counts: { email: number; ip: number }) {
  const removeExpired = mock(async () => undefined);
  const values = mock(async () => undefined);
  let execution = 0;
  const execute = mock(async () => {
    execution += 1;
    if (execution % 3 === 0) {
      return { rows: [counts], rowCount: 1 };
    }
    return { rows: [], rowCount: 1 };
  });
  const transaction = async <T>(work: (transaction: unknown) => Promise<T>): Promise<T> =>
    work(database);
  const database = {
    execute,
    delete: mock(() => ({ where: removeExpired })),
    insert: mock(() => ({ values })),
    transaction,
  };
  return { database, removeExpired, values };
}

function exchangeRecord(): PendingExchangeRecord {
  return {
    id: "00000000-0000-4000-8000-000000000002",
    magicLinkId: LINK_ID,
    nonceHash: "nonce",
    csrfHash: "csrf",
    expiresAt: new Date("2026-01-01T00:05:00Z"),
    consumedAt: null,
  };
}

describe("DrizzleAuthRepository bounded storage", () => {
  it("removes expired admission rows and does not persist denied requests", async () => {
    const fixture = admissionDatabase({ email: 5, ip: 5 });
    const repository = new DrizzleAuthRepository(fixture.database as never);

    for (let request = 0; request < 100; request += 1) {
      expect(await repository.admitMagicLinkRequest("email", "ip", SINCE, NOW, 5, 20)).toBe(false);
    }

    expect(fixture.removeExpired).toHaveBeenCalledTimes(100);
    expect(fixture.values).not.toHaveBeenCalled();
  });

  it("persists an admitted request after cleaning its retention window", async () => {
    const fixture = admissionDatabase({ email: 4, ip: 19 });
    const repository = new DrizzleAuthRepository(fixture.database as never);

    expect(await repository.admitMagicLinkRequest("email", "ip", SINCE, NOW, 5, 20)).toBe(true);
    expect(fixture.removeExpired).toHaveBeenCalledTimes(1);
    expect(fixture.values).toHaveBeenCalledTimes(1);
  });

  it("cleans terminal preparations and rejects a second live preparation", async () => {
    const removeTerminal = mock(async () => undefined);
    const returning = mock(async () => []);
    const onConflictDoNothing = mock(() => ({ returning }));
    const values = mock(() => ({ onConflictDoNothing }));
    const database = {
      delete: mock(() => ({ where: removeTerminal })),
      insert: mock(() => ({ values })),
    };
    const repository = new DrizzleAuthRepository({} as never);

    await expect(
      repository.createPendingExchange(exchangeRecord(), new TransactionHandle(database as never)),
    ).rejects.toThrow(/INVALID_OR_EXPIRED_LINK/);
    expect(removeTerminal).toHaveBeenCalledTimes(1);
    expect(onConflictDoNothing).toHaveBeenCalledTimes(1);
  });
});

const capabilityIntegrationEnabled = process.env.POSTGRES_CAPABILITY_INTEGRATION === "1";

async function capabilityDatabaseUrl(): Promise<string> {
  if (process.env.CAPABILITY_DATABASE_URL) {
    return process.env.CAPABILITY_DATABASE_URL;
  }
  const path = process.env.CAPABILITY_DATABASE_URL_FILE;
  if (!path) {
    throw new Error("CAPABILITY_DATABASE_URL or CAPABILITY_DATABASE_URL_FILE is required");
  }
  const value = (await readFile(path, "utf8")).trim();
  if (!value) {
    throw new Error("capability database URL is empty");
  }
  return value;
}

(capabilityIntegrationEnabled ? describe : describe.skip)(
  "dedicated capability publisher PostgreSQL contract",
  () => {
    it("publishes a fresh and duplicate revision without access to unrelated tables", async () => {
      const client = new Client({
        connectionString: await capabilityDatabaseUrl(),
      });
      await client.connect();
      try {
        const database = drizzle(client, { schema });
        const repository = new DrizzleLanguageRepository(database);
        const profileId = crypto.randomUUID();
        const capabilityHash = "a".repeat(64);
        const refresh = {
          profileId,
          revision: 1,
          profileName: `capability-contract-${profileId}`,
          capabilityHash,
          adapterBuilds: { translationWorker: "contract" },
          policy: { name: "contract" },
          credentialReferences: [],
          complete: true,
          rows: [
            {
              sourceLocale: "en-US",
              targetLocale: "en-US",
              mode: "same_language" as const,
              enabled: true,
              snapshot: {
                mode: "same_language",
                sourceLocale: "en-US",
                targetLocale: "en-US",
                stt: {
                  provider: "fixture",
                  endpoint: "fixture://stt",
                  model: "deterministic",
                  encoding: "pcm_s16le",
                  limits: {},
                  region: "local",
                  adapterBuild: "contract",
                },
              },
              capabilityHash,
              freshUntil: new Date("2027-01-01T00:00:00.000Z"),
            },
          ],
        };
        const service = new LanguageService(
          repository,
          { now: () => new Date("2026-01-01T00:00:00.000Z") },
          { uuid: () => crypto.randomUUID() },
        );

        await service.publishRevision(refresh);
        await service.publishRevision({
          ...refresh,
          rows: refresh.rows.map((row) => ({
            ...row,
            freshUntil: new Date("2027-02-01T00:00:00.000Z"),
          })),
        });

        const persisted = await client.query<{
          revisions: string;
          capabilities: string;
          fresh_until: Date;
        }>(
          `SELECT
             (SELECT count(*)::text FROM provider_profile_revisions WHERE profile_id = $1) AS revisions,
             count(*)::text AS capabilities,
             max(fresh_until) AS fresh_until
           FROM language_capabilities
           WHERE profile_id = $1`,
          [profileId],
        );
        expect(persisted.rows[0]).toEqual({
          revisions: "1",
          capabilities: "1",
          fresh_until: new Date("2027-02-01T00:00:00.000Z"),
        });
        await expect(client.query("SELECT id FROM users LIMIT 1")).rejects.toMatchObject({
          code: "42501",
        });
      } finally {
        await client.end();
      }
    });
  },
);

const migrationIntegrationEnabled = process.env.POSTGRES_CONCURRENCY_INTEGRATION === "1";
const MIGRATION_LOCK_NAMESPACE = 0x5452414e;
const MIGRATION_LOCK_ID = 0x53484f4f;

async function integrationDatabaseUrl(): Promise<string> {
  if (process.env.DATABASE_URL) {
    return process.env.DATABASE_URL;
  }
  const path = process.env.DATABASE_URL_FILE;
  if (!path) {
    throw new Error("DATABASE_URL or DATABASE_URL_FILE is required");
  }
  const value = (await readFile(path, "utf8")).trim();
  if (!value) {
    throw new Error("database URL is empty");
  }
  return value;
}

async function waitForQueuedMigrators(client: Client): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    const result = await client.query<{ waiting: string }>(
      `SELECT count(*)::text AS waiting
       FROM pg_locks lock
       JOIN pg_stat_activity activity ON activity.pid = lock.pid
       WHERE lock.locktype = 'advisory'
         AND lock.classid = $1::oid
         AND lock.objid = $2::oid
         AND lock.database = (
           SELECT oid FROM pg_database WHERE datname = current_database()
         )
         AND NOT lock.granted
         AND activity.application_name = 'transhooter-migrator'`,
      [MIGRATION_LOCK_NAMESPACE, MIGRATION_LOCK_ID],
    );
    if (result.rows[0]?.waiting === "2") {
      return;
    }
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  throw new Error("two migrators did not queue behind the advisory lock");
}

async function migratorResult(subprocess: Bun.Subprocess<"ignore", "pipe", "pipe">): Promise<{
  exitCode: number;
  output: string;
}> {
  const [exitCode, stdout, stderr] = await Promise.all([
    subprocess.exited,
    new Response(subprocess.stdout).text(),
    new Response(subprocess.stderr).text(),
  ]);
  return { exitCode, output: `${stdout}${stderr}` };
}

(migrationIntegrationEnabled ? describe : describe.skip)(
  "PostgreSQL migration and terminal settlement integration",
  () => {
    it("serializes concurrent migrators and atomically settles an exchange once", async () => {
      const connectionString = await integrationDatabaseUrl();
      const database = new Client({ connectionString });
      let databaseConnected = false;
      const migrators: Bun.Subprocess<"ignore", "pipe", "pipe">[] = [];
      const temporaryDirectory = await mkdtemp(join(tmpdir(), "transhooter-migrations-"));
      const secretFile = join(temporaryDirectory, "database-url");
      await writeFile(secretFile, `${connectionString}\n`, { mode: 0o600 });

      try {
        await database.connect();
        databaseConnected = true;
        const ownership = await database.query<{
          current_user: string;
          database_name: string;
          database_owner: string;
          public_schema_owner: string;
          history_schema_owner: string;
          runtime_can_connect: boolean;
          user_tables: string;
        }>(
          `SELECT
             current_user,
             current_database() AS database_name,
             pg_get_userbyid(database.datdba) AS database_owner,
             pg_get_userbyid(public_schema.nspowner) AS public_schema_owner,
             pg_get_userbyid(history_schema.nspowner) AS history_schema_owner,
             (
               has_database_privilege('transhooter_web', current_database(), 'CONNECT')
               OR has_database_privilege('transhooter_control', current_database(), 'CONNECT')
               OR has_database_privilege('transhooter_translation', current_database(), 'CONNECT')
               OR has_database_privilege('transhooter_capability', current_database(), 'CONNECT')
             ) AS runtime_can_connect,
             (
               SELECT count(*)::text
               FROM information_schema.tables
               WHERE table_schema IN ('public', 'drizzle')
             ) AS user_tables
           FROM pg_database AS database
           JOIN pg_namespace AS public_schema ON public_schema.nspname = 'public'
           JOIN pg_namespace AS history_schema ON history_schema.nspname = 'drizzle'
           WHERE database.datname = current_database()`,
        );
        expect(ownership.rows).toEqual([
          {
            current_user: "transhooter_migrator",
            database_name: "transhooter_integration",
            database_owner: "transhooter_migrator",
            public_schema_owner: "transhooter_migrator",
            history_schema_owner: "transhooter_migrator",
            runtime_can_connect: false,
            user_tables: "0",
          },
        ]);
        const clean = await database.query<{ history: string | null }>(
          "SELECT to_regclass('drizzle.__drizzle_migrations')::text AS history",
        );
        expect(clean.rows[0]?.history).toBeNull();

        await database.query("SELECT pg_advisory_lock($1, $2)", [
          MIGRATION_LOCK_NAMESPACE,
          MIGRATION_LOCK_ID,
        ]);
        const environment = {
          ...process.env,
          DATABASE_URL_FILE: secretFile,
        };
        const first = Bun.spawn([process.execPath, "deploy/scripts/migrate.mjs"], {
          cwd: join(import.meta.dir, "../../.."),
          env: environment,
          stdout: "pipe",
          stderr: "pipe",
        });
        const second = Bun.spawn([process.execPath, "deploy/scripts/migrate.mjs"], {
          cwd: join(import.meta.dir, "../../.."),
          env: environment,
          stdout: "pipe",
          stderr: "pipe",
        });
        migrators.push(first, second);

        await waitForQueuedMigrators(database);
        const locks = await database.query<{ granted: boolean; count: string }>(
          `SELECT granted, count(*)::text AS count
           FROM pg_locks lock
           LEFT JOIN pg_stat_activity activity ON activity.pid = lock.pid
           WHERE lock.locktype = 'advisory'
             AND lock.classid = $1::oid
             AND lock.objid = $2::oid
             AND lock.database = (
               SELECT oid FROM pg_database WHERE datname = current_database()
             )
             AND (lock.pid = pg_backend_pid()
               OR activity.application_name = 'transhooter-migrator')
           GROUP BY granted
           ORDER BY granted`,
          [MIGRATION_LOCK_NAMESPACE, MIGRATION_LOCK_ID],
        );
        expect(locks.rows).toEqual([
          { granted: false, count: "2" },
          { granted: true, count: "1" },
        ]);
        await database.query("SELECT pg_advisory_unlock($1, $2)", [
          MIGRATION_LOCK_NAMESPACE,
          MIGRATION_LOCK_ID,
        ]);

        const migrationResults = await Promise.all([migratorResult(first), migratorResult(second)]);
        for (const result of migrationResults) {
          if (result.exitCode !== 0) {
            throw new Error(`migrator failed with exit ${result.exitCode}:\n${result.output}`);
          }
          expect(result.output).toContain("database migrations applied");
        }

        const journal = JSON.parse(
          await readFile(join(import.meta.dir, "../drizzle/meta/_journal.json"), "utf8"),
        ) as { entries: readonly unknown[] };
        expect(journal.entries).toHaveLength(1);
        const history = await database.query<{ total: string; unique_hashes: string }>(
          `SELECT count(*)::text AS total, count(DISTINCT hash)::text AS unique_hashes
           FROM drizzle.__drizzle_migrations`,
        );
        expect(history.rows[0]).toEqual({
          total: "1",
          unique_hashes: "1",
        });
        const migratedTables = await database.query<{ name: string }>(
          `SELECT table_name AS name
           FROM information_schema.tables
           WHERE table_schema = 'public'
             AND table_name IN ('magic_links', 'pending_exchanges', 'provider_attempts')
           ORDER BY table_name`,
        );
        expect(migratedTables.rows).toEqual([
          { name: "magic_links" },
          { name: "pending_exchanges" },
          { name: "provider_attempts" },
        ]);

        const linkId = "10000000-0000-4000-8000-000000000001";
        const exchangeId = "10000000-0000-4000-8000-000000000002";
        const settledAt = new Date("2026-01-01T00:00:00.000Z");
        await database.query(
          `INSERT INTO magic_links(
             id, purpose, token_hash, expires_at, consumed_at, revoked_at, created_at,
             sealed_raw_token, sealed_token_key_id
           ) VALUES ($1, 'sign_in', $2, $3, NULL, NULL, $4, $5, $6)`,
          [
            linkId,
            `token-${exchangeId}`,
            new Date("2026-01-01T01:00:00.000Z"),
            settledAt,
            `sealed-${exchangeId}`,
            "integration-v1",
          ],
        );
        await database.query(
          `INSERT INTO pending_exchanges(
             id, magic_link_id, nonce_hash, csrf_hash, expires_at, consumed_at, created_at
           ) VALUES ($1, $2, $3, $4, $5, NULL, $6)`,
          [
            exchangeId,
            linkId,
            `nonce-${exchangeId}`,
            `csrf-${exchangeId}`,
            new Date("2026-01-01T00:05:00.000Z"),
            settledAt,
          ],
        );

        const leftClient = new Client({ connectionString });
        const rightClient = new Client({ connectionString });
        await Promise.all([leftClient.connect(), rightClient.connect()]);
        try {
          const left = new DrizzleAuthRepository(drizzle(leftClient));
          const right = new DrizzleAuthRepository(drizzle(rightClient));
          const settlements = await Promise.all([
            left.transaction((transaction) =>
              left.consumeExchangeAndLink(exchangeId, linkId, settledAt, transaction),
            ),
            right.transaction((transaction) =>
              right.consumeExchangeAndLink(exchangeId, linkId, settledAt, transaction),
            ),
          ]);
          expect(settlements.sort()).toEqual([false, true]);
        } finally {
          await Promise.all([leftClient.end(), rightClient.end()]);
        }

        const terminalRows = await database.query<{
          exchange_terminal_rows: string;
          link_terminal_rows: string;
          exchange_consumed_at: Date;
          link_consumed_at: Date;
        }>(
          `SELECT
             count(*) FILTER (
               WHERE exchange.id = $1 AND exchange.consumed_at IS NOT NULL
             )::text AS exchange_terminal_rows,
             count(*) FILTER (
               WHERE link.id = $2 AND link.consumed_at IS NOT NULL
             )::text AS link_terminal_rows,
             max(exchange.consumed_at) AS exchange_consumed_at,
             max(link.consumed_at) AS link_consumed_at
           FROM pending_exchanges exchange
           JOIN magic_links link ON link.id = exchange.magic_link_id
           WHERE exchange.id = $1 AND link.id = $2`,
          [exchangeId, linkId],
        );
        expect(terminalRows.rows[0]).toEqual({
          exchange_terminal_rows: "1",
          link_terminal_rows: "1",
          exchange_consumed_at: settledAt,
          link_consumed_at: settledAt,
        });
      } finally {
        for (const migrator of migrators) {
          if (migrator.exitCode === null) {
            migrator.kill();
          }
        }
        if (databaseConnected) {
          try {
            await database.query("SELECT pg_advisory_unlock($1, $2)", [
              MIGRATION_LOCK_NAMESPACE,
              MIGRATION_LOCK_ID,
            ]);
          } finally {
            await database.end();
          }
        }
        await rm(temporaryDirectory, { recursive: true, force: true });
      }
    });
  },
);
