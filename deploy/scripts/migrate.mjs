import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { resolve } from "node:path";

const require = createRequire(new URL("../../apps/web/package.json", import.meta.url));
const { drizzle } = require("drizzle-orm/node-postgres");
const { migrate } = require("drizzle-orm/node-postgres/migrator");
const pg = require("pg");

const migrationsFolder = resolve("packages/server-core/drizzle");
const migrationLockNamespace = 0x5452414e;
const migrationLockId = 0x53484f4f;

async function readConnectionString() {
  const databaseUrlFile = process.env.DATABASE_URL_FILE;
  if (!databaseUrlFile) {
    throw new Error("DATABASE_URL_FILE is required");
  }

  const connectionString = (await readFile(databaseUrlFile, "utf8")).trim();
  if (!connectionString) {
    throw new Error("database URL secret is empty");
  }
  return connectionString;
}

async function runMigrations() {
  const connectionString = await readConnectionString();
  const client = new pg.Client({ connectionString });
  let lockAcquired = false;
  let operationError;
  let cleanupError;
  try {
    await client.connect();
    await client.query("SELECT pg_advisory_lock($1, $2)", [
      migrationLockNamespace,
      migrationLockId,
    ]);
    lockAcquired = true;

    const database = drizzle(client);
    await migrate(database, { migrationsFolder });
    console.log("database migrations applied");
  } catch (error) {
    operationError = error;
  } finally {
    if (lockAcquired) {
      try {
        const result = await client.query("SELECT pg_advisory_unlock($1, $2) AS unlocked", [
          migrationLockNamespace,
          migrationLockId,
        ]);
        if (result.rows[0]?.unlocked !== true) {
          cleanupError = new Error("database migration advisory lock was not released");
        }
      } catch (error) {
        cleanupError = error;
      }
    }

    try {
      await client.end();
    } catch (error) {
      cleanupError ??= error;
    }
  }

  if (operationError) {
    throw operationError;
  }
  if (cleanupError) {
    throw cleanupError;
  }
}

await runMigrations();
