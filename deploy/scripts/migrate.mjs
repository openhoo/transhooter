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
const migrationsSchema = "drizzle";
const migrationsTable = "__drizzle_migrations";

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
  const client = new pg.Client({
    connectionString,
    application_name: "transhooter-migrator",
  });
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

    const privileges = await client.query(
      `
      SELECT
        current_user = 'transhooter_migrator' AS is_migrator,
        current_schema() = 'public' AS uses_public_schema,
        has_database_privilege(current_user, current_database(), 'CONNECT') AS can_connect,
        has_database_privilege(current_user, current_database(), 'CREATE') AS can_create_schema,
        pg_get_userbyid(public_schema.nspowner) = current_user AS owns_public_schema,
        pg_get_userbyid(history_schema.nspowner) = current_user AS owns_history_schema
      FROM pg_namespace AS public_schema
      JOIN pg_namespace AS history_schema ON history_schema.nspname = $1
      WHERE public_schema.nspname = 'public'
    `,
      [migrationsSchema],
    );
    const privilege = privileges.rows[0];
    if (
      !privilege?.is_migrator ||
      !privilege.uses_public_schema ||
      !privilege.can_connect ||
      !privilege.can_create_schema ||
      !privilege.owns_public_schema ||
      !privilege.owns_history_schema
    ) {
      throw new Error(
        "database migrator role is not bootstrapped with the required ownership and privileges",
      );
    }

    const database = drizzle(client);
    await migrate(database, { migrationsFolder, migrationsSchema, migrationsTable });
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
