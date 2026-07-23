import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(new URL("../../packages/server-core/package.json", import.meta.url));
const pg = require("pg");
const prismaCli = resolve(require.resolve("prisma/package.json"), "../build/index.js");

const workspaceRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const prismaConfig = resolve(workspaceRoot, "packages/server-core/prisma.config.ts");
const migrationLockNamespace = 0x5452414e;
const migrationLockId = 0x53484f4f;
const baselineMigration = "0000_baseline";

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

async function runPrisma(args, connectionString) {
  await new Promise((resolvePromise, reject) => {
    const child = spawn(process.execPath, [prismaCli, ...args, "--config", prismaConfig], {
      cwd: workspaceRoot,
      env: { ...process.env, DATABASE_URL_FILE: "", DATABASE_URL: connectionString },
      stdio: "inherit",
    });
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (code === 0) {
        resolvePromise();
        return;
      }
      reject(
        new Error(
          signal
            ? `Prisma Migrate terminated by signal ${signal}`
            : `Prisma Migrate exited with status ${code}`,
        ),
      );
    });
  });
}

async function assertMigratorPreflight(client) {
  const result = await client.query(`
    SELECT
      current_user = 'transhooter_migrator' AS is_migrator,
      current_schema() = 'public' AS uses_public_schema,
      has_database_privilege(current_user, current_database(), 'CONNECT') AS can_connect,
      has_database_privilege(current_user, current_database(), 'CREATE') AS can_create_schema,
      pg_get_userbyid(public_schema.nspowner) = current_user AS owns_public_schema,
      NOT role.rolsuper AS is_not_superuser,
      NOT role.rolcreatedb AS cannot_create_database,
      NOT role.rolcreaterole AS cannot_create_role,
      NOT role.rolreplication AS cannot_replicate,
      NOT role.rolbypassrls AS cannot_bypass_rls,
      COALESCE(pg_get_userbyid(prisma_history.relowner) = current_user, true) AS owns_prisma_history
    FROM pg_namespace AS public_schema
    JOIN pg_roles AS role ON role.rolname = current_user
    LEFT JOIN pg_class AS prisma_history
      ON prisma_history.relnamespace = public_schema.oid
     AND prisma_history.relname = '_prisma_migrations'
     AND prisma_history.relkind = 'r'
    WHERE public_schema.nspname = 'public'
  `);
  const preflight = result.rows[0];
  if (!preflight || Object.values(preflight).some((value) => value !== true)) {
    throw new Error(
      "database migrator role is not bootstrapped with the required identity, ownership, and privileges",
    );
  }
}

async function publicSchemaIsNonempty(client) {
  const result = await client.query(`
    SELECT
      EXISTS (
        SELECT 1
        FROM pg_class AS object
        JOIN pg_namespace AS schema ON schema.oid = object.relnamespace
        WHERE schema.nspname = 'public'
          AND object.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
          AND object.relname <> '_prisma_migrations'
      ) OR EXISTS (
        SELECT 1
        FROM pg_type AS type
        JOIN pg_namespace AS schema ON schema.oid = type.typnamespace
        WHERE schema.nspname = 'public'
          AND type.typrelid = 0
          AND type.typtype IN ('d', 'e', 'r', 'm')
      ) OR EXISTS (
        SELECT 1
        FROM pg_proc AS function
        JOIN pg_namespace AS schema ON schema.oid = function.pronamespace
        WHERE schema.nspname = 'public'
      ) AS nonempty
  `);
  return result.rows[0]?.nonempty === true;
}

async function assertNoUntrackedSchemas(client) {
  const result = await client.query(`
    SELECT schema.nspname AS name
    FROM pg_namespace AS schema
    WHERE schema.nspname NOT IN ('public', 'information_schema')
      AND schema.nspname NOT LIKE 'pg\\_%' ESCAPE '\\'
      AND (
        EXISTS (SELECT 1 FROM pg_class AS object WHERE object.relnamespace = schema.oid)
        OR EXISTS (SELECT 1 FROM pg_proc AS function WHERE function.pronamespace = schema.oid)
        OR EXISTS (
          SELECT 1
          FROM pg_type AS type
          WHERE type.typnamespace = schema.oid
            AND type.typrelid = 0
            AND type.typtype IN ('d', 'e', 'r', 'm')
        )
      )
    ORDER BY schema.nspname
  `);
  if (result.rows.length > 0) {
    throw new Error(
      `refusing to migrate database with untracked nonempty schemas: ${result.rows
        .map((row) => row.name)
        .join(", ")}`,
    );
  }
}

async function prismaHistoryExists(client) {
  const result = await client.query(
    "SELECT to_regclass('public._prisma_migrations') IS NOT NULL AS exists",
  );
  return result.rows[0]?.exists === true;
}

async function inspectPrismaHistory(client) {
  const history = await client.query(`
    SELECT
      migration_name,
      finished_at IS NOT NULL AS finished,
      rolled_back_at IS NULL AS not_rolled_back,
      applied_steps_count
    FROM public._prisma_migrations
    ORDER BY started_at, id
  `);
  if (history.rows.length === 0) {
    throw new Error("refusing to migrate database with an empty Prisma migration history");
  }
  const baseline = history.rows[0];
  const appliedSteps = Number(baseline?.applied_steps_count);
  if (
    baseline?.migration_name !== baselineMigration ||
    baseline.finished !== true ||
    baseline.not_rolled_back !== true ||
    ![0, 1].includes(appliedSteps)
  ) {
    throw new Error("refusing to migrate database without the applied Prisma baseline");
  }
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

    await assertMigratorPreflight(client);
    await assertNoUntrackedSchemas(client);

    const [hasPrismaHistory, hasPublicObjects] = await Promise.all([
      prismaHistoryExists(client),
      publicSchemaIsNonempty(client),
    ]);
    if (hasPrismaHistory) {
      await inspectPrismaHistory(client);
    } else if (hasPublicObjects) {
      throw new Error("refusing to migrate an untracked nonempty public schema");
    }
    await runPrisma(["migrate", "deploy"], connectionString);
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

  if (operationError && cleanupError) {
    throw new AggregateError(
      [operationError, cleanupError],
      "database migration failed and cleanup did not complete",
    );
  }
  if (operationError) {
    throw operationError;
  }
  if (cleanupError) {
    throw cleanupError;
  }
}

await runMigrations();
