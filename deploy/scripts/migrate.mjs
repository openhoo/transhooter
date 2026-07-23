import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { baselineMigration, planMigration } from "./migration-plan.mjs";

const require = createRequire(new URL("../../packages/server-core/package.json", import.meta.url));
const pg = require("pg");
const prismaCli = resolve(require.resolve("prisma/package.json"), "../build/index.js");

const workspaceRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const prismaConfig = resolve(workspaceRoot, "packages/server-core/prisma.config.ts");
const migrationLockNamespace = 0x5452414e;
const migrationLockId = 0x53484f4f;
const drizzleBaselineHash = "5aa4112ae489b2f93205314f84b58dc45aba4844a2ee1d7d5606736ba6bf9d8e";
const drizzleBaselineCreatedAt = "1784549760398";
const expectedDrizzleRelations = [
  "S:__drizzle_migrations_id_seq",
  "i:__drizzle_migrations_pkey",
  "r:__drizzle_migrations",
];

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
    WHERE schema.nspname NOT IN ('public', 'drizzle', 'information_schema')
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

async function inspectDrizzleBaseline(client) {
  const schemaResult = await client.query(`
    SELECT pg_get_userbyid(schema.nspowner) = current_user AS owned
    FROM pg_namespace AS schema
    WHERE schema.nspname = 'drizzle'
  `);
  if (schemaResult.rows.length === 0) {
    return { exists: false, proven: false };
  }
  if (schemaResult.rows[0]?.owned !== true) {
    return { exists: true, proven: false };
  }

  const relations = await client.query(`
    SELECT object.relkind || ':' || object.relname AS identity,
           pg_get_userbyid(object.relowner) = current_user AS owned
    FROM pg_class AS object
    JOIN pg_namespace AS schema ON schema.oid = object.relnamespace
    WHERE schema.nspname = 'drizzle'
    ORDER BY identity
  `);
  const relationIdentities = relations.rows.map((row) => row.identity);
  const relationsMatch =
    relationIdentities.length === expectedDrizzleRelations.length &&
    relationIdentities.every((identity, index) => identity === expectedDrizzleRelations[index]) &&
    relations.rows.every((row) => row.owned === true);
  if (!relationsMatch) {
    return { exists: true, proven: false };
  }

  const columns = await client.query(`
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_schema = 'drizzle'
      AND table_name = '__drizzle_migrations'
    ORDER BY ordinal_position
  `);
  const expectedColumns = [
    {
      column_name: "id",
      data_type: "integer",
      is_nullable: "NO",
      column_default: "nextval('drizzle.__drizzle_migrations_id_seq'::regclass)",
    },
    { column_name: "hash", data_type: "text", is_nullable: "NO", column_default: null },
    { column_name: "created_at", data_type: "bigint", is_nullable: "YES", column_default: null },
  ];
  const columnsMatch =
    columns.rows.length === expectedColumns.length &&
    columns.rows.every(
      (column, index) => JSON.stringify(column) === JSON.stringify(expectedColumns[index]),
    );
  if (!columnsMatch) {
    return { exists: true, proven: false };
  }
  const constraints = await client.query(`
    SELECT constraint_object.conname AS name,
           constraint_object.contype AS type,
           pg_get_constraintdef(constraint_object.oid) AS definition
    FROM pg_constraint AS constraint_object
    JOIN pg_class AS table_object ON table_object.oid = constraint_object.conrelid
    JOIN pg_namespace AS schema ON schema.oid = table_object.relnamespace
    WHERE schema.nspname = 'drizzle'
      AND table_object.relname = '__drizzle_migrations'
    ORDER BY constraint_object.conname
  `);
  if (
    constraints.rows.length !== 1 ||
    constraints.rows[0]?.name !== "__drizzle_migrations_pkey" ||
    constraints.rows[0]?.type !== "p" ||
    constraints.rows[0]?.definition !== "PRIMARY KEY (id)"
  ) {
    return { exists: true, proven: false };
  }

  const proof = await client.query(
    `
      SELECT
        count(*) = 1 AS has_one_entry,
        bool_and(id = 1 AND hash = $1 AND created_at = $2::bigint) AS is_exact_baseline,
        pg_get_serial_sequence('drizzle.__drizzle_migrations', 'id') =
          'drizzle.__drizzle_migrations_id_seq' AS has_expected_sequence
      FROM drizzle.__drizzle_migrations
    `,
    [drizzleBaselineHash, drizzleBaselineCreatedAt],
  );
  const row = proof.rows[0];
  return {
    exists: true,
    proven:
      row?.has_one_entry === true &&
      row.is_exact_baseline === true &&
      row.has_expected_sequence === true,
  };
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

async function removeDrizzleHistory(client) {
  await client.query("BEGIN");
  try {
    await client.query('DROP SCHEMA "drizzle" CASCADE');
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
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

    const hasPrismaHistory = await prismaHistoryExists(client);
    const [hasPublicObjects, drizzleBaseline] = await Promise.all([
      publicSchemaIsNonempty(client),
      inspectDrizzleBaseline(client),
    ]);
    const actions = planMigration({ hasPrismaHistory, hasPublicObjects, drizzleBaseline });
    for (const action of actions) {
      if (action === "inspect-prisma-history") {
        await inspectPrismaHistory(client);
      } else if (action === "resolve-baseline") {
        await runPrisma(["migrate", "resolve", "--applied", baselineMigration], connectionString);
      } else if (action === "remove-drizzle-history") {
        await removeDrizzleHistory(client);
      } else {
        await runPrisma(["migrate", "deploy"], connectionString);
      }
    }
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
