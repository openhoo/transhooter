import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { drizzle } from "drizzle-orm/node-postgres";
import { migrate } from "drizzle-orm/node-postgres/migrator";
import pg from "pg";

const migrationsFolder = resolve("packages/server-core/drizzle");

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
  const pool = new pg.Pool({ connectionString, max: 1 });

  try {
    const database = drizzle(pool);
    await migrate(database, { migrationsFolder });
    console.log("database migrations applied");
  } finally {
    await pool.end();
  }
}

await runMigrations();
