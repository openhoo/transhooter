import { readFileSync } from "node:fs";
import { defineConfig } from "prisma/config";

const packageRoot = import.meta.dirname;

const OFFLINE_DATABASE_URL = "postgresql://offline:offline@127.0.0.1:1/offline";

function allowsOfflineDatabaseUrl(): boolean {
  const command = process.argv[2];
  return (command === "generate" && !process.argv.includes("--sql")) || command === "validate";
}

function databaseUrl(): string {
  const direct = process.env.DATABASE_URL?.trim();
  if (direct) {
    return direct;
  }

  const file = process.env.DATABASE_URL_FILE?.trim();
  if (!file) {
    if (allowsOfflineDatabaseUrl()) {
      return OFFLINE_DATABASE_URL;
    }
    throw new Error("DATABASE_URL or DATABASE_URL_FILE is required for Prisma database operations");
  }

  const value = readFileSync(file, "utf8").trim();
  if (!value) {
    throw new Error("DATABASE_URL_FILE is empty");
  }

  return value;
}

export default defineConfig({
  schema: `${packageRoot}/prisma/schema.prisma`,
  migrations: {
    path: `${packageRoot}/prisma/migrations`,
  },
  datasource: {
    url: databaseUrl(),
  },
});
