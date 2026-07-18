import { defineConfig } from "drizzle-kit";

export default defineConfig({
  dialect: "postgresql",
  schema: "./src/persistence/schema.ts",
  out: "./drizzle",
  strict: true,
  verbose: true,
});
