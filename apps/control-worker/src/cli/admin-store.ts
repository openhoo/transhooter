import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createPrismaDatabase, Prisma } from "@transhooter/server-core/persistence";

export async function databaseUrlFromEnvironment(environment: NodeJS.ProcessEnv): Promise<string> {
  const path = environment.DATABASE_URL_FILE;
  if (path === undefined || path.length === 0) {
    throw new Error("DATABASE_URL_FILE is required");
  }

  const value = (await readFile(path, "utf8")).trim();
  if (value.length === 0) {
    throw new Error("database URL secret is empty");
  }

  const protocol = new URL(value).protocol;
  if (protocol !== "postgres:" && protocol !== "postgresql:") {
    throw new Error("database URL must use PostgreSQL");
  }

  return value;
}

interface CreateStaffInput {
  readonly email: string;
  readonly displayName: string;
  readonly role: "employee" | "admin";
}

interface CreateStaffResult {
  readonly id: string;
  readonly created: boolean;
}

export async function createStaff(
  databaseUrl: string,
  input: CreateStaffInput,
): Promise<CreateStaffResult> {
  const database = createPrismaDatabase({
    connectionString: databaseUrl,
    pool: { max: 1 },
  });
  try {
    const normalizedEmail = input.email.trim().normalize("NFKC").toLocaleLowerCase("en-US");
    const id = randomUUID();
    const rows = await database.client.$queryRaw<CreateStaffResult[]>(Prisma.sql`
      INSERT INTO users(id,email,display_name,staff_role,created_at)
      VALUES (${id},${normalizedEmail},${input.displayName.trim()},${input.role},now())
      ON CONFLICT (email) DO UPDATE
      SET display_name=EXCLUDED.display_name,staff_role=EXCLUDED.staff_role
      RETURNING id,(xmax::text='0') AS created
    `);
    const result = rows[0];
    if (result === undefined) {
      throw new Error("staff upsert returned no row");
    }

    return {
      id: result.id,
      created: result.created,
    };
  } finally {
    await database.disconnect();
  }
}

interface SetLanguageInput {
  readonly profileId: string;
  readonly revision: number;
  readonly source: string;
  readonly target: string;
  readonly enabled: boolean;
}

export async function setLanguage(databaseUrl: string, input: SetLanguageInput): Promise<void> {
  const database = createPrismaDatabase({
    connectionString: databaseUrl,
    pool: { max: 1 },
  });
  try {
    const result = await database.client.$executeRaw(Prisma.sql`
      WITH candidate AS (
        SELECT capability.id
        FROM language_capabilities AS capability
        JOIN provider_profiles AS profile
          ON profile.id=capability.profile_id
         AND profile.current_revision=capability.revision
        WHERE capability.profile_id=${input.profileId}
          AND capability.revision=${input.revision}
          AND capability.source_locale=${input.source}
          AND capability.target_locale=${input.target}
      )
      UPDATE language_capabilities
      SET enabled=${input.enabled}
      WHERE id=(
        SELECT id
        FROM candidate
        WHERE (SELECT count(*) FROM candidate)=1
      )
    `);
    if (result !== 1) {
      throw new Error("language capability revision is stale or does not exist");
    }
  } finally {
    await database.disconnect();
  }
}
