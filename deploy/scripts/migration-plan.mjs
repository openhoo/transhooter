export const baselineMigration = "0000_baseline";

export function planMigration({ hasPrismaHistory, hasPublicObjects, drizzleBaseline }) {
  if (drizzleBaseline.exists && !drizzleBaseline.proven) {
    throw new Error("refusing to migrate database with unrecognized Drizzle migration history");
  }
  if (hasPrismaHistory) {
    return drizzleBaseline.proven
      ? ["inspect-prisma-history", "remove-drizzle-history", "deploy"]
      : ["inspect-prisma-history", "deploy"];
  }
  if (hasPublicObjects) {
    if (!drizzleBaseline.proven) {
      throw new Error(
        "refusing to baseline an untracked nonempty public schema; expected the exact Drizzle baseline history",
      );
    }
    return ["resolve-baseline", "deploy", "remove-drizzle-history"];
  }
  if (drizzleBaseline.exists) {
    throw new Error("refusing to adopt Drizzle history for an empty public schema");
  }
  return ["deploy"];
}
