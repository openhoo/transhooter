import { z } from "zod";
import { createStaff, databaseUrlFromEnvironment } from "./admin-store";
import { parseLongOptions } from "./long-options";

const argumentSchema = z.object({
  email: z.email(),
  name: z.string().trim().min(1).max(200),
  role: z.enum(["employee", "admin"]),
});

const args = parseLongOptions(process.argv.slice(2));
const input = argumentSchema.parse(args);
const databaseUrl = await databaseUrlFromEnvironment(process.env);
const result = await createStaff(databaseUrl, {
  email: input.email,
  displayName: input.name,
  role: input.role,
});
console.log(
  JSON.stringify({
    id: result.id,
    created: result.created,
  }),
);
