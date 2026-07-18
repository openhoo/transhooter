import { z } from "zod";
import { databaseUrlFromEnvironment, setLanguage } from "./admin-store";
import { parseLongOptions } from "./long-options";

const argumentSchema = z.object({
  profile: z.string().min(1),
  revision: z.coerce.number().int().positive(),
  source: z.string().min(2),
  target: z.string().min(2),
  enabled: z.enum(["true", "false"]).transform((value) => value === "true"),
});

const args = parseLongOptions(process.argv.slice(2));
const input = argumentSchema.parse(args);
const databaseUrl = await databaseUrlFromEnvironment(process.env);
await setLanguage(databaseUrl, {
  profileId: input.profile,
  revision: input.revision,
  source: input.source,
  target: input.target,
  enabled: input.enabled,
});
console.log(JSON.stringify({ updated: true }));
