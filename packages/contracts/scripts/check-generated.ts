import { readFile } from "node:fs/promises";
import { generatedSchemaText } from "./schema-output";

const outputUrl = new URL("../generated/contracts.schema.json", import.meta.url);
const committed = await readFile(outputUrl, "utf8");
const generated = generatedSchemaText();
if (committed !== generated) {
  throw new Error("generated/contracts.schema.json is stale; run bun contracts:generate");
}
