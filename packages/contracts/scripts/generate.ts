import { mkdir, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { generatedSchemaText } from "./schema-output";

const outputUrl = new URL("../generated/contracts.schema.json", import.meta.url);
await mkdir(fileURLToPath(new URL("../generated/", import.meta.url)), { recursive: true });
await writeFile(outputUrl, generatedSchemaText(), "utf8");
