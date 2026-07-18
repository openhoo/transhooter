import { z } from "zod";
import { CONTRACT_SCHEMAS } from "../src/index";

function sortJson(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortJson);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
        .map(([key, nested]) => [key, sortJson(nested)]),
    );
  }
  return value;
}

export function generatedSchemaText(): string {
  const schemas = Object.fromEntries(
    Object.entries(CONTRACT_SCHEMAS).map(([name, schema]) => [
      name,
      z.toJSONSchema(schema, { target: "draft-2020-12", unrepresentable: "any" }),
    ]),
  );
  return `${JSON.stringify(
    sortJson({
      $schema: "https://json-schema.org/draft/2020-12/schema",
      package: "@transhooter/contracts",
      schemas,
    }),
    null,
    2,
  )}\n`;
}
