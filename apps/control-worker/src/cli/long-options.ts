import { parseArgs } from "node:util";

export function parseLongOptions<const Name extends string>(
  argumentsList: readonly string[],
  optionNames: readonly Name[],
): Record<Name, string | undefined> {
  const options = Object.fromEntries(
    optionNames.map((name) => [name, { type: "string" as const }]),
  );
  const { tokens, values } = parseArgs({
    args: [...argumentsList],
    options,
    strict: true,
    allowPositionals: false,
    tokens: true,
  });
  const seen = new Set<string>();

  for (const token of tokens) {
    if (token.kind !== "option") {
      continue;
    }
    if (token.inlineValue === true) {
      throw new TypeError(`Option '${token.rawName}' does not support an inline value`);
    }
    if (seen.has(token.name)) {
      throw new TypeError(`Option '${token.rawName}' may only be specified once`);
    }
    seen.add(token.name);
  }

  return values as Record<Name, string | undefined>;
}
