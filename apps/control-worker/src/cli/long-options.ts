export function parseLongOptions(
  argumentsList: readonly string[],
): Record<string, string | undefined> {
  const entries = argumentsList.flatMap((value, index) => {
    if (!value.startsWith("--")) {
      return [];
    }

    return [[value.slice(2), argumentsList[index + 1]] as const];
  });

  return Object.fromEntries(entries);
}
