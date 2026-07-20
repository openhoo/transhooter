import { parseLongOptions } from "./long-options";

const createStaffOptions = ["email", "name", "role"] as const;
const setLanguageOptions = ["profile", "revision", "source", "target", "enabled"] as const;

export function parseCreateStaffOptions(
  argumentsList: readonly string[],
): Record<(typeof createStaffOptions)[number], string | undefined> {
  return parseLongOptions(argumentsList, createStaffOptions);
}

export function parseSetLanguageOptions(
  argumentsList: readonly string[],
): Record<(typeof setLanguageOptions)[number], string | undefined> {
  return parseLongOptions(argumentsList, setLanguageOptions);
}
