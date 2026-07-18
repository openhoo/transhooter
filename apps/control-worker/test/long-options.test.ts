import { test } from "bun:test";
import assert from "node:assert/strict";
import { parseLongOptions } from "../src/cli/long-options";

test("long options preserve the existing last-wins parsing contract", () => {
  assert.deepEqual(
    parseLongOptions([
      "positional",
      "--email",
      "first@example.test",
      "--unknown",
      "ignored-by-schema",
      "--email",
      "last@example.test",
    ]),
    {
      email: "last@example.test",
      unknown: "ignored-by-schema",
    },
  );
});

test("a missing long-option value remains undefined", () => {
  assert.deepEqual(parseLongOptions(["--enabled"]), { enabled: undefined });
});
