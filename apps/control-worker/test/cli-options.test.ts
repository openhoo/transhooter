import { test } from "bun:test";
import assert from "node:assert/strict";
import { parseCreateStaffOptions, parseSetLanguageOptions } from "../src/cli/admin-options";

test("admin options preserve every documented command option", () => {
  assert.deepEqual(
    parseCreateStaffOptions([
      "--email",
      "person@example.test",
      "--name",
      "Example Person",
      "--role",
      "admin",
    ]),
    {
      email: "person@example.test",
      name: "Example Person",
      role: "admin",
    },
  );
  assert.deepEqual(
    parseSetLanguageOptions([
      "--profile",
      "profile-id",
      "--revision",
      "2",
      "--source",
      "en",
      "--target",
      "de",
      "--enabled",
      "true",
    ]),
    {
      profile: "profile-id",
      revision: "2",
      source: "en",
      target: "de",
      enabled: "true",
    },
  );
});

test("admin options reject unknown and repeated options", () => {
  assert.throws(() =>
    parseCreateStaffOptions(["--email", "person@example.test", "--unknown", "value"]),
  );
  assert.throws(() =>
    parseCreateStaffOptions(["--email", "first@example.test", "--email", "second@example.test"]),
  );
  assert.throws(() =>
    parseSetLanguageOptions(["--profile", "profile-id", "--email", "person@example.test"]),
  );
});

test("admin options reject inline and missing string values", () => {
  assert.throws(() => parseCreateStaffOptions(["--email=person@example.test"]));
  assert.throws(() => parseSetLanguageOptions(["--enabled"]));
  assert.throws(() => parseSetLanguageOptions(["--profile", "--revision", "2"]));
});
