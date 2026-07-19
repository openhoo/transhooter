import { test } from "bun:test";
import assert from "node:assert/strict";
import {
  createExclusiveActionGate,
  persistDevicePreference,
} from "../components/interface-state.ts";

void test("system-default devices clear consultation session preferences", () => {
  const values = new Map<string, string>([
    ["transhooter.microphone", "old-microphone"],
    ["transhooter.camera", "old-camera"],
  ]);
  const storage = {
    removeItem(key: string) {
      values.delete(key);
    },
    setItem(key: string, value: string) {
      values.set(key, value);
    },
  };

  persistDevicePreference(storage, "transhooter.microphone", "");
  persistDevicePreference(storage, "transhooter.camera", "new-camera");

  assert.equal(values.has("transhooter.microphone"), false);
  assert.equal(values.get("transhooter.camera"), "new-camera");
});

void test("language mutations remain single-flight until the active request settles", () => {
  const gate = createExclusiveActionGate();

  assert.equal(gate.tryEnter(), true);
  assert.equal(gate.tryEnter(), false);
  gate.leave();
  assert.equal(gate.tryEnter(), true);
});
