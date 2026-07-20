import { test } from "bun:test";
import assert from "node:assert/strict";
import {
  createExclusiveActionGate,
  createWithDeviceFallback,
  isUnavailableSelectedDeviceError,
  persistDevicePreference,
  readDevicePreference,
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

void test("blocked session storage does not prevent device preference use", () => {
  const blockedStorage = () => {
    throw new DOMException("blocked", "SecurityError");
  };
  assert.equal(readDevicePreference(blockedStorage, "transhooter.microphone"), undefined);
  assert.doesNotThrow(() => {
    persistDevicePreference(blockedStorage, "transhooter.microphone", "microphone");
    persistDevicePreference(blockedStorage, "transhooter.microphone", "");
  });
});

void test("missing selected devices retry with the browser default and forget stale IDs", async () => {
  const attempts: Array<string | undefined> = [];
  let forgotSelection = false;
  const track = {};

  const result = await createWithDeviceFallback(
    "removed-camera",
    (deviceId) => {
      attempts.push(deviceId);
      if (deviceId) {
        return Promise.reject(new DOMException("missing", "NotFoundError"));
      }
      return Promise.resolve(track);
    },
    () => {
      forgotSelection = true;
    },
  );

  assert.equal(result, track);
  assert.deepEqual(attempts, ["removed-camera", undefined]);
  assert.equal(forgotSelection, true);
});

void test("only missing or overconstrained selected devices are recoverable", () => {
  assert.equal(
    isUnavailableSelectedDeviceError(new DOMException("missing", "NotFoundError")),
    true,
  );
  assert.equal(
    isUnavailableSelectedDeviceError(new DOMException("invalid", "OverconstrainedError")),
    true,
  );
  assert.equal(
    isUnavailableSelectedDeviceError(new DOMException("denied", "NotAllowedError")),
    false,
  );
});

void test("explicit media permission denial remains an error without a default retry", async () => {
  const attempts: Array<string | undefined> = [];
  const denial = new DOMException("denied", "NotAllowedError");

  await assert.rejects(
    createWithDeviceFallback(
      "camera",
      (deviceId) => {
        attempts.push(deviceId);
        return Promise.reject(denial);
      },
      () => {
        assert.fail("permission denial must not clear the selected device");
      },
    ),
    denial,
  );
  assert.deepEqual(attempts, ["camera"]);
});

void test("language mutations remain single-flight until the active request settles", () => {
  const gate = createExclusiveActionGate();

  assert.equal(gate.tryEnter(), true);
  assert.equal(gate.tryEnter(), false);
  gate.leave();
  assert.equal(gate.tryEnter(), true);
});
