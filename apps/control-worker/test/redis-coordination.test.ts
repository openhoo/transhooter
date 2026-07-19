import { test } from "bun:test";
import assert from "node:assert/strict";
import type { Redis } from "ioredis";
import { RedisCoordination } from "../src/adapters/redis-coordination";

test("one consultation reserves every provider quota dimension independently", async () => {
  const sttQuotaKey = "provider:google:stage:stt:dimension:sessions";
  const ttsQuotaKey = "provider:google:stage:tts:dimension:sessions";
  const consultationId = "50000000-0000-4000-8000-000000000001:1";
  const limit = 8;
  const amount = 2;
  const windowMs = 60_000;
  const calls: unknown[][] = [];
  const redis = {
    eval: async (...args: unknown[]) => {
      calls.push(args);
      return 1;
    },
  } as unknown as Redis;
  const coordination = new RedisCoordination(redis);

  await coordination.reserve(sttQuotaKey, limit, amount, windowMs, consultationId);
  await coordination.reserve(ttsQuotaKey, limit, amount, windowMs, consultationId);

  assert.equal(calls.length, 2);
  const firstCall = calls[0];
  const secondCall = calls[1];
  assert.ok(firstCall);
  assert.ok(secondCall);
  assert.notEqual(firstCall[3], secondCall[3]);
  assert.match(String(firstCall[3]), /stage:stt/);
  assert.match(String(secondCall[3]), /stage:tts/);
  assert.equal(firstCall[7], amount);
  assert.equal(secondCall[7], amount);
});

test("reservations expire independently and renewal prevents oversubscription", async () => {
  let now = 0;
  const expirations = new Map<string, number>();
  const units = new Map<string, number>();
  const redis = {
    eval: async (script: string, _keyCount: number, ...args: unknown[]) => {
      const reservationId = String(args[2]);
      for (const [id, expiresAt] of expirations) {
        if (expiresAt <= now) {
          expirations.delete(id);
          units.delete(id);
        }
      }
      if (script.includes("local capacity")) {
        const capacity = Number(args[3]);
        const ttlMs = Number(args[4]);
        const amount = Number(args[5]);
        if (units.has(reservationId)) {
          expirations.set(reservationId, now + ttlMs);
          return 1;
        }
        const reserved = [...units.values()].reduce((total, value) => total + value, 0);
        if (reserved + amount > capacity) {
          return 0;
        }
        units.set(reservationId, amount);
        expirations.set(reservationId, now + ttlMs);
        return 1;
      }
      if (script.includes("HEXISTS")) {
        const ttlMs = Number(args[3]);
        if (!units.has(reservationId)) {
          return 0;
        }
        expirations.set(reservationId, now + ttlMs);
        return 1;
      }
      expirations.delete(reservationId);
      return units.delete(reservationId) ? 1 : 0;
    },
  } as unknown as Redis;
  const coordination = new RedisCoordination(redis);

  assert.equal(await coordination.reserve("sessions", 2, 1, 100, "a"), true);
  assert.equal(await coordination.reserve("sessions", 2, 1, 200, "b"), true);
  now = 110;
  assert.equal(await coordination.renew("sessions", 200, "b"), true);
  assert.equal(await coordination.reserve("sessions", 2, 1, 100, "c"), true);
  assert.equal(await coordination.reserve("sessions", 2, 1, 100, "d"), false);
  now = 250;
  assert.equal(await coordination.reserve("sessions", 2, 1, 100, "d"), true);
  assert.equal(await coordination.reserve("sessions", 2, 1, 100, "e"), false);
});
