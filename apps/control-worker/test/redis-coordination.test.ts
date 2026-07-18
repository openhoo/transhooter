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
  assert.equal(firstCall[6], amount);
  assert.equal(secondCall[6], amount);
});
