import type { Redis } from "ioredis";

const reserveScript = `
local reservations_key = KEYS[1]
local units_key = KEYS[2]
local reservation_id = ARGV[1]
local capacity = tonumber(ARGV[2])
local ttl_ms = tonumber(ARGV[3])
local units = tonumber(ARGV[4])
local time = redis.call('TIME')
local now_ms = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local expired = redis.call('ZRANGEBYSCORE', reservations_key, '-inf', now_ms)

if #expired > 0 then
  redis.call('ZREM', reservations_key, unpack(expired))
  redis.call('HDEL', units_key, unpack(expired))
end

local existing = redis.call('HGET', units_key, reservation_id)
if existing then
  redis.call('ZADD', reservations_key, now_ms + ttl_ms, reservation_id)
  return 1
end

local current = 0
for _, amount in ipairs(redis.call('HVALS', units_key)) do
  current = current + tonumber(amount)
end
if current + units > capacity then
  return 0
end

redis.call('HSET', units_key, reservation_id, units)
redis.call('ZADD', reservations_key, now_ms + ttl_ms, reservation_id)
return 1
`;

const renewScript = `
local reservations_key = KEYS[1]
local units_key = KEYS[2]
local reservation_id = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local time = redis.call('TIME')
local now_ms = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local expired = redis.call('ZRANGEBYSCORE', reservations_key, '-inf', now_ms)

if #expired > 0 then
  redis.call('ZREM', reservations_key, unpack(expired))
  redis.call('HDEL', units_key, unpack(expired))
end
if redis.call('HEXISTS', units_key, reservation_id) == 0 then
  return 0
end

redis.call('ZADD', reservations_key, now_ms + ttl_ms, reservation_id)
return 1
`;

const releaseScript = `
local reservations_key = KEYS[1]
local units_key = KEYS[2]
local reservation_id = ARGV[1]
redis.call('ZREM', reservations_key, reservation_id)
return redis.call('HDEL', units_key, reservation_id)
`;

export class RedisCoordination {
  constructor(
    private readonly redis: Redis,
    private readonly prefix = "transhooter:reservation:",
  ) {}

  async reserve(
    key: string,
    capacity: number,
    units: number,
    ttlMs: number,
    reservationId = key,
  ): Promise<boolean> {
    validateReservationLimits(capacity, units, ttlMs);
    const result = await this.redis.eval(
      reserveScript,
      2,
      this.reservationsKey(key),
      this.unitsKey(key),
      reservationId,
      capacity,
      ttlMs,
      units,
    );
    return Number(result) === 1;
  }

  async renew(key: string, ttlMs: number, reservationId = key): Promise<boolean> {
    validateTtl(ttlMs);
    const result = await this.redis.eval(
      renewScript,
      2,
      this.reservationsKey(key),
      this.unitsKey(key),
      reservationId,
      ttlMs,
    );
    return Number(result) === 1;
  }

  async release(key: string, reservationId = key): Promise<void> {
    await this.redis.eval(
      releaseScript,
      2,
      this.reservationsKey(key),
      this.unitsKey(key),
      reservationId,
    );
  }

  private reservationsKey(key: string): string {
    return `${this.prefix}expirations:${key}`;
  }

  private unitsKey(key: string): string {
    return `${this.prefix}units:${key}`;
  }
}

function validateReservationLimits(capacity: number, units: number, ttlMs: number): void {
  validateTtl(ttlMs);
  if (
    !Number.isInteger(capacity) ||
    capacity < 1 ||
    !Number.isInteger(units) ||
    units < 1 ||
    units > capacity
  ) {
    throw new Error("invalid reservation limits");
  }
}

function validateTtl(ttlMs: number): void {
  if (!Number.isInteger(ttlMs) || ttlMs < 1) {
    throw new Error("invalid reservation limits");
  }
}
