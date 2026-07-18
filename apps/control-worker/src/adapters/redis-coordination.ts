import type { Redis } from "ioredis";

const reserveScript = `
local quota_key = KEYS[1]
local token_key = KEYS[2]
local capacity = tonumber(ARGV[1])
local ttl_ms = ARGV[2]
local units = tonumber(ARGV[3])

if redis.call('EXISTS', token_key) == 1 then
  return 1
end

local current = tonumber(redis.call('GET', quota_key) or '0')
if current + units > capacity then
  return 0
end

redis.call('INCRBY', quota_key, units)
redis.call('PEXPIRE', quota_key, ttl_ms)
redis.call('SET', token_key, units, 'PX', ttl_ms)
return 1
`;

const releaseScript = `
local quota_key = KEYS[1]
local token_key = KEYS[2]
local units = tonumber(redis.call('GET', token_key) or '0')

if units == 0 then
  return 0
end

redis.call('DEL', token_key)
local current = tonumber(redis.call('GET', quota_key) or '0')
if current <= units then
  redis.call('DEL', quota_key)
  return 0
end

return redis.call('DECRBY', quota_key, units)
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
      this.quotaKey(key),
      this.tokenKey(key, reservationId),
      capacity,
      ttlMs,
      units,
    );
    return Number(result) === 1;
  }

  async release(key: string, reservationId = key): Promise<void> {
    await this.redis.eval(releaseScript, 2, this.quotaKey(key), this.tokenKey(key, reservationId));
  }

  private quotaKey(key: string): string {
    return `${this.prefix}quota:${key}`;
  }

  private tokenKey(key: string, reservationId: string): string {
    return `${this.prefix}token:${key}:${reservationId}`;
  }
}

function validateReservationLimits(capacity: number, units: number, ttlMs: number): void {
  const isValid =
    Number.isInteger(capacity) &&
    capacity >= 1 &&
    Number.isInteger(units) &&
    units >= 1 &&
    units <= capacity &&
    Number.isInteger(ttlMs) &&
    ttlMs >= 1;
  if (!isValid) {
    throw new Error("invalid reservation limits");
  }
}
