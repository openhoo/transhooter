from __future__ import annotations

import asyncio
import math
import time
from urllib.parse import unquote, urlparse

_MINUTE_MS = 60_000
_QUOTA_HEADROOM = 0.8
_ACTIVE_DIMENSIONS = {"streams", "sessions"}


def _headroom_limit(capacity: int) -> int:
    return max(1, int(capacity * _QUOTA_HEADROOM))


def _minute_dimensions(
    stage: str,
    is_start_reservation: bool,
    available_limits: dict[str, int],
) -> list[tuple[str, int]]:
    if is_start_reservation:
        return [
            (dimension, capacity)
            for dimension, capacity in available_limits.items()
            if dimension == "starts_minute"
        ]
    return [
        (dimension, capacity)
        for dimension, capacity in available_limits.items()
        if dimension.endswith("_minute") and not (stage == "stt" and dimension == "starts_minute")
    ]


def _reserved_units(dimension: str, amount: int) -> int:
    if dimension == "characters_minute":
        return amount
    if dimension == "audio_seconds_minute":
        return max(1, math.ceil(amount / 16_000))
    return 1


def _quota_key(
    provider: str,
    account: str,
    region: str,
    stage: str,
    dimension: str,
    bucket: str,
) -> str:
    return f"quota:{provider}:{account}:{region}:{stage}:{dimension}:{bucket}"


class RedisQuotaGate:
    _ACTIVE_RESERVATION_SCRIPT = """
local now = tonumber(ARGV[1])
local expiry = tonumber(ARGV[2])
local member = ARGV[3]

for index = 1, #KEYS do
    redis.call('ZREMRANGEBYSCORE', KEYS[index], '-inf', now)
    if not redis.call('ZSCORE', KEYS[index], member)
        and redis.call('ZCARD', KEYS[index]) >= tonumber(ARGV[index + 3]) then
        return 0
    end
end

for index = 1, #KEYS do
    redis.call('ZADD', KEYS[index], expiry, member)
    redis.call('PEXPIRE', KEYS[index], expiry - now)
end
return 1
"""
    _ACTIVE_RELEASE_SCRIPT = """
for index = 1, #KEYS do
    redis.call('ZREM', KEYS[index], ARGV[1])
end
return 1
"""
    _WINDOW_RESERVATION_SCRIPT = """
for index = 1, #KEYS do
    local argument_offset = (index - 1) * 3
    local amount = tonumber(ARGV[argument_offset + 1])
    local limit = tonumber(ARGV[argument_offset + 3])
    local current = tonumber(redis.call('GET', KEYS[index]) or '0')
    if current + amount > limit then
        return 0
    end
end

for index = 1, #KEYS do
    local argument_offset = (index - 1) * 3
    local amount = ARGV[argument_offset + 1]
    local expiry_ms = ARGV[argument_offset + 2]
    local total = redis.call('INCRBY', KEYS[index], amount)
    if total == tonumber(amount) then
        redis.call('PEXPIRE', KEYS[index], expiry_ms)
    end
end

return 1
"""

    def __init__(
        self,
        url: str,
        provider: str,
        account: str,
        region: str,
        limits: dict[str, dict[str, int]],
    ) -> None:
        parsed = urlparse(url)
        self._host = parsed.hostname or "redis"
        self._port = parsed.port or 6379
        self._password = unquote(parsed.password) if parsed.password else None
        self._provider = provider
        self._account = account
        self._region = region
        self._limits = limits

    async def __call__(self, stage: str, amount: int) -> None:
        is_start_reservation = stage.endswith("_start")
        base_stage = stage.removesuffix("_start")
        dimensions = _minute_dimensions(
            base_stage,
            is_start_reservation,
            self._limits.get(base_stage, {}),
        )
        if not dimensions:
            return

        minute_bucket = str(int(time.time() * 1000) // _MINUTE_MS)
        keys = [
            _quota_key(
                self._provider,
                self._account,
                self._region,
                base_stage,
                dimension,
                minute_bucket,
            )
            for dimension, _ in dimensions
        ]
        arguments: list[str] = []
        for dimension, capacity in dimensions:
            arguments.extend(
                (
                    str(_reserved_units(dimension, amount)),
                    str(_MINUTE_MS),
                    str(_headroom_limit(capacity)),
                )
            )
        if not await self._reserve(keys, arguments):
            raise RuntimeError(f"provider quota rejected: {base_stage}")

    async def reserve_active(
        self,
        stage: str,
        reservation_id: str,
        ttl_ms: int = 15_000,
    ) -> None:
        dimensions = [
            (dimension, capacity)
            for dimension, capacity in self._limits.get(stage, {}).items()
            if dimension in _ACTIVE_DIMENSIONS
        ]
        if not dimensions:
            return
        now_ms = int(time.time() * 1000)
        keys = [
            _quota_key(
                self._provider,
                self._account,
                self._region,
                stage,
                dimension,
                "active",
            )
            for dimension, _ in dimensions
        ]
        accepted = await self._eval(
            self._ACTIVE_RESERVATION_SCRIPT,
            keys,
            [
                str(now_ms),
                str(now_ms + ttl_ms),
                reservation_id,
                *(str(_headroom_limit(capacity)) for _, capacity in dimensions),
            ],
        )
        if not accepted:
            raise RuntimeError(f"provider active quota rejected: {stage}")

    async def release_active(self, stage: str, reservation_id: str) -> None:
        keys = [
            _quota_key(
                self._provider,
                self._account,
                self._region,
                stage,
                dimension,
                "active",
            )
            for dimension in self._limits.get(stage, {})
            if dimension in _ACTIVE_DIMENSIONS
        ]
        if keys:
            await self._eval(self._ACTIVE_RELEASE_SCRIPT, keys, [reservation_id])

    async def _eval(self, script: str, keys: list[str], arguments: list[str]) -> bool:
        return (
            await self._execute_redis_command("EVAL", script, str(len(keys)), *keys, *arguments)
            == 1
        )

    async def _execute_redis_command(self, *parts: str) -> int | str:
        reader, writer = await asyncio.open_connection(self._host, self._port)
        try:
            if self._password:
                await self._send_command(reader, writer, "AUTH", self._password)
            return await self._send_command(reader, writer, *parts)
        finally:
            writer.close()
            await writer.wait_closed()

    async def _reserve(self, keys: list[str], arguments: list[str]) -> bool:
        return await self._eval(self._WINDOW_RESERVATION_SCRIPT, keys, arguments)

    @staticmethod
    async def _send_command(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *parts: str,
    ) -> int | str:
        encoded_parts = [part.encode() for part in parts]
        payload = (
            b"*"
            + str(len(encoded_parts)).encode()
            + b"\r\n"
            + b"".join(
                b"$" + str(len(encoded_part)).encode() + b"\r\n" + encoded_part + b"\r\n"
                for encoded_part in encoded_parts
            )
        )
        writer.write(payload)
        await writer.drain()
        prefix = await reader.readexactly(1)
        line = await reader.readline()
        if prefix == b":":
            return int(line[:-2])
        if prefix == b"+":
            return line[:-2].decode()
        if prefix == b"-":
            raise RuntimeError("Redis quota command failed")
        raise RuntimeError("unsupported Redis quota response")
