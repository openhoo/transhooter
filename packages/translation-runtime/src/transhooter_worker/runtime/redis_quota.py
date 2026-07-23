from __future__ import annotations

import asyncio
import secrets
from contextlib import suppress
from urllib.parse import unquote, urlparse

_AUDIO_SAMPLES_PER_SECOND = 16_000
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
    if dimension in {"characters_minute", "audio_seconds_minute"}:
        return amount
    return 1


def _capacity_units(dimension: str, capacity: int) -> int:
    limit = _headroom_limit(capacity)
    if dimension == "audio_seconds_minute":
        return limit * _AUDIO_SAMPLES_PER_SECOND
    return limit


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
local redis_time = redis.call('TIME')
local now = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local ttl = tonumber(ARGV[1])
local expiry = now + ttl
local owner = ARGV[2]

for index = 1, #KEYS do
    redis.call('ZREMRANGEBYSCORE', KEYS[index], '-inf', now)
    if not redis.call('ZSCORE', KEYS[index], owner)
        and redis.call('ZCARD', KEYS[index]) >= tonumber(ARGV[index + 2]) then
        return 0
    end
end

for index = 1, #KEYS do
    redis.call('ZADD', KEYS[index], expiry, owner)
    local latest = redis.call('ZRANGE', KEYS[index], -1, -1, 'WITHSCORES')
    redis.call('PEXPIRE', KEYS[index], math.ceil(tonumber(latest[2]) - now))
end
return 1
"""
    _ACTIVE_RELEASE_SCRIPT = """
local owner = ARGV[1]
for index = 1, #KEYS do
    redis.call('ZREM', KEYS[index], owner)
end
return 1
"""
    _WINDOW_RESERVATION_SCRIPT = """
local redis_time = redis.call('TIME')
local now = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local bucket = tostring(math.floor(now / 60000))
local expires_in = 60000 - (now % 60000)

for index = 1, #KEYS do
    local argument_offset = (index - 1) * 2
    local amount = tonumber(ARGV[argument_offset + 1])
    local limit = tonumber(ARGV[argument_offset + 2])
    local stored_bucket = redis.call('HGET', KEYS[index], 'bucket')
    local current = 0
    if stored_bucket == bucket then
        current = tonumber(redis.call('HGET', KEYS[index], 'amount') or '0')
    end
    if current + amount > limit then
        return 0
    end
end

for index = 1, #KEYS do
    local argument_offset = (index - 1) * 2
    local amount = tonumber(ARGV[argument_offset + 1])
    local stored_bucket = redis.call('HGET', KEYS[index], 'bucket')
    if stored_bucket == bucket then
        redis.call('HINCRBY', KEYS[index], 'amount', amount)
    else
        redis.call('HSET', KEYS[index], 'bucket', bucket, 'amount', amount)
    end
    redis.call('PEXPIRE', KEYS[index], expires_in)
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
        self._active_owner_tokens: dict[tuple[str, str], str] = {}
        self._connection_lock = asyncio.Lock()
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._closed = False

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

        keys = [
            _quota_key(
                self._provider,
                self._account,
                self._region,
                base_stage,
                dimension,
                "window-v2",
            )
            for dimension, _ in dimensions
        ]
        arguments: list[str] = []
        for dimension, capacity in dimensions:
            arguments.extend(
                (
                    str(_reserved_units(dimension, amount)),
                    str(_capacity_units(dimension, capacity)),
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
        lease_key = (stage, reservation_id)
        owner = self._active_owner_tokens.get(lease_key)
        created_owner = owner is None
        if owner is None:
            owner = secrets.token_urlsafe(32)
            self._active_owner_tokens[lease_key] = owner
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
                str(ttl_ms),
                owner,
                *(str(_headroom_limit(capacity)) for _, capacity in dimensions),
            ],
        )
        if not accepted and created_owner:
            self._active_owner_tokens.pop(lease_key, None)
        if not accepted:
            raise RuntimeError(f"provider active quota rejected: {stage}")

    async def release_active(self, stage: str, reservation_id: str) -> None:
        lease_key = (stage, reservation_id)
        owner = self._active_owner_tokens.get(lease_key)
        if owner is None:
            return
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
        if keys and await self._eval(self._ACTIVE_RELEASE_SCRIPT, keys, [owner]):
            self._active_owner_tokens.pop(lease_key, None)

    async def _eval(self, script: str, keys: list[str], arguments: list[str]) -> bool:
        return (
            await self._execute_redis_command("EVAL", script, str(len(keys)), *keys, *arguments)
            == 1
        )

    async def _execute_redis_command(self, *parts: str) -> int | str:
        async with self._connection_lock:
            if self._closed:
                raise RuntimeError("Redis quota gate is closed")
            try:
                reader, writer = await self._connection()
                return await self._send_command(reader, writer, *parts)
            except asyncio.CancelledError:
                await self._disconnect()
                raise
            except (ConnectionError, OSError, asyncio.IncompleteReadError):
                await self._disconnect()
                raise

    async def _connection(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        if self._reader is not None and self._writer is not None:
            return self._reader, self._writer

        reader, writer = await asyncio.open_connection(self._host, self._port)
        try:
            if self._password:
                await self._send_command(reader, writer, "AUTH", self._password)
        except BaseException:
            await self._close_writer(writer)
            raise
        self._reader = reader
        self._writer = writer
        return reader, writer

    async def aclose(self) -> None:
        async with self._connection_lock:
            self._closed = True
            await self._disconnect()

    async def _disconnect(self) -> None:
        writer = self._writer
        self._reader = None
        self._writer = None
        if writer is not None:
            await self._close_writer(writer)

    @staticmethod
    async def _close_writer(writer: asyncio.StreamWriter) -> None:
        writer.close()
        with suppress(Exception):
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
