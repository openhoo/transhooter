from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class QuotaDimension:
    provider: str
    account: str
    region: str
    stage: str
    dimension: str
    capacity: int
    amount: int
    window_ms: int

    @property
    def key(self) -> str:
        return f"quota:{self.provider}:{self.account}:{self.region}:{self.stage}:{self.dimension}"


class QuotaLimiter(Protocol):
    async def reserve(
        self, dimensions: tuple[QuotaDimension, ...], reservation_id: str, ttl_ms: int
    ) -> bool: ...
    async def release(self, reservation_id: str) -> None: ...


class AdmissionQuota:
    """Reserves all frozen dimensions atomically with mandatory 20% headroom."""

    def __init__(self, limiter: QuotaLimiter) -> None:
        self._limiter = limiter

    async def admit(
        self, dimensions: tuple[QuotaDimension, ...], reservation_id: str, ttl_ms: int
    ) -> None:
        if not dimensions:
            raise ValueError("provider profile declares no quota dimensions")
        for dimension in dimensions:
            if (
                dimension.amount <= 0
                or dimension.capacity <= 0
                or dimension.amount > int(dimension.capacity * 0.8)
            ):
                raise RuntimeError(f"quota dimension has insufficient headroom: {dimension.key}")
        if not await self._limiter.reserve(dimensions, reservation_id, ttl_ms):
            raise RuntimeError("provider quota reservation rejected")
