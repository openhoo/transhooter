from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from uuid import UUID

from transhooter_worker.domain.models import RawRef


@dataclass(frozen=True, slots=True)
class SpoolCheckpointDelivery:
    checkpoint_id: UUID
    meeting_id: UUID
    source_id: UUID
    worker_epoch: int
    checkpoint_hash: str
    previous_hash: str | None
    control_event_id: UUID
    acknowledged: bool
    body: bytes
    raw_ref: RawRef


@dataclass(frozen=True, slots=True)
class SpoolCapacity:
    total_bytes: int
    used_bytes: int


class CapacityProbe(Protocol):
    def __call__(self, path: Path, /) -> SpoolCapacity: ...


def statvfs_capacity(path: Path) -> SpoolCapacity:
    stats = os.statvfs(path)
    total = stats.f_blocks * stats.f_frsize
    used = total - stats.f_bavail * stats.f_frsize
    return SpoolCapacity(total_bytes=total, used_bytes=used)


def deterministic_roomy_capacity(_path: Path) -> SpoolCapacity:
    return SpoolCapacity(total_bytes=1 << 40, used_bytes=0)


class SpoolUnavailable(RuntimeError):
    pass
