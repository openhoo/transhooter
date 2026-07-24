from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, Protocol
from uuid import UUID

RecordState = Literal["committed", "uploaded", "permanent", "quarantined"]
DeliveryState = Literal["pending", "acknowledged", "permanent"]
CompletionState = Literal["pending", "acknowledged"]
HandoffState = Literal["active", "settling", "sealed", "relinquished"]
Metadata = tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class SampleRange:
    start: int
    end: int

    def __post_init__(self) -> None:
        if self.start < 0 or self.end <= self.start:
            raise ValueError("sample range must be non-empty and inclusive-exclusive")

    @property
    def length(self) -> int:
        return self.end - self.start


@dataclass(frozen=True, slots=True)
class RawRef:
    object_id: UUID
    ordinal: int
    sha256: str
    size: int
    media_type: str


@dataclass(frozen=True, slots=True)
class ObjectRecord:
    object_id: str
    key: str
    version_id: str
    size: int
    sha256: str
    s3_checksum: str
    content_type: str


@dataclass(frozen=True, slots=True)
class SpoolRecordContext:
    meeting_id: UUID
    attempt_id: UUID
    stage: str
    transport: str
    direction: str
    media_type: str
    ordinal: int
    generation: int
    worker_id: UUID
    worker_epoch: int
    write_epoch: int
    metadata: Metadata


@dataclass(frozen=True, slots=True)
class SpoolRecordDelivery:
    raw_ref: RawRef
    sample_range: SampleRange | None
    context: SpoolRecordContext
    state: RecordState
    version_id: str | None
    s3_checksum: str | None
    error_kind: str | None
    failed_at: datetime | None


@dataclass(frozen=True, slots=True)
class SpoolCheckpointDelivery:
    checkpoint_id: UUID
    record_id: UUID
    meeting_id: UUID
    generation: int
    worker_id: UUID
    worker_epoch: int
    write_epoch: int
    source_id: UUID
    checkpoint_hash: str
    previous_hash: str | None
    control_event_id: UUID
    object_key: str
    evidence_ordinal: int | None
    body: bytes
    raw_ref: RawRef
    delivery_state: DeliveryState
    error_kind: str | None
    failed_at: datetime | None


@dataclass(frozen=True, slots=True)
class TerminalCheckpointIntent:
    checkpoint_id: UUID
    source_id: UUID
    checkpoint_hash: str
    previous_hash: str | None
    control_event_id: UUID
    object_key: str
    body: bytes


@dataclass(frozen=True, slots=True)
class SpoolConsultationSeal:
    seal_id: UUID
    meeting_id: UUID
    generation: int
    worker_id: UUID
    worker_epoch: int
    write_epoch: int
    evidence_ordinal: int
    terminal_outcome: str
    completion_event_id: UUID
    failure: dict[str, object] | None
    first_checkpoint_id: UUID
    second_checkpoint_id: UUID
    completion_state: CompletionState
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
