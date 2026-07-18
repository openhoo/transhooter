from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from transhooter_worker.domain.models import RawRef, SampleRange


@dataclass(frozen=True, slots=True)
class ObjectRecord:
    object_id: str
    key: str
    version_id: str
    size: int
    sha256: str
    s3_checksum: str
    content_type: str


class SpoolRecords(Protocol):
    def committed(
        self,
        stage: str | None = None,
    ) -> list[
        tuple[
            RawRef,
            SampleRange | None,
        ]
    ]: ...

    def committed_scoped(
        self,
        meeting_id: UUID,
        stage: str,
        direction: str,
        include_uploaded: bool = False,
    ) -> list[
        tuple[
            RawRef,
            SampleRange | None,
        ]
    ]: ...

    def pcm_scopes(
        self,
        include_uploaded: bool = False,
    ) -> list[
        tuple[
            UUID,
            str,
            str,
        ]
    ]: ...

    def covering_checkpoint(
        self,
        meeting_id: UUID,
        stage: str,
        direction: str,
        sample_end: int,
        terminal_only: bool = False,
    ) -> UUID | None: ...

    def read(
        self,
        object_id: UUID,
    ) -> bytes: ...

    def mark_uploaded(
        self,
        object_id: UUID,
        version_id: str,
        checksum: str,
    ) -> None: ...

    def checkpoint_covers(
        self,
        checkpoint_id: UUID,
        stage: str,
        direction: str,
        sample_end: int,
    ) -> bool: ...


class ArchiveStore(Protocol):
    def put_create_once(
        self,
        key: str,
        body: bytes,
        content_type: str,
        sha256: str,
    ) -> ObjectRecord: ...

    def verify(
        self,
        record: ObjectRecord,
    ) -> bool: ...
