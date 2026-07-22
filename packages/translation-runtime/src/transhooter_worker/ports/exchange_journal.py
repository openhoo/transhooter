from __future__ import annotations

from typing import Protocol
from uuid import UUID

from transhooter_worker.domain.models import RawRef, SampleRange


class ExchangeJournal(Protocol):
    def append(
        self,
        *,
        meeting_id: UUID,
        attempt_id: UUID,
        stage: str,
        transport: str,
        direction: str,
        media_type: str,
        payload: bytes,
        sample_range: SampleRange | None = None,
        metadata: tuple[tuple[str, str], ...] = (),
    ) -> RawRef: ...

    def terminal(self, attempt_id: UUID, payload: bytes) -> RawRef: ...


class PublicationJournal(Protocol):
    def frame(
        self,
        meeting_id: UUID,
        publication_id: UUID,
        destination_id: UUID,
        sequence: int,
        pcm: bytes,
        samples: SampleRange,
    ) -> RawRef: ...
