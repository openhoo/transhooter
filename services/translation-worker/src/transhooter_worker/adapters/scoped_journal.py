from __future__ import annotations

from uuid import UUID

from transhooter_spool import ConsultationProducerAuthority, RawRef, SampleRange

from transhooter_worker.ports.exchange_journal import ProducerJournal


class ScopedExchangeJournal:
    """Prepends immutable admission scope to every provider wire object's metadata."""

    def __init__(
        self,
        delegate: ProducerJournal,
        authority: ConsultationProducerAuthority,
        scope: tuple[tuple[str, str], ...],
    ) -> None:
        self._delegate = delegate
        self._authority = authority
        self._scope = scope

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
    ) -> RawRef:
        return self._delegate.append(
            self._authority,
            meeting_id=meeting_id,
            attempt_id=attempt_id,
            stage=stage,
            transport=transport,
            direction=direction,
            media_type=media_type,
            payload=payload,
            sample_range=sample_range,
            metadata=self._scope + metadata,
        )

    def terminal(self, attempt_id: UUID, payload: bytes) -> RawRef:
        return self._delegate.terminal(self._authority, attempt_id, payload)
