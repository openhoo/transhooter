import hashlib
from uuid import UUID, uuid4

import httpx
import pytest

from transhooter_worker.adapters.deepl.provider import DeepLConfig, DeepLProvider
from transhooter_worker.domain.models import RawRef, SampleRange, TranslationRequest


class MemoryJournal:
    def __init__(self) -> None:
        self.rows: list[tuple[str, bytes, tuple[tuple[str, str], ...]]] = []
        self.terminals: dict[UUID, RawRef] = {}

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
        self.rows.append((direction, payload, metadata))
        return RawRef(
            uuid4(),
            len(self.rows),
            hashlib.sha256(payload).hexdigest(),
            len(payload),
            media_type,
        )

    def terminal(self, attempt_id: UUID, payload: bytes) -> RawRef:
        if attempt_id not in self.terminals:
            self.terminals[attempt_id] = self.append(
                meeting_id=UUID(int=0),
                attempt_id=attempt_id,
                stage="terminal",
                transport="http",
                direction="terminal",
                media_type="application/json",
                payload=payload,
            )
        return self.terminals[attempt_id]


@pytest.mark.asyncio
async def test_deepl_journals_before_parse_and_requires_one_result() -> None:
    journal = MemoryJournal()

    async def handler(request: httpx.Request) -> httpx.Response:
        assert journal.rows[-1][0] == "out"
        return httpx.Response(
            200,
            headers=[("X-Trace-ID", "trace-1")],
            json={"translations": [{"text": "Guten Tag"}]},
        )

    config = DeepLConfig("secret", uuid4())
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = DeepLProvider(config, journal, client)
    request = TranslationRequest(
        uuid4(),
        uuid4(),
        "final",
        "EN",
        "DE",
        "Good day",
        SampleRange(0, 100),
    )
    outcome = await (await provider.start(request)).result()
    assert outcome.result is not None
    assert outcome.result.text == "Guten Tag"
    assert [row[0] for row in journal.rows[:2]] == ["out", "in"]
    assert b"secret" not in b"".join(row[1] for row in journal.rows)
    assert request.attempt_id in journal.terminals


@pytest.mark.asyncio
async def test_deepl_429_normalizes_without_adapter_retry() -> None:
    journal = MemoryJournal()

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            headers={"Retry-After": "1.5", "X-Trace-ID": "trace"},
            json={"message": "slow"},
        )

    config = DeepLConfig("secret", uuid4())
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = DeepLProvider(config, journal, client)
    request = TranslationRequest(
        uuid4(),
        uuid4(),
        "final",
        "EN",
        "DE",
        "x",
        SampleRange(0, 1),
    )
    outcome = await (await provider.start(request)).result()
    assert outcome.result is None
    assert outcome.terminal.error is not None
    assert outcome.terminal.error.retry_delay_ms == 1500
    assert outcome.terminal.accepted_input == 1
    assert outcome.terminal.received_output == 0
