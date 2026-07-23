import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from threading import Event, Lock, get_ident
from typing import Protocol
from uuid import UUID, uuid4

import httpx
import pytest

from transhooter_worker.adapters.fixture.provider import (
    FixtureSttProvider,
    FixtureSynthesisAttempt,
    FixtureTranslationProvider,
    FixtureTtsProvider,
    FixtureTtsSession,
)
from transhooter_worker.adapters.fixture.scenario import FixtureScenario
from transhooter_worker.adapters.spool import EncryptedSpool, deterministic_roomy_capacity
from transhooter_worker.application.session import DirectionSession, DirectionSpec
from transhooter_worker.domain.models import (
    AudioChunk,
    BoundaryReceipt,
    ErrorKind,
    OperationTerminal,
    OperationTerminalEvent,
    Outcome,
    ProviderError,
    ProviderHealth,
    RawRef,
    RetryAdvice,
    SampleRange,
    SessionTerminal,
    SessionTerminalEvent,
    StageCapabilities,
    SynthesisUtterance,
    Transport,
)
from transhooter_worker.ports.archive import ObjectRecord
from transhooter_worker.ports.providers import SttSession
from transhooter_worker.runtime.checkpoints import CheckpointChainState, _persist_checkpoint
from transhooter_worker.runtime.job import _drain_runtime
from transhooter_worker.runtime.provider_registry import Providers

REF = RawRef(UUID(int=1), 1, "0" * 64, 1, "application/json")


async def ignore_sink(*_: object) -> None:
    return None


class SpoolOperation(Protocol):
    def __call__(self, *args: object, **kwargs: object) -> object: ...


class CommittedOperation(Protocol):
    def __call__(self) -> list[tuple[RawRef, SampleRange | None]]: ...


def failed_terminal(session_id: UUID) -> SessionTerminal:
    error = ProviderError(
        kind=ErrorKind.TRANSPORT,
        scope="session",
        provider_retry_advice=RetryAdvice.UNSPECIFIED,
        provider_code="closed",
        provider_request_id=None,
        retry_delay_ms=None,
        attempt_id=session_id,
        raw_refs=(REF,),
        message="closed",
    )
    return SessionTerminal(
        terminal_id=uuid4(),
        session_id=session_id,
        outcome=Outcome.FAILED,
        error=error,
        accepted_input=0,
        received_output=0,
        emitted_output=0,
        transport=Transport.WEBSOCKET,
        raw_refs=(REF,),
    )


class ImmediatelyFailedSession:
    def __init__(self, session_id: UUID) -> None:
        self.terminal = failed_terminal(session_id)

    async def send_audio(self, chunk: AudioChunk) -> None:
        return None

    async def events(self) -> AsyncIterator[SessionTerminalEvent]:
        yield SessionTerminalEvent(self.terminal)

    async def request_boundary(self, boundary_id: UUID) -> BoundaryReceipt:
        return BoundaryReceipt(True, boundary_id)

    async def finish(self) -> SessionTerminal:
        return self.terminal

    async def cancel(self) -> SessionTerminal:
        return self.terminal


class SendFailedSession(ImmediatelyFailedSession):
    async def send_audio(self, chunk: AudioChunk) -> None:
        raise ConnectionError("STT send failed")


class BlockingReplacementSession(ImmediatelyFailedSession):
    def __init__(self, session_id: UUID) -> None:
        super().__init__(session_id)
        self.cancelled = False
        self.terminal = SessionTerminal(
            terminal_id=uuid4(),
            session_id=session_id,
            outcome=Outcome.CANCELLED,
            error=None,
            accepted_input=0,
            received_output=0,
            emitted_output=0,
            transport=Transport.WEBSOCKET,
            raw_refs=(REF,),
        )
        self.closed = asyncio.Event()
        self.sent: list[AudioChunk] = []

    async def events(self) -> AsyncIterator[SessionTerminalEvent]:
        await self.closed.wait()
        yield SessionTerminalEvent(self.terminal)

    async def send_audio(self, chunk: AudioChunk) -> None:
        self.sent.append(chunk)

    async def cancel(self) -> SessionTerminal:
        self.cancelled = True
        self.closed.set()
        return self.terminal


class RetryingSttProvider:
    def __init__(self) -> None:
        self.opens = 0
        self.opening = asyncio.Event()
        self.release = asyncio.Event()
        self.replacement: BlockingReplacementSession | None = None
        self.open_cancelled = asyncio.Event()
        self.return_after_cancel = False
        self.open_parameters: list[tuple[int, int]] = []

    async def capabilities(self) -> StageCapabilities:
        raise AssertionError

    async def health(self, snapshot: str) -> ProviderHealth:
        raise AssertionError

    async def open(
        self,
        session_id: UUID,
        language: str,
        *,
        resume_at_sample: int = 0,
        commit_watermark: int = 0,
    ) -> SttSession:
        self.open_parameters.append((resume_at_sample, commit_watermark))
        self.opens += 1
        if self.opens == 1:
            return SendFailedSession(session_id)
        self.replacement = BlockingReplacementSession(session_id)
        self.opening.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.open_cancelled.set()
            if not self.return_after_cancel:
                raise
        return self.replacement


@pytest.mark.asyncio
async def test_same_language_session_allocates_no_translation_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    created: list[str] = []

    class RejectAssembler:
        def __init__(self, *_: object, **__: object) -> None:
            created.append("assembler")

    class RejectQueue:
        def __init__(self, *_: object, **__: object) -> None:
            created.append("queue")

    monkeypatch.setattr(
        "transhooter_worker.application.session.UtteranceAssembler", RejectAssembler
    )
    monkeypatch.setattr("transhooter_worker.application.session.OrderedStageQueue", RejectQueue)

    session = DirectionSession(
        DirectionSpec(uuid4(), uuid4(), "de-DE", "de-DE", None, True),
        FixtureSttProvider(),
        FixtureTranslationProvider(),
        FixtureTtsProvider(),
        ignore_sink,
        ignore_sink,
        ignore_sink,
    )

    assert created == []
    assert session._assembler is None
    assert session._stage_queue is None
    assert session._stage_task is None


@pytest.mark.asyncio
async def test_finish_during_retry_open_cancels_replacement_without_hanging(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    provider = RetryingSttProvider()

    spec = DirectionSpec(
        source_participant_id=uuid4(),
        destination_participant_id=uuid4(),
        source_language="en-US",
        target_language="de-DE",
        voice="fixture-voice",
        same_language=False,
    )
    session = DirectionSession(
        spec,
        provider,
        FixtureTranslationProvider(),
        FixtureTtsProvider(),
        ignore_sink,
        ignore_sink,
        ignore_sink,
    )
    await session.start()
    await asyncio.wait_for(provider.opening.wait(), 1)
    finishing = asyncio.create_task(session.finish())
    with pytest.raises(RuntimeError, match="failed outcome"):
        await asyncio.wait_for(finishing, 1)

    assert provider.replacement is not None
    assert not provider.release.is_set()


@pytest.mark.asyncio
async def test_failed_send_waits_for_installed_replacement_without_lock_deadlock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    provider = RetryingSttProvider()
    session = DirectionSession(
        DirectionSpec(uuid4(), uuid4(), "en-US", "en-US", None, True),
        provider,
        FixtureTranslationProvider(),
        FixtureTtsProvider(),
        ignore_sink,
        ignore_sink,
        ignore_sink,
    )
    await session.start()
    chunk = AudioChunk(
        uuid4(),
        0,
        SampleRange(0, 4_000),
        b"\0\0" * 4_000,
        16_000,
        1,
        REF,
    )
    sending = asyncio.create_task(session.send_audio(chunk))
    await asyncio.wait_for(provider.opening.wait(), 1)
    provider.release.set()
    await asyncio.wait_for(sending, 1)
    await session.cancel()
    replacement = provider.replacement
    assert replacement is not None
    assert replacement.sent == [chunk]
    assert provider.open_parameters[-1] == (0, 0)


@pytest.mark.asyncio
async def test_abandoned_opened_replacement_terminal_is_normalized_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    provider = RetryingSttProvider()
    provider.return_after_cancel = True
    normalized: list[object] = []

    async def capture_normalized(event: object) -> None:
        normalized.append(event)

    session = DirectionSession(
        DirectionSpec(uuid4(), uuid4(), "en-US", "en-US", None, True),
        provider,
        FixtureTranslationProvider(),
        FixtureTtsProvider(),
        ignore_sink,
        ignore_sink,
        ignore_sink,
        capture_normalized,
    )
    await session.start()
    await asyncio.wait_for(provider.opening.wait(), 1)
    with pytest.raises(RuntimeError, match="failed outcome"):
        await asyncio.wait_for(session.finish(), 1)

    replacement = provider.replacement
    assert replacement is not None and replacement.cancelled
    matching = [
        event
        for event in normalized
        if isinstance(event, SessionTerminalEvent)
        and event.terminal.terminal_id == replacement.terminal.terminal_id
    ]
    assert len(matching) == 1
    terminal_events = [event for event in normalized if isinstance(event, SessionTerminalEvent)]
    assert len({event.terminal.terminal_id for event in terminal_events}) == len(terminal_events)
    assert normalized[-1] is matching[0]


@pytest.mark.asyncio
async def test_terminal_stt_failure_is_exposed_to_job_supervisor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    session_id = uuid4()
    error = ProviderError(
        kind=ErrorKind.INVALID_REQUEST,
        scope="session",
        provider_retry_advice=RetryAdvice.NEVER,
        provider_code="invalid",
        provider_request_id=None,
        retry_delay_ms=None,
        attempt_id=session_id,
        raw_refs=(REF,),
        message="invalid",
    )
    fatal_session = ImmediatelyFailedSession(session_id)
    fatal_session.terminal = SessionTerminal(
        terminal_id=uuid4(),
        session_id=session_id,
        outcome=Outcome.FAILED,
        error=error,
        accepted_input=0,
        received_output=0,
        emitted_output=0,
        transport=Transport.WEBSOCKET,
        raw_refs=(REF,),
    )

    class ImmediateFatalProvider:
        async def capabilities(self) -> StageCapabilities:
            raise AssertionError

        async def health(self, snapshot: str) -> ProviderHealth:
            raise AssertionError

        async def open(
            self,
            session_id: UUID,
            language: str,
            *,
            resume_at_sample: int = 0,
            commit_watermark: int = 0,
        ) -> SttSession:
            return fatal_session

    spec = DirectionSpec(
        source_participant_id=uuid4(),
        destination_participant_id=uuid4(),
        source_language="en-US",
        target_language="de-DE",
        voice="fixture-voice",
        same_language=False,
    )
    session = DirectionSession(
        spec,
        ImmediateFatalProvider(),
        FixtureTranslationProvider(),
        FixtureTtsProvider(),
        ignore_sink,
        ignore_sink,
        ignore_sink,
    )
    await session.start()
    observed = await asyncio.wait_for(session.failure, 1)

    assert isinstance(observed, RuntimeError)
    assert "STT degraded" in str(observed)
    with pytest.raises(RuntimeError, match="STT degraded: unsafe or exhausted"):
        await session.cancel()


class BlockingSynthesisAttempt(FixtureSynthesisAttempt):
    def __init__(self, utterance: SynthesisUtterance, scenario: FixtureScenario) -> None:
        super().__init__(utterance, scenario)
        self.started = asyncio.Event()
        self.released = asyncio.Event()
        self.cancelled = False

    def events(self) -> AsyncIterator[OperationTerminalEvent]:
        async def stream() -> AsyncIterator[OperationTerminalEvent]:
            self.started.set()
            await self.released.wait()
            yield OperationTerminalEvent(await self.finish())

        return stream()

    async def cancel(self) -> OperationTerminal:
        self.cancelled = True
        terminal = await super().cancel()
        self.released.set()
        return terminal


class BlockingTtsSession(FixtureTtsSession):
    def __init__(self, session_id: UUID, scenario: FixtureScenario) -> None:
        super().__init__(session_id, scenario)
        self.attempt: BlockingSynthesisAttempt | None = None
        self.finished = False
        self.start_after_finish = False
        self.attempt_created = asyncio.Event()

    async def start(self, utterance: SynthesisUtterance) -> BlockingSynthesisAttempt:
        if self.finished:
            self.start_after_finish = True
            raise AssertionError("synthesis started after TTS session finish")
        self.attempt = BlockingSynthesisAttempt(utterance, self._scenario)
        self.attempt_created.set()
        return self.attempt

    async def finish(self) -> SessionTerminal:
        self.finished = True
        return await super().finish()


class BlockingTtsProvider(FixtureTtsProvider):
    def __init__(self) -> None:
        super().__init__()
        self.session: BlockingTtsSession | None = None

    async def open(self, session_id: UUID, language: str, voice: str) -> BlockingTtsSession:
        self.session = BlockingTtsSession(session_id, self._scenario)
        return self.session


@pytest.mark.asyncio
async def test_cancel_interrupts_blocked_active_synthesis_before_queue_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    tts_provider = BlockingTtsProvider()
    session = DirectionSession(
        DirectionSpec(uuid4(), uuid4(), "en-US", "de-DE", "fixture-voice", False),
        FixtureSttProvider(),
        FixtureTranslationProvider(),
        tts_provider,
        ignore_sink,
        ignore_sink,
        ignore_sink,
    )
    await session.start()
    await session.send_audio(
        AudioChunk(uuid4(), 0, SampleRange(0, 4_000), b"\0\0" * 4_000, 16_000, 1, REF)
    )
    await session.boundary()
    tts_session = tts_provider.session
    assert tts_session is not None
    await asyncio.wait_for(tts_session.attempt_created.wait(), 1)
    attempt = tts_session.attempt
    assert attempt is not None
    await asyncio.wait_for(attempt.started.wait(), 1)

    await asyncio.wait_for(
        asyncio.gather(session.finish(), session.cancel()),
        1,
    )

    assert attempt.cancelled
    assert not tts_session.start_after_finish


def fixture_direction(
    checkpoint_sink: Callable[[int, int, bool], Awaitable[None]],
) -> DirectionSession:
    spec = DirectionSpec(
        source_participant_id=uuid4(),
        destination_participant_id=uuid4(),
        source_language="en-US",
        target_language="en-US",
        voice=None,
        same_language=True,
    )
    return DirectionSession(
        spec,
        FixtureSttProvider(),
        FixtureTranslationProvider(),
        FixtureTtsProvider(),
        ignore_sink,
        ignore_sink,
        checkpoint_sink,
    )


@pytest.mark.asyncio
async def test_runtime_drain_closes_providers_after_sessions_and_publishers_settle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    events: list[str] = []

    class Session:
        async def finish(self) -> None:
            events.append("session")

    class Publisher:
        async def drain(self) -> None:
            assert events == ["session"]
            events.append("publisher")

    async def close_providers() -> None:
        assert events == ["session", "publisher"]
        events.append("providers")

    providers = Providers(
        FixtureSttProvider(),
        FixtureTranslationProvider(),
        FixtureTtsProvider(),
        _close=close_providers,
    )

    await _drain_runtime(
        set(),
        {uuid4(): Session()},  # type: ignore[dict-item]
        {uuid4(): Publisher()},  # type: ignore[dict-item]
        [providers],
    )
    await providers.aclose()

    assert events == ["session", "publisher", "providers"]


@pytest.mark.asyncio
async def test_fixture_stt_fails_after_exact_configured_chunk_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    meeting_id = uuid4()
    scenario_path = tmp_path / "scenario.json"
    scenario_path.write_text(
        json.dumps(
            {
                "consultations": {
                    str(meeting_id): {
                        "stt": {"failAfterChunks": 1},
                    }
                }
            }
        ),
        "utf-8",
    )
    session = await FixtureSttProvider(FixtureScenario(meeting_id, scenario_path)).open(
        uuid4(), "en-US"
    )
    first = AudioChunk(uuid4(), 0, SampleRange(0, 4_000), b"\0\0" * 4_000)
    second = AudioChunk(uuid4(), 1, SampleRange(4_000, 8_000), b"\0\0" * 4_000)

    await session.send_audio(first)
    with pytest.raises(RuntimeError, match="injected fixture STT transport failure"):
        await session.send_audio(second)
    events = [event async for event in session.events()]
    terminal = events[-1].terminal

    assert terminal.outcome is Outcome.FAILED
    assert terminal.accepted_input == first.samples.end
    assert sum(isinstance(event, SessionTerminalEvent) for event in events) == 1


@pytest.mark.asyncio
async def test_failed_terminal_checkpoint_can_be_retried_by_shutdown_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    terminal_attempts = 0
    terminal_evidence = asyncio.Event()

    async def checkpoint_sink(_: int, __: int, terminal: bool) -> None:
        nonlocal terminal_attempts
        if not terminal:
            return
        terminal_attempts += 1
        if terminal_attempts == 1:
            raise httpx.ReadError("checkpoint response lost")
        terminal_evidence.set()

    session = fixture_direction(checkpoint_sink)
    await session.start()

    with pytest.raises(httpx.ReadError, match="response lost"):
        await session.finish()

    await session.cancel()
    await asyncio.wait_for(terminal_evidence.wait(), 0.1)
    await session.finish()

    assert terminal_attempts == 2


@pytest.mark.asyncio
async def test_concurrent_finish_and_shutdown_cancel_emit_one_terminal_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    terminal_flags: list[bool] = []

    async def checkpoint_sink(_: int, __: int, terminal: bool) -> None:
        terminal_flags.append(terminal)

    session = fixture_direction(checkpoint_sink)
    await session.start()

    await asyncio.gather(session.finish(), session.cancel())

    assert terminal_flags == [True]


class CheckpointMetadata:
    def __init__(self, consultation_id: UUID) -> None:
        self.consultation_id = consultation_id
        self.worker_epoch = 3
        self.write_epoch = 4


@pytest.mark.asyncio
async def test_terminal_checkpoint_archive_drain_keeps_event_loop_responsive(
    tmp_path: Path,
) -> None:
    meeting_id = uuid4()
    source_id = uuid4()
    destination_id = uuid4()
    spool = EncryptedSpool(
        tmp_path / "payloads",
        tmp_path / "journal.sqlite3",
        {"v1": b"k" * 32},
        "v1",
        capacity_probe=deterministic_roomy_capacity,
    )
    evidence = spool.append(
        meeting_id=meeting_id,
        attempt_id=uuid4(),
        stage="terminal",
        transport="http",
        direction="internal",
        media_type="application/json",
        payload=b'{"outcome":"succeeded"}',
    )
    archive_entered = asyncio.Event()
    loop = asyncio.get_running_loop()
    release_archive = Event()
    archive_threads: list[int] = []
    loop_thread = get_ident()
    operations: list[str] = []
    spool_threads: list[int] = []
    observations_lock = Lock()

    original_acknowledge: SpoolOperation = spool.mark_checkpoint_delivery_acknowledged

    def acknowledge_observed(*args: object, **kwargs: object) -> object:
        with observations_lock:
            spool_threads.append(get_ident())
        operations.append("ack")
        return original_acknowledge(*args, **kwargs)

    spool.mark_checkpoint_delivery_acknowledged = acknowledge_observed

    original_committed: CommittedOperation = spool.committed
    for method_name in (
        "register_checkpoint_delivery",
        "committed",
        "context",
        "committed_scoped",
        "read",
        "mark_uploaded",
        "compact_uploaded_envelopes",
        "pcm_scopes",
        "covering_checkpoint",
        "checkpoint_covers",
    ):
        original: SpoolOperation = getattr(spool, method_name)

        def observed(
            *args: object,
            _operation: SpoolOperation = original,
            **kwargs: object,
        ) -> object:
            with observations_lock:
                spool_threads.append(get_ident())
            return _operation(*args, **kwargs)

        setattr(spool, method_name, observed)

    class Archive:
        def put_create_once(
            self, key: str, body: bytes, content_type: str, sha256: str
        ) -> ObjectRecord:
            with observations_lock:
                archive_threads.append(get_ident())
            if "/pipeline/terminal/" in key:
                loop.call_soon_threadsafe(archive_entered.set)
                release_archive.wait()
            return ObjectRecord(str(uuid4()), key, "v1", len(body), sha256, "crc", content_type)

        def verify(self, _record: ObjectRecord) -> bool:
            with observations_lock:
                archive_threads.append(get_ident())
            return True

    class Control:
        async def record_object(
            self, payload: dict[str, object], *, event_id: UUID | None = None
        ) -> None:
            assert event_id is not None
            object_payload = payload["object"]
            assert isinstance(object_payload, dict)
            operations.append(f"record:{object_payload['objectId']}")

        async def checkpoint(
            self, _payload: dict[str, object], *, event_id: UUID | None = None
        ) -> None:
            assert event_id is not None
            operations.append("checkpoint")

    metadata = CheckpointMetadata(meeting_id)
    task = asyncio.create_task(
        _persist_checkpoint(
            metadata,
            spool,
            Archive(),
            Control(),
            CheckpointChainState.empty(),
            source_id,
            destination_id,
            4_000,
            960,
            True,
        )
    )
    try:
        await archive_entered.wait()
        await asyncio.wait_for(asyncio.sleep(0), 0.1)
    finally:
        release_archive.set()
    await task

    assert archive_threads
    assert spool_threads
    assert len(set(spool_threads)) == 1
    assert len(set(archive_threads)) == 1
    assert spool_threads[0] == archive_threads[0]
    assert spool_threads[0] != loop_thread
    assert operations.index(f"record:{evidence.object_id}") < operations.index("checkpoint")
    assert operations[-2:] == ["checkpoint", "ack"]
    committed = original_committed()
    assert evidence.object_id not in {reference.object_id for reference, _ in committed}
