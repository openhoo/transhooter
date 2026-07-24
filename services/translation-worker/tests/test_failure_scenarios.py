import asyncio
import json
from pathlib import Path
from uuid import uuid4

import pytest
from transhooter_spool import RawRef, SampleRange

from transhooter_worker.adapters.fixture.provider import (
    FixtureSttProvider,
    FixtureTranslationProvider,
    FixtureTtsProvider,
)
from transhooter_worker.adapters.fixture.scenario import FixtureScenario
from transhooter_worker.application.pipeline import UtteranceAssembler
from transhooter_worker.domain.models import (
    AudioChunk,
    AudioEvent,
    Finality,
    OperationTerminalEvent,
    SynthesisUtterance,
    TranscriptEvent,
    TranslationRequest,
)
from transhooter_worker.ports.providers import TranslationAttempt
from transhooter_worker.runtime.redis_quota import RedisQuotaGate


def wildcard_scenario_file(
    tmp_path: Path,
    scenario: dict[str, object],
) -> Path:
    path = tmp_path / "scenario.json"
    write_wildcard_scenario(path, scenario)
    return path


def write_wildcard_scenario(
    path: Path,
    scenario: dict[str, object],
) -> None:
    path.write_text(json.dumps({"consultations": {"*": scenario}}))


@pytest.mark.asyncio
async def test_provider_failure_scenarios_are_hot_reloaded(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    path = wildcard_scenario_file(
        tmp_path,
        {
            "translation": {"failure": "rate_limit"},
            "tts": {"partialSamples": 960, "failAfterPartial": True},
        },
    )
    scenario = FixtureScenario(uuid4(), path)
    source_range = SampleRange(0, 10)
    request = TranslationRequest(
        operation_id=uuid4(),
        attempt_id=uuid4(),
        purpose="final",
        source_language="en",
        target_language="de",
        text="hello",
        source_range=source_range,
    )

    translation_attempt = await FixtureTranslationProvider(scenario).start(request)
    failed_translation = await translation_attempt.result()

    assert failed_translation.result is None
    assert failed_translation.terminal.error is not None
    assert failed_translation.terminal.error.kind.value == "rate_limit"

    tts_session = await FixtureTtsProvider(scenario).open(
        uuid4(),
        "de",
        "fixture-voice",
    )
    synthesis_attempt = await tts_session.start(
        SynthesisUtterance(
            operation_id=uuid4(),
            attempt_id=uuid4(),
            text="x",
            language="de",
            voice="fixture-voice",
            source_range=source_range,
        )
    )
    synthesis_events = [event async for event in synthesis_attempt.events()]

    first_audio_event = synthesis_events[0]
    assert isinstance(first_audio_event, AudioEvent)
    assert first_audio_event.samples.length == 960

    terminal_event = synthesis_events[-1]
    assert isinstance(terminal_event, OperationTerminalEvent)
    assert terminal_event.terminal.outcome.value == "failed"

    write_wildcard_scenario(path, {})
    recovered_attempt = await FixtureTranslationProvider(scenario).start(request)
    recovered_translation = await recovered_attempt.result()
    assert recovered_translation.result is not None


@pytest.mark.asyncio
async def test_translation_retries_safe_uncommitted_attempts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    path = wildcard_scenario_file(
        tmp_path,
        {"translation": {"failure": "rate_limit"}},
    )
    scenario = FixtureScenario(uuid4(), path)

    class RecoveringTranslationProvider(FixtureTranslationProvider):
        calls = 0

        async def start(
            self,
            request: TranslationRequest,
        ) -> TranslationAttempt:
            self.calls += 1
            if self.calls == 3:
                write_wildcard_scenario(path, {})
            return await super().start(request)

    async def skip_retry_delay(_: float) -> None:
        return None

    monkeypatch.setattr(
        "transhooter_worker.application.pipeline.asyncio.sleep",
        skip_retry_delay,
    )
    provider = RecoveringTranslationProvider(scenario)
    assembler = UtteranceAssembler(provider, "en", "de")
    revision = await assembler.transcript(
        TranscriptEvent(
            samples=SampleRange(0, 10),
            revision=1,
            finality=Finality.PROVISIONAL,
            text="hello",
            words=(),
            confidence=1.0,
            raw_ref=RawRef(
                object_id=uuid4(),
                ordinal=1,
                sha256="0" * 64,
                size=1,
                media_type="application/json",
            ),
        ),
        0,
    )

    assert provider.calls == 3
    assert revision is not None
    assert revision.translated_text == "[de] hello"


@pytest.mark.asyncio
async def test_stt_transport_failure_after_first_chunk(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    path = wildcard_scenario_file(
        tmp_path,
        {"stt": {"failAfterChunks": 1}},
    )
    scenario = FixtureScenario(uuid4(), path)
    stt_session = await FixtureSttProvider(scenario).open(uuid4(), "en-US")
    audio_chunk = AudioChunk(
        operation_id=uuid4(),
        sequence=0,
        samples=SampleRange(0, 1),
        pcm=b"\0\0",
    )
    await stt_session.send_audio(audio_chunk)
    second_chunk = AudioChunk(
        operation_id=uuid4(),
        sequence=1,
        samples=SampleRange(1, 2),
        pcm=b"\0\0",
    )

    with pytest.raises(RuntimeError, match="STT transport"):
        await stt_session.send_audio(second_chunk)




@pytest.mark.asyncio
async def test_redis_quota_reconnects_after_transport_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened = 0

    class SuccessfulReader:
        async def readexactly(self, _: int) -> bytes:
            return b":"

        async def readline(self) -> bytes:
            return b"1\r\n"

    class RedisWriter:
        def __init__(self, fails: bool) -> None:
            self.fails = fails
            self.close_calls = 0

        def write(self, _: bytes) -> None:
            return None

        async def drain(self) -> None:
            if self.fails:
                raise ConnectionResetError("connection lost")

        def close(self) -> None:
            self.close_calls += 1
            self.closed = True

        async def wait_closed(self) -> None:
            return None

    writers: list[RedisWriter] = []

    async def open_connection(*_: object) -> tuple[SuccessfulReader, RedisWriter]:
        nonlocal opened
        opened += 1
        writer = RedisWriter(fails=opened == 1)
        writers.append(writer)
        return SuccessfulReader(), writer

    monkeypatch.setattr(asyncio, "open_connection", open_connection)
    gate = RedisQuotaGate(
        "redis://redis:6379",
        "provider",
        "account",
        "region",
        {"translation": {"characters_minute": 100}},
    )

    with pytest.raises(ConnectionResetError, match="connection lost"):
        await gate("translation", 4)
    await gate("translation", 5)

    assert opened == 2
    assert all(writer.closed for writer in writers[:1])
    await gate.aclose()
    await gate.aclose()
    assert all(writer.closed for writer in writers)
    assert [writer.close_calls for writer in writers] == [1, 1]
