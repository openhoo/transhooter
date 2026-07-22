from uuid import uuid4

import pytest

from transhooter_worker.adapters.fixture.provider import (
    FixtureSttProvider,
    FixtureTranslationProvider,
    FixtureTtsProvider,
)
from transhooter_worker.domain.models import (
    AudioChunk,
    OperationTerminalEvent,
    SampleRange,
    SessionTerminalEvent,
    SynthesisUtterance,
    TranslationRequest,
)


def test_fixture_refuses_non_test_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_ENV", "development")
    with pytest.raises(RuntimeError, match="APP_ENV=test"):
        FixtureSttProvider()


@pytest.mark.asyncio
async def test_fixture_sessions_have_one_terminal_and_are_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    session_id = uuid4()
    chunk_id = uuid4()
    audio_range = SampleRange(0, 4000)
    stt = await FixtureSttProvider().open(session_id, "en-US")
    await stt.send_audio(
        AudioChunk(
            operation_id=chunk_id,
            sequence=0,
            samples=audio_range,
            pcm=b"\0" * 8000,
        )
    )

    boundary_id = uuid4()
    boundary_receipt = await stt.request_boundary(boundary_id)
    assert boundary_receipt.accepted

    first_terminal = await stt.finish()
    repeated_terminal = await stt.finish()
    assert repeated_terminal == first_terminal

    events = [event async for event in stt.events()]
    terminal_events = [event for event in events if isinstance(event, SessionTerminalEvent)]
    assert len(terminal_events) == 1


@pytest.mark.asyncio
async def test_fixture_translation_and_tts_are_neutral(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
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
    translation_attempt = await FixtureTranslationProvider().start(request)
    outcome = await translation_attempt.result()

    assert outcome.result is not None
    assert outcome.result.text == "[de] hello"
    assert await translation_attempt.result() == outcome

    tts_session = await FixtureTtsProvider().open(
        uuid4(),
        "de",
        "fixture-voice",
    )
    synthesis_attempt = await tts_session.start(
        SynthesisUtterance(
            operation_id=uuid4(),
            attempt_id=uuid4(),
            text=outcome.result.text,
            language="de",
            voice="fixture-voice",
            source_range=source_range,
        )
    )
    events = [event async for event in synthesis_attempt.events()]
    terminal_events = [event for event in events if isinstance(event, OperationTerminalEvent)]
    assert len(terminal_events) == 1

    first_terminal = await synthesis_attempt.finish()
    repeated_terminal = await synthesis_attempt.finish()
    assert repeated_terminal == first_terminal
