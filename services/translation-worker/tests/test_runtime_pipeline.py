import json
import wave
from pathlib import Path
from uuid import uuid4

import pytest

import transhooter_worker.application.pipeline as pipeline_module
from transhooter_worker.adapters.fixture.provider import (
    FixtureSttProvider,
    FixtureTranslationProvider,
    FixtureTtsProvider,
)
from transhooter_worker.adapters.fixture.scenario import FixtureScenario
from transhooter_worker.adapters.terminal import terminal_bytes
from transhooter_worker.application.pipeline import CaptionRevision
from transhooter_worker.application.session import DirectionSession, DirectionSpec
from transhooter_worker.domain.models import (
    AudioChunk,
    OperationTerminal,
    ProviderHealth,
    RetryDecision,
    SampleRange,
    SessionTerminal,
    StageCapabilities,
    TranslationRequest,
)
from transhooter_worker.ports.providers import TranslationAttempt
from transhooter_worker.runtime.probe import execute_probe
from transhooter_worker.runtime.provider_registry import Providers


def write_wav_fixture(
    path: Path,
    *,
    sample_rate: int,
    frames: bytes,
) -> None:
    with wave.open(str(path), "wb") as target:
        target.setnchannels(1)
        target.setsampwidth(2)
        target.setframerate(sample_rate)
        target.writeframes(frames)


@pytest.mark.asyncio
async def test_probe_executes_stt_translation_and_tts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    audio_path = tmp_path / "speech.wav"
    write_wav_fixture(
        audio_path,
        sample_rate=16_000,
        frames=b"\0\0" * 4_000,
    )
    providers = Providers(
        stt=FixtureSttProvider(),
        translation=FixtureTranslationProvider(),
        tts=FixtureTtsProvider(),
    )
    result = await execute_probe(
        providers,
        audio_path,
        "en-US",
        "de-DE",
        "fixture-voice",
    )

    assert result.transcript == "fixture speech"
    assert result.translation == "[de-DE] fixture speech"
    assert len(result.synthesized_pcm) == 19_200
    assert any(result.synthesized_pcm)
    assert len(result.provider_attempt_ids) == 3


@pytest.mark.asyncio
async def test_probe_resamples_48k_fixture_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    audio_path = tmp_path / "speech-48k.wav"
    write_wav_fixture(
        audio_path,
        sample_rate=48_000,
        frames=b"\1\0\2\0\3\0" * 4_000,
    )
    result = await execute_probe(
        Providers(
            stt=FixtureSttProvider(),
            translation=FixtureTranslationProvider(),
            tts=FixtureTtsProvider(),
        ),
        audio_path,
        "en-US",
        "de-DE",
        "fixture-voice",
    )

    assert result.transcript == "fixture speech"
    assert result.synthesized_pcm


@pytest.mark.asyncio
async def test_direction_session_publishes_only_final_tts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    captions: list[CaptionRevision] = []
    audio_frames: list[bytes] = []
    checkpoints: list[tuple[int, int, bool]] = []

    async def capture_caption(caption: CaptionRevision) -> None:
        captions.append(caption)

    async def capture_audio(frame: bytes) -> None:
        audio_frames.append(frame)

    async def capture_checkpoint(
        input_sample: int,
        output_sample: int,
        terminal: bool,
    ) -> None:
        checkpoints.append((input_sample, output_sample, terminal))

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
        FixtureSttProvider(),
        FixtureTranslationProvider(),
        FixtureTtsProvider(),
        capture_caption,
        capture_audio,
        capture_checkpoint,
    )
    await session.start()
    await session.send_audio(
        AudioChunk(
            operation_id=uuid4(),
            sequence=0,
            samples=SampleRange(0, 4_000),
            pcm=b"\0\0" * 4_000,
        )
    )
    await session.boundary()
    await session.finish()

    assert len(captions) == 2
    assert not captions[0].final
    assert captions[1].final
    assert len(audio_frames) == 10
    assert all(len(frame) == 1_920 for frame in audio_frames)
    assert any(b"".join(audio_frames))
    assert checkpoints[-1][2] is True


@pytest.mark.asyncio
async def test_missing_translated_voice_fails_before_provider_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")

    async def reject_open(*_: object) -> None:
        raise AssertionError("provider opened before direction validation")

    monkeypatch.setattr(FixtureSttProvider, "open", reject_open)
    monkeypatch.setattr(FixtureTtsProvider, "open", reject_open)

    async def ignore(*_: object) -> None:
        return None

    session = DirectionSession(
        DirectionSpec(uuid4(), uuid4(), "en-US", "de-DE", None, False),
        FixtureSttProvider(),
        FixtureTranslationProvider(),
        FixtureTtsProvider(),
        ignore,
        ignore,
        ignore,
    )

    with pytest.raises(ValueError, match="capability-approved voice"):
        await session.start()


@pytest.mark.asyncio
async def test_synthesis_normalization_encodes_pcm_and_pads_final_frame(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    scenario_path = tmp_path / "scenario.json"
    scenario_path.write_text(
        json.dumps(
            {
                "consultations": {
                    "*": {"tts": {"partialSamples": 961}},
                }
            }
        )
    )
    scenario = FixtureScenario(uuid4(), scenario_path)
    audio_frames: list[bytes] = []
    normalized_envelopes: list[bytes] = []

    async def ignore_caption(_: CaptionRevision) -> None:
        return None

    async def capture_audio(frame: bytes) -> None:
        audio_frames.append(frame)

    async def ignore_checkpoint(_: int, __: int, ___: bool) -> None:
        return None

    async def capture_normalized_envelope(value: object) -> None:
        normalized_envelopes.append(terminal_bytes(value))

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
        FixtureSttProvider(scenario),
        FixtureTranslationProvider(scenario),
        FixtureTtsProvider(scenario),
        ignore_caption,
        capture_audio,
        ignore_checkpoint,
        capture_normalized_envelope,
    )
    await session.start()
    await session.send_audio(
        AudioChunk(
            operation_id=uuid4(),
            sequence=0,
            samples=SampleRange(0, 4_000),
            pcm=b"\0\0" * 4_000,
        )
    )
    await session.boundary()
    await session.finish()

    assert len(audio_frames) == 2
    assert all(len(frame) == 1_920 for frame in audio_frames)
    base64_envelopes = [payload for payload in normalized_envelopes if b'"base64"' in payload]
    assert base64_envelopes
    assert any(b'"length":1922' in payload for payload in base64_envelopes)


@pytest.mark.asyncio
async def test_same_language_direction_bypasses_translation_and_tts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    captions: list[CaptionRevision] = []

    class SameLanguageTranslationGuard:
        async def capabilities(self) -> StageCapabilities:
            raise AssertionError("same-language direction requested capabilities")

        async def health(self, snapshot: str) -> ProviderHealth:
            raise AssertionError("same-language direction requested health")

        async def start(
            self,
            request: TranslationRequest,
        ) -> TranslationAttempt:
            raise AssertionError("same-language direction translated")

    async def capture_caption(caption: CaptionRevision) -> None:
        captions.append(caption)

    async def reject_synthesized_audio(_: bytes) -> None:
        raise AssertionError("same-language direction synthesized")

    async def ignore_checkpoint(_: int, __: int, ___: bool) -> None:
        return None

    spec = DirectionSpec(
        source_participant_id=uuid4(),
        destination_participant_id=uuid4(),
        source_language="de-DE",
        target_language="de-DE",
        voice=None,
        same_language=True,
    )
    session = DirectionSession(
        spec,
        FixtureSttProvider(),
        SameLanguageTranslationGuard(),
        FixtureTtsProvider(),
        capture_caption,
        reject_synthesized_audio,
        ignore_checkpoint,
    )
    await session.start()
    await session.send_audio(
        AudioChunk(
            operation_id=uuid4(),
            sequence=0,
            samples=SampleRange(0, 4_000),
            pcm=b"\0\0" * 4_000,
        )
    )
    await session.boundary()
    await session.finish()

    assert len(captions) == 1
    assert captions[0].source_text == captions[0].translated_text


@pytest.mark.asyncio
async def test_pcm_framing_uses_cursor_without_front_deleting_buffer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    frames: list[bytes] = []

    async def capture(frame: bytes) -> None:
        frames.append(frame)

    async def ignore(*_: object) -> None:
        return None

    session = DirectionSession(
        DirectionSpec(uuid4(), uuid4(), "en-US", "en-US", None, True),
        FixtureSttProvider(),
        FixtureTranslationProvider(),
        FixtureTtsProvider(),
        ignore,
        capture,
        ignore,
    )
    original = bytes(range(256)) * 7_502 + b"tail"
    pending = bytearray(original)

    cursor = await session._flush_complete_pcm_frames(pending, 0)

    assert bytes(pending) == original
    assert b"".join(frames) == original[:cursor]
    assert cursor % 1_920 == 0
    remainder = pending[cursor:]
    await session._flush_final_pcm_frame(remainder)
    assert frames[-1] == bytes(remainder).ljust(1_920, b"\0")


@pytest.mark.asyncio
async def test_two_fixture_directions_report_every_provider_terminal_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    reports: list[
        tuple[
            str,
            OperationTerminal | SessionTerminal,
            int,
            object,
            RetryDecision,
            int,
            int,
        ]
    ] = []

    async def ignore(*_: object) -> None:
        return None

    async def capture_terminal(
        stage: str,
        terminal: OperationTerminal | SessionTerminal,
        attempt_number: int,
        retry_of_attempt_id: object,
        decision: RetryDecision,
        started_at_ms: int,
        occurred_at_ms: int,
    ) -> None:
        reports.append(
            (
                stage,
                terminal,
                attempt_number,
                retry_of_attempt_id,
                decision,
                started_at_ms,
                occurred_at_ms,
            )
        )

    specs = (
        DirectionSpec(uuid4(), uuid4(), "en-US", "de-DE", "fixture-voice", False),
        DirectionSpec(uuid4(), uuid4(), "de-DE", "en-US", "fixture-voice", False),
    )
    for spec in specs:
        session = DirectionSession(
            spec,
            FixtureSttProvider(),
            FixtureTranslationProvider(),
            FixtureTtsProvider(),
            ignore,
            ignore,
            ignore,
            terminal_sink=capture_terminal,
        )
        await session.start()
        await session.send_audio(AudioChunk(uuid4(), 0, SampleRange(0, 4_000), b"\0\0" * 4_000))
        await session.boundary()
        await session.finish()
        await session.finish()

    terminal_ids = [report[1].terminal_id for report in reports]
    assert len(terminal_ids) == len(set(terminal_ids))
    assert len(reports) >= 6
    for direction_index in range(2):
        direction_reports = reports[direction_index * 5 : (direction_index + 1) * 5]
        assert {report[0] for report in direction_reports} == {"stt", "translation", "tts"}
    assert all(report[2] == 1 for report in reports)
    assert all(report[5] <= report[6] for report in reports)


@pytest.mark.asyncio
async def test_ordered_queue_processing_failure_does_not_count_as_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RecordingCounter:
        def __init__(self) -> None:
            self.measurements: list[tuple[int, dict[str, str]]] = []

        def add(self, amount: int, attributes: dict[str, str]) -> None:
            self.measurements.append((amount, attributes))

    submissions = RecordingCounter()
    executions = RecordingCounter()
    monkeypatch.setattr(pipeline_module, "_ORDERED_QUEUE_SUBMISSIONS", submissions)
    monkeypatch.setattr(pipeline_module, "_ORDERED_QUEUE_EXECUTIONS", executions)
    queue = pipeline_module.OrderedStageQueue(maximum=2)
    discarded_work_ran = False

    async def fail_during_processing() -> None:
        raise RuntimeError("processing failed")

    async def discarded_work() -> None:
        nonlocal discarded_work_ran
        discarded_work_ran = True

    await queue.submit(final=False, work=fail_during_processing)
    await queue.submit(final=False, work=discarded_work)

    with pytest.raises(RuntimeError, match="processing failed"):
        await queue.run()

    assert sum(amount for amount, _ in submissions.measurements) == 2
    assert [attributes["result"] for _, attributes in submissions.measurements] == [
        "accepted",
        "accepted",
    ]
    assert executions.measurements == [(1, {"stage": "ordered", "result": "failed"})]
    assert discarded_work_ran is False
