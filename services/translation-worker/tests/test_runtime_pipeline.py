import asyncio
import json
import wave
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

import pytest
from transhooter_spool import SampleRange

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
    SessionTerminal,
    StageCapabilities,
    TranslationRequest,
)
from transhooter_worker.ports.providers import TranslationAttempt
from transhooter_worker.runtime.probe import execute_probe
from transhooter_worker.runtime.provider_registry import Providers
from transhooter_worker.runtime.redis_quota import RedisQuotaGate


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


class RedisResponseReader:
    def __init__(self) -> None:
        self.responses: asyncio.Queue[tuple[bytes, bytes]] = asyncio.Queue()
        self.current_line: bytes | None = None

    def respond(self, prefix: bytes, line: bytes) -> None:
        self.responses.put_nowait((prefix, line))

    async def readexactly(self, _: int) -> bytes:
        prefix, self.current_line = await self.responses.get()
        return prefix

    async def readline(self) -> bytes:
        assert self.current_line is not None
        line = self.current_line
        self.current_line = None
        return line


class RecordingRedisWriter:
    def __init__(
        self,
        reader: RedisResponseReader,
        on_command: Callable[[tuple[str, ...]], None],
    ) -> None:
        self.reader = reader
        self.on_command = on_command
        self.commands: list[tuple[str, ...]] = []
        self.closed = False
        self.waited_closed = False

    def write(self, payload: bytes) -> None:
        lines = payload.split(b"\r\n")
        command = tuple(lines[index].decode() for index in range(2, len(lines) - 1, 2))
        self.commands.append(command)
        self.on_command(command)

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        self.waited_closed = True


def quota_gate(url: str = "redis://redis:6379") -> RedisQuotaGate:
    return RedisQuotaGate(
        url,
        "provider",
        "account",
        "region",
        {"translation": {"characters_minute": 100}},
    )


@pytest.mark.asyncio
async def test_redis_quota_reuses_authenticated_connection_and_closes_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connections: list[RecordingRedisWriter] = []

    async def open_connection(
        *_: object,
    ) -> tuple[RedisResponseReader, RecordingRedisWriter]:
        reader = RedisResponseReader()

        def respond(command: tuple[str, ...]) -> None:
            reader.respond(b"+", b"OK\r\n") if command[0] == "AUTH" else reader.respond(
                b":", b"1\r\n"
            )

        writer = RecordingRedisWriter(reader, respond)
        connections.append(writer)
        return reader, writer

    monkeypatch.setattr(asyncio, "open_connection", open_connection)
    gate = quota_gate("redis://:secret@redis:6379")

    await gate("translation", 4)
    await gate("translation", 5)
    await gate.aclose()

    assert len(connections) == 1
    assert [command[0] for command in connections[0].commands] == [
        "AUTH",
        "EVAL",
        "EVAL",
    ]
    assert connections[0].closed
    assert connections[0].waited_closed
    with pytest.raises(RuntimeError, match="closed"):
        await gate("translation", 1)


@pytest.mark.asyncio
async def test_redis_quota_serializes_concurrent_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_command = asyncio.Event()
    release_first = asyncio.Event()
    eval_count = 0
    reader = RedisResponseReader()

    class BlockingRedisWriter(RecordingRedisWriter):
        async def drain(self) -> None:
            nonlocal eval_count
            eval_count += 1
            if eval_count == 1:
                first_command.set()
                await release_first.wait()
            self.reader.respond(b":", b"1\r\n")

    writer = BlockingRedisWriter(reader, lambda _: None)

    async def open_connection(
        *_: object,
    ) -> tuple[RedisResponseReader, BlockingRedisWriter]:
        return reader, writer

    monkeypatch.setattr(asyncio, "open_connection", open_connection)
    gate = quota_gate()
    first = asyncio.create_task(gate("translation", 4))
    await first_command.wait()
    second = asyncio.create_task(gate("translation", 5))
    await asyncio.sleep(0)

    assert eval_count == 1
    release_first.set()
    await asyncio.gather(first, second)
    assert eval_count == 2
    await gate.aclose()


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
    final = await session.finish()

    assert len(captions) == 2
    assert not captions[0].final
    assert captions[1].final
    assert len(audio_frames) == 10
    assert all(len(frame) == 1_920 for frame in audio_frames)
    assert any(b"".join(audio_frames))
    assert all(not terminal for _, _, terminal in checkpoints)
    assert final.input_sample == 4_000
    assert final.output_sample == 9_600


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
async def test_same_language_direction_does_not_allocate_translation_pipeline(
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

    async def ignore(*_: object) -> None:
        return None

    session = DirectionSession(
        DirectionSpec(uuid4(), uuid4(), "de-DE", "de-DE", None, True),
        FixtureSttProvider(),
        FixtureTranslationProvider(),
        FixtureTtsProvider(),
        ignore,
        ignore,
        ignore,
    )
    await session.start()

    assert created == []
    assert session._assembler is None
    assert session._stage_queue is None
    assert session._stage_task is None
    assert session._boundary_task is not None
    assert not session._boundary_task.done()

    await session.cancel()


@pytest.mark.asyncio
async def test_pcm_framing_removes_consumed_prefix_and_preserves_remainder(
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
    remainder_length = len(original) % 1_920

    published_samples = await pipeline_module.publish_complete_pcm_frames(pending, capture, 1_920)

    assert published_samples == (len(original) - remainder_length) // 2
    assert b"".join(frames) == original[:-remainder_length]
    assert bytes(pending) == original[-remainder_length:]
    complete_frame_count = len(frames)

    await pipeline_module.publish_complete_pcm_frames(pending, capture, 1_920)

    assert len(frames) == complete_frame_count
    assert bytes(pending) == original[-remainder_length:]

    await session._flush_final_pcm_frame(pending)
    assert frames[-1] == original[-remainder_length:].ljust(1_920, b"\0")


@pytest.mark.asyncio
async def test_partial_pcm_publication_checkpoints_delivered_frame_before_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    first_frame = b"\x01\x00" * 960
    second_frame = b"\x02\x00" * 960
    third_frame = b"\x03\x00" * 960
    trailing_partial_frame = b"\x04\x00" * 100
    pending = bytearray(first_frame + second_frame + third_frame + trailing_partial_frame)
    delivered: list[bytes] = []
    checkpoints: list[tuple[int, int, bool]] = []
    publication_attempt = 0

    async def publish(frame: bytes) -> None:
        nonlocal publication_attempt
        publication_attempt += 1
        if publication_attempt == 3:
            raise RuntimeError("injected publication failure")
        delivered.append(frame)

    async def checkpoint(
        input_sample: int,
        output_sample: int,
        terminal: bool,
    ) -> None:
        checkpoints.append((input_sample, output_sample, terminal))

    async def ignore(*_: object) -> None:
        return None

    session = DirectionSession(
        DirectionSpec(uuid4(), uuid4(), "en-US", "de-DE", "fixture-voice", False),
        FixtureSttProvider(),
        FixtureTranslationProvider(),
        FixtureTtsProvider(),
        ignore,
        publish,
        checkpoint,
    )
    session._last_input = 4_000

    with pytest.raises(RuntimeError, match="injected publication failure"):
        await session._publish_complete_pcm_frames(pending)

    assert delivered == [first_frame, second_frame]
    assert bytes(pending) == third_frame + trailing_partial_frame
    assert checkpoints == [(4_000, 1_920, False)]
    assert session._last_output == 1_920

    await session._publish_complete_pcm_frames(pending)

    assert delivered == [first_frame, second_frame, third_frame]
    assert bytes(pending) == trailing_partial_frame
    assert checkpoints == [(4_000, 1_920, False)]
    assert session._last_output == 2_880


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
