import asyncio
import hashlib
import io
import json
import wave
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from google.cloud.location import locations_pb2
from google.cloud.speech_v2.types import locations_metadata
from google.protobuf.any_pb2 import Any as ProtobufAny

from transhooter_worker.__main__ import _capability_refresh
from transhooter_worker.adapters.google import provider as google_provider
from transhooter_worker.domain.models import (
    AudioEvent,
    BoundaryEvent,
    Finality,
    OperationTerminal,
    OperationTerminalEvent,
    Outcome,
    RawRef,
    RetryAction,
    RetryAdvice,
    RetryDecision,
    SampleRange,
    StageCapabilities,
    SynthesisUtterance,
    TranscriptEvent,
    TranslationRequest,
    Transport,
)
from transhooter_worker.runtime.provider_registry import FixtureProfile


class Journal:
    def append(self, **_: Any) -> RawRef:
        raise AssertionError("fake RPC owns evidence")

    def terminal(self, *_: object) -> RawRef:
        return RawRef(UUID(int=9), 9, "9" * 64, 1, "application/json")


class RecordingJournal:
    def __init__(self) -> None:
        self.rows: dict[UUID, dict[str, Any]] = {}
        self.terminals: list[tuple[UUID, bytes]] = []
        self.ordinal = 0

    def append(self, **row: Any) -> RawRef:
        self.ordinal += 1
        payload = bytes(row.get("payload", b""))
        object_id = uuid4()
        self.rows[object_id] = row
        return RawRef(
            object_id,
            self.ordinal,
            hashlib.sha256(payload).hexdigest(),
            len(payload),
            str(row.get("media_type", "application/octet-stream")),
        )

    def terminal(self, attempt_id: UUID, payload: bytes) -> RawRef:
        self.terminals.append((attempt_id, payload))
        return self.append(
            attempt_id=attempt_id,
            direction="terminal",
            media_type="application/json",
            payload=payload,
        )


class SuccessfulUnaryCall:
    def __init__(self, payload: bytes, deserializer: Any) -> None:
        self.payload = payload
        self.deserializer = deserializer

    def __await__(self):
        async def result() -> Any:
            return self.deserializer(self.payload)

        return result().__await__()

    async def initial_metadata(self) -> tuple[tuple[str, str], ...]:
        return (("request-id", "test"),)

    async def trailing_metadata(self) -> tuple[tuple[str, str], ...]:
        return (("region", "eu"),)

    async def code(self) -> str:
        return "StatusCode.OK"

    async def details(self) -> str:
        return ""


class SuccessfulUnaryChannel:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def unary_unary(self, _: str, request_serializer: Any, response_deserializer: Any):
        def invoke(request: Any, **__: Any) -> SuccessfulUnaryCall:
            request_serializer(request)
            return SuccessfulUnaryCall(self.payload, response_deserializer)

        return invoke


@pytest.mark.asyncio
async def test_speech_capabilities_decode_nested_language_model_maps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata = locations_metadata.LocationsMetadata(
        languages=locations_metadata.LanguageMetadata(
            models={
                "de-DE": locations_metadata.ModelMetadata(
                    model_features={"long": locations_metadata.ModelFeatures()}
                ),
                "en-US": locations_metadata.ModelMetadata(
                    model_features={"short": locations_metadata.ModelFeatures()}
                ),
            }
        )
    )
    packed = ProtobufAny()
    packed.Pack(locations_metadata.LocationsMetadata.pb(metadata))
    location = locations_pb2.Location(name="projects/project/locations/eu", metadata=packed)
    evidence = RawRef(UUID(int=1), 0, "0" * 64, 1, "application/protobuf")

    async def rpc(*_: object, **__: object) -> google_provider._CapabilityRpcResult:
        return google_provider._CapabilityRpcResult(
            location.SerializeToString(), UUID(int=10), (evidence,)
        )

    monkeypatch.setattr(google_provider, "_capability_rpc", rpc)
    config = google_provider.GoogleConfig(
        "project",
        "quota",
        UUID(int=2),
        "credential",
    )
    provider = google_provider.GoogleSttProvider(
        config,
        Journal(),
        object(),  # type: ignore[arg-type]
    )
    capabilities = await provider.capabilities()

    assert capabilities.languages == ("de-DE", "en-US")
    assert capabilities.models == ("long", "short")
    assert capabilities.evidence == evidence


@pytest.mark.asyncio
@pytest.mark.parametrize(("owned", "expected_closes"), [(True, 1), (False, 0)])
async def test_speech_capability_transport_closes_only_owned_channel(
    monkeypatch: pytest.MonkeyPatch,
    owned: bool,
    expected_closes: int,
) -> None:
    metadata = locations_metadata.LocationsMetadata(
        languages=locations_metadata.LanguageMetadata(
            models={
                "en-US": locations_metadata.ModelMetadata(
                    model_features={"long": locations_metadata.ModelFeatures()}
                )
            }
        )
    )
    packed = ProtobufAny()
    packed.Pack(locations_metadata.LocationsMetadata.pb(metadata))
    location = locations_pb2.Location(name="projects/project/locations/eu", metadata=packed)
    evidence = RawRef(UUID(int=8), 8, "8" * 64, 1, "application/protobuf")

    class Channel:
        def __init__(self) -> None:
            self.closes = 0

        async def close(self) -> None:
            self.closes += 1

    channel = Channel()

    async def rpc(*_: object, **__: object) -> google_provider._CapabilityRpcResult:
        return google_provider._CapabilityRpcResult(
            location.SerializeToString(), UUID(int=11), (evidence,)
        )

    monkeypatch.setattr(google_provider, "_capability_rpc", rpc)
    monkeypatch.setattr(google_provider, "authenticated_channel", lambda *_: channel)
    provider = google_provider.GoogleSttProvider(
        google_provider.GoogleConfig("project", "quota", UUID(int=2), "credential"),
        Journal(),
        None if owned else channel,  # type: ignore[arg-type]
    )

    capabilities = await provider.capabilities()

    assert capabilities.languages == ("en-US",)
    assert channel.closes == expected_closes


@pytest.mark.asyncio
async def test_replayed_final_trims_words_committed_before_watermark() -> None:
    raw_ref = RawRef(UUID(int=8), 8, "8" * 64, 1, "application/protobuf")
    boundary_id = UUID(int=12)
    session = object.__new__(google_provider.GoogleSttSession)
    session._received = 0
    session._refs = [raw_ref]
    session._commit_watermark = 16_000
    session._last_result_end = 16_000
    session._boundaries = [boundary_id]
    session._events = asyncio.Queue()
    session._event_slots = asyncio.Semaphore(64)
    response = google_provider.cloud_speech.StreamingRecognizeResponse(
        results=[
            google_provider.cloud_speech.StreamingRecognitionResult(
                alternatives=[
                    google_provider.cloud_speech.SpeechRecognitionAlternative(
                        transcript="committed new suffix",
                        words=[
                            google_provider.cloud_speech.WordInfo(
                                word="committed",
                                start_offset=timedelta(seconds=0),
                                end_offset=timedelta(seconds=1),
                            ),
                            google_provider.cloud_speech.WordInfo(
                                word="new",
                                start_offset=timedelta(seconds=1),
                                end_offset=timedelta(seconds=1.5),
                            ),
                            google_provider.cloud_speech.WordInfo(
                                word="suffix",
                                start_offset=timedelta(seconds=1.5),
                                end_offset=timedelta(seconds=2),
                            ),
                        ],
                    )
                ],
                is_final=True,
                result_end_offset=timedelta(seconds=2),
            )
        ]
    )

    await session._emit_transcript_results(response, base_sample=0)

    transcript = session._events.get_nowait()
    boundary = session._events.get_nowait()
    assert isinstance(transcript, TranscriptEvent)
    assert transcript.finality is Finality.SPAN_FINAL
    assert transcript.text == "new suffix"
    assert transcript.samples == SampleRange(16_000, 32_000)
    assert tuple(word.text for word in transcript.words) == ("new", "suffix")
    assert transcript.raw_ref == raw_ref
    assert isinstance(boundary, BoundaryEvent)
    assert boundary.committed_through == 32_000
    assert boundary.raw_ref == raw_ref


def test_capability_refresh_rejects_unsupported_source_locale() -> None:
    evidence = RawRef(UUID(int=7), 7, "7" * 64, 1, "application/json")
    capabilities = (
        StageCapabilities(
            "fixture",
            "stt",
            "fixture://stt",
            ("test",),
            ("en-US", "de-DE"),
            ("deterministic",),
            (),
            evidence,
        ),
        StageCapabilities(
            "fixture",
            "translation",
            "fixture://translation",
            ("test",),
            ("en", "de"),
            ("deterministic",),
            (),
            evidence,
        ),
        StageCapabilities(
            "fixture",
            "tts",
            "fixture://tts",
            ("test",),
            ("en-US", "de-DE"),
            ("deterministic",),
            (("sample_rate", 48_000),),
            evidence,
            ("fixture-voice",),
        ),
    )

    with pytest.raises(RuntimeError):
        _capability_refresh(
            "fixture",
            FixtureProfile(kind="fixture"),
            "fr-FR",
            "de-DE",
            capabilities,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(("sample_rate", "healthy"), [(48000, True), (24000, False)])
async def test_tts_health_runs_streaming_synthesis_and_validates_returned_format(
    monkeypatch: pytest.MonkeyPatch,
    sample_rate: int,
    healthy: bool,
) -> None:
    evidence = RawRef(UUID(int=3), 1, "1" * 64, 2, "application/protobuf")
    operation_id = UUID(int=4)
    attempt_id = UUID(int=5)
    terminal = OperationTerminal(
        UUID(int=6),
        operation_id,
        attempt_id,
        Outcome.SUCCEEDED,
        None,
        RetryDecision(RetryAction.STOP, None, "done", None),
        1,
        1,
        2,
        Transport.GRPC,
        (evidence,),
        "credential",
    )

    class Attempt:
        def events(self):
            async def stream():
                yield AudioEvent(
                    operation_id,
                    0,
                    SampleRange(0, 2),
                    b"\0\0\0\0",
                    sample_rate,
                    1,
                    evidence,
                )
                yield OperationTerminalEvent(terminal)

            return stream()

    captured: dict[str, object] = {}

    def attempt_factory(*args: object) -> Attempt:
        captured["utterance"] = args[3]
        captured["language"] = args[4]
        captured["voice"] = args[5]
        return Attempt()

    monkeypatch.setattr(google_provider, "GoogleTtsAttempt", attempt_factory)
    config = google_provider.GoogleConfig(
        project="project",
        quota_project="quota",
        meeting_id=UUID(int=2),
        credential_fingerprint="credential",
        probe_voice="de-DE-Chirp3-HD-Algenib",
        probe_voice_locale="de-DE",
    )
    provider = google_provider.GoogleTtsProvider(
        config,
        Journal(),
        object(),  # type: ignore[arg-type]
    )
    result = await provider.health("snapshot")

    assert result.healthy is healthy
    if healthy:
        assert result.evidence == evidence
    else:
        assert result.evidence is None
    utterance = captured["utterance"]
    assert isinstance(utterance, google_provider.SynthesisUtterance)
    assert utterance.language == "de-DE"
    assert captured["language"] == "de-DE"
    assert captured["voice"] == "de-DE-Chirp3-HD-Algenib"


@pytest.mark.asyncio
async def test_tts_health_reports_channel_setup_exception_without_masking_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SetupError(RuntimeError):
        pass

    def fail_channel_setup(*_: object) -> object:
        raise SetupError("credentials unavailable")

    monkeypatch.setattr(google_provider, "authenticated_channel", fail_channel_setup)
    provider = google_provider.GoogleTtsProvider(
        google_provider.GoogleConfig(
            "project",
            "quota",
            UUID(int=2),
            "credential",
            probe_voice="de-DE-Chirp3-HD-Algenib",
            probe_voice_locale="de-DE",
        ),
        Journal(),
    )

    health = await provider.health("snapshot")

    assert not health.healthy
    assert health.reason == "SetupError"
    assert health.evidence is None


@pytest.mark.asyncio
async def test_google_tts_requests_reconstruct_multibyte_text_within_byte_limit() -> None:
    text = "ä" * 2_499 + "€" + "🙂" * 1_251
    attempt = object.__new__(google_provider.GoogleTtsAttempt)
    attempt._utterance = SynthesisUtterance(
        uuid4(),
        uuid4(),
        text,
        "de-DE",
        "voice",
        SampleRange(0, 1),
    )
    attempt._language = "de-DE"
    attempt._voice = "voice"

    requests = [request async for request in attempt._requests()]
    chunks = [request.input.text for request in requests[1:]]

    assert requests[0].streaming_config.voice.language_code == "de-DE"
    assert requests[0].streaming_config.voice.name == "voice"
    assert "".join(chunks) == text
    assert len(chunks) > 1
    assert all(len(chunk.encode("utf-8")) <= 5_000 for chunk in chunks)


def test_linear16_decoder_strips_split_wav_header_and_preserves_pcm() -> None:
    pcm = b"\x01\x00\x02\x00\x03\x00"
    encoded = io.BytesIO()
    with wave.open(encoded, "wb") as target:
        target.setnchannels(1)
        target.setsampwidth(2)
        target.setframerate(48000)
        target.writeframes(pcm)
    payload = encoded.getvalue()
    decoder = google_provider.Linear16StreamDecoder()
    first_chunk = decoder.feed(payload[:17])
    second_chunk = decoder.feed(payload[17:43])
    final_chunk = decoder.feed(payload[43:])
    decoded = first_chunk + second_chunk + final_chunk
    decoder.finish()

    assert decoded == pcm
    assert not decoded.startswith(b"RIFF")


def test_linear16_decoder_rejects_wrong_wav_sample_rate() -> None:
    encoded = io.BytesIO()
    with wave.open(encoded, "wb") as target:
        target.setnchannels(1)
        target.setsampwidth(2)
        target.setframerate(24000)
        target.writeframes(b"\0\0")
    with pytest.raises(RuntimeError):
        google_provider.Linear16StreamDecoder().feed(encoded.getvalue())


@pytest.mark.asyncio
async def test_translation_sync_setup_failure_records_one_terminal() -> None:
    class SetupFailureChannel:
        def unary_unary(self, *_: object, **__: object) -> object:
            raise RuntimeError("translation setup failed")

    journal = RecordingJournal()
    request = TranslationRequest(
        uuid4(), uuid4(), "final", "en", "de", "hello", SampleRange(10, 20)
    )
    attempt = google_provider.GoogleTranslationAttempt(
        google_provider.GoogleConfig("project", "quota", uuid4(), "credential"),
        journal,
        SetupFailureChannel(),  # type: ignore[arg-type]
        request,
    )

    outcome = await asyncio.wait_for(attempt.result(), 1)

    assert outcome.result is None
    assert outcome.terminal.outcome is Outcome.FAILED
    assert outcome.terminal.received_output == 0
    assert len(journal.terminals) == 1
    assert [journal.rows[ref.object_id]["direction"] for ref in outcome.terminal.raw_refs] == [
        "status-in"
    ]
    status = json.loads(journal.rows[outcome.terminal.raw_refs[0].object_id]["payload"])
    assert status["code"] == "LOCAL_SETUP_ERROR"
    assert status["errorType"] == "RuntimeError"


@pytest.mark.asyncio
async def test_tts_sync_setup_failure_terminates_events_once() -> None:
    class SetupFailureChannel:
        def stream_stream(self, *_: object, **__: object) -> object:
            raise RuntimeError("tts setup failed")

    journal = RecordingJournal()
    utterance = SynthesisUtterance(uuid4(), uuid4(), "hello", "de-DE", "voice", SampleRange(10, 20))
    attempt = google_provider.GoogleTtsAttempt(
        google_provider.GoogleConfig("project", "quota", uuid4(), "credential"),
        journal,
        SetupFailureChannel(),  # type: ignore[arg-type]
        utterance,
        "de-DE",
        "voice",
    )

    events = await asyncio.wait_for(
        _collect_events(attempt.events()),
        1,
    )

    assert len(events) == 1
    assert isinstance(events[0], OperationTerminalEvent)
    terminal = events[0].terminal
    assert terminal.outcome is Outcome.FAILED
    assert terminal.received_output == terminal.emitted_output == 0
    assert len(journal.terminals) == 1
    status = json.loads(journal.rows[terminal.raw_refs[0].object_id]["payload"])
    assert status["code"] == "LOCAL_SETUP_ERROR"


async def _collect_events(events: AsyncIterator[Any]) -> list[Any]:
    return [event async for event in events]


@pytest.mark.asyncio
async def test_tts_partial_wav_header_failure_has_zero_retryable_watermarks() -> None:
    response = google_provider.cloud_tts.StreamingSynthesizeResponse(audio_content=b"RIFF")
    raw_response = bytes(
        google_provider.cloud_tts.StreamingSynthesizeResponse.pb(response).SerializeToString()
    )

    class PartialHeaderCall:
        def __init__(
            self, requests: AsyncIterator[Any], serializer: Any, deserializer: Any
        ) -> None:
            self.requests = requests
            self.serializer = serializer
            self.deserializer = deserializer

        def __aiter__(self) -> AsyncIterator[Any]:
            async def stream() -> AsyncIterator[Any]:
                async for request in self.requests:
                    self.serializer(request)
                yield self.deserializer(raw_response)
                raise RuntimeError("stream failed after partial header")

            return stream()

        async def code(self) -> str:
            return "StatusCode.INTERNAL"

        async def details(self) -> str:
            return "stream failed"

    class PartialHeaderChannel:
        def stream_stream(self, _: str, request_serializer: Any, response_deserializer: Any):
            def invoke(requests: AsyncIterator[Any], **__: Any) -> PartialHeaderCall:
                return PartialHeaderCall(requests, request_serializer, response_deserializer)

            return invoke

    journal = RecordingJournal()
    utterance = SynthesisUtterance(uuid4(), uuid4(), "hello", "de-DE", "voice", SampleRange(10, 20))
    attempt = google_provider.GoogleTtsAttempt(
        google_provider.GoogleConfig("project", "quota", uuid4(), "credential"),
        journal,
        PartialHeaderChannel(),  # type: ignore[arg-type]
        utterance,
        "de-DE",
        "voice",
    )

    events = await asyncio.wait_for(_collect_events(attempt.events()), 1)

    assert len(events) == 1
    assert isinstance(events[0], OperationTerminalEvent)
    terminal = events[0].terminal
    assert terminal.outcome is Outcome.FAILED
    assert terminal.received_output == terminal.emitted_output == 0
    assert terminal.error is not None
    assert terminal.error.provider_retry_advice is RetryAdvice.UNSPECIFIED
    assert len(journal.terminals) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["stt", "translation", "tts"])
async def test_empty_successful_capability_payload_records_failed_terminal(
    stage: str,
) -> None:
    if stage == "stt":
        payload = locations_pb2.Location(name="projects/project/locations/eu").SerializeToString()
        provider_type = google_provider.GoogleSttProvider
    elif stage == "translation":
        response = google_provider.translation_service.SupportedLanguages()
        payload = bytes(
            google_provider.translation_service.SupportedLanguages.pb(response).SerializeToString()
        )
        provider_type = google_provider.GoogleTranslationProvider
    else:
        response = google_provider.cloud_tts.ListVoicesResponse()
        payload = bytes(
            google_provider.cloud_tts.ListVoicesResponse.pb(response).SerializeToString()
        )
        provider_type = google_provider.GoogleTtsProvider

    journal = RecordingJournal()
    provider = provider_type(
        google_provider.GoogleConfig("project", "quota", uuid4(), "credential"),
        journal,
        SuccessfulUnaryChannel(payload),  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError):
        await asyncio.wait_for(provider.capabilities(), 1)

    assert len(journal.terminals) == 1
    _, terminal_payload = journal.terminals[0]
    terminal = json.loads(terminal_payload)
    assert terminal["outcome"] == "failed"
    assert terminal["stage"] == stage
    assert len(terminal["rawRefs"]) == 3
    evidence_rows = [row for row in journal.rows.values() if row.get("direction") != "terminal"]
    assert [row["direction"] for row in evidence_rows] == [
        "message-out",
        "message-in",
        "status-in",
    ]
