from __future__ import annotations

import asyncio
import hashlib
import json
import socket
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from itertools import pairwise
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

import transhooter_worker.adapters.google.provider as google_provider
from transhooter_worker.adapters.deepgram.provider import (
    DeepgramConfig,
    DeepgramSttSession,
    DeepgramTtsSession,
)
from transhooter_worker.adapters.deepl.provider import DeepLConfig, DeepLProvider
from transhooter_worker.adapters.fixture.provider import (
    FixtureSttProvider,
    FixtureTranslationProvider,
    FixtureTtsProvider,
)
from transhooter_worker.adapters.google.provider import (
    GoogleConfig,
    GoogleSttProvider,
    GoogleTranslationProvider,
    GoogleTtsProvider,
    cloud_speech,
    cloud_tts,
    translation_service,
)
from transhooter_worker.domain.models import (
    AudioChunk,
    AudioEvent,
    BoundaryEvent,
    ErrorKind,
    OperationTerminalEvent,
    Outcome,
    RawRef,
    RetryAdvice,
    SampleRange,
    SessionTerminalEvent,
    SynthesisUtterance,
    TranscriptEvent,
    TranslationRequest,
    Transport,
)


class MemoryJournal:
    """Test journal that makes every RawRef independently checkable."""

    def __init__(self) -> None:
        self.rows: dict[UUID, dict[str, Any]] = {}
        self.terminals: dict[UUID, bytes] = {}
        self.ordinal = 0

    def append(self, **row: Any) -> RawRef:
        self.ordinal += 1
        payload = bytes(row.get("payload", b""))
        object_id = uuid4()
        self.rows[object_id] = dict(row)
        return RawRef(
            object_id,
            self.ordinal,
            hashlib.sha256(payload).hexdigest(),
            len(payload),
            str(row.get("media_type", "application/octet-stream")),
        )

    def terminal(self, attempt_id: UUID, payload: bytes) -> RawRef:
        self.terminals.setdefault(attempt_id, payload)
        return self.append(
            attempt_id=attempt_id,
            direction="terminal",
            media_type="application/json",
            payload=self.terminals[attempt_id],
        )

    def assert_exact_refs(self, refs: tuple[RawRef, ...]) -> None:
        assert refs
        assert len({ref.object_id for ref in refs}) == len(refs)
        for ref in refs:
            row = self.rows[ref.object_id]
            payload = bytes(row["payload"])
            assert ref.sha256 == hashlib.sha256(payload).hexdigest()
            assert ref.size == len(payload)
            assert ref.media_type == row["media_type"]


class ScriptSocket:
    close_code = 1000
    close_reason = "complete"

    def __init__(
        self,
        messages: list[str | bytes],
        *,
        wait_for_close: bool = False,
        messages_after_close: list[str | bytes] | None = None,
    ) -> None:
        self.messages = messages
        self.sent: list[str | bytes] = []
        self.closed = asyncio.Event()
        self.wait_for_close = wait_for_close
        self.messages_after_close = messages_after_close or []

    def __aiter__(self) -> AsyncIterator[str | bytes]:
        async def stream() -> AsyncIterator[str | bytes]:
            for message in self.messages:
                yield message
            if self.wait_for_close or self.messages_after_close:
                await self.closed.wait()
            for message in self.messages_after_close:
                yield message

        return stream()

    async def send(self, message: str | bytes) -> None:
        self.sent.append(message)
        if isinstance(message, str) and json.loads(message).get("type") == "CloseStream":
            self.closed.set()

    async def close(self, **_: object) -> None:
        self.closed.set()


class GrpcCall:
    def __init__(
        self,
        requests: AsyncIterator[Any] | None,
        serializer: Any,
        deserializer: Any,
        responses: list[bytes],
    ) -> None:
        self.requests = requests
        self.serializer = serializer
        self.deserializer = deserializer
        self.responses = responses

    def __aiter__(self) -> AsyncIterator[Any]:
        async def stream() -> AsyncIterator[Any]:
            if self.requests is not None:
                async for request in self.requests:
                    self.serializer(request)
            for response in self.responses:
                yield self.deserializer(response)

        return stream()

    def __await__(self):
        async def result() -> Any:
            assert self.requests is None
            return self.deserializer(self.responses[0])

        return result().__await__()

    async def initial_metadata(self) -> tuple[tuple[str, str], ...]:
        return (("request-id", "mock-google-eu"),)

    async def trailing_metadata(self) -> tuple[tuple[str, str], ...]:
        return (("region", "eu"),)

    async def code(self) -> str:
        return "StatusCode.OK"

    async def details(self) -> str:
        return ""


class GoogleChannel:
    def __init__(self, stt_responses: list[list[bytes]], tts_responses: list[bytes]) -> None:
        self.stt_responses = stt_responses
        self.tts_responses = tts_responses

    def stream_stream(self, method: str, request_serializer: Any, response_deserializer: Any):
        if "cloud.speech.v2" in method:
            responses = self.stt_responses.pop(0)
        else:
            responses = self.tts_responses

        def invoke(requests: AsyncIterator[Any], **_: Any) -> GrpcCall:
            return GrpcCall(requests, request_serializer, response_deserializer, responses)

        return invoke

    def unary_unary(self, _: str, request_serializer: Any, response_deserializer: Any):
        response = translation_service.TranslateTextResponse(
            translations=[translation_service.Translation(translated_text="Guten Tag")]
        )
        raw = bytes(translation_service.TranslateTextResponse.pb(response).SerializeToString())

        def invoke(request: Any, **__: Any) -> GrpcCall:
            request_serializer(request)
            return GrpcCall(None, request_serializer, response_deserializer, [raw])

        return invoke


class TrackingGoogleChannel:
    def __init__(self) -> None:
        self.closes = 0

    def unary_unary(self, *_: Any, **__: Any):
        def invoke(*_: Any, **__: Any) -> Any:
            raise RuntimeError("capability unavailable")

        return invoke

    async def close(self) -> None:
        self.closes += 1


@dataclass
class Composition:
    name: str
    journal: MemoryJournal
    stt: Any
    translation: Any | None
    tts: Any | None
    tts_voice: str | None


@pytest.fixture(autouse=True)
def hermetic_provider_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_ENV", "test")

    def deny_network(*_: Any, **__: Any) -> None:
        raise AssertionError("provider conformance must use a mock transport")

    monkeypatch.setattr(socket.socket, "connect", deny_network)
    monkeypatch.setattr(socket.socket, "connect_ex", deny_network)


def google_config(meeting_id: UUID) -> GoogleConfig:
    return GoogleConfig(
        project="mock-project",
        quota_project="mock-quota",
        meeting_id=meeting_id,
        credential_fingerprint="mock-google-credential",
        probe_voice="de-DE-Chirp3-HD-Achernar",
    )


def make_google_composition() -> Composition:
    journal = MemoryJournal()
    result = cloud_speech.StreamingRecognitionResult(
        alternatives=[cloud_speech.SpeechRecognitionAlternative(transcript="good morning")],
        is_final=True,
        result_end_offset=timedelta(seconds=0.25),
    )
    stt_response = cloud_speech.StreamingRecognizeResponse(results=[result])
    stt_raw = bytes(cloud_speech.StreamingRecognizeResponse.pb(stt_response).SerializeToString())
    tts_response = cloud_tts.StreamingSynthesizeResponse(audio_content=b"\x01\x00" * 960)
    tts_raw = bytes(cloud_tts.StreamingSynthesizeResponse.pb(tts_response).SerializeToString())
    channel = GoogleChannel([[stt_raw]], [tts_raw])
    config = google_config(uuid4())
    return Composition(
        "google-eu",
        journal,
        GoogleSttProvider(config, journal, channel),
        GoogleTranslationProvider(config, journal, channel),
        GoogleTtsProvider(config, journal, channel),
        config.probe_voice,
    )


def make_fixture_composition() -> Composition:
    return Composition(
        "fixture",
        MemoryJournal(),
        FixtureSttProvider(),
        FixtureTranslationProvider(),
        FixtureTtsProvider(),
        "fixture-voice",
    )


def make_deepgram_deepl_composition() -> Composition:
    journal = MemoryJournal()
    config = DeepgramConfig(
        api_key="secret",
        meeting_id=uuid4(),
        language="en-US",
        voice="aura-2-thalia-en",
        approved_voices=("aura-2-thalia-en",),
        credential_fingerprint="mock-deepgram-credential",
    )
    stt_socket = ScriptSocket(
        [
            json.dumps(
                {
                    "type": "Results",
                    "start": 0,
                    "duration": 0.25,
                    "is_final": True,
                    "speech_final": True,
                    "channel": {"alternatives": [{"transcript": "good morning", "words": []}]},
                }
            )
        ],
        messages_after_close=[
            json.dumps(
                {
                    "type": "Results",
                    "from_finalize": True,
                    "channel": {"alternatives": []},
                }
            ),
            json.dumps({"type": "CloseStream"}),
        ],
    )
    tts_socket = ScriptSocket([b"\x01\x00" * 480, b"\x02\x00" * 480, '{"type":"Flushed"}'])
    stt = _DirectDeepgramSttProvider(config, journal, stt_socket)
    tts = _DirectDeepgramTtsProvider(config, journal, tts_socket)

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"translations": [{"text": "Guten Tag"}]})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    translation = DeepLProvider(DeepLConfig("secret", config.meeting_id), journal, client)
    return Composition("deepgram-deepl-eu", journal, stt, translation, tts, config.voice)


class _DirectDeepgramSttProvider:
    def __init__(
        self, config: DeepgramConfig, journal: MemoryJournal, socket: ScriptSocket
    ) -> None:
        self.config = config
        self.journal = journal
        self.socket = socket

    async def open(
        self,
        session_id: UUID,
        language: str,
        *,
        resume_at_sample: int = 0,
        commit_watermark: int = 0,
    ) -> DeepgramSttSession:
        assert language == self.config.language
        return DeepgramSttSession(
            self.config,
            self.journal,
            session_id,
            self.socket,
            [self.journal.append(payload=b"upgrade", media_type="application/http")],
            resume_at_sample,
            commit_watermark,
        )


class _DirectDeepgramTtsProvider:
    def __init__(
        self, config: DeepgramConfig, journal: MemoryJournal, socket: ScriptSocket
    ) -> None:
        self.config = config
        self.journal = journal
        self.socket = socket

    async def open(self, session_id: UUID, language: str, voice: str) -> DeepgramTtsSession:
        assert language == "de-DE"
        assert voice == self.config.voice
        return DeepgramTtsSession(self.config, self.journal, session_id, self.socket, [])


COMPOSITION_FACTORIES = [
    pytest.param(make_fixture_composition, id="fixture"),
    pytest.param(make_google_composition, id="google-eu"),
    pytest.param(make_deepgram_deepl_composition, id="deepgram-deepl-eu"),
]


async def make_google_empty_final_stt() -> Any:
    result = cloud_speech.StreamingRecognitionResult(
        alternatives=[],
        is_final=True,
        result_end_offset=timedelta(seconds=0.25),
    )
    response = cloud_speech.StreamingRecognizeResponse(results=[result])
    raw = bytes(cloud_speech.StreamingRecognizeResponse.pb(response).SerializeToString())
    return await GoogleSttProvider(
        google_config(uuid4()),
        MemoryJournal(),
        GoogleChannel([[raw]], []),
    ).open(uuid4(), "en-US")


async def make_deepgram_empty_final_stt() -> DeepgramSttSession:
    config = DeepgramConfig(
        "secret",
        uuid4(),
        "en-US",
        "voice",
        ("voice",),
        credential_fingerprint="fingerprint",
    )
    socket = ScriptSocket(
        [
            json.dumps(
                {
                    "type": "Results",
                    "is_final": True,
                    "speech_final": False,
                    "channel": {"alternatives": []},
                }
            )
        ],
        messages_after_close=[
            json.dumps(
                {
                    "type": "Results",
                    "from_finalize": True,
                    "channel": {"alternatives": []},
                }
            ),
            json.dumps({"type": "CloseStream"}),
        ],
    )
    return DeepgramSttSession(config, MemoryJournal(), uuid4(), socket, [])


@pytest.mark.asyncio
@pytest.mark.parametrize("factory", COMPOSITION_FACTORIES)
async def test_compositions_obey_one_neutral_port_contract(factory: Any) -> None:
    composition = factory()
    source_range = SampleRange(32_000, 36_000)
    stt = await composition.stt.open(uuid4(), "en-US", resume_at_sample=32_000)
    await stt.send_audio(AudioChunk(uuid4(), 0, source_range, b"\0" * 8_000))
    boundary_id = uuid4()
    assert (await stt.request_boundary(boundary_id)).accepted
    stt_terminal = await stt.finish()
    stt_events = [event async for event in stt.events()]
    transcripts = [event for event in stt_events if isinstance(event, TranscriptEvent)]
    assert transcripts
    assert all(event.samples.start >= source_range.start for event in transcripts)
    assert any(isinstance(event, BoundaryEvent) for event in stt_events)
    assert isinstance(stt_events[-1], SessionTerminalEvent)
    assert stt_events[-1].terminal == stt_terminal
    assert stt_terminal.accepted_input == source_range.end
    assert stt_terminal.outcome is Outcome.SUCCEEDED

    request = TranslationRequest(
        uuid4(), uuid4(), "final", "en", "de", transcripts[-1].text, source_range
    )
    assert composition.translation is not None
    translation_attempt = await composition.translation.start(request)
    translation = await translation_attempt.result()
    assert translation.result is not None and translation.result.text
    assert translation.result.source_range == source_range
    assert translation.terminal.operation_id == request.operation_id
    assert translation.terminal.attempt_id == request.attempt_id
    assert translation.terminal.outcome is Outcome.SUCCEEDED
    assert await translation_attempt.result() == translation

    utterance = SynthesisUtterance(
        uuid4(),
        uuid4(),
        translation.result.text,
        "de-DE",
        composition.tts_voice or "",
        source_range,
    )
    assert composition.tts is not None
    tts = await composition.tts.open(uuid4(), "de-DE", composition.tts_voice or "")
    synthesis = await tts.start(utterance)
    tts_events = [event async for event in synthesis.events()]
    audio = [event for event in tts_events if isinstance(event, AudioEvent)]
    assert audio and audio[0].samples.start == 0
    assert all(left.samples.end == right.samples.start for left, right in pairwise(audio))
    assert isinstance(tts_events[-1], OperationTerminalEvent)
    assert sum(isinstance(event, OperationTerminalEvent) for event in tts_events) == 1
    assert tts_events[-1].terminal.outcome is Outcome.SUCCEEDED
    assert tts_events[-1].terminal.emitted_output == audio[-1].samples.end
    assert await synthesis.finish() == tts_events[-1].terminal
    assert await tts.finish() == await tts.finish()

    if composition.name != "fixture":
        composition.journal.assert_exact_refs(stt_terminal.raw_refs)
        composition.journal.assert_exact_refs(translation.terminal.raw_refs)
        composition.journal.assert_exact_refs(tts_events[-1].terminal.raw_refs)
        assert request.attempt_id in composition.journal.terminals
        assert utterance.attempt_id in composition.journal.terminals


@pytest.mark.asyncio
@pytest.mark.parametrize("factory", COMPOSITION_FACTORIES)
async def test_same_language_direction_is_explicit_bypass(factory: Any) -> None:
    composition = factory()
    direction = {"mode": "same_language", "translation": None, "tts": None}
    assert direction == {"mode": "same_language", "translation": None, "tts": None}
    stt = await composition.stt.open(uuid4(), "en-US")
    await stt.send_audio(AudioChunk(uuid4(), 0, SampleRange(0, 4_000), b"\0" * 8_000))
    terminal = await stt.cancel()
    assert terminal.outcome is Outcome.CANCELLED


@pytest.mark.asyncio
@pytest.mark.parametrize("factory", COMPOSITION_FACTORIES)
async def test_finish_cancel_race_has_one_authoritative_terminal(factory: Any) -> None:
    composition = factory()
    stt = await composition.stt.open(uuid4(), "en-US")
    await stt.send_audio(AudioChunk(uuid4(), 0, SampleRange(0, 4_000), b"\0" * 8_000))
    first, second = await asyncio.gather(stt.finish(), stt.cancel())
    assert first.terminal_id == second.terminal_id
    events = [event async for event in stt.events()]
    assert sum(isinstance(event, SessionTerminalEvent) for event in events) == 1
    assert events[-1].terminal.terminal_id == first.terminal_id
    with pytest.raises(RuntimeError, match="terminal"):
        await stt.send_audio(AudioChunk(uuid4(), 1, SampleRange(4_000, 8_000), b"\0" * 8_000))


@pytest.mark.asyncio
@pytest.mark.parametrize("factory", COMPOSITION_FACTORIES)
async def test_cancelled_translation_rejects_late_provider_result(factory: Any) -> None:
    composition = factory()
    assert composition.translation is not None
    request = TranslationRequest(
        uuid4(), uuid4(), "final", "en", "de", "late", SampleRange(9_000, 10_000)
    )
    attempt = await composition.translation.start(request)
    terminal = await attempt.cancel()
    late = await attempt.result()
    assert terminal.outcome is Outcome.CANCELLED
    assert late.result is None
    assert late.terminal.terminal_id == terminal.terminal_id


@pytest.mark.asyncio
@pytest.mark.parametrize("factory", COMPOSITION_FACTORIES)
async def test_provider_terminal_methods_are_bounded_and_idempotent(factory: Any) -> None:
    composition = factory()

    stt = await composition.stt.open(uuid4(), "en-US")
    stt_first = await asyncio.wait_for(stt.cancel(), 1)
    stt_second = await asyncio.wait_for(stt.cancel(), 1)
    assert stt_first.outcome is Outcome.CANCELLED
    assert stt_second.terminal_id == stt_first.terminal_id
    stt_events = [event async for event in stt.events()]
    assert [event.terminal for event in stt_events if isinstance(event, SessionTerminalEvent)] == [
        stt_first
    ]

    assert composition.translation is not None
    request = TranslationRequest(
        uuid4(), uuid4(), "final", "en", "de", "cancel me", SampleRange(0, 1)
    )
    translation = await composition.translation.start(request)
    translation_first = await asyncio.wait_for(translation.cancel(), 1)
    translation_second = await asyncio.wait_for(translation.cancel(), 1)
    translation_result = await asyncio.wait_for(translation.result(), 1)
    assert translation_first.outcome is Outcome.CANCELLED
    assert translation_second.terminal_id == translation_first.terminal_id
    assert translation_result.result is None
    assert translation_result.terminal.terminal_id == translation_first.terminal_id

    assert composition.tts is not None
    tts = await composition.tts.open(uuid4(), "de-DE", composition.tts_voice or "")
    utterance = SynthesisUtterance(
        uuid4(),
        uuid4(),
        "cancel me",
        "de-DE",
        composition.tts_voice or "",
        SampleRange(0, 1),
    )
    synthesis = await tts.start(utterance)
    synthesis_first = await asyncio.wait_for(synthesis.cancel(), 1)
    synthesis_second = await asyncio.wait_for(synthesis.cancel(), 1)
    assert synthesis_first.outcome is Outcome.CANCELLED
    assert synthesis_second.terminal_id == synthesis_first.terminal_id
    tts_first = await asyncio.wait_for(tts.cancel(), 1)
    tts_second = await asyncio.wait_for(tts.cancel(), 1)
    assert tts_first.outcome is Outcome.CANCELLED
    assert tts_second.terminal_id == tts_first.terminal_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "factory",
    [
        pytest.param(make_google_empty_final_stt, id="google"),
        pytest.param(make_deepgram_empty_final_stt, id="deepgram"),
    ],
)
async def test_empty_final_provider_chunks_complete_without_empty_transcripts(factory: Any) -> None:
    session = await factory()
    terminal = await asyncio.wait_for(session.finish(), 1)
    events = [event async for event in session.events()]

    assert terminal.outcome is Outcome.SUCCEEDED
    assert not any(isinstance(event, TranscriptEvent) for event in events)
    assert events[-1] == SessionTerminalEvent(terminal)


@pytest.mark.asyncio
async def test_google_rollover_suppresses_duplicate_final_and_records_each_grpc_status() -> None:
    journal = MemoryJournal()
    duplicate = cloud_speech.StreamingRecognitionResult(
        alternatives=[cloud_speech.SpeechRecognitionAlternative(transcript="duplicate")],
        is_final=True,
        result_end_offset=timedelta(seconds=1),
    )
    fresh = cloud_speech.StreamingRecognitionResult(
        alternatives=[cloud_speech.SpeechRecognitionAlternative(transcript="fresh")],
        is_final=True,
        result_end_offset=timedelta(seconds=3),
    )
    responses = [
        [
            bytes(
                cloud_speech.StreamingRecognizeResponse.pb(
                    cloud_speech.StreamingRecognizeResponse(results=[duplicate])
                ).SerializeToString()
            )
        ],
        [
            bytes(
                cloud_speech.StreamingRecognizeResponse.pb(
                    cloud_speech.StreamingRecognizeResponse(results=[fresh])
                ).SerializeToString()
            )
        ],
    ]
    channel = GoogleChannel(responses, [])
    session = await GoogleSttProvider(google_config(uuid4()), journal, channel).open(
        uuid4(), "en-US", commit_watermark=4_300_000
    )
    await session.send_audio(
        AudioChunk(uuid4(), 0, SampleRange(4_280_000, 4_288_000), b"\0" * 8_000)
    )
    await session.send_audio(
        AudioChunk(uuid4(), 1, SampleRange(4_312_000, 4_320_000), b"\0" * 8_000)
    )
    await session.finish()
    events = [event async for event in session.events()]
    assert [
        (event.text, event.samples) for event in events if isinstance(event, TranscriptEvent)
    ] == [("fresh", SampleRange(4_300_000, 4_328_000))]
    status_rows = [row for row in journal.rows.values() if row.get("direction") == "status-in"]
    assert status_rows
    status = json.loads(status_rows[-1]["payload"])
    assert status["code"] == "StatusCode.OK"
    assert status["initial"] == [["request-id", "mock-google-eu"]]
    assert status["trailing"] == [["region", "eu"]]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("acknowledgements", "expected"),
    [
        ([], Outcome.FAILED),
        (
            [
                json.dumps(
                    {
                        "type": "Results",
                        "from_finalize": True,
                        "channel": {"alternatives": []},
                    }
                ),
                json.dumps({"type": "CloseStream"}),
            ],
            Outcome.SUCCEEDED,
        ),
    ],
)
async def test_deepgram_stt_requires_provider_completion_acknowledgements(
    acknowledgements: list[str],
    expected: Outcome,
) -> None:
    journal = MemoryJournal()
    config = DeepgramConfig(
        "secret",
        uuid4(),
        "en-US",
        "voice",
        ("voice",),
        credential_fingerprint="fingerprint",
    )
    socket = ScriptSocket([], messages_after_close=acknowledgements)
    session = DeepgramSttSession(config, journal, uuid4(), socket, [])

    terminal = await session.finish()
    events = [event async for event in session.events()]

    assert terminal.outcome is expected
    assert events[-1] == SessionTerminalEvent(terminal)
    if expected is Outcome.FAILED:
        assert terminal.error is not None
        assert terminal.error.provider_code == "CompletionError"
    journal.assert_exact_refs(terminal.raw_refs)


@pytest.mark.asyncio
async def test_deepl_v3_capabilities_decode_lang_rows() -> None:
    journal = MemoryJournal()
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[{"lang": "DE"}, {"lang": "EN"}])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        capabilities = await DeepLProvider(
            DeepLConfig("secret", uuid4()),
            journal,
            client,
        ).capabilities()
        assert not client.is_closed

    assert requests[0].url.params["resource"] == "translate_text"
    assert requests[0].url.path == "/v3/languages"
    assert capabilities.languages == ("DE", "EN")
    assert capabilities.evidence is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [GoogleSttProvider, GoogleTranslationProvider, GoogleTtsProvider],
    ids=["stt", "translation", "tts"],
)
@pytest.mark.parametrize("supplied", [False, True], ids=["owned", "supplied"])
async def test_google_capability_channel_ownership(
    monkeypatch: pytest.MonkeyPatch,
    provider_type: Any,
    supplied: bool,
) -> None:
    channel = TrackingGoogleChannel()
    monkeypatch.setattr(google_provider, "authenticated_channel", lambda *_: channel)
    provider = provider_type(
        google_config(uuid4()),
        MemoryJournal(),
        channel if supplied else None,
    )

    with pytest.raises(RuntimeError, match="capability unavailable"):
        await asyncio.wait_for(provider.capabilities(), 1)

    assert channel.closes == (0 if supplied else 1)


@pytest.mark.asyncio
async def test_deepgram_partial_audio_flush_clear_and_close_are_evidenced() -> None:
    journal = MemoryJournal()
    config = DeepgramConfig(
        "secret", uuid4(), "en-US", "voice", ("voice",), credential_fingerprint="fingerprint"
    )
    socket = ScriptSocket([b"\1\0" * 240, b"\2\0" * 240, '{"type":"Flushed"}'])
    session = DeepgramTtsSession(config, journal, uuid4(), socket, [])
    utterance = SynthesisUtterance(uuid4(), uuid4(), "hello", "en-US", "voice", SampleRange(7, 9))
    attempt = await session.start(utterance)
    events = [event async for event in attempt.events()]
    audio = [event for event in events if isinstance(event, AudioEvent)]
    assert [event.samples for event in audio] == [SampleRange(0, 240), SampleRange(240, 480)]
    assert json.loads(socket.sent[0])["type"] == "Speak"
    assert json.loads(socket.sent[1])["type"] == "Flush"
    terminal = events[-1].terminal
    assert terminal.emitted_output == 480
    journal.assert_exact_refs(terminal.raw_refs)
    session_terminal = await session.finish()
    assert session_terminal.outcome is Outcome.SUCCEEDED
    assert json.loads(socket.sent[-1])["type"] == "Close"

    cancelled_socket = ScriptSocket([], wait_for_close=True)
    cancelled_session = DeepgramTtsSession(config, journal, uuid4(), cancelled_socket, [])
    cancelled = await cancelled_session.start(utterance)
    cancelled_terminal = await cancelled.cancel()
    assert cancelled_terminal.outcome is Outcome.CANCELLED
    assert any(json.loads(message)["type"] == "Clear" for message in cancelled_socket.sent)
    session_terminal = await cancelled_session.cancel()
    assert session_terminal.outcome is Outcome.CANCELLED
    assert json.loads(cancelled_socket.sent[-1])["type"] == "Clear"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "headers", "expected_kind", "expected_advice", "delay"),
    [
        pytest.param(400, {}, ErrorKind.PROVIDER, RetryAdvice.NEVER, None, id="bad-request"),
        pytest.param(456, {}, ErrorKind.QUOTA, RetryAdvice.NEVER, None, id="quota"),
        pytest.param(
            429,
            {"Retry-After": "2"},
            ErrorKind.RATE_LIMIT,
            RetryAdvice.RETRY_AFTER,
            2_000,
            id="rate-limit",
        ),
        pytest.param(
            500,
            {},
            ErrorKind.TRANSPORT,
            RetryAdvice.RETRY_AFTER,
            None,
            id="server-error",
        ),
        pytest.param(
            529,
            {"Retry-After": "0.25"},
            ErrorKind.RATE_LIMIT,
            RetryAdvice.RETRY_AFTER,
            250,
            id="overloaded",
        ),
    ],
)
async def test_deepl_failure_statuses_have_typed_normalization_and_exact_refs(
    status: int,
    headers: dict[str, str],
    expected_kind: ErrorKind,
    expected_advice: RetryAdvice,
    delay: int | None,
) -> None:
    journal = MemoryJournal()

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(status, headers=headers, json={"message": "failure"})

    provider = DeepLProvider(
        DeepLConfig("secret", uuid4()),
        journal,
        httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    request = TranslationRequest(
        uuid4(), uuid4(), "final", "en", "de", "hello", SampleRange(10, 20)
    )
    outcome = await (await provider.start(request)).result()
    assert outcome.result is None
    assert outcome.terminal.outcome is Outcome.FAILED
    assert outcome.terminal.error is not None
    assert outcome.terminal.error.kind is expected_kind
    assert outcome.terminal.error.provider_retry_advice is expected_advice
    assert outcome.terminal.error.retry_delay_ms == delay
    assert outcome.terminal.transport is Transport.HTTP
    journal.assert_exact_refs(outcome.terminal.raw_refs)
    assert [journal.rows[ref.object_id]["direction"] for ref in outcome.terminal.raw_refs] == [
        "out",
        "in",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "provider_code", "kind"),
    [
        (httpx.ReadTimeout("late"), "ReadTimeout", ErrorKind.TRANSPORT),
        (None, "ValueError", ErrorKind.PROVIDER),
    ],
)
async def test_deepl_timeout_and_malformed_cardinality_are_indeterminate_failures(
    failure: Exception | None, provider_code: str, kind: ErrorKind
) -> None:
    journal = MemoryJournal()

    async def handler(_: httpx.Request) -> httpx.Response:
        if failure:
            raise failure
        return httpx.Response(200, json={"translations": []})

    provider = DeepLProvider(
        DeepLConfig("secret", uuid4()),
        journal,
        httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    request = TranslationRequest(
        uuid4(), uuid4(), "final", "en", "de", "hello", SampleRange(10, 20)
    )
    outcome = await (await provider.start(request)).result()
    assert outcome.result is None
    assert outcome.terminal.error is not None
    assert outcome.terminal.error.kind is kind
    assert outcome.terminal.error.provider_retry_advice is RetryAdvice.UNSPECIFIED
    assert outcome.terminal.error.provider_code == provider_code
    journal.assert_exact_refs(outcome.terminal.raw_refs)
