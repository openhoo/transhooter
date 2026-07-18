from __future__ import annotations

import asyncio
import json
import struct
import time
from collections.abc import AsyncIterable, AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from typing import cast
from uuid import UUID, uuid4

import google.auth
import grpc  # type: ignore[import-untyped]
from google.auth.transport.grpc import AuthMetadataPlugin
from google.auth.transport.requests import Request
from google.cloud.location import locations_pb2  # type: ignore[import-untyped]
from google.cloud.speech_v2.types import cloud_speech, locations_metadata
from google.cloud.texttospeech_v1.types import cloud_tts
from google.cloud.translate_v3.types import translation_service
from google.protobuf import duration_pb2

from transhooter_worker.adapters.terminal import terminal_bytes
from transhooter_worker.domain.models import (
    AudioChunk,
    AudioEvent,
    BoundaryEvent,
    BoundaryReceipt,
    ErrorKind,
    Finality,
    OperationTerminal,
    OperationTerminalEvent,
    Outcome,
    ProviderError,
    ProviderHealth,
    RawRef,
    RetryAction,
    RetryAdvice,
    RetryDecision,
    SampleRange,
    SessionTerminal,
    SessionTerminalEvent,
    StageCapabilities,
    SynthesisBoundary,
    SynthesisUtterance,
    TranscriptEvent,
    TranslationOutcome,
    TranslationRequest,
    TranslationResult,
    Transport,
    WordTiming,
)
from transhooter_worker.ports.exchange_journal import ExchangeJournal
from transhooter_worker.ports.providers import SttEvent, TtsEvent

_NANOSECONDS_PER_SECOND = 1_000_000_000


def _duration_samples(duration: duration_pb2.Duration | timedelta, sample_rate: int) -> int:
    if isinstance(duration, timedelta):
        whole_seconds = duration.days * 86400 + duration.seconds
        total_nanoseconds = whole_seconds * _NANOSECONDS_PER_SECOND + duration.microseconds * 1000
    else:
        total_nanoseconds = duration.seconds * _NANOSECONDS_PER_SECOND + duration.nanos
    scaled_nanoseconds = total_nanoseconds * sample_rate
    if scaled_nanoseconds >= 0:
        return scaled_nanoseconds // _NANOSECONDS_PER_SECOND
    return -((-scaled_nanoseconds) // _NANOSECONDS_PER_SECOND)


async def _capability_rpc(
    config: GoogleConfig,
    journal: ExchangeJournal,
    channel: grpc.aio.Channel,
    stage: str,
    path: str,
    payload: bytes,
) -> tuple[bytes, RawRef]:
    attempt_id = uuid4()
    request_ref = _append_capability_evidence(
        config=config,
        journal=journal,
        attempt_id=attempt_id,
        direction="message-out",
        media_type="application/protobuf",
        payload=payload,
    )
    try:
        callable_ = channel.unary_unary(
            path,
            request_serializer=lambda value: value,
            response_deserializer=lambda value: value,
        )
        call = callable_(
            payload,
            metadata=(("x-goog-user-project", config.quota_project),),
            timeout=20,
        )
        response = cast(bytes, await call)
        response_ref = _append_capability_evidence(
            config=config,
            journal=journal,
            attempt_id=attempt_id,
            direction="message-in",
            media_type="application/protobuf",
            payload=response,
        )
        status_ref = await _record_capability_status(
            config=config,
            journal=journal,
            attempt_id=attempt_id,
            call=call,
            request_ref=request_ref,
            response_ref=response_ref,
        )
        _record_capability_terminal(
            journal=journal,
            attempt_id=attempt_id,
            stage=stage,
            outcome="succeeded",
            raw_refs=(request_ref, response_ref, status_ref),
        )
        return response, status_ref
    except BaseException as error:
        status_ref = _append_capability_evidence(
            config=config,
            journal=journal,
            attempt_id=attempt_id,
            direction="status-in",
            media_type="application/json",
            payload=json.dumps(
                {
                    "outcome": "failed",
                    "stage": stage,
                    "errorType": type(error).__name__,
                },
                separators=(",", ":"),
            ).encode(),
        )
        _record_capability_terminal(
            journal=journal,
            attempt_id=attempt_id,
            stage=stage,
            outcome="failed",
            raw_refs=(request_ref, status_ref),
        )
        raise


def _append_capability_evidence(
    *,
    config: GoogleConfig,
    journal: ExchangeJournal,
    attempt_id: UUID,
    direction: str,
    media_type: str,
    payload: bytes,
) -> RawRef:
    return journal.append(
        meeting_id=config.meeting_id,
        attempt_id=attempt_id,
        stage="capabilities",
        transport="grpc",
        direction=direction,
        media_type=media_type,
        payload=payload,
    )


async def _record_capability_status(
    *,
    config: GoogleConfig,
    journal: ExchangeJournal,
    attempt_id: UUID,
    call: object,
    request_ref: RawRef,
    response_ref: RawRef,
) -> RawRef:
    status = json.dumps(
        {
            "code": str(await call.code()),  # type: ignore[attr-defined]
            "details": await call.details(),  # type: ignore[attr-defined]
            "initialMetadata": list(
                await call.initial_metadata()  # type: ignore[attr-defined]
            ),
            "trailingMetadata": list(
                await call.trailing_metadata()  # type: ignore[attr-defined]
            ),
            "requestRef": str(request_ref.object_id),
            "responseRef": str(response_ref.object_id),
        },
        separators=(",", ":"),
        default=str,
    ).encode()
    return _append_capability_evidence(
        config=config,
        journal=journal,
        attempt_id=attempt_id,
        direction="status-in",
        media_type="application/json",
        payload=status,
    )


def _record_capability_terminal(
    *,
    journal: ExchangeJournal,
    attempt_id: UUID,
    stage: str,
    outcome: str,
    raw_refs: tuple[RawRef, ...],
) -> None:
    journal.terminal(
        attempt_id,
        json.dumps(
            {
                "outcome": outcome,
                "stage": stage,
                "transport": "grpc",
                "rawRefs": [str(ref.object_id) for ref in raw_refs],
            },
            separators=(",", ":"),
        ).encode(),
    )


@dataclass(frozen=True, slots=True)
class GoogleConfig:
    project: str
    quota_project: str
    meeting_id: UUID
    credential_fingerprint: str
    probe_voice: str = "test-probe"
    probe_voice_locale: str = "en-US"
    speech_endpoint: str = "eu-speech.googleapis.com:443"
    translation_endpoint: str = "translate-eu.googleapis.com:443"
    tts_endpoint: str = "eu-texttospeech.googleapis.com:443"

    def __post_init__(self) -> None:
        if not self.project or not self.quota_project:
            raise ValueError("Google project and quota project are required")
        if not self.probe_voice or not self.probe_voice_locale:
            raise ValueError("Google probe voice and locale are required")
        expected = (
            "eu-speech.googleapis.com:443",
            "translate-eu.googleapis.com:443",
            "eu-texttospeech.googleapis.com:443",
        )
        if (self.speech_endpoint, self.translation_endpoint, self.tts_endpoint) != expected:
            raise ValueError("Google EU endpoints are mandatory")

    @property
    def recognizer(self) -> str:
        return f"projects/{self.project}/locations/eu/recognizers/_"

    @property
    def parent(self) -> str:
        return f"projects/{self.project}/locations/eu"

    @property
    def model(self) -> str:
        return f"projects/{self.project}/locations/eu/models/general/nmt"


class Linear16StreamDecoder:
    """Incrementally removes an optional RIFF/WAVE container and yields aligned PCM."""

    def __init__(self, expected_rate: int = 48000) -> None:
        self._buffer = bytearray()
        self._mode: str | None = None
        self._expected_rate = expected_rate
        self._data_remaining: int | None = None

    def feed(self, payload: bytes) -> bytes:
        self._buffer.extend(payload)
        if self._mode is None and not self._detect_stream_mode():
            return b""
        return self._drain_aligned_pcm()

    def _detect_stream_mode(self) -> bool:
        if len(self._buffer) < 12:
            return False
        if self._buffer[:4] != b"RIFF" or self._buffer[8:12] != b"WAVE":
            self._mode = "raw"
            return True
        return self._consume_wave_header()

    def _consume_wave_header(self) -> bool:
        offset = 12
        format_verified = False
        while True:
            if len(self._buffer) < offset + 8:
                return False
            kind = bytes(self._buffer[offset : offset + 4])
            size = struct.unpack_from("<I", self._buffer, offset + 4)[0]
            body_offset = offset + 8
            if kind == b"fmt ":
                if len(self._buffer) < body_offset + size:
                    return False
                self._verify_wave_format(body_offset, size)
                format_verified = True
            elif kind == b"data":
                if not format_verified:
                    raise RuntimeError("Google LINEAR16 WAV data precedes verified format")
                del self._buffer[:body_offset]
                self._mode = "wav"
                self._data_remaining = None if size in (0, 0xFFFFFFFF) else size
                return True
            padded_size = size + (size & 1)
            if len(self._buffer) < body_offset + padded_size:
                return False
            offset = body_offset + padded_size

    def _verify_wave_format(self, body_offset: int, size: int) -> None:
        if size < 16:
            raise RuntimeError("Google LINEAR16 WAV fmt chunk is truncated")
        encoding, channels, rate = struct.unpack_from("<HHI", self._buffer, body_offset)
        bits = struct.unpack_from("<H", self._buffer, body_offset + 14)[0]
        if (encoding, channels, rate, bits) != (
            1,
            1,
            self._expected_rate,
            16,
        ):
            raise RuntimeError("Google LINEAR16 WAV format does not match mono 48 kHz PCM16")

    def _drain_aligned_pcm(self) -> bytes:
        available = len(self._buffer)
        if self._data_remaining is not None:
            available = min(available, self._data_remaining)
        aligned = available - (available % 2)
        result = bytes(self._buffer[:aligned])
        del self._buffer[:aligned]
        if self._data_remaining is not None:
            self._data_remaining -= aligned
        return result

    def finish(self) -> None:
        if self._mode is None or self._buffer or (self._data_remaining not in (None, 0)):
            raise RuntimeError("Google LINEAR16 stream ended with incomplete container or sample")


def authenticated_channel(endpoint: str, quota_project: str) -> grpc.aio.Channel:
    credentials, _ = google.auth.default(
        scopes=("https://www.googleapis.com/auth/cloud-platform",), quota_project_id=quota_project
    )  # type: ignore[no-untyped-call]
    plugin = AuthMetadataPlugin(credentials, Request(), default_host=endpoint.removesuffix(":443"))  # type: ignore[no-untyped-call]
    composite = grpc.composite_channel_credentials(
        grpc.ssl_channel_credentials(), grpc.metadata_call_credentials(plugin)
    )
    return grpc.aio.secure_channel(endpoint, composite, options=(("grpc.enable_retries", 0),))


class GoogleSttProvider:
    def __init__(
        self,
        c: GoogleConfig,
        j: ExchangeJournal,
        channel: grpc.aio.Channel | None = None,
    ) -> None:
        self._config = c
        self._journal = j
        self._channel = channel

    async def capabilities(self) -> StageCapabilities:
        channel = self._channel or authenticated_channel(
            self._config.speech_endpoint, self._config.quota_project
        )
        request = locations_pb2.GetLocationRequest(
            name=f"projects/{self._config.project}/locations/eu"
        )
        raw, evidence = await _capability_rpc(
            self._config,
            self._journal,
            channel,
            "stt",
            "/google.cloud.location.Locations/GetLocation",
            request.SerializeToString(),
        )
        location = locations_pb2.Location.FromString(raw)
        metadata = locations_metadata.LocationsMetadata()
        if not location.metadata.Unpack(locations_metadata.LocationsMetadata.pb(metadata)):
            raise RuntimeError("Google Speech EU location omitted LocationsMetadata")
        languages = tuple(sorted(metadata.languages.models.keys()))
        models = tuple(
            sorted(
                {
                    model
                    for language in metadata.languages.models.values()
                    for model in language.model_features.keys()
                }
            )
        )
        if not languages or "long" not in models:
            raise RuntimeError("Google Speech EU location capability is incomplete")
        return StageCapabilities(
            "google",
            "stt",
            self._config.speech_endpoint,
            ("eu",),
            languages,
            models,
            (
                ("chunk_bytes", 8000),
                ("session_seconds", 270),
                ("messages_minute", 500),
                ("streams", 2),
            ),
            evidence,
        )

    async def health(self, snapshot: str) -> ProviderHealth:
        try:
            capability = await self.capabilities()
            return ProviderHealth(True, int(time.time() * 1000), None, capability.evidence)
        except Exception as exc:
            return ProviderHealth(False, int(time.time() * 1000), type(exc).__name__, None)

    async def open(
        self,
        session_id: UUID,
        language: str,
        *,
        resume_at_sample: int = 0,
        commit_watermark: int = 0,
    ) -> GoogleSttSession:
        channel = self._channel or authenticated_channel(
            self._config.speech_endpoint, self._config.quota_project
        )
        return GoogleSttSession(
            self._config,
            self._journal,
            channel,
            session_id,
            language,
            resume_at_sample,
            commit_watermark,
        )


class GoogleSttSession:
    _ROTATE = object()

    def __init__(
        self,
        c: GoogleConfig,
        j: ExchangeJournal,
        channel: grpc.aio.Channel,
        sid: UUID,
        language: str,
        resume_at_sample: int = 0,
        commit_watermark: int = 0,
    ) -> None:
        self._config = c
        self._journal = j
        self._channel = channel
        self._id = sid
        self._language = language
        self._input: asyncio.Queue[AudioChunk | object | None] = asyncio.Queue()
        self._events: asyncio.Queue[SttEvent] = asyncio.Queue()
        self._terminal: SessionTerminal | None = None
        self._lock = asyncio.Lock()
        self._refs: list[RawRef] = []
        self._accepted = 0
        self._received = 0
        self._boundaries: list[UUID] = []
        self._last_result_end = commit_watermark
        self._commit_watermark = commit_watermark
        self._recent: list[AudioChunk] = []
        self._stream_bases: list[int] = [resume_at_sample]
        self._next_rotation = resume_at_sample + 270 * 16000
        self._finishing = False
        self._task = asyncio.create_task(self._run())

    async def send_audio(self, chunk: AudioChunk) -> None:
        if len(chunk.pcm) > 8000:
            raise ValueError("Google Speech chunks must be <=8,000 bytes")
        async with self._lock:
            if self._terminal or self._finishing:
                raise RuntimeError("session terminal")
            if self._accepted == 0 and chunk.samples.start:
                self._stream_bases[0] = chunk.samples.start
            if chunk.samples.end >= self._next_rotation:
                await self._enqueue_rollover(chunk)
            await self._input.put(chunk)
            self._accepted = max(self._accepted, chunk.samples.end)
            self._remember_recent_chunk(chunk)

    async def _enqueue_rollover(self, chunk: AudioChunk) -> None:
        overlap_start = chunk.samples.start - 32000
        overlap = [item for item in self._recent if item.samples.end > overlap_start]
        stream_base = overlap[0].samples.start if overlap else chunk.samples.start
        self._stream_bases.append(stream_base)
        await self._input.put(self._ROTATE)
        for item in overlap:
            await self._input.put(item)
        self._next_rotation += 270 * 16000

    def _remember_recent_chunk(self, chunk: AudioChunk) -> None:
        self._recent.append(chunk)
        cutoff = chunk.samples.end - 32000
        self._recent = [item for item in self._recent if item.samples.end > cutoff]

    def events(self) -> AsyncIterator[SttEvent]:
        async def stream() -> AsyncIterator[SttEvent]:
            while True:
                event = await self._events.get()
                yield event
                if isinstance(event, SessionTerminalEvent):
                    return

        return stream()

    async def request_boundary(self, boundary_id: UUID) -> BoundaryReceipt:
        async with self._lock:
            if self._terminal or self._finishing:
                raise RuntimeError("session terminal")
            self._boundaries.append(boundary_id)
            return BoundaryReceipt(True, boundary_id)

    async def finish(self) -> SessionTerminal:
        async with self._lock:
            if self._terminal:
                return self._terminal
            self._finishing = True
            await self._input.put(None)
        await self._task
        assert self._terminal
        return self._terminal

    async def cancel(self) -> SessionTerminal:
        async with self._lock:
            if self._terminal:
                return self._terminal
            self._finishing = True
            self._task.cancel()
        return await self._terminalize(
            Outcome.CANCELLED,
            ProviderError(
                ErrorKind.CANCELLED,
                "session",
                RetryAdvice.NEVER,
                "cancelled",
                None,
                None,
                self._id,
                tuple(self._refs),
                "cancelled",
            ),
        )

    async def _requests(self) -> AsyncIterable[cloud_speech.StreamingRecognizeRequest]:
        config = cloud_speech.StreamingRecognitionConfig(
            config=cloud_speech.RecognitionConfig(
                explicit_decoding_config=cloud_speech.ExplicitDecodingConfig(
                    encoding=cloud_speech.ExplicitDecodingConfig.AudioEncoding.LINEAR16,
                    sample_rate_hertz=16000,
                    audio_channel_count=1,
                ),
                language_codes=[self._language],
                model="long",
                features=cloud_speech.RecognitionFeatures(enable_word_time_offsets=True),
            ),
            streaming_features=cloud_speech.StreamingRecognitionFeatures(interim_results=True),
        )
        yield cloud_speech.StreamingRecognizeRequest(
            recognizer=self._config.recognizer, streaming_config=config
        )
        while True:
            item = await self._input.get()
            if item is None or item is self._ROTATE:
                return
            assert isinstance(item, AudioChunk)
            yield cloud_speech.StreamingRecognizeRequest(audio=item.pcm)

    async def _run(self) -> None:
        try:
            stream_index = 0
            while True:
                base = self._stream_bases[stream_index]
                stream_index += 1
                call = self._channel.stream_stream(
                    "/google.cloud.speech.v2.Speech/StreamingRecognize",
                    request_serializer=self._serialize,
                    response_deserializer=self._deserialize,
                )(self._requests(), metadata=(("x-goog-user-project", self._config.quota_project),))
                async for response in call:
                    await self._emit_transcript_results(response, base)
                self._refs.append(
                    await self._record_stream_status(
                        call=call,
                        stream_index=stream_index - 1,
                        base_sample=base,
                    )
                )
                if stream_index < len(self._stream_bases):
                    continue
                if self._finishing:
                    break
                raise RuntimeError("Google Speech stream ended before rollover or finish")
            while self._boundaries:
                await self._events.put(
                    BoundaryEvent(self._boundaries.pop(0), self._last_result_end, self._refs[-1])
                )
            await self._terminalize(Outcome.SUCCEEDED, None)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            ref = self._journal.append(
                meeting_id=self._config.meeting_id,
                attempt_id=self._id,
                stage="stt",
                transport="grpc",
                direction="status-in",
                media_type="application/json",
                payload=json.dumps(
                    {"errorType": type(exc).__name__, "details": str(exc)}, separators=(",", ":")
                ).encode(),
            )
            self._refs.append(ref)
            error = ProviderError(
                ErrorKind.TRANSPORT,
                "session",
                RetryAdvice.UNSPECIFIED,
                type(exc).__name__,
                None,
                None,
                self._id,
                tuple(self._refs),
                str(exc),
            )
            await self._terminalize(Outcome.FAILED, error)

    async def _emit_transcript_results(
        self,
        response: cloud_speech.StreamingRecognizeResponse,
        base_sample: int,
    ) -> None:
        self._received += 1
        raw_ref = self._refs[-1]
        for result in response.results:
            if not result.alternatives:
                continue
            alternative = result.alternatives[0]
            words = tuple(
                WordTiming(
                    text=word.word,
                    samples=SampleRange(
                        start=base_sample + _duration_samples(word.start_offset, 16000),
                        end=max(
                            base_sample + _duration_samples(word.start_offset, 16000) + 1,
                            base_sample + _duration_samples(word.end_offset, 16000),
                        ),
                    ),
                    confidence=None,
                )
                for word in alternative.words
            )
            reported_end = base_sample + _duration_samples(result.result_end_offset, 16000)
            if result.is_final and reported_end <= self._commit_watermark:
                continue
            start = words[0].samples.start if words else max(base_sample, self._last_result_end)
            end = max(start + 1, reported_end)
            self._last_result_end = max(self._last_result_end, end)
            await self._events.put(
                TranscriptEvent(
                    samples=SampleRange(start=start, end=end),
                    revision=self._received,
                    finality=(Finality.SPAN_FINAL if result.is_final else Finality.PROVISIONAL),
                    text=alternative.transcript,
                    words=words,
                    confidence=(float(alternative.confidence) if alternative.confidence else None),
                    raw_ref=raw_ref,
                )
            )
            if result.is_final and self._boundaries:
                self._commit_watermark = end
                await self._events.put(
                    BoundaryEvent(
                        boundary_id=self._boundaries.pop(0),
                        committed_through=end,
                        raw_ref=raw_ref,
                    )
                )

    async def _record_stream_status(
        self,
        *,
        call: object,
        stream_index: int,
        base_sample: int,
    ) -> RawRef:
        payload = json.dumps(
            {
                "initial": list(
                    await call.initial_metadata()  # type: ignore[attr-defined]
                ),
                "trailing": list(
                    await call.trailing_metadata()  # type: ignore[attr-defined]
                ),
                "code": str(await call.code()),  # type: ignore[attr-defined]
                "details": await call.details(),  # type: ignore[attr-defined]
                "streamIndex": stream_index,
                "baseSample": base_sample,
            },
            default=str,
            separators=(",", ":"),
        ).encode()
        return self._journal.append(
            meeting_id=self._config.meeting_id,
            attempt_id=self._id,
            stage="stt",
            transport="grpc",
            direction="status-in",
            media_type="application/json",
            payload=payload,
        )

    def _serialize(self, req: cloud_speech.StreamingRecognizeRequest) -> bytes:
        raw = bytes(cloud_speech.StreamingRecognizeRequest.pb(req).SerializeToString())
        self._refs.append(
            self._journal.append(
                meeting_id=self._config.meeting_id,
                attempt_id=self._id,
                stage="stt",
                transport="grpc",
                direction="message-out",
                media_type="application/protobuf",
                payload=raw,
            )
        )
        return raw

    def _deserialize(self, raw: bytes) -> cloud_speech.StreamingRecognizeResponse:
        self._refs.append(
            self._journal.append(
                meeting_id=self._config.meeting_id,
                attempt_id=self._id,
                stage="stt",
                transport="grpc",
                direction="message-in",
                media_type="application/protobuf",
                payload=raw,
            )
        )
        return cast(
            cloud_speech.StreamingRecognizeResponse,
            cloud_speech.StreamingRecognizeResponse.deserialize(raw),
        )

    async def _terminalize(self, outcome: Outcome, error: ProviderError | None) -> SessionTerminal:
        if self._terminal:
            return self._terminal
        if not self._refs:
            self._refs.append(
                self._journal.append(
                    meeting_id=self._config.meeting_id,
                    attempt_id=self._id,
                    stage="stt",
                    transport="grpc",
                    direction="status-in",
                    media_type="application/json",
                    payload=b'{"code":"CANCELLED"}',
                )
            )
        self._terminal = SessionTerminal(
            terminal_id=uuid4(),
            session_id=self._id,
            outcome=outcome,
            error=error,
            accepted_input=self._accepted,
            received_output=self._received,
            emitted_output=0,
            transport=Transport.GRPC,
            raw_refs=tuple(self._refs),
        )
        self._journal.terminal(self._id, terminal_bytes(self._terminal))
        await self._events.put(SessionTerminalEvent(self._terminal))
        return self._terminal


class GoogleTranslationProvider:
    def __init__(
        self,
        c: GoogleConfig,
        j: ExchangeJournal,
        channel: grpc.aio.Channel | None = None,
    ) -> None:
        self._config = c
        self._journal = j
        self._channel = channel

    async def capabilities(self) -> StageCapabilities:
        channel = self._channel or authenticated_channel(
            self._config.translation_endpoint, self._config.quota_project
        )
        request = translation_service.GetSupportedLanguagesRequest(
            parent=f"projects/{self._config.project}/locations/eu", display_language_code="en"
        )
        payload = translation_service.GetSupportedLanguagesRequest.pb(request).SerializeToString()
        raw, evidence = await _capability_rpc(
            self._config,
            self._journal,
            channel,
            "translation",
            "/google.cloud.translation.v3.TranslationService/GetSupportedLanguages",
            payload,
        )
        response = translation_service.SupportedLanguages.deserialize(raw)
        languages = tuple(sorted(item.language_code for item in response.languages))
        if not languages:
            raise RuntimeError("Google Translation EU returned no supported languages")
        return StageCapabilities(
            "google",
            "translation",
            self._config.translation_endpoint,
            ("eu",),
            languages,
            ("general/nmt",),
            (("requests_minute", 100), ("characters_minute", 100000)),
            evidence,
        )

    async def health(self, snapshot: str) -> ProviderHealth:
        try:
            capability = await self.capabilities()
            return ProviderHealth(True, int(time.time() * 1000), None, capability.evidence)
        except Exception as exc:
            return ProviderHealth(False, int(time.time() * 1000), type(exc).__name__, None)

    async def start(self, request: TranslationRequest) -> GoogleTranslationAttempt:
        return GoogleTranslationAttempt(
            self._config,
            self._journal,
            self._channel
            or authenticated_channel(self._config.translation_endpoint, self._config.quota_project),
            request,
        )


class GoogleTranslationAttempt:
    def __init__(
        self,
        c: GoogleConfig,
        j: ExchangeJournal,
        channel: grpc.aio.Channel,
        r: TranslationRequest,
    ) -> None:
        self._config = c
        self._journal = j
        self._channel = channel
        self._request = r
        self._task: asyncio.Task[TranslationOutcome] | None = None
        self._terminal: OperationTerminal | None = None

    async def result(self) -> TranslationOutcome:
        if self._task is not None and not self._task.cancelled():
            return await self._task
        if self._terminal is not None:
            return TranslationOutcome(None, self._terminal)
        self._task = asyncio.create_task(self._run())
        return await self._task

    async def cancel(self) -> OperationTerminal:
        if self._terminal:
            return self._terminal
        if self._task:
            self._task.cancel()
        error = ProviderError(
            ErrorKind.CANCELLED,
            "operation",
            RetryAdvice.NEVER,
            "cancelled",
            None,
            None,
            self._request.attempt_id,
            (),
            "cancelled",
        )
        return self._make_terminal(Outcome.CANCELLED, (), error)

    async def _run(self) -> TranslationOutcome:
        request = self._build_request()
        refs: list[RawRef] = []
        call = self._channel.unary_unary(
            "/google.cloud.translation.v3.TranslationService/TranslateText",
            request_serializer=lambda value: self._serialize_request(value, refs),
            response_deserializer=lambda raw: self._deserialize_response(raw, refs),
        )(
            request,
            metadata=(("x-goog-user-project", self._config.quota_project),),
            timeout=20,
        )
        try:
            response = await call
            refs.append(await self._record_status(call, error=None))
            if len(response.translations) != 1:
                raise RuntimeError("Google translation response cardinality must be one")
            terminal = self._make_terminal(Outcome.SUCCEEDED, tuple(refs), None)
            result = None
            if terminal.outcome is Outcome.SUCCEEDED:
                result = TranslationResult(
                    operation_id=self._request.operation_id,
                    attempt_id=self._request.attempt_id,
                    text=response.translations[0].translated_text,
                    source_range=self._request.source_range,
                    raw_ref=refs[-2],
                )
            return TranslationOutcome(result=result, terminal=terminal)
        except Exception as exc:
            refs.append(await self._record_status(call, error=exc))
            error = ProviderError(
                kind=ErrorKind.TRANSPORT,
                scope="operation",
                provider_retry_advice=RetryAdvice.UNSPECIFIED,
                provider_code=type(exc).__name__,
                provider_request_id=None,
                retry_delay_ms=None,
                attempt_id=self._request.attempt_id,
                raw_refs=tuple(refs),
                message=str(exc),
            )
            terminal = self._make_terminal(Outcome.FAILED, tuple(refs), error)
            return TranslationOutcome(result=None, terminal=terminal)

    def _build_request(self) -> translation_service.TranslateTextRequest:
        return translation_service.TranslateTextRequest(
            parent=self._config.parent,
            contents=[self._request.text],
            mime_type="text/plain",
            source_language_code=self._request.source_language,
            target_language_code=self._request.target_language,
            model=self._config.model,
        )

    def _serialize_request(
        self,
        value: translation_service.TranslateTextRequest,
        refs: list[RawRef],
    ) -> bytes:
        raw = cast(
            bytes,
            translation_service.TranslateTextRequest.pb(value).SerializeToString(),
        )
        refs.append(
            self._journal.append(
                meeting_id=self._config.meeting_id,
                attempt_id=self._request.attempt_id,
                stage="translation",
                transport="grpc",
                direction="message-out",
                media_type="application/protobuf",
                payload=raw,
                sample_range=self._request.source_range,
            )
        )
        return raw

    def _deserialize_response(
        self, raw: bytes, refs: list[RawRef]
    ) -> translation_service.TranslateTextResponse:
        refs.append(
            self._journal.append(
                meeting_id=self._config.meeting_id,
                attempt_id=self._request.attempt_id,
                stage="translation",
                transport="grpc",
                direction="message-in",
                media_type="application/protobuf",
                payload=raw,
                sample_range=self._request.source_range,
            )
        )
        return cast(
            translation_service.TranslateTextResponse,
            translation_service.TranslateTextResponse.deserialize(raw),
        )

    async def _record_status(self, call: object, error: Exception | None) -> RawRef:
        if error is None:
            status = {
                "initial": list(
                    await call.initial_metadata()  # type: ignore[attr-defined]
                ),
                "trailing": list(
                    await call.trailing_metadata()  # type: ignore[attr-defined]
                ),
                "code": str(await call.code()),  # type: ignore[attr-defined]
                "details": await call.details(),  # type: ignore[attr-defined]
            }
            payload = json.dumps(status, default=str, separators=(",", ":")).encode()
        else:
            payload = json.dumps(
                {
                    "code": str(
                        await call.code()  # type: ignore[attr-defined]
                    ),
                    "details": str(
                        await call.details()  # type: ignore[attr-defined]
                    ),
                    "errorType": type(error).__name__,
                },
                separators=(",", ":"),
            ).encode()
        return self._journal.append(
            meeting_id=self._config.meeting_id,
            attempt_id=self._request.attempt_id,
            stage="translation",
            transport="grpc",
            direction="status-in",
            media_type="application/json",
            payload=payload,
            sample_range=self._request.source_range,
        )

    def _make_terminal(
        self, outcome: Outcome, refs: tuple[RawRef, ...], error: ProviderError | None
    ) -> OperationTerminal:
        if self._terminal:
            return self._terminal
        if not refs:
            ref = self._journal.append(
                meeting_id=self._config.meeting_id,
                attempt_id=self._request.attempt_id,
                stage="translation",
                transport="grpc",
                direction="status-in",
                media_type="application/json",
                payload=b'{"code":"CANCELLED"}',
                sample_range=self._request.source_range,
            )
            refs = (ref,)
        self._terminal = OperationTerminal(
            terminal_id=uuid4(),
            operation_id=self._request.operation_id,
            attempt_id=self._request.attempt_id,
            outcome=outcome,
            error=error,
            retry=RetryDecision(
                action=RetryAction.STOP,
                delay_ms=None,
                reason="application decides replay",
                previous_attempt_id=None,
            ),
            accepted_input=1,
            received_output=max(0, len(refs) - 2),
            emitted_output=0,
            transport=Transport.GRPC,
            raw_refs=refs,
            credential_fingerprint=self._config.credential_fingerprint,
        )
        self._journal.terminal(self._request.attempt_id, terminal_bytes(self._terminal))
        return self._terminal


class GoogleTtsProvider:
    def __init__(
        self,
        c: GoogleConfig,
        j: ExchangeJournal,
        channel: grpc.aio.Channel | None = None,
    ) -> None:
        self._config = c
        self._journal = j
        self._channel = channel

    async def capabilities(self) -> StageCapabilities:
        channel = self._channel or authenticated_channel(
            self._config.tts_endpoint, self._config.quota_project
        )
        request = cloud_tts.ListVoicesRequest()
        payload = cloud_tts.ListVoicesRequest.pb(request).SerializeToString()
        raw, evidence = await _capability_rpc(
            self._config,
            self._journal,
            channel,
            "tts",
            "/google.cloud.texttospeech.v1.TextToSpeech/ListVoices",
            payload,
        )
        response = cloud_tts.ListVoicesResponse.deserialize(raw)
        voices = tuple(sorted(voice.name for voice in response.voices if "Chirp3-HD" in voice.name))
        languages = tuple(
            sorted(
                {
                    code
                    for voice in response.voices
                    if voice.name in voices
                    for code in voice.language_codes
                }
            )
        )
        if not voices or not languages:
            raise RuntimeError("Google EU TTS returned no Chirp 3 HD capabilities")
        return StageCapabilities(
            "google",
            "tts",
            self._config.tts_endpoint,
            ("eu",),
            languages,
            ("Chirp3-HD",),
            (("sample_rate", 48000), ("sessions", 2), ("starts_minute", 40)),
            evidence,
            voices,
        )

    async def health(self, snapshot: str) -> ProviderHealth:
        attempt_id = uuid4()
        try:
            channel = self._channel or authenticated_channel(
                self._config.tts_endpoint, self._config.quota_project
            )
            utterance = SynthesisUtterance(
                uuid4(),
                attempt_id,
                "Streaming synthesis health check",
                self._config.probe_voice_locale,
                self._config.probe_voice,
                SampleRange(0, 1),
            )
            attempt = GoogleTtsAttempt(
                self._config,
                self._journal,
                channel,
                utterance,
                self._config.probe_voice_locale,
                self._config.probe_voice,
            )
            audio = False
            terminal = None
            async for event in attempt.events():
                if isinstance(event, AudioEvent):
                    audio = audio or (
                        event.sample_rate == 48000 and event.channels == 1 and bool(event.pcm)
                    )
                elif isinstance(event, OperationTerminalEvent):
                    terminal = event.terminal
            if not audio or terminal is None or terminal.outcome is not Outcome.SUCCEEDED:
                raise RuntimeError(
                    "Google EU streaming TTS probe returned no verified 48 kHz LINEAR16 audio"
                )
            return ProviderHealth(
                True,
                int(time.time() * 1000),
                None,
                terminal.raw_refs[-1] if terminal.raw_refs else None,
            )
        except Exception as exc:
            return ProviderHealth(False, int(time.time() * 1000), type(exc).__name__, None)

    async def open(self, session_id: UUID, language: str, voice: str) -> GoogleTtsSession:
        ref = self._journal.append(
            meeting_id=self._config.meeting_id,
            attempt_id=session_id,
            stage="tts",
            transport="grpc",
            direction="open",
            media_type="application/json",
            payload=json.dumps(
                {"language": language, "voice": voice}, separators=(",", ":")
            ).encode(),
        )
        return GoogleTtsSession(
            self._config,
            self._journal,
            self._channel
            or authenticated_channel(self._config.tts_endpoint, self._config.quota_project),
            session_id,
            language,
            voice,
            ref,
        )


class GoogleTtsSession:
    def __init__(
        self,
        c: GoogleConfig,
        j: ExchangeJournal,
        channel: grpc.aio.Channel,
        sid: UUID,
        language: str,
        voice: str,
        open_ref: RawRef,
    ) -> None:
        self._config = c
        self._journal = j
        self._channel = channel
        self._id = sid
        self._language = language
        self._voice = voice
        self._terminal: SessionTerminal | None = None
        self._events: asyncio.Queue[SessionTerminalEvent] = asyncio.Queue()
        self._open_ref = open_ref

    async def start(self, u: SynthesisUtterance) -> GoogleTtsAttempt:
        return GoogleTtsAttempt(
            self._config,
            self._journal,
            self._channel,
            u,
            self._language,
            self._voice,
        )

    def session_events(self) -> AsyncIterator[SessionTerminalEvent]:
        async def stream() -> AsyncIterator[SessionTerminalEvent]:
            yield await self._events.get()

        return stream()

    async def finish(self) -> SessionTerminal:
        return await self._end(Outcome.SUCCEEDED)

    async def cancel(self) -> SessionTerminal:
        return await self._end(Outcome.CANCELLED)

    async def _end(self, outcome: Outcome) -> SessionTerminal:
        if self._terminal:
            return self._terminal
        self._terminal = SessionTerminal(
            terminal_id=uuid4(),
            session_id=self._id,
            outcome=outcome,
            error=None,
            accepted_input=0,
            received_output=0,
            emitted_output=0,
            transport=Transport.GRPC,
            raw_refs=(self._open_ref,),
        )
        self._journal.terminal(self._id, terminal_bytes(self._terminal))
        await self._events.put(SessionTerminalEvent(self._terminal))
        return self._terminal


class GoogleTtsAttempt:
    def __init__(
        self,
        c: GoogleConfig,
        j: ExchangeJournal,
        channel: grpc.aio.Channel,
        u: SynthesisUtterance,
        language: str,
        voice: str,
    ) -> None:
        self._config = c
        self._journal = j
        self._channel = channel
        self._utterance = u
        self._language = language
        self._voice = voice
        self._events: asyncio.Queue[TtsEvent] = asyncio.Queue()
        self._terminal: OperationTerminal | None = None
        self._task = asyncio.create_task(self._run())

    def events(self) -> AsyncIterator[TtsEvent]:
        async def stream() -> AsyncIterator[TtsEvent]:
            while True:
                event = await self._events.get()
                yield event
                if isinstance(event, OperationTerminalEvent):
                    return

        return stream()

    async def finish(self) -> OperationTerminal:
        await self._task
        assert self._terminal
        return self._terminal

    async def cancel(self) -> OperationTerminal:
        self._task.cancel()
        error = ProviderError(
            ErrorKind.CANCELLED,
            "operation",
            RetryAdvice.NEVER,
            "cancelled",
            None,
            None,
            self._utterance.attempt_id,
            (),
            "cancelled",
        )
        return await self._end(Outcome.CANCELLED, (), 0, error)

    async def _run(self) -> None:
        refs: list[RawRef] = []
        call = self._channel.stream_stream(
            "/google.cloud.texttospeech.v1.TextToSpeech/StreamingSynthesize",
            request_serializer=lambda value: self._serialize_request(value, refs),
            response_deserializer=lambda raw: self._deserialize_response(raw, refs),
        )(
            self._requests(),
            metadata=(("x-goog-user-project", self._config.quota_project),),
            timeout=20,
        )
        emitted = 0
        sequence = 0
        decoder = Linear16StreamDecoder(48000)
        try:
            async for response in call:
                pcm = decoder.feed(response.audio_content)
                if not pcm:
                    continue
                emitted = await self._emit_audio(
                    pcm=pcm,
                    emitted=emitted,
                    sequence=sequence,
                    raw_ref=refs[-1],
                )
                sequence += 1
            decoder.finish()
            refs.append(await self._record_status(call, error=None))
            if emitted <= 0:
                raise RuntimeError("Google streaming TTS returned no audio")
            boundary = SampleRange(start=0, end=emitted)
            await self._events.put(
                SynthesisBoundary(
                    operation_id=self._utterance.operation_id,
                    samples=boundary,
                    raw_ref=refs[-1],
                )
            )
            await self._end(Outcome.SUCCEEDED, tuple(refs), emitted, None)
        except Exception as exc:
            refs.append(await self._record_status(call, error=exc))
            error = ProviderError(
                kind=ErrorKind.TRANSPORT,
                scope="operation",
                provider_retry_advice=(RetryAdvice.NEVER if emitted else RetryAdvice.UNSPECIFIED),
                provider_code=type(exc).__name__,
                provider_request_id=None,
                retry_delay_ms=None,
                attempt_id=self._utterance.attempt_id,
                raw_refs=tuple(refs),
                message=str(exc),
            )
            await self._end(Outcome.FAILED, tuple(refs), emitted, error)

    async def _requests(
        self,
    ) -> AsyncIterator[cloud_tts.StreamingSynthesizeRequest]:
        yield cloud_tts.StreamingSynthesizeRequest(
            streaming_config=cloud_tts.StreamingSynthesizeConfig(
                voice=cloud_tts.VoiceSelectionParams(
                    language_code=self._language,
                    name=self._voice,
                ),
                streaming_audio_config=cloud_tts.StreamingAudioConfig(
                    audio_encoding=cloud_tts.AudioEncoding.LINEAR16,
                    sample_rate_hertz=48000,
                ),
            )
        )
        yield cloud_tts.StreamingSynthesizeRequest(
            input=cloud_tts.StreamingSynthesisInput(text=self._utterance.text)
        )

    def _serialize_request(
        self,
        value: cloud_tts.StreamingSynthesizeRequest,
        refs: list[RawRef],
    ) -> bytes:
        raw = cast(
            bytes,
            cloud_tts.StreamingSynthesizeRequest.pb(value).SerializeToString(),
        )
        refs.append(
            self._journal.append(
                meeting_id=self._config.meeting_id,
                attempt_id=self._utterance.attempt_id,
                stage="tts",
                transport="grpc",
                direction="message-out",
                media_type="application/protobuf",
                payload=raw,
                sample_range=self._utterance.source_range,
            )
        )
        return raw

    def _deserialize_response(
        self, raw: bytes, refs: list[RawRef]
    ) -> cloud_tts.StreamingSynthesizeResponse:
        refs.append(
            self._journal.append(
                meeting_id=self._config.meeting_id,
                attempt_id=self._utterance.attempt_id,
                stage="tts",
                transport="grpc",
                direction="message-in",
                media_type="application/protobuf",
                payload=raw,
                sample_range=self._utterance.source_range,
            )
        )
        return cast(
            cloud_tts.StreamingSynthesizeResponse,
            cloud_tts.StreamingSynthesizeResponse.deserialize(raw),
        )

    async def _emit_audio(
        self,
        *,
        pcm: bytes,
        emitted: int,
        sequence: int,
        raw_ref: RawRef,
    ) -> int:
        samples = SampleRange(start=emitted, end=emitted + len(pcm) // 2)
        await self._events.put(
            AudioEvent(
                operation_id=self._utterance.operation_id,
                sequence=sequence,
                samples=samples,
                pcm=pcm,
                sample_rate=48000,
                channels=1,
                raw_ref=raw_ref,
            )
        )
        return samples.end

    async def _record_status(self, call: object, error: Exception | None) -> RawRef:
        if error is None:
            status = {
                "initial": list(
                    await call.initial_metadata()  # type: ignore[attr-defined]
                ),
                "trailing": list(
                    await call.trailing_metadata()  # type: ignore[attr-defined]
                ),
                "code": str(await call.code()),  # type: ignore[attr-defined]
                "details": await call.details(),  # type: ignore[attr-defined]
            }
            payload = json.dumps(status, default=str, separators=(",", ":")).encode()
        else:
            payload = json.dumps(
                {
                    "code": str(
                        await call.code()  # type: ignore[attr-defined]
                    ),
                    "details": str(
                        await call.details()  # type: ignore[attr-defined]
                    ),
                    "errorType": type(error).__name__,
                },
                separators=(",", ":"),
            ).encode()
        return self._journal.append(
            meeting_id=self._config.meeting_id,
            attempt_id=self._utterance.attempt_id,
            stage="tts",
            transport="grpc",
            direction="status-in",
            media_type="application/json",
            payload=payload,
            sample_range=self._utterance.source_range,
        )

    async def _end(
        self,
        outcome: Outcome,
        refs: tuple[RawRef, ...],
        emitted: int,
        error: ProviderError | None,
    ) -> OperationTerminal:
        if self._terminal:
            return self._terminal
        if not refs:
            ref = self._journal.append(
                meeting_id=self._config.meeting_id,
                attempt_id=self._utterance.attempt_id,
                stage="tts",
                transport="grpc",
                direction="status-in",
                media_type="application/json",
                payload=b'{"code":"CANCELLED"}',
                sample_range=self._utterance.source_range,
            )
            refs = (ref,)
        self._terminal = OperationTerminal(
            terminal_id=uuid4(),
            operation_id=self._utterance.operation_id,
            attempt_id=self._utterance.attempt_id,
            outcome=outcome,
            error=error,
            retry=RetryDecision(
                action=RetryAction.STOP,
                delay_ms=None,
                reason="application decides replay",
                previous_attempt_id=None,
            ),
            accepted_input=1,
            received_output=max(0, len(refs) - 2),
            emitted_output=emitted,
            transport=Transport.GRPC,
            raw_refs=refs,
            credential_fingerprint=self._config.credential_fingerprint,
        )
        self._journal.terminal(self._utterance.attempt_id, terminal_bytes(self._terminal))
        await self._events.put(OperationTerminalEvent(self._terminal))
        return self._terminal
