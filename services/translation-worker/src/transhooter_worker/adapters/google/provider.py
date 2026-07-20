from __future__ import annotations

import asyncio
import json
import struct
import time
from collections.abc import AsyncIterable, AsyncIterator, Awaitable
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
_TTS_MESSAGE_BYTE_LIMIT = 5_000


def _tts_text_messages(text: str) -> tuple[str, ...]:
    if not text:
        return ("",)
    chunks: list[str] = []
    current: list[str] = []
    current_bytes = 0
    for character in text:
        encoded_bytes = len(character.encode("utf-8"))
        if current and current_bytes + encoded_bytes > _TTS_MESSAGE_BYTE_LIMIT:
            chunks.append("".join(current))
            current = []
            current_bytes = 0
        current.append(character)
        current_bytes += encoded_bytes
    chunks.append("".join(current))
    return tuple(chunks)


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


@dataclass(frozen=True, slots=True)
class _CapabilityRpcResult:
    payload: bytes
    attempt_id: UUID
    raw_refs: tuple[RawRef, ...]

    @property
    def evidence(self) -> RawRef:
        return self.raw_refs[-1]


async def _capability_rpc(
    config: GoogleConfig,
    journal: ExchangeJournal,
    channel: grpc.aio.Channel,
    stage: str,
    path: str,
    payload: bytes,
) -> _CapabilityRpcResult:
    attempt_id = uuid4()
    request_ref = _append_capability_evidence(
        config=config,
        journal=journal,
        attempt_id=attempt_id,
        direction="message-out",
        media_type="application/protobuf",
        payload=payload,
    )
    response_ref: RawRef | None = None
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
        return _CapabilityRpcResult(
            payload=response,
            attempt_id=attempt_id,
            raw_refs=(request_ref, response_ref, status_ref),
        )
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
        raw_refs = (
            (request_ref, status_ref)
            if response_ref is None
            else (request_ref, response_ref, status_ref)
        )
        _record_capability_terminal(
            journal=journal,
            attempt_id=attempt_id,
            stage=stage,
            outcome="failed",
            raw_refs=raw_refs,
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
        owned_channel = self._channel is None
        channel = self._channel or authenticated_channel(
            self._config.speech_endpoint, self._config.quota_project
        )
        rpc: _CapabilityRpcResult | None = None
        try:
            request = locations_pb2.GetLocationRequest(
                name=f"projects/{self._config.project}/locations/eu"
            )
            rpc = await _capability_rpc(
                self._config,
                self._journal,
                channel,
                "stt",
                "/google.cloud.location.Locations/GetLocation",
                request.SerializeToString(),
            )
            location = locations_pb2.Location.FromString(rpc.payload)
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
            capability = StageCapabilities(
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
                rpc.evidence,
            )
        except BaseException:
            if rpc is not None:
                _record_capability_terminal(
                    journal=self._journal,
                    attempt_id=rpc.attempt_id,
                    stage="stt",
                    outcome="failed",
                    raw_refs=rpc.raw_refs,
                )
            raise
        else:
            _record_capability_terminal(
                journal=self._journal,
                attempt_id=rpc.attempt_id,
                stage="stt",
                outcome="succeeded",
                raw_refs=rpc.raw_refs,
            )
            return capability
        finally:
            if owned_channel:
                await channel.close()

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
        owned_channel = self._channel is None
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
            owned_channel=owned_channel,
        )


class GoogleSttSession:
    _ROTATE = object()
    _INPUT_QUEUE_SIZE = 32
    _EVENT_QUEUE_SIZE = 64

    def __init__(
        self,
        c: GoogleConfig,
        j: ExchangeJournal,
        channel: grpc.aio.Channel,
        sid: UUID,
        language: str,
        resume_at_sample: int = 0,
        commit_watermark: int = 0,
        *,
        owned_channel: bool = False,
    ) -> None:
        self._config = c
        self._journal = j
        self._channel = channel
        self._owned_channel = owned_channel
        self._id = sid
        self._language = language
        self._input: asyncio.Queue[AudioChunk | object | None] = asyncio.Queue(
            maxsize=self._INPUT_QUEUE_SIZE
        )
        self._events: asyncio.Queue[SttEvent] = asyncio.Queue(maxsize=self._EVENT_QUEUE_SIZE + 1)
        self._event_slots = asyncio.Semaphore(self._EVENT_QUEUE_SIZE)
        self._terminal: SessionTerminal | None = None
        self._lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._cancel_lock = asyncio.Lock()
        self._input_closed = asyncio.Event()
        self._terminal_ready = asyncio.Event()
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
        async with self._send_lock:
            async with self._lock:
                if self._terminal or self._finishing:
                    raise RuntimeError("session terminal")
                if self._accepted == 0 and chunk.samples.start:
                    self._stream_bases[0] = chunk.samples.start
            if chunk.samples.end >= self._next_rotation:
                await self._enqueue_rollover(chunk)
            await self._put_input(chunk)
            async with self._lock:
                if self._terminal or self._finishing:
                    raise RuntimeError("session terminal")
                self._accepted = max(self._accepted, chunk.samples.end)
                self._remember_recent_chunk(chunk)

    async def _put_input(self, item: AudioChunk | object | None) -> None:
        try:
            self._input.put_nowait(item)
            return
        except asyncio.QueueFull:
            pass
        put = asyncio.create_task(self._input.put(item))
        closed = asyncio.create_task(self._input_closed.wait())
        try:
            done, _ = await asyncio.wait(
                {put, closed},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if closed in done and self._input_closed.is_set():
                raise RuntimeError("session terminal")
            await put
        finally:
            for task in (put, closed):
                if not task.done():
                    task.cancel()
            await asyncio.gather(put, closed, return_exceptions=True)

    async def _enqueue_rollover(self, chunk: AudioChunk) -> None:
        overlap_start = chunk.samples.start - 32000
        overlap = [item for item in self._recent if item.samples.end > overlap_start]
        stream_base = overlap[0].samples.start if overlap else chunk.samples.start
        self._stream_bases.append(stream_base)
        await self._put_input(self._ROTATE)
        for item in overlap:
            await self._put_input(item)
        self._next_rotation += 270 * 16000

    def _remember_recent_chunk(self, chunk: AudioChunk) -> None:
        self._recent.append(chunk)
        cutoff = chunk.samples.end - 32000
        self._recent = [item for item in self._recent if item.samples.end > cutoff]

    def events(self) -> AsyncIterator[SttEvent]:
        async def stream() -> AsyncIterator[SttEvent]:
            while True:
                event = await self._events.get()
                if not isinstance(event, SessionTerminalEvent):
                    self._event_slots.release()
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
        async with self._send_lock:
            async with self._lock:
                if self._terminal:
                    return self._terminal
                self._finishing = True
            await self._put_input(None)
        await self._task
        await self._terminal_ready.wait()
        assert self._terminal
        return self._terminal

    async def cancel(self) -> SessionTerminal:
        async with self._cancel_lock:
            async with self._lock:
                if self._terminal:
                    return self._terminal
                self._finishing = True
                self._input_closed.set()
                self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            async with self._lock:
                if self._terminal:
                    return self._terminal
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
            refs = tuple(self._refs)
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
                    refs,
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
                await self._emit_event(
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
        finally:
            if self._owned_channel:
                await self._channel.close()

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
            transcript = alternative.transcript
            if result.is_final and words and words[0].samples.start < self._commit_watermark:
                words = tuple(word for word in words if word.samples.end > self._commit_watermark)
                if words:
                    trimmed_transcript = " ".join(word.text for word in words).strip()
                    if trimmed_transcript:
                        transcript = trimmed_transcript
            start = words[0].samples.start if words else max(base_sample, self._last_result_end)
            if result.is_final:
                start = max(start, self._commit_watermark)
            end = max(start + 1, reported_end, words[-1].samples.end if words else reported_end)
            self._last_result_end = max(self._last_result_end, end)
            await self._emit_event(
                TranscriptEvent(
                    samples=SampleRange(start=start, end=end),
                    revision=self._received,
                    finality=(Finality.SPAN_FINAL if result.is_final else Finality.PROVISIONAL),
                    text=transcript,
                    words=words,
                    confidence=(float(alternative.confidence) if alternative.confidence else None),
                    raw_ref=raw_ref,
                )
            )
            if result.is_final and self._boundaries:
                self._commit_watermark = max(self._commit_watermark, end)
                await self._emit_event(
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

    async def _emit_event(self, event: SttEvent) -> None:
        await self._event_slots.acquire()
        try:
            self._events.put_nowait(event)
        except BaseException:
            self._event_slots.release()
            raise

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
        self._input_closed.set()
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
        self._terminal_ready.set()
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
        owned_channel = self._channel is None
        channel = self._channel or authenticated_channel(
            self._config.translation_endpoint, self._config.quota_project
        )
        rpc: _CapabilityRpcResult | None = None
        try:
            request = translation_service.GetSupportedLanguagesRequest(
                parent=f"projects/{self._config.project}/locations/eu",
                display_language_code="en",
            )
            payload = translation_service.GetSupportedLanguagesRequest.pb(
                request
            ).SerializeToString()
            rpc = await _capability_rpc(
                self._config,
                self._journal,
                channel,
                "translation",
                "/google.cloud.translation.v3.TranslationService/GetSupportedLanguages",
                payload,
            )
            response = translation_service.SupportedLanguages.deserialize(rpc.payload)
            languages = tuple(sorted(item.language_code for item in response.languages))
            if not languages:
                raise RuntimeError("Google Translation EU returned no supported languages")
            capability = StageCapabilities(
                "google",
                "translation",
                self._config.translation_endpoint,
                ("eu",),
                languages,
                ("general/nmt",),
                (("requests_minute", 100), ("characters_minute", 100000)),
                rpc.evidence,
            )
        except BaseException:
            if rpc is not None:
                _record_capability_terminal(
                    journal=self._journal,
                    attempt_id=rpc.attempt_id,
                    stage="translation",
                    outcome="failed",
                    raw_refs=rpc.raw_refs,
                )
            raise
        else:
            _record_capability_terminal(
                journal=self._journal,
                attempt_id=rpc.attempt_id,
                stage="translation",
                outcome="succeeded",
                raw_refs=rpc.raw_refs,
            )
            return capability
        finally:
            if owned_channel:
                await channel.close()

    async def health(self, snapshot: str) -> ProviderHealth:
        try:
            capability = await self.capabilities()
            return ProviderHealth(True, int(time.time() * 1000), None, capability.evidence)
        except Exception as exc:
            return ProviderHealth(False, int(time.time() * 1000), type(exc).__name__, None)

    async def start(self, request: TranslationRequest) -> GoogleTranslationAttempt:
        owned_channel = self._channel is None
        channel = self._channel or authenticated_channel(
            self._config.translation_endpoint, self._config.quota_project
        )
        return GoogleTranslationAttempt(
            self._config,
            self._journal,
            channel,
            request,
            owned_channel=owned_channel,
        )


class GoogleTranslationAttempt:
    def __init__(
        self,
        c: GoogleConfig,
        j: ExchangeJournal,
        channel: grpc.aio.Channel,
        r: TranslationRequest,
        *,
        owned_channel: bool = False,
    ) -> None:
        self._config = c
        self._journal = j
        self._channel = channel
        self._owned_channel = owned_channel
        self._request = r
        self._task: asyncio.Task[TranslationOutcome] | None = None
        self._terminal: OperationTerminal | None = None
        self._lock = asyncio.Lock()
        self._cancel_lock = asyncio.Lock()
        self._cancel_task: asyncio.Task[OperationTerminal] | None = None
        self._cancelling = False
        self._cancelled_terminal_ready = asyncio.Event()
        self._refs: list[RawRef] = []
        self._received = 0

    async def result(self) -> TranslationOutcome:
        async with self._lock:
            if self._terminal is not None:
                if self._task is not None and self._task.done() and not self._task.cancelled():
                    return self._task.result()
                return TranslationOutcome(None, self._terminal)
            if self._task is not None:
                task = self._task
            elif self._cancelling:
                task = None
            else:
                self._task = asyncio.create_task(self._run())
                task = self._task
        if task is None:
            await self._cancelled_terminal_ready.wait()
            assert self._terminal is not None
            return TranslationOutcome(None, self._terminal)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            async with self._lock:
                terminal = self._terminal
                cancelling = self._cancelling
            if terminal is not None:
                return TranslationOutcome(None, terminal)
            if cancelling:
                await self._cancelled_terminal_ready.wait()
                assert self._terminal is not None
                return TranslationOutcome(None, self._terminal)
            raise

    async def cancel(self) -> OperationTerminal:
        async with self._cancel_lock:
            async with self._lock:
                if self._terminal is not None:
                    return self._terminal
                if self._cancel_task is None:
                    self._cancelling = True
                    task = self._task
                    if task is not None:
                        task.cancel()
                    self._cancel_task = asyncio.create_task(self._finish_cancellation(task))
                cancellation = self._cancel_task
        assert cancellation is not None
        return await asyncio.shield(cancellation)

    async def _finish_cancellation(
        self, task: asyncio.Task[TranslationOutcome] | None
    ) -> OperationTerminal:
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        elif self._owned_channel:
            await self._channel.close()
        async with self._lock:
            if self._terminal is not None:
                terminal = self._terminal
            else:
                refs, error = self._cancellation_details()
                terminal = self._make_terminal(Outcome.CANCELLED, refs, error)
        self._cancelled_terminal_ready.set()
        return terminal

    async def _run(self) -> TranslationOutcome:
        try:
            return await self._run_rpc()
        finally:
            if self._owned_channel:
                await self._channel.close()

    async def _run_rpc(self) -> TranslationOutcome:
        call: object | None = None
        try:
            request = self._build_request()
            callable_ = self._channel.unary_unary(
                "/google.cloud.translation.v3.TranslationService/TranslateText",
                request_serializer=lambda value: self._serialize_request(value, self._refs),
                response_deserializer=lambda raw: self._deserialize_response(raw, self._refs),
            )
            call = callable_(
                request,
                metadata=(("x-goog-user-project", self._config.quota_project),),
                timeout=20,
            )
            response = await cast(Awaitable[translation_service.TranslateTextResponse], call)
            self._refs.append(await self._record_status(call, error=None))
            if len(response.translations) != 1:
                raise RuntimeError("Google translation response cardinality must be one")
            terminal = self._make_terminal(Outcome.SUCCEEDED, tuple(self._refs), None)
            result = None
            if terminal.outcome is Outcome.SUCCEEDED:
                result = TranslationResult(
                    operation_id=self._request.operation_id,
                    attempt_id=self._request.attempt_id,
                    text=response.translations[0].translated_text,
                    source_range=self._request.source_range,
                    raw_ref=self._refs[-2],
                )
            return TranslationOutcome(result=result, terminal=terminal)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._refs.append(await self._record_status(call, error=exc))
            if self._cancelling:
                raise asyncio.CancelledError from exc
            error = ProviderError(
                kind=ErrorKind.TRANSPORT,
                scope="operation",
                provider_retry_advice=RetryAdvice.UNSPECIFIED,
                provider_code=type(exc).__name__,
                provider_request_id=None,
                retry_delay_ms=None,
                attempt_id=self._request.attempt_id,
                raw_refs=tuple(self._refs),
                message=str(exc),
            )
            terminal = self._make_terminal(Outcome.FAILED, tuple(self._refs), error)
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
        self._received += 1
        return cast(
            translation_service.TranslateTextResponse,
            translation_service.TranslateTextResponse.deserialize(raw),
        )

    async def _record_status(self, call: object | None, error: Exception | None) -> RawRef:
        if call is None:
            assert error is not None
            payload = json.dumps(
                {
                    "code": "LOCAL_SETUP_ERROR",
                    "errorType": type(error).__name__,
                    "message": str(error)[:256],
                },
                separators=(",", ":"),
            ).encode()
        elif error is None:
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

    def _cancellation_details(self) -> tuple[tuple[RawRef, ...], ProviderError]:
        self._refs.append(
            self._journal.append(
                meeting_id=self._config.meeting_id,
                attempt_id=self._request.attempt_id,
                stage="translation",
                transport="grpc",
                direction="status-in",
                media_type="application/json",
                payload=b'{"code":"CANCELLED"}',
                sample_range=self._request.source_range,
            )
        )
        refs = tuple(self._refs)
        return refs, ProviderError(
            ErrorKind.CANCELLED,
            "operation",
            RetryAdvice.NEVER,
            "cancelled",
            None,
            None,
            self._request.attempt_id,
            refs,
            "cancelled",
        )

    def _make_terminal(
        self, outcome: Outcome, refs: tuple[RawRef, ...], error: ProviderError | None
    ) -> OperationTerminal:
        if self._terminal:
            return self._terminal
        if self._cancelling and outcome is not Outcome.CANCELLED:
            refs, error = self._cancellation_details()
            outcome = Outcome.CANCELLED
        elif not refs:
            refs, error = self._cancellation_details()
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
            received_output=self._received,
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
        owned_channel = self._channel is None
        channel = self._channel or authenticated_channel(
            self._config.tts_endpoint, self._config.quota_project
        )
        rpc: _CapabilityRpcResult | None = None
        try:
            request = cloud_tts.ListVoicesRequest()
            payload = cloud_tts.ListVoicesRequest.pb(request).SerializeToString()
            rpc = await _capability_rpc(
                self._config,
                self._journal,
                channel,
                "tts",
                "/google.cloud.texttospeech.v1.TextToSpeech/ListVoices",
                payload,
            )
            response = cloud_tts.ListVoicesResponse.deserialize(rpc.payload)
            voices = tuple(
                sorted(voice.name for voice in response.voices if "Chirp3-HD" in voice.name)
            )
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
            capability = StageCapabilities(
                "google",
                "tts",
                self._config.tts_endpoint,
                ("eu",),
                languages,
                ("Chirp3-HD",),
                (
                    ("sample_rate", 48000),
                    ("sessions", 2),
                    ("starts_minute", 40),
                    ("bytes_message", _TTS_MESSAGE_BYTE_LIMIT),
                ),
                rpc.evidence,
                voices,
            )
        except BaseException:
            if rpc is not None:
                _record_capability_terminal(
                    journal=self._journal,
                    attempt_id=rpc.attempt_id,
                    stage="tts",
                    outcome="failed",
                    raw_refs=rpc.raw_refs,
                )
            raise
        else:
            _record_capability_terminal(
                journal=self._journal,
                attempt_id=rpc.attempt_id,
                stage="tts",
                outcome="succeeded",
                raw_refs=rpc.raw_refs,
            )
            return capability
        finally:
            if owned_channel:
                await channel.close()

    async def health(self, snapshot: str) -> ProviderHealth:
        attempt_id = uuid4()
        attempt: GoogleTtsAttempt | None = None
        owned_channel = False
        channel: grpc.aio.Channel | None = None
        try:
            owned_channel = self._channel is None
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
            if owned_channel:
                attempt = GoogleTtsAttempt(
                    self._config,
                    self._journal,
                    channel,
                    utterance,
                    self._config.probe_voice_locale,
                    self._config.probe_voice,
                    owned_channel=True,
                )
            else:
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
        finally:
            if owned_channel and channel is not None:
                if attempt is None:
                    await channel.close()
                elif attempt._terminal is None:
                    await asyncio.shield(attempt.cancel())
                else:
                    await asyncio.shield(attempt.finish())

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
        owned_channel = self._channel is None
        channel = self._channel or authenticated_channel(
            self._config.tts_endpoint, self._config.quota_project
        )
        return GoogleTtsSession(
            self._config,
            self._journal,
            channel,
            session_id,
            language,
            voice,
            ref,
            owned_channel=owned_channel,
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
        *,
        owned_channel: bool = False,
    ) -> None:
        self._config = c
        self._journal = j
        self._channel = channel
        self._owned_channel = owned_channel
        self._id = sid
        self._language = language
        self._voice = voice
        self._terminal: SessionTerminal | None = None
        self._events: asyncio.Queue[SessionTerminalEvent] = asyncio.Queue()
        self._open_ref = open_ref
        self._attempts: list[GoogleTtsAttempt] = []
        self._end_lock = asyncio.Lock()

    async def start(self, u: SynthesisUtterance) -> GoogleTtsAttempt:
        async with self._end_lock:
            if self._terminal is not None:
                raise RuntimeError("session terminal")
            attempt = GoogleTtsAttempt(
                self._config,
                self._journal,
                self._channel,
                u,
                self._language,
                self._voice,
            )
            self._attempts.append(attempt)
            return attempt

    def session_events(self) -> AsyncIterator[SessionTerminalEvent]:
        async def stream() -> AsyncIterator[SessionTerminalEvent]:
            yield await self._events.get()

        return stream()

    async def finish(self) -> SessionTerminal:
        return await self._end(Outcome.SUCCEEDED)

    async def cancel(self) -> SessionTerminal:
        return await self._end(Outcome.CANCELLED)

    async def _end(self, outcome: Outcome) -> SessionTerminal:
        async with self._end_lock:
            if self._terminal:
                return self._terminal
            try:
                if outcome is Outcome.CANCELLED:
                    await asyncio.gather(
                        *(attempt.cancel() for attempt in self._attempts),
                        return_exceptions=True,
                    )
                else:
                    await asyncio.gather(
                        *(attempt.finish() for attempt in self._attempts),
                        return_exceptions=True,
                    )
            finally:
                if self._owned_channel:
                    await self._channel.close()
            self._terminal = SessionTerminal(
                terminal_id=uuid4(),
                session_id=self._id,
                outcome=outcome,
                error=None,
                accepted_input=len(self._attempts),
                received_output=sum(attempt.received_output for attempt in self._attempts),
                emitted_output=sum(attempt.emitted_output for attempt in self._attempts),
                transport=Transport.GRPC,
                raw_refs=(self._open_ref,),
            )
            self._journal.terminal(self._id, terminal_bytes(self._terminal))
            await self._events.put(SessionTerminalEvent(self._terminal))
            return self._terminal


class GoogleTtsAttempt:
    _EVENT_QUEUE_SIZE = 64

    def __init__(
        self,
        c: GoogleConfig,
        j: ExchangeJournal,
        channel: grpc.aio.Channel,
        u: SynthesisUtterance,
        language: str,
        voice: str,
        *,
        owned_channel: bool = False,
    ) -> None:
        self._config = c
        self._journal = j
        self._channel = channel
        self._owned_channel = owned_channel
        self._utterance = u
        self._language = language
        self._voice = voice
        self._events: asyncio.Queue[TtsEvent] = asyncio.Queue(maxsize=self._EVENT_QUEUE_SIZE + 1)
        self._event_slots = asyncio.Semaphore(self._EVENT_QUEUE_SIZE)
        self._terminal: OperationTerminal | None = None
        self._refs: list[RawRef] = []
        self._received = 0
        self._emitted = 0
        self._cancelling = False
        self._cancel_lock = asyncio.Lock()
        self._end_lock = asyncio.Lock()
        self._task = asyncio.create_task(self._run())

    def events(self) -> AsyncIterator[TtsEvent]:
        async def stream() -> AsyncIterator[TtsEvent]:
            while True:
                event = await self._events.get()
                if not isinstance(event, OperationTerminalEvent):
                    self._event_slots.release()
                yield event
                if isinstance(event, OperationTerminalEvent):
                    return

        return stream()

    @property
    def received_output(self) -> int:
        return self._received

    @property
    def emitted_output(self) -> int:
        return self._emitted

    async def finish(self) -> OperationTerminal:
        await self._task
        assert self._terminal
        return self._terminal

    async def cancel(self) -> OperationTerminal:
        async with self._cancel_lock:
            if self._terminal is not None:
                return self._terminal
            self._cancelling = True
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            if self._terminal is not None:
                return self._terminal
            self._refs.append(
                self._journal.append(
                    meeting_id=self._config.meeting_id,
                    attempt_id=self._utterance.attempt_id,
                    stage="tts",
                    transport="grpc",
                    direction="status-in",
                    media_type="application/json",
                    payload=b'{"code":"CANCELLED"}',
                    sample_range=self._utterance.source_range,
                )
            )
            refs = tuple(self._refs)
            error = ProviderError(
                ErrorKind.CANCELLED,
                "operation",
                RetryAdvice.NEVER,
                "cancelled",
                None,
                None,
                self._utterance.attempt_id,
                refs,
                "cancelled",
            )
            return await self._end(Outcome.CANCELLED, refs, self._emitted, error)

    async def _run(self) -> None:
        try:
            await self._run_rpc()
        finally:
            if self._owned_channel:
                await self._channel.close()

    async def _run_rpc(self) -> None:
        call: object | None = None
        sequence = 0
        decoder = Linear16StreamDecoder(48000)
        try:
            callable_ = self._channel.stream_stream(
                "/google.cloud.texttospeech.v1.TextToSpeech/StreamingSynthesize",
                request_serializer=lambda value: self._serialize_request(value, self._refs),
                response_deserializer=lambda raw: self._deserialize_response(raw, self._refs),
            )
            call = callable_(
                self._requests(),
                metadata=(("x-goog-user-project", self._config.quota_project),),
                timeout=20,
            )
            async for response in cast(AsyncIterable[cloud_tts.StreamingSynthesizeResponse], call):
                pcm = decoder.feed(response.audio_content)
                if not pcm:
                    continue
                self._received += len(pcm) // 2
                self._emitted = await self._emit_audio(
                    pcm=pcm,
                    emitted=self._emitted,
                    sequence=sequence,
                    raw_ref=self._refs[-1],
                )
                sequence += 1
            decoder.finish()
            self._refs.append(await self._record_status(call, error=None))
            if self._emitted <= 0:
                raise RuntimeError("Google streaming TTS returned no audio")
            boundary = SampleRange(start=0, end=self._emitted)
            await self._emit_event(
                SynthesisBoundary(
                    operation_id=self._utterance.operation_id,
                    samples=boundary,
                    raw_ref=self._refs[-1],
                )
            )
            await self._end(Outcome.SUCCEEDED, tuple(self._refs), self._emitted, None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._refs.append(await self._record_status(call, error=exc))
            if self._cancelling:
                raise asyncio.CancelledError from exc
            error = ProviderError(
                kind=ErrorKind.TRANSPORT,
                scope="operation",
                provider_retry_advice=(
                    RetryAdvice.NEVER if self._emitted else RetryAdvice.UNSPECIFIED
                ),
                provider_code=type(exc).__name__,
                provider_request_id=None,
                retry_delay_ms=None,
                attempt_id=self._utterance.attempt_id,
                raw_refs=tuple(self._refs),
                message=str(exc),
            )
            await self._end(Outcome.FAILED, tuple(self._refs), self._emitted, error)

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
        for text in _tts_text_messages(self._utterance.text):
            yield cloud_tts.StreamingSynthesizeRequest(
                input=cloud_tts.StreamingSynthesisInput(text=text)
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
        await self._emit_event(
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

    async def _record_status(self, call: object | None, error: Exception | None) -> RawRef:
        if call is None:
            assert error is not None
            payload = json.dumps(
                {
                    "code": "LOCAL_SETUP_ERROR",
                    "errorType": type(error).__name__,
                    "message": str(error)[:256],
                },
                separators=(",", ":"),
            ).encode()
        elif error is None:
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

    async def _emit_event(self, event: TtsEvent) -> None:
        await self._event_slots.acquire()
        try:
            self._events.put_nowait(event)
        except BaseException:
            self._event_slots.release()
            raise

    async def _end(
        self,
        outcome: Outcome,
        refs: tuple[RawRef, ...],
        emitted: int,
        error: ProviderError | None,
    ) -> OperationTerminal:
        async with self._end_lock:
            if self._terminal:
                return self._terminal
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
                received_output=self._received,
                emitted_output=emitted,
                transport=Transport.GRPC,
                raw_refs=refs,
                credential_fingerprint=self._config.credential_fingerprint,
            )
            self._journal.terminal(self._utterance.attempt_id, terminal_bytes(self._terminal))
            await self._events.put(OperationTerminalEvent(self._terminal))
            return self._terminal
