from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any
from urllib.parse import urlencode
from uuid import UUID, uuid4

from transhooter_spool import RawRef, SampleRange

from transhooter_worker.adapters.deepgram.sansio import SansIoWebSocket
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
    RetryAction,
    RetryAdvice,
    RetryDecision,
    SessionTerminal,
    SessionTerminalEvent,
    StageCapabilities,
    SynthesisBoundary,
    SynthesisUtterance,
    TranscriptEvent,
    Transport,
    WordTiming,
)
from transhooter_worker.ports.exchange_journal import ExchangeJournal
from transhooter_worker.ports.providers import SttEvent, TtsEvent

_COMPLETION_TIMEOUT_SECONDS = 10.0
_CLOSE_TIMEOUT_SECONDS = 5.0
_TTS_MESSAGE_CHARACTER_LIMIT = 2_000


def _tts_text_messages(text: str) -> tuple[str, ...]:
    if not text:
        return ("",)
    return tuple(
        text[offset : offset + _TTS_MESSAGE_CHARACTER_LIMIT]
        for offset in range(0, len(text), _TTS_MESSAGE_CHARACTER_LIMIT)
    )


class _SttCompletionState(Enum):
    OPEN = auto()
    FINALIZE_SENT = auto()
    CLOSE_SENT = auto()
    FINALIZE_ACKNOWLEDGED = auto()
    COMPLETE = auto()


class _TtsCompletionState(Enum):
    OPEN = auto()
    FLUSH_SENT = auto()
    FLUSH_ACKNOWLEDGED = auto()
    CLOSE_SENT = auto()
    COMPLETE = auto()


@dataclass(frozen=True, slots=True)
class DeepgramConfig:
    api_key: str
    meeting_id: UUID
    language: str
    voice: str
    approved_voices: tuple[str, ...]
    credential_fingerprint: str
    endpoint: str = "api.eu.deepgram.com"
    streams: int = 1
    audio_seconds_minute: int = 60
    supported_languages: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.api_key.strip():
            raise ValueError("Deepgram API key is required")
        if self.endpoint != "api.eu.deepgram.com":
            raise ValueError("Deepgram EU endpoint is required")
        if self.voice not in self.approved_voices:
            raise ValueError("Deepgram voice must be explicitly capability-approved")
        if self.streams <= 0 or self.audio_seconds_minute <= 0:
            raise ValueError("Deepgram effective quota limits are required")
        if not self.supported_languages:
            object.__setattr__(self, "supported_languages", (self.language,))


def _websocket_recorder(
    config: DeepgramConfig,
    journal: ExchangeJournal,
    attempt_id: UUID,
    stage: str,
    references: list[RawRef],
) -> Callable[[str, str, bytes, tuple[tuple[str, str], ...]], None]:
    def record(
        direction: str,
        kind: str,
        payload: bytes,
        metadata: tuple[tuple[str, str], ...],
    ) -> None:
        reference = journal.append(
            meeting_id=config.meeting_id,
            attempt_id=attempt_id,
            stage=stage,
            transport="websocket",
            direction=direction,
            media_type=("application/http" if kind == "http" else "application/octet-stream"),
            payload=payload,
            metadata=metadata,
        )
        references.append(reference)

    return record


def _deepgram_stt_url(endpoint: str, language: str) -> str:
    query = urlencode(
        {
            "model": "nova-3",
            "language": language,
            "encoding": "linear16",
            "sample_rate": 16000,
            "channels": 1,
            "interim_results": "true",
            "utterance_end_ms": 1000,
        }
    )
    return f"wss://{endpoint}/v1/listen?{query}"


def _deepgram_tts_url(endpoint: str, voice: str) -> str:
    query = urlencode(
        {
            "model": voice,
            "encoding": "linear16",
            "sample_rate": 48000,
        }
    )
    return f"wss://{endpoint}/v1/speak?{query}"


def _json_command(kind: str, **fields: str) -> bytes:
    return json.dumps({"type": kind, **fields}, separators=(",", ":")).encode()


def _finish_probe(
    config: DeepgramConfig,
    journal: ExchangeJournal,
    attempt_id: UUID,
    references: list[RawRef],
    *,
    succeeded: bool,
    error_type: str | None = None,
) -> RawRef:
    if references:
        evidence = references[-1]
    else:
        status: dict[str, str] = {"outcome": "succeeded" if succeeded else "failed"}
        if error_type is not None:
            status["errorType"] = error_type
        evidence = journal.append(
            meeting_id=config.meeting_id,
            attempt_id=attempt_id,
            stage="capabilities",
            transport="websocket",
            direction="status-in",
            media_type="application/json",
            payload=json.dumps(status, separators=(",", ":")).encode(),
        )
        references.append(evidence)
    journal.terminal(
        attempt_id,
        json.dumps(
            {
                "outcome": "succeeded" if succeeded else "failed",
                "transport": "websocket",
                "rawRefs": [str(reference.object_id) for reference in references],
            },
            separators=(",", ":"),
        ).encode(),
    )
    return evidence


class DeepgramSttProvider:
    def __init__(self, config: DeepgramConfig, journal: ExchangeJournal) -> None:
        self._config = config
        self._journal = journal

    async def capabilities(self) -> StageCapabilities:
        return StageCapabilities(
            "deepgram",
            "stt",
            "wss://api.eu.deepgram.com/v1/listen",
            ("eu",),
            self._config.supported_languages,
            ("nova-3",),
            (
                ("chunk_bytes", 8000),
                ("boundary_ms", 15000),
                ("streams", self._config.streams),
                ("audio_seconds_minute", self._config.audio_seconds_minute),
            ),
            None,
        )

    async def health(self, snapshot: str) -> ProviderHealth:
        attempt_id = uuid4()
        references: list[RawRef] = []
        record = _websocket_recorder(
            self._config,
            self._journal,
            attempt_id,
            "capabilities",
            references,
        )
        try:
            websocket = await SansIoWebSocket.connect(
                "wss://api.eu.deepgram.com/v1/listen?model=nova-3",
                f"Token {self._config.api_key}",
                record,
            )
            await websocket.close()
            evidence = _finish_probe(
                self._config,
                self._journal,
                attempt_id,
                references,
                succeeded=True,
            )
            return ProviderHealth(True, int(time.time() * 1000), None, evidence)
        except Exception as error:
            evidence = _finish_probe(
                self._config,
                self._journal,
                attempt_id,
                references,
                succeeded=False,
                error_type=type(error).__name__,
            )
            return ProviderHealth(
                False,
                int(time.time() * 1000),
                type(error).__name__,
                evidence,
            )

    async def open(
        self,
        session_id: UUID,
        language: str,
        *,
        resume_at_sample: int = 0,
        commit_watermark: int = 0,
    ) -> DeepgramSttSession:
        url = _deepgram_stt_url(self._config.endpoint, language)
        references: list[RawRef] = []
        record = _websocket_recorder(
            self._config,
            self._journal,
            session_id,
            "stt",
            references,
        )
        websocket = await SansIoWebSocket.connect(
            url,
            f"Token {self._config.api_key}",
            record,
        )
        return DeepgramSttSession(
            self._config,
            self._journal,
            session_id,
            websocket,
            references,
            resume_at_sample,
            commit_watermark,
        )


class DeepgramSttSession:
    _EVENT_QUEUE_SIZE = 64

    def __init__(
        self,
        config: DeepgramConfig,
        journal: ExchangeJournal,
        session_id: UUID,
        websocket: SansIoWebSocket,
        references: list[RawRef],
        resume_at_sample: int = 0,
        commit_watermark: int = 0,
    ) -> None:
        self._config = config
        self._journal = journal
        self._id = session_id
        self._websocket = websocket
        self._refs = references
        self._events: asyncio.Queue[SttEvent] = asyncio.Queue(maxsize=self._EVENT_QUEUE_SIZE + 1)
        self._event_slots = asyncio.Semaphore(self._EVENT_QUEUE_SIZE)
        self._send_lock = asyncio.Lock()
        self._terminal_lock = asyncio.Lock()
        self._terminal: SessionTerminal | None = None
        self._accepted = resume_at_sample
        self._received = 0
        self._boundaries: list[tuple[UUID, int]] = []
        self._base: int | None = resume_at_sample
        self._commit_watermark = commit_watermark
        self._emitted_watermark = commit_watermark
        self._completion_state = _SttCompletionState.OPEN
        self._closing_outcome: Outcome | None = None
        self._reader = asyncio.create_task(self._read())
        self._completion = asyncio.create_task(self._publish_reader_completion())

    async def send_audio(self, chunk: AudioChunk) -> None:
        if len(chunk.pcm) > 8000:
            raise ValueError("Deepgram STT chunks must be <=8,000 bytes")
        async with self._send_lock:
            if self._terminal:
                raise RuntimeError("session is terminal")
            if self._base is None:
                self._base = chunk.samples.start
            ref = self._record_exchange(
                chunk.operation_id,
                "media-out",
                "application/octet-stream",
                chunk.pcm,
                chunk.samples,
            )
            await self._websocket.send(chunk.pcm)
            self._refs.append(ref)
            self._accepted = max(self._accepted, chunk.samples.end)

    def events(self) -> AsyncIterator[SttEvent]:
        async def stream() -> AsyncIterator[SttEvent]:
            while True:
                item = await self._events.get()
                if not isinstance(item, SessionTerminalEvent):
                    self._event_slots.release()
                yield item
                if isinstance(item, SessionTerminalEvent):
                    return

        return stream()

    async def request_boundary(self, boundary_id: UUID) -> BoundaryReceipt:
        boundary = (boundary_id, self._accepted)
        self._boundaries.append(boundary)
        try:
            await self._send_json({"type": "Finalize"}, "finalize-out")
        except BaseException:
            self._boundaries.remove(boundary)
            raise
        return BoundaryReceipt(True, boundary_id)

    async def finish(self) -> SessionTerminal:
        async with self._terminal_lock:
            if self._terminal:
                return self._terminal
            self._completion_state = _SttCompletionState.FINALIZE_SENT
            try:
                async with asyncio.timeout(_COMPLETION_TIMEOUT_SECONDS):
                    await self._send_json({"type": "Finalize"}, "finalize-out")
                    self._completion_state = _SttCompletionState.CLOSE_SENT
                    await self._send_json({"type": "CloseStream"}, "close-stream-out")
            except TimeoutError:
                pass
        try:
            await asyncio.wait_for(
                asyncio.shield(self._completion),
                _COMPLETION_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            await self._settle_reader("Deepgram STT completion timed out")
        if self._terminal is not None:
            return self._terminal
        return await self._terminalize(
            Outcome.FAILED,
            self._transport_error("Deepgram STT closed without completion acknowledgement"),
        )

    async def cancel(self) -> SessionTerminal:
        async with self._terminal_lock:
            if self._terminal:
                return self._terminal
            self._closing_outcome = Outcome.CANCELLED
        await self._settle_reader(None, code=1000, reason="cancelled")
        return await self._terminalize(Outcome.CANCELLED, None)

    async def _send_json(self, payload: dict[str, Any], kind: str) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode()
        ref = self._record_exchange(self._id, kind, "application/json", body, None)
        self._refs.append(ref)
        await self._websocket.send(body.decode())

    async def _settle_reader(
        self,
        failure: str | None,
        *,
        code: int = 1011,
        reason: str = "completion timeout",
    ) -> None:
        try:
            await asyncio.wait_for(
                self._websocket.close(code=code, reason=reason),
                _CLOSE_TIMEOUT_SECONDS,
            )
        except (Exception, asyncio.CancelledError):
            pass
        if not self._reader.done():
            self._reader.cancel()
        await asyncio.gather(self._reader, return_exceptions=True)
        await asyncio.gather(self._completion, return_exceptions=True)
        if failure is not None and self._terminal is None:
            await self._terminalize(Outcome.FAILED, self._transport_error(failure))

    def _transport_error(self, detail: str) -> ProviderError:
        return ProviderError(
            ErrorKind.TRANSPORT,
            "session",
            RetryAdvice.UNSPECIFIED,
            "CompletionError",
            None,
            None,
            self._id,
            tuple(self._refs),
            detail,
        )

    async def _read(self) -> tuple[Outcome, ProviderError | None]:
        error: ProviderError | None = None
        try:
            async for message in self._websocket:
                await self._handle_inbound_message(message)
        except Exception as exception:
            error = self._transport_error(f"{type(exception).__name__}: {exception}")
        if (
            self._completion_state is _SttCompletionState.FINALIZE_ACKNOWLEDGED
            and self._websocket.close_code == 1000
        ):
            self._completion_state = _SttCompletionState.COMPLETE

        outcome = self._closing_outcome
        if outcome is None:
            completed = self._completion_state is _SttCompletionState.COMPLETE
            if not completed and error is None:
                error = self._transport_error(
                    "Deepgram STT transport ended before Finalize/CloseStream acknowledgement"
                )
            if not completed:
                try:
                    await asyncio.wait_for(
                        self._websocket.close(
                            code=1011,
                            reason="completion acknowledgement missing",
                        ),
                        _CLOSE_TIMEOUT_SECONDS,
                    )
                except (Exception, asyncio.CancelledError):
                    pass
            outcome = Outcome.SUCCEEDED if completed else Outcome.FAILED
        self._record_close()
        return outcome, error

    async def _publish_reader_completion(self) -> None:
        try:
            outcome, error = await self._reader
        except asyncio.CancelledError:
            return
        await self._terminalize(outcome, error)

    async def _handle_inbound_message(self, message: str | bytes) -> None:
        if isinstance(message, bytes):
            reference = self._record_exchange(
                self._id,
                "frame-in",
                "application/octet-stream",
                message,
                None,
            )
            self._refs.append(reference)
            self._received += 1
            return

        reference = self._record_exchange(
            self._id,
            "frame-in",
            "application/json",
            message.encode(),
            None,
        )
        self._refs.append(reference)
        self._received += 1
        data = json.loads(message)
        message_type = data.get("type")
        if message_type == "Results":
            await self._handle_results(data, reference)
            if data.get("from_finalize"):
                if self._boundaries:
                    await self._emit_requested_boundary(reference)
                if self._completion_state in {
                    _SttCompletionState.FINALIZE_SENT,
                    _SttCompletionState.CLOSE_SENT,
                }:
                    self._completion_state = _SttCompletionState.FINALIZE_ACKNOWLEDGED
        elif message_type == "UtteranceEnd":
            await self._emit_spontaneous_boundary(reference)
        elif message_type == "CloseStream":
            if self._completion_state is _SttCompletionState.FINALIZE_ACKNOWLEDGED:
                while self._boundaries:
                    await self._emit_requested_boundary(reference)
                self._completion_state = _SttCompletionState.COMPLETE
        elif message_type not in {"Metadata", "SpeechStarted"}:
            raise ValueError(f"unsupported Deepgram STT message {message_type}")

    async def _handle_results(
        self,
        data: dict[str, Any],
        reference: RawRef,
    ) -> None:
        alternatives = (data.get("channel") or {}).get("alternatives") or [{}]
        alternative = alternatives[0]
        text = str(alternative.get("transcript", ""))
        if text:
            span = self._result_sample_range(data)
            is_final = bool(data.get("is_final"))
            words = self._word_timings(alternative)
            is_committed_final = is_final and span.end <= self._commit_watermark
            if not is_committed_final:
                if is_final and span.start < self._commit_watermark:
                    words, text, span = self._trim_replayed_final(words, text, span)
                await self._emit_event(
                    TranscriptEvent(
                        span,
                        self._received,
                        Finality.SPAN_FINAL if is_final else Finality.PROVISIONAL,
                        text,
                        words,
                        (float(alternative["confidence"]) if "confidence" in alternative else None),
                        reference,
                    )
                )
        if data.get("speech_final") and not data.get("from_finalize"):
            await self._emit_spontaneous_boundary(reference)

    def _result_sample_range(self, data: dict[str, Any]) -> SampleRange:
        base_sample = self._base or 0
        start_sample = base_sample + int(float(data.get("start", 0)) * 16000)
        duration_samples = max(
            1,
            int(float(data.get("duration", 0)) * 16000),
        )
        return SampleRange(start_sample, start_sample + duration_samples)

    def _word_timings(self, alternative: dict[str, Any]) -> tuple[WordTiming, ...]:
        base_sample = self._base or 0
        timings: list[WordTiming] = []
        for word in alternative.get("words", []):
            start_sample = base_sample + int(float(word.get("start", 0)) * 16000)
            end_sample = max(
                start_sample + 1,
                base_sample + int(float(word.get("end", 0)) * 16000),
            )
            confidence = float(word["confidence"]) if "confidence" in word else None
            timings.append(
                WordTiming(
                    str(word.get("word", "")),
                    SampleRange(start_sample, end_sample),
                    confidence,
                )
            )
        return tuple(timings)

    def _trim_replayed_final(
        self,
        words: tuple[WordTiming, ...],
        text: str,
        span: SampleRange,
    ) -> tuple[tuple[WordTiming, ...], str, SampleRange]:
        retained = tuple(word for word in words if word.samples.end > self._commit_watermark)
        if not retained:
            raise ValueError(
                "Deepgram replay final crossed the committed watermark without "
                "sufficient word timing"
            )
        first = retained[0]
        if first.samples.start < self._commit_watermark:
            retained = (
                WordTiming(
                    first.text,
                    SampleRange(self._commit_watermark, first.samples.end),
                    first.confidence,
                ),
                *retained[1:],
            )
        suffix = " ".join(word.text.strip() for word in retained if word.text.strip())
        if not suffix:
            raise ValueError(
                "Deepgram replay final crossed the committed watermark without timed suffix text"
            )
        return (
            retained,
            suffix,
            SampleRange(retained[0].samples.start, retained[-1].samples.end),
        )

    async def _emit_requested_boundary(self, reference: RawRef) -> None:
        boundary_id, requested_through = self._boundaries.pop(0)
        committed_through = max(self._emitted_watermark, requested_through)
        self._emitted_watermark = committed_through
        self._commit_watermark = max(self._commit_watermark, committed_through)
        await self._emit_event(BoundaryEvent(boundary_id, committed_through, reference))

    async def _emit_spontaneous_boundary(self, reference: RawRef) -> None:
        committed_through = max(self._emitted_watermark, self._accepted)
        self._emitted_watermark = committed_through
        self._commit_watermark = max(self._commit_watermark, committed_through)
        await self._emit_event(BoundaryEvent(uuid4(), committed_through, reference))

    def _record_close(self) -> None:
        close_payload = json.dumps(
            {
                "code": self._websocket.close_code,
                "reason": self._websocket.close_reason,
            },
            separators=(",", ":"),
        ).encode()
        self._refs.append(
            self._record_exchange(
                self._id,
                "close-in",
                "application/json",
                close_payload,
                None,
            )
        )

    async def _emit_event(self, event: SttEvent) -> None:
        await self._event_slots.acquire()
        try:
            self._events.put_nowait(event)
        except BaseException:
            self._event_slots.release()
            raise

    async def _terminalize(self, outcome: Outcome, error: ProviderError | None) -> SessionTerminal:
        async with self._terminal_lock:
            if self._terminal:
                return self._terminal
            terminal = SessionTerminal(
                uuid4(),
                self._id,
                outcome,
                error,
                self._accepted,
                self._received,
                0,
                Transport.WEBSOCKET,
                tuple(self._refs),
            )
            self._terminal = terminal
            self._journal.terminal(self._id, terminal_bytes(terminal))
            await self._events.put(SessionTerminalEvent(terminal))
            return terminal

    def _record_exchange(
        self,
        attempt: UUID,
        direction: str,
        media_type: str,
        payload: bytes,
        samples: SampleRange | None,
    ) -> RawRef:
        return self._journal.append(
            meeting_id=self._config.meeting_id,
            attempt_id=attempt,
            stage="stt",
            transport="websocket",
            direction=direction,
            media_type=media_type,
            payload=payload,
            sample_range=samples,
        )


class DeepgramTtsProvider:
    def __init__(self, config: DeepgramConfig, journal: ExchangeJournal) -> None:
        self._config = config
        self._journal = journal

    async def capabilities(self) -> StageCapabilities:
        return StageCapabilities(
            "deepgram",
            "tts",
            "wss://api.eu.deepgram.com/v1/speak",
            ("eu",),
            self._config.supported_languages,
            ("aura-2",),
            (
                ("characters_message", _TTS_MESSAGE_CHARACTER_LIMIT),
                ("characters_minute", 2400),
                ("flushes_minute", 20),
            ),
            None,
            self._config.approved_voices,
        )

    async def health(self, snapshot: str) -> ProviderHealth:
        if self._config.voice not in self._config.approved_voices:
            return ProviderHealth(
                False,
                int(time.time() * 1000),
                "voice is not approved",
                None,
            )

        attempt_id = uuid4()
        references: list[RawRef] = []
        record = _websocket_recorder(
            self._config,
            self._journal,
            attempt_id,
            "capabilities",
            references,
        )
        websocket: SansIoWebSocket | None = None
        try:
            websocket = await SansIoWebSocket.connect(
                _deepgram_tts_url(self._config.endpoint, self._config.voice),
                f"Token {self._config.api_key}",
                record,
            )
            has_audio, was_flushed = await self._run_probe(websocket)
            await websocket.close()
            healthy = has_audio and was_flushed
            evidence = _finish_probe(
                self._config,
                self._journal,
                attempt_id,
                references,
                succeeded=healthy,
            )
            return ProviderHealth(
                healthy,
                int(time.time() * 1000),
                None if healthy else "probe returned no audio/flushed",
                evidence,
            )
        except Exception as error:
            if websocket is not None:
                try:
                    await asyncio.wait_for(
                        websocket.close(code=1011, reason="health probe failed"),
                        _CLOSE_TIMEOUT_SECONDS,
                    )
                except (Exception, asyncio.CancelledError):
                    pass
            evidence = _finish_probe(
                self._config,
                self._journal,
                attempt_id,
                references,
                succeeded=False,
                error_type=type(error).__name__,
            )
            return ProviderHealth(
                False,
                int(time.time() * 1000),
                type(error).__name__,
                evidence,
            )

    async def _run_probe(
        self,
        websocket: SansIoWebSocket,
    ) -> tuple[bool, bool]:
        await websocket.send(_json_command("Speak", text=".").decode())
        await websocket.send(_json_command("Flush").decode())
        has_audio = False
        was_flushed = False
        async with asyncio.timeout(10):
            async for message in websocket:
                if isinstance(message, bytes):
                    has_audio = has_audio or bool(message)
                else:
                    was_flushed = was_flushed or json.loads(message).get("type") == "Flushed"
                if has_audio and was_flushed:
                    break
        return has_audio, was_flushed

    async def open(
        self,
        session_id: UUID,
        language: str,
        voice: str,
    ) -> DeepgramTtsSession:
        if voice not in self._config.approved_voices:
            raise ValueError("voice is not capability-approved")
        references: list[RawRef] = []
        record = _websocket_recorder(
            self._config,
            self._journal,
            session_id,
            "tts",
            references,
        )
        websocket = await SansIoWebSocket.connect(
            _deepgram_tts_url(self._config.endpoint, voice),
            f"Token {self._config.api_key}",
            record,
        )
        return DeepgramTtsSession(
            self._config,
            self._journal,
            session_id,
            websocket,
            references,
        )


class DeepgramTtsSession:
    def __init__(
        self,
        config: DeepgramConfig,
        journal: ExchangeJournal,
        session_id: UUID,
        websocket: SansIoWebSocket,
        references: list[RawRef],
    ) -> None:
        self._config = config
        self._journal = journal
        self._id = session_id
        self._websocket = websocket
        self._lock = asyncio.Lock()
        self._terminal: SessionTerminal | None = None
        self._session_events: asyncio.Queue[SessionTerminalEvent] = asyncio.Queue()
        self._refs = references
        self._active: DeepgramSynthesisAttempt | None = None
        self._completion_state = _TtsCompletionState.OPEN

    async def start(self, utterance: SynthesisUtterance) -> DeepgramSynthesisAttempt:
        async with self._lock:
            if self._terminal:
                raise RuntimeError("session terminal")
            if self._active is not None and not self._active.done:
                raise RuntimeError("Deepgram TTS permits one active operation per socket")
            self._active = DeepgramSynthesisAttempt(
                self._config,
                self._journal,
                self._websocket,
                utterance,
            )
            return self._active

    def session_events(self) -> AsyncIterator[SessionTerminalEvent]:
        async def stream() -> AsyncIterator[SessionTerminalEvent]:
            yield await self._session_events.get()

        return stream()

    async def finish(self) -> SessionTerminal:
        if self._active is not None:
            await self._active.finish()
        return await self._end(Outcome.SUCCEEDED, {"type": "Close"})

    async def cancel(self) -> SessionTerminal:
        if self._active is not None:
            await self._active.cancel()
        return await self._end(Outcome.CANCELLED, {"type": "Clear"})

    async def _end(self, outcome: Outcome, message: dict[str, str]) -> SessionTerminal:
        async with self._lock:
            if self._terminal:
                return self._terminal
            body = json.dumps(message, separators=(",", ":")).encode()
            ref = self._journal.append(
                meeting_id=self._config.meeting_id,
                attempt_id=self._id,
                stage="tts",
                transport="websocket",
                direction=message["type"].lower() + "-out",
                media_type="application/json",
                payload=body,
            )
            self._refs.append(ref)
            error: ProviderError | None = None
            try:
                async with asyncio.timeout(_COMPLETION_TIMEOUT_SECONDS):
                    await self._websocket.send(body.decode())
                    self._completion_state = _TtsCompletionState.CLOSE_SENT
                    await self._websocket.close()
                    self._completion_state = _TtsCompletionState.COMPLETE
            except Exception as exception:
                outcome = Outcome.FAILED
                error = ProviderError(
                    ErrorKind.TRANSPORT,
                    "session",
                    RetryAdvice.UNSPECIFIED,
                    type(exception).__name__,
                    None,
                    None,
                    self._id,
                    tuple(self._refs),
                    str(exception),
                )
            self._terminal = SessionTerminal(
                uuid4(),
                self._id,
                outcome,
                error,
                0,
                0,
                0,
                Transport.WEBSOCKET,
                tuple(self._refs),
            )
            self._journal.terminal(self._id, terminal_bytes(self._terminal))
            await self._session_events.put(SessionTerminalEvent(self._terminal))
            return self._terminal


class DeepgramSynthesisAttempt:
    _EVENT_QUEUE_SIZE = 64

    def __init__(
        self,
        config: DeepgramConfig,
        journal: ExchangeJournal,
        websocket: SansIoWebSocket,
        utterance: SynthesisUtterance,
    ) -> None:
        self._config = config
        self._journal = journal
        self._websocket = websocket
        self._utterance = utterance
        self._event_queue: asyncio.Queue[TtsEvent] = asyncio.Queue(
            maxsize=self._EVENT_QUEUE_SIZE + 1
        )
        self._event_slots = asyncio.Semaphore(self._EVENT_QUEUE_SIZE)
        self._terminal: OperationTerminal | None = None
        self._terminal_lock = asyncio.Lock()
        self._samples = 0
        self._refs: list[RawRef] = []
        self._completion_state = _TtsCompletionState.OPEN
        self._task = asyncio.create_task(self._run())

    @property
    def done(self) -> bool:
        return self._terminal is not None

    def events(self) -> AsyncIterator[TtsEvent]:
        async def stream() -> AsyncIterator[TtsEvent]:
            while True:
                event = await self._event_queue.get()
                if not isinstance(event, OperationTerminalEvent):
                    self._event_slots.release()
                yield event
                if isinstance(event, OperationTerminalEvent):
                    return

        return stream()

    async def finish(self) -> OperationTerminal:
        await self._task
        assert self._terminal
        return self._terminal

    async def cancel(self) -> OperationTerminal:
        async with self._terminal_lock:
            if self._terminal:
                return self._terminal
            self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        body = b'{"type":"Clear"}'
        ref = self._record("clear-out", "application/json", body)
        self._refs.append(ref)
        try:
            await asyncio.wait_for(
                self._websocket.send(body.decode()),
                _COMPLETION_TIMEOUT_SECONDS,
            )
        except Exception as exception:
            return await self._terminalize(
                Outcome.FAILED,
                self._operation_error(exception),
            )
        return await self._terminalize(Outcome.CANCELLED, None)

    async def _run(self) -> None:
        try:
            async with asyncio.timeout(_COMPLETION_TIMEOUT_SECONDS):
                for text in _tts_text_messages(self._utterance.text):
                    await self._send_command(
                        "text-out",
                        _json_command("Speak", text=text),
                    )
                await self._send_command("flush-out", _json_command("Flush"))
                self._completion_state = _TtsCompletionState.FLUSH_SENT
                async for message in self._websocket:
                    if await self._handle_inbound_message(message):
                        break
                if self._completion_state is not _TtsCompletionState.FLUSH_ACKNOWLEDGED:
                    raise ConnectionError(
                        "Deepgram TTS transport ended before Flush acknowledgement"
                    )
            await self._terminalize(Outcome.SUCCEEDED, None)
        except Exception as exception:
            try:
                await asyncio.wait_for(
                    self._websocket.close(
                        code=1011,
                        reason="synthesis completion failed",
                    ),
                    _CLOSE_TIMEOUT_SECONDS,
                )
            except (Exception, asyncio.CancelledError):
                pass
            await self._terminalize(
                Outcome.FAILED,
                self._operation_error(exception),
            )

    def _operation_error(self, exception: BaseException) -> ProviderError:
        return ProviderError(
            ErrorKind.TRANSPORT,
            "operation",
            RetryAdvice.NEVER if self._samples else RetryAdvice.UNSPECIFIED,
            type(exception).__name__,
            None,
            None,
            self._utterance.attempt_id,
            tuple(self._refs),
            str(exception),
        )

    async def _send_command(self, direction: str, body: bytes) -> None:
        reference = self._record(direction, "application/json", body)
        self._refs.append(reference)
        await self._websocket.send(body.decode())

    async def _handle_inbound_message(self, message: str | bytes) -> bool:
        if isinstance(message, bytes):
            reference = self._record(
                "frame-in",
                "application/octet-stream",
                message,
            )
            self._refs.append(reference)
            await self._handle_audio(message, reference)
            return False

        reference = self._record(
            "frame-in",
            "application/json",
            message.encode(),
        )
        self._refs.append(reference)
        return await self._handle_control_message(message, reference)

    async def _handle_audio(self, payload: bytes, reference: RawRef) -> None:
        if len(payload) % 2:
            raise ValueError("Deepgram LINEAR16 payload is not sample-aligned")
        sample_range = SampleRange(
            self._samples,
            self._samples + len(payload) // 2,
        )
        self._samples = sample_range.end
        await self._emit_event(
            AudioEvent(
                self._utterance.operation_id,
                len(self._refs),
                sample_range,
                payload,
                48000,
                1,
                reference,
            )
        )

    async def _handle_control_message(
        self,
        message: str,
        reference: RawRef,
    ) -> bool:
        message_type = json.loads(message).get("type")
        if message_type == "Flushed":
            self._completion_state = _TtsCompletionState.FLUSH_ACKNOWLEDGED
            await self._emit_event(
                SynthesisBoundary(
                    self._utterance.operation_id,
                    SampleRange(0, max(1, self._samples)),
                    reference,
                )
            )
            return True
        if message_type not in {"Metadata", "Cleared", "Warning"}:
            raise ValueError(f"unsupported Deepgram TTS message {message_type}")
        return False

    async def _emit_event(self, event: TtsEvent) -> None:
        await self._event_slots.acquire()
        try:
            self._event_queue.put_nowait(event)
        except BaseException:
            self._event_slots.release()
            raise

    async def _terminalize(
        self, outcome: Outcome, error: ProviderError | None
    ) -> OperationTerminal:
        async with self._terminal_lock:
            if self._terminal:
                return self._terminal
            self._terminal = OperationTerminal(
                uuid4(),
                self._utterance.operation_id,
                self._utterance.attempt_id,
                outcome,
                error,
                RetryDecision(RetryAction.STOP, None, "application decides replay", None),
                1,
                len(self._refs),
                self._samples,
                Transport.WEBSOCKET,
                tuple(self._refs),
                self._config.credential_fingerprint,
            )
            self._journal.terminal(self._utterance.attempt_id, terminal_bytes(self._terminal))
            await self._event_queue.put(OperationTerminalEvent(self._terminal))
            return self._terminal

    def _record(self, direction: str, media_type: str, payload: bytes) -> RawRef:
        return self._journal.append(
            meeting_id=self._config.meeting_id,
            attempt_id=self._utterance.attempt_id,
            stage="tts",
            transport="websocket",
            direction=direction,
            media_type=media_type,
            payload=payload,
            sample_range=self._utterance.source_range,
        )
