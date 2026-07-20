from __future__ import annotations

import asyncio
import math
import os
import struct
import time
from collections.abc import AsyncIterator
from uuid import UUID, uuid4

from transhooter_worker.adapters.fixture.scenario import FixtureScenario
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
)
from transhooter_worker.ports.providers import SttEvent, TtsEvent

FIXTURE_REF = RawRef(UUID(int=0), 0, "0" * 64, 0, "application/x-fixture")


def require_test_environment() -> None:
    if os.environ.get("APP_ENV") != "test":
        raise RuntimeError("fixture providers are restricted to APP_ENV=test")


def _provider_error(kind: str, attempt_id: UUID, scope: str) -> ProviderError:
    mapped = {
        "rate_limit": ErrorKind.RATE_LIMIT,
        "quota": ErrorKind.QUOTA,
        "transport": ErrorKind.TRANSPORT,
    }
    error_kind = mapped.get(kind, ErrorKind.PROVIDER)
    if error_kind in {ErrorKind.RATE_LIMIT, ErrorKind.TRANSPORT}:
        advice = RetryAdvice.RETRY_AFTER
        retry_delay_ms = 100
    else:
        advice = RetryAdvice.NEVER
        retry_delay_ms = None
    return ProviderError(
        error_kind,
        scope,
        advice,
        kind,
        "fixture-request",
        retry_delay_ms,
        attempt_id,
        (FIXTURE_REF,),
        f"injected fixture {kind}",
    )


class FixtureSttProvider:
    def __init__(self, scenario: FixtureScenario | None = None) -> None:
        require_test_environment()
        self._scenario = scenario or FixtureScenario(UUID(int=0), None)

    async def capabilities(self) -> StageCapabilities:
        return StageCapabilities(
            "fixture",
            "stt",
            "fixture://stt",
            ("test",),
            ("en-US", "de-DE"),
            ("deterministic",),
            (),
            FIXTURE_REF,
        )

    async def health(self, snapshot: str) -> ProviderHealth:
        return ProviderHealth(True, int(time.time() * 1000), None, FIXTURE_REF)

    async def open(
        self,
        session_id: UUID,
        language: str,
        *,
        resume_at_sample: int = 0,
        commit_watermark: int = 0,
    ) -> FixtureSttSession:
        return FixtureSttSession(session_id, self._scenario)


class FixtureSttSession:
    def __init__(self, session_id: UUID, scenario: FixtureScenario) -> None:
        self._id = session_id
        self._scenario = scenario
        self._event_queue: asyncio.Queue[SttEvent] = asyncio.Queue()
        self._terminal: SessionTerminal | None = None
        self._accepted = 0
        self._revision = 0
        self._chunks = 0

    async def send_audio(self, chunk: AudioChunk) -> None:
        if self._terminal:
            raise RuntimeError("session terminal")
        self._chunks += 1
        failure_after = self._configured_failure_after_chunks()
        if failure_after is not None and self._chunks > failure_after:
            await self._end(Outcome.FAILED, _provider_error("transport", self._id, "session"))
            raise RuntimeError("injected fixture STT transport failure")
        self._accepted = chunk.samples.end
        self._revision += 1
        await self._event_queue.put(self._transcript_event(chunk.samples, Finality.PROVISIONAL))

    def _configured_failure_after_chunks(self) -> int | None:
        return self._scenario.optional_nonnegative_int("stt", "failAfterChunks")

    def _transcript_event(
        self,
        sample_range: SampleRange,
        finality: Finality,
    ) -> TranscriptEvent:
        revision = self._revision + 1 if finality is Finality.SPAN_FINAL else self._revision
        return TranscriptEvent(
            sample_range,
            revision,
            finality,
            "fixture speech",
            (),
            1.0,
            FIXTURE_REF,
        )

    def events(self) -> AsyncIterator[SttEvent]:
        async def stream() -> AsyncIterator[SttEvent]:
            while True:
                event = await self._event_queue.get()
                yield event
                if isinstance(event, SessionTerminalEvent):
                    return

        return stream()

    async def request_boundary(self, boundary_id: UUID) -> BoundaryReceipt:
        final_range = SampleRange(
            max(0, self._accepted - 1),
            max(1, self._accepted),
        )
        await self._event_queue.put(self._transcript_event(final_range, Finality.SPAN_FINAL))
        await self._event_queue.put(BoundaryEvent(boundary_id, self._accepted, FIXTURE_REF))
        return BoundaryReceipt(True, boundary_id)

    async def finish(self) -> SessionTerminal:
        return await self._end(Outcome.SUCCEEDED, None)

    async def cancel(self) -> SessionTerminal:
        return await self._end(Outcome.CANCELLED, None)

    async def _end(self, outcome: Outcome, error: ProviderError | None) -> SessionTerminal:
        if not self._terminal:
            self._terminal = SessionTerminal(
                uuid4(),
                self._id,
                outcome,
                error,
                self._accepted,
                0,
                0,
                Transport.GRPC,
                (FIXTURE_REF,),
            )
            await self._event_queue.put(SessionTerminalEvent(self._terminal))
        return self._terminal


class FixtureTranslationProvider:
    def __init__(self, scenario: FixtureScenario | None = None) -> None:
        require_test_environment()
        self._scenario = scenario or FixtureScenario(UUID(int=0), None)

    async def capabilities(self) -> StageCapabilities:
        return StageCapabilities(
            "fixture",
            "translation",
            "fixture://translation",
            ("test",),
            ("en", "de", "fr"),
            ("deterministic",),
            (),
            FIXTURE_REF,
        )

    async def health(self, snapshot: str) -> ProviderHealth:
        return ProviderHealth(True, int(time.time() * 1000), None, FIXTURE_REF)

    async def start(self, request: TranslationRequest) -> FixtureTranslationAttempt:
        return FixtureTranslationAttempt(request, self._scenario)


class FixtureTranslationAttempt:
    def __init__(
        self,
        request: TranslationRequest,
        scenario: FixtureScenario,
    ) -> None:
        self._request = request
        self._scenario = scenario
        self._terminal: OperationTerminal | None = None

    async def result(self) -> TranslationOutcome:
        if self._terminal is not None:
            result = (
                self._translation_result() if self._terminal.outcome is Outcome.SUCCEEDED else None
            )
            return TranslationOutcome(result, self._terminal)
        failure = self._configured_failure()
        if failure:
            error = _provider_error(failure, self._request.attempt_id, "operation")
            terminal = await self._end(Outcome.FAILED, error)
            return TranslationOutcome(None, terminal)
        terminal = await self._end(Outcome.SUCCEEDED, None)
        result = self._translation_result() if terminal.outcome is Outcome.SUCCEEDED else None
        return TranslationOutcome(result, terminal)

    def _configured_failure(self) -> str:
        return str(self._scenario.section("translation").get("failure", ""))

    def _translation_result(self) -> TranslationResult:
        return TranslationResult(
            self._request.operation_id,
            self._request.attempt_id,
            f"[{self._request.target_language}] {self._request.text}",
            self._request.source_range,
            FIXTURE_REF,
        )

    async def cancel(self) -> OperationTerminal:
        return await self._end(Outcome.CANCELLED, None)

    async def _end(self, outcome: Outcome, error: ProviderError | None) -> OperationTerminal:
        if not self._terminal:
            self._terminal = OperationTerminal(
                uuid4(),
                self._request.operation_id,
                self._request.attempt_id,
                outcome,
                error,
                RetryDecision(RetryAction.STOP, None, "fixture", None),
                1,
                1 if outcome is Outcome.SUCCEEDED else 0,
                0,
                Transport.HTTP,
                (FIXTURE_REF,),
                "fixture",
            )
        return self._terminal


class FixtureTtsProvider:
    def __init__(self, scenario: FixtureScenario | None = None) -> None:
        require_test_environment()
        self._scenario = scenario or FixtureScenario(UUID(int=0), None)

    async def capabilities(self) -> StageCapabilities:
        return StageCapabilities(
            "fixture",
            "tts",
            "fixture://tts",
            ("test",),
            ("en-US", "de-DE", "fr-FR"),
            ("deterministic",),
            (("sample_rate", 48000),),
            FIXTURE_REF,
            ("fixture-voice",),
        )

    async def health(self, snapshot: str) -> ProviderHealth:
        return ProviderHealth(True, int(time.time() * 1000), None, FIXTURE_REF)

    async def open(self, session_id: UUID, language: str, voice: str) -> FixtureTtsSession:
        return FixtureTtsSession(session_id, self._scenario)


class FixtureTtsSession:
    def __init__(self, session_id: UUID, scenario: FixtureScenario) -> None:
        self._id = session_id
        self._scenario = scenario
        self._terminal: SessionTerminal | None = None
        self._session_events: asyncio.Queue[SessionTerminalEvent] = asyncio.Queue()

    async def start(
        self,
        utterance: SynthesisUtterance,
    ) -> FixtureSynthesisAttempt:
        return FixtureSynthesisAttempt(utterance, self._scenario)

    def session_events(self) -> AsyncIterator[SessionTerminalEvent]:
        async def stream() -> AsyncIterator[SessionTerminalEvent]:
            yield await self._session_events.get()

        return stream()

    async def finish(self) -> SessionTerminal:
        return await self._end(Outcome.SUCCEEDED)

    async def cancel(self) -> SessionTerminal:
        return await self._end(Outcome.CANCELLED)

    async def _end(self, outcome: Outcome) -> SessionTerminal:
        if not self._terminal:
            self._terminal = SessionTerminal(
                uuid4(),
                self._id,
                outcome,
                None,
                0,
                0,
                0,
                Transport.WEBSOCKET,
                (FIXTURE_REF,),
            )
            await self._session_events.put(SessionTerminalEvent(self._terminal))
        return self._terminal


_TONE_SAMPLES = 9_600
_TONE_PCM = b"".join(
    struct.pack("<h", int(6_000 * math.sin(2 * math.pi * 440 * sample / 48_000)))
    for sample in range(_TONE_SAMPLES)
)


class FixtureSynthesisAttempt:
    def __init__(
        self,
        utterance: SynthesisUtterance,
        scenario: FixtureScenario,
    ) -> None:
        self._utterance = utterance
        self._scenario = scenario
        self._terminal: OperationTerminal | None = None
        self._emitted = 0

    def events(self) -> AsyncIterator[TtsEvent]:
        async def stream() -> AsyncIterator[TtsEvent]:
            sample_count, fail_after_partial = self._configured_output()
            sample_range = SampleRange(0, sample_count)
            self._emitted = sample_count
            yield self._audio_event(sample_range)
            if not fail_after_partial:
                yield SynthesisBoundary(
                    self._utterance.operation_id,
                    sample_range,
                    FIXTURE_REF,
                )
            error = None
            if fail_after_partial:
                error = _provider_error(
                    "transport",
                    self._utterance.attempt_id,
                    "operation",
                )
            terminal = await self._end(
                (Outcome.FAILED if fail_after_partial else Outcome.SUCCEEDED),
                error,
            )
            yield OperationTerminalEvent(terminal)

        return stream()

    def _configured_output(self) -> tuple[int, bool]:
        config = self._scenario.section("tts")
        configured_samples = int(config.get("partialSamples", _TONE_SAMPLES))
        sample_count = max(
            1,
            min(_TONE_SAMPLES, configured_samples),
        )
        return sample_count, bool(config.get("failAfterPartial", False))

    def _audio_event(self, sample_range: SampleRange) -> AudioEvent:
        pcm = _TONE_PCM[: sample_range.end * 2]
        return AudioEvent(
            self._utterance.operation_id,
            0,
            sample_range,
            pcm,
            48000,
            1,
            FIXTURE_REF,
        )

    async def finish(self) -> OperationTerminal:
        return await self._end(Outcome.SUCCEEDED, None)

    async def cancel(self) -> OperationTerminal:
        return await self._end(Outcome.CANCELLED, None)

    async def _end(self, outcome: Outcome, error: ProviderError | None) -> OperationTerminal:
        if not self._terminal:
            self._terminal = OperationTerminal(
                uuid4(),
                self._utterance.operation_id,
                self._utterance.attempt_id,
                outcome,
                error,
                RetryDecision(RetryAction.STOP, None, "fixture", None),
                1,
                1,
                self._emitted if outcome is not Outcome.CANCELLED else 0,
                Transport.WEBSOCKET,
                (FIXTURE_REF,),
                "fixture",
            )
        return self._terminal
