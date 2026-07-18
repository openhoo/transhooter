from __future__ import annotations

import asyncio
import random
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID, uuid4

from transhooter_worker.application.retry import FrozenRetryPolicy
from transhooter_worker.domain.models import (
    BoundaryEvent,
    Finality,
    OperationTerminal,
    Outcome,
    RetryAction,
    RetryDecision,
    SampleRange,
    SessionTerminal,
    TranscriptEvent,
    TranslationOutcome,
    TranslationRequest,
)
from transhooter_worker.ports.providers import TextTranslationProvider

StageGate = Callable[[str, int], Awaitable[None]]
EvidenceSink = Callable[[object], Awaitable[None]]
ProviderTerminalSink = Callable[
    [
        str,
        OperationTerminal | SessionTerminal,
        int,
        UUID | None,
        RetryDecision,
        int,
        int,
    ],
    Awaitable[None],
]


@dataclass(frozen=True, slots=True)
class CaptionRevision:
    utterance_id: UUID
    revision: int
    final: bool
    source_text: str
    translated_text: str
    samples: SampleRange


def upsert_final_span(spans: list[TranscriptEvent], event: TranscriptEvent) -> None:
    matching_index = next(
        (index for index, span in enumerate(spans) if span.samples == event.samples),
        None,
    )
    if matching_index is None:
        spans.append(event)
        spans.sort(key=lambda span: (span.samples.start, span.samples.end))
    elif event.revision > spans[matching_index].revision:
        spans[matching_index] = event


class UtteranceAssembler:
    """Vendor-blind final-span assembly and bounded provisional coalescing."""

    def __init__(
        self,
        translate: TextTranslationProvider,
        source_language: str,
        target_language: str,
        stage_gate: StageGate | None = None,
        evidence_sink: EvidenceSink | None = None,
        terminal_sink: ProviderTerminalSink | None = None,
    ) -> None:
        self._translate = translate
        self._source = source_language
        self._target = target_language
        self._spans: list[TranscriptEvent] = []
        self._utterance_id = uuid4()
        self._revision = 0
        self._last_provisional_ms = -2_000
        self._last_provisional_event_ms = -300
        self._lock = asyncio.Lock()
        self._retry_policy = FrozenRetryPolicy(3, 100, 2000)
        self._stage_gate = stage_gate
        self._evidence_sink = evidence_sink
        self._terminal_sink = terminal_sink

    async def transcript(
        self,
        event: TranscriptEvent,
        now_ms: int,
    ) -> CaptionRevision | None:
        async with self._lock:
            if event.finality is Finality.SPAN_FINAL:
                upsert_final_span(self._spans, event)
                return None
            if now_ms - self._last_provisional_event_ms < 300:
                return None
            self._last_provisional_event_ms = now_ms
            if now_ms - self._last_provisional_ms < 2_000:
                return None
            self._last_provisional_ms = now_ms
            return await self._translate_text(
                event.text,
                event.samples,
                "provisional",
                False,
            )

    async def boundary(self, event: BoundaryEvent) -> tuple[CaptionRevision, ...]:
        async with self._lock:
            ready = [span for span in self._spans if span.samples.end <= event.committed_through]
            self._spans = [
                span for span in self._spans if span.samples.end > event.committed_through
            ]
            if not ready:
                return ()
            text = " ".join(span.text.strip() for span in ready).strip()
            bounds = SampleRange(ready[0].samples.start, ready[-1].samples.end)
            pieces = split_text(text, 4_500)
            revisions: list[CaptionRevision] = []
            for piece in pieces:
                revisions.append(await self._translate_text(piece, bounds, "final", True))
                self._utterance_id = uuid4()
                self._revision = 0
            return tuple(revisions)

    async def _translate_text(
        self, text: str, samples: SampleRange, purpose: str, final: bool
    ) -> CaptionRevision:
        operation_id = uuid4()
        previous_attempt_id: UUID | None = None
        for attempt_number in range(1, self._retry_policy.maximum_attempts + 1):
            request = TranslationRequest(
                operation_id,
                uuid4(),
                purpose,
                self._source,
                self._target,
                text,
                samples,
            )
            started_at_ms = int(time.time() * 1000)
            outcome = await self._run_translation_attempt(request)
            occurred_at_ms = int(time.time() * 1000)
            if outcome.result is not None:
                await self._report_terminal(
                    outcome.terminal,
                    attempt_number,
                    previous_attempt_id,
                    outcome.terminal.retry,
                    started_at_ms,
                    occurred_at_ms,
                )
                self._revision += 1
                return CaptionRevision(
                    self._utterance_id,
                    self._revision,
                    final,
                    text,
                    outcome.result.text,
                    samples,
                )
            decision = await self._handle_failed_translation(
                outcome.terminal,
                attempt_number,
            )
            await self._report_terminal(
                outcome.terminal,
                attempt_number,
                previous_attempt_id,
                decision,
                started_at_ms,
                occurred_at_ms,
            )
            previous_attempt_id = request.attempt_id
            if decision.action is not RetryAction.RETRY:
                raise RuntimeError(f"translation degraded: {decision.reason}")
            await asyncio.sleep((decision.delay_ms or 0) / 1000)
        raise RuntimeError("translation retry policy exhausted")

    async def _run_translation_attempt(self, request: TranslationRequest) -> TranslationOutcome:
        if self._stage_gate is not None:
            await self._stage_gate("translation", len(request.text))
        attempt = await self._translate.start(request)
        return await attempt.result()

    async def _handle_failed_translation(
        self, terminal: OperationTerminal, attempt_number: int
    ) -> RetryDecision:
        if terminal.error is None:
            raise RuntimeError("translation attempt ended without a result or error")
        decision = self._retry_policy.decide(
            terminal.error,
            attempt_number,
            terminal.accepted_input,
            terminal.received_output,
            terminal.emitted_output,
            random.SystemRandom().random(),
        )
        if self._evidence_sink is not None:
            await self._evidence_sink(decision)
        return decision

    async def _report_terminal(
        self,
        terminal: OperationTerminal,
        attempt_number: int,
        retry_of_attempt_id: UUID | None,
        decision: RetryDecision,
        started_at_ms: int,
        occurred_at_ms: int,
    ) -> None:
        if self._terminal_sink is None:
            return
        try:
            await self._terminal_sink(
                "translation",
                terminal,
                attempt_number,
                retry_of_attempt_id,
                decision,
                started_at_ms,
                occurred_at_ms,
            )
        except BaseException as reporting_error:
            if terminal.outcome is Outcome.FAILED:
                provider_failure = RuntimeError(
                    f"provider {terminal.transport.value} operation ended with failed outcome"
                )
                provider_failure.add_note(f"terminal reporting also failed: {reporting_error}")
                raise provider_failure from reporting_error
            raise


class OrderedStageQueue:
    def __init__(self, maximum: int = 64) -> None:
        if maximum <= 0:
            raise ValueError("maximum must be positive")
        self._maximum = maximum
        self._error: BaseException | None = None
        self._queue: deque[Callable[[], Awaitable[None]]] = deque()
        self._condition = asyncio.Condition()
        self._unfinished = 0
        self._joined = asyncio.Event()
        self._joined.set()

    async def submit(self, final: bool, work: Callable[[], Awaitable[None]]) -> None:
        async with self._condition:
            if self._error is not None:
                raise self._error
            if not final and len(self._queue) >= self._maximum:
                return
            while len(self._queue) >= self._maximum:
                await self._condition.wait()
                if self._error is not None:
                    raise self._error
            self._queue.append(work)
            self._unfinished += 1
            self._joined.clear()
            self._condition.notify()

    async def run(self) -> None:
        while True:
            async with self._condition:
                while not self._queue:
                    if self._error is not None:
                        raise self._error
                    await self._condition.wait()
                work = self._queue.popleft()
                self._condition.notify_all()
            try:
                await work()
            except BaseException as exc:
                async with self._condition:
                    self._error = exc
                    self._unfinished -= 1 + len(self._queue)
                    self._queue.clear()
                    if self._unfinished == 0:
                        self._joined.set()
                    self._condition.notify_all()
                raise
            else:
                async with self._condition:
                    self._unfinished -= 1
                    if self._unfinished == 0:
                        self._joined.set()

    async def join(self) -> None:
        await self._joined.wait()
        if self._error is not None:
            raise self._error


def split_text(text: str, maximum_codepoints: int) -> tuple[str, ...]:
    if len(text) <= maximum_codepoints:
        return (text,)
    pieces: list[str] = []
    remaining = text
    while len(remaining) > maximum_codepoints:
        cut = max(
            remaining.rfind(mark, 0, maximum_codepoints + 1)
            for mark in (". ", "! ", "? ", "。", "！", "？")
        )
        if cut <= 0:
            cut = maximum_codepoints
        else:
            cut += 1
        pieces.append(remaining[:cut].strip())
        remaining = remaining[cut:].lstrip()
    if remaining:
        pieces.append(remaining)
    return tuple(pieces)
