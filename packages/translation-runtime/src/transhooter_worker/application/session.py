from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID, uuid4

from transhooter_worker.application.pipeline import (
    CaptionRevision,
    OrderedStageQueue,
    ProviderTerminalSink,
    UtteranceAssembler,
    publish_complete_pcm_frames,
    upsert_final_span,
)
from transhooter_worker.application.retry import FrozenRetryPolicy
from transhooter_worker.application.session_audio import (
    _audio_after_watermark as _audio_after_watermark,
)
from transhooter_worker.domain.models import (
    AudioChunk,
    AudioEvent,
    BoundaryEvent,
    Finality,
    Lifecycle,
    OperationTerminal,
    OperationTerminalEvent,
    Outcome,
    RetryAction,
    RetryDecision,
    SampleRange,
    SessionTerminal,
    SessionTerminalEvent,
    SynthesisUtterance,
    TranscriptEvent,
)
from transhooter_worker.ports.providers import (
    StreamingSttProvider,
    StreamingTtsProvider,
    SttSession,
    SynthesisAttempt,
    TextTranslationProvider,
    TtsSession,
)

CaptionSink = Callable[[CaptionRevision], Awaitable[None]]
AudioSink = Callable[[bytes], Awaitable[None]]
CheckpointSink = Callable[[int, int, bool], Awaitable[None]]
NormalizedSink = Callable[[object], Awaitable[None]]
StageGate = Callable[[str, int], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class DirectionSpec:
    source_participant_id: UUID
    destination_participant_id: UUID
    source_language: str
    target_language: str
    voice: str | None
    same_language: bool


class DirectionSession:
    """One source STT session and one ordered Translation/TTS queue per destination."""

    def __init__(
        self,
        spec: DirectionSpec,
        stt_provider: StreamingSttProvider,
        translation_provider: TextTranslationProvider,
        tts_provider: StreamingTtsProvider,
        caption_sink: CaptionSink,
        audio_sink: AudioSink,
        checkpoint_sink: CheckpointSink,
        normalized_sink: NormalizedSink | None = None,
        stage_gate: StageGate | None = None,
        terminal_sink: ProviderTerminalSink | None = None,
    ) -> None:
        self._spec = spec
        self._stt_provider = stt_provider
        self._translation_provider = translation_provider
        self._tts_provider = tts_provider
        self._caption_sink = caption_sink
        self._audio_sink = audio_sink
        self._checkpoint_sink = checkpoint_sink
        self._normalized_sink = normalized_sink
        self._stage_gate = stage_gate
        self._stt: SttSession | None = None
        self._tts: TtsSession | None = None
        self._events_task: asyncio.Task[None] | None = None
        self._terminal_sink = terminal_sink
        self._assembler = UtteranceAssembler(
            translation_provider,
            spec.source_language,
            spec.target_language,
            stage_gate,
            normalized_sink,
            terminal_sink,
        )
        self._stage_queue = OrderedStageQueue(16)
        self._stage_task: asyncio.Task[None] | None = None
        self._boundary_task: asyncio.Task[None] | None = None
        self._lifecycle_lock = asyncio.Lock()
        self._state = Lifecycle.OPEN
        self._finish_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._normalized_lock = asyncio.Lock()
        self._last_input = 0
        self._last_output = 0
        self._recent_audio: list[AudioChunk] = []
        self._committed_input = 0
        self._stt_attempt = 1
        self._same_language_spans: list[TranscriptEvent] = []
        self._same_language_utterance = uuid4()
        self._stop_requested = asyncio.Event()
        self._cancel_requested = asyncio.Event()
        self._replacement_ready: asyncio.Future[SttSession | None] = (
            asyncio.get_running_loop().create_future()
        )
        self._replacement_open_task: asyncio.Task[SttSession] | None = None
        self._replacement_candidate: SttSession | None = None
        self._active_synthesis_attempt: SynthesisAttempt | None = None
        self._last_stt_terminal: SessionTerminal | None = None
        self._emitted_terminal_ids: set[UUID] = set()
        self._reported_terminal_ids: set[UUID] = set()
        self._session_started_at_ms: dict[UUID, int] = {}
        self._operation_started_at_ms: dict[UUID, int] = {}
        self._stt_session_ids: set[UUID] = set()
        self._failure: asyncio.Future[BaseException] = asyncio.get_running_loop().create_future()
        self._shutdown_failure: BaseException | None = None

    @property
    def failure(self) -> asyncio.Future[BaseException]:
        return self._failure

    def _observe_task(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        error = task.exception()
        if (
            error is not None
            and not (self._stop_requested.is_set() and self._is_missing_terminal_error(error))
            and not self._failure.done()
        ):
            self._failure.set_result(error)

    async def start(self) -> None:
        if self._stt is not None:
            return
        if not self._spec.same_language and not self._spec.voice:
            raise ValueError("translated direction requires an explicit capability-approved voice")
        if self._stage_gate is not None:
            await self._stage_gate("stt_start", 1)
        stt_session_id = uuid4()
        self._stt_session_ids.add(stt_session_id)
        self._session_started_at_ms[stt_session_id] = int(time.time() * 1000)
        self._stt = await self._stt_provider.open(
            stt_session_id,
            self._spec.source_language,
        )
        self._replacement_ready.set_result(self._stt)
        if not self._spec.same_language:
            assert self._spec.voice is not None
            tts_session_id = uuid4()
            self._session_started_at_ms[tts_session_id] = int(time.time() * 1000)
            self._tts = await self._tts_provider.open(
                tts_session_id, self._spec.target_language, self._spec.voice
            )
        self._events_task = asyncio.create_task(self._consume_stt())
        self._events_task.add_done_callback(self._observe_task)
        self._stage_task = asyncio.create_task(self._stage_queue.run())
        self._stage_task.add_done_callback(self._observe_task)
        self._boundary_task = asyncio.create_task(self._periodic_boundaries())
        self._boundary_task.add_done_callback(self._observe_task)

    async def send_audio(self, chunk: AudioChunk) -> None:
        async with self._send_lock:
            async with self._lifecycle_lock:
                if self._state is not Lifecycle.OPEN or self._stop_requested.is_set():
                    raise RuntimeError("direction session is draining or terminal")
                if self._stt is None:
                    raise RuntimeError("direction session has not started")
                if chunk.samples.start != self._last_input:
                    raise ValueError("source audio must be contiguous and ordered")
                if self._stage_gate is not None:
                    await self._stage_gate("stt", chunk.samples.length)
                self._recent_audio.append(chunk)
                cutoff = chunk.samples.end - 32_000
                self._recent_audio = [
                    item
                    for item in self._recent_audio
                    if item.samples.end > cutoff and item.samples.end > self._committed_input
                ]
                current = self._stt
                self._last_input = chunk.samples.end
                send_failed = False
                try:
                    await current.send_audio(chunk)
                except Exception as error:
                    if self._stop_requested.is_set():
                        raise RuntimeError("direction session is draining or terminal") from error
                    send_failed = True
                    if self._replacement_ready.done():
                        self._replacement_ready = asyncio.get_running_loop().create_future()
                    replacement_ready = self._replacement_ready
                if not send_failed and self._stop_requested.is_set():
                    raise RuntimeError("direction session is draining or terminal")

            if send_failed:
                replacement = await replacement_ready
                if replacement is None:
                    raise RuntimeError("STT replacement was interrupted")
                async with self._lifecycle_lock:
                    if self._state is not Lifecycle.OPEN:
                        raise RuntimeError("direction session is draining or terminal")
                    if self._stt is current:
                        raise RuntimeError("STT replacement did not become current")
        await self._checkpoint_sink(self._last_input, self._last_output, False)

    async def boundary(self) -> UUID:
        async with self._lifecycle_lock:
            if self._state is not Lifecycle.OPEN or self._stop_requested.is_set():
                raise RuntimeError("direction session is draining or terminal")
            if self._stt is None:
                raise RuntimeError("direction session has not started")
            boundary_id = uuid4()
            receipt = await self._stt.request_boundary(boundary_id)
        if not receipt.accepted:
            raise RuntimeError("provider does not support required utterance boundaries")
        return boundary_id

    async def _periodic_boundaries(self) -> None:
        while True:
            await asyncio.sleep(15)
            if self._last_input > 0:
                await self.boundary()

    async def finish(self) -> None:
        async with self._finish_lock:
            if self._state is Lifecycle.TERMINAL:
                return
            self._stop_requested.set()
            self._resolve_replacement_waiter(None)
            await self._cancel_replacement_open()
            await self._cancel_replacement_candidate()

            returned_stt = await self._stt.finish() if self._stt else None
            async with self._lifecycle_lock:
                self._state = Lifecycle.DRAINING
            await self._cancel_and_await_task(self._boundary_task)

            shutdown_error: BaseException | None = self._shutdown_failure
            if self._events_task:
                result = (await asyncio.gather(self._events_task, return_exceptions=True))[0]
                if isinstance(result, BaseException):
                    internally_cancelled = (
                        isinstance(result, asyncio.CancelledError)
                        and self._cancel_requested.is_set()
                    )
                    if not internally_cancelled and not self._is_missing_terminal_error(result):
                        shutdown_error = shutdown_error or result
                        self._record_failure(result)
            stt_terminal = returned_stt
            if (
                returned_stt is not None
                and self._last_stt_terminal is not None
                and self._last_stt_terminal.session_id == returned_stt.session_id
            ):
                stt_terminal = self._last_stt_terminal
            if stt_terminal is not None:
                await self._emit_normalized(SessionTerminalEvent(stt_terminal))
                await self._report_terminal("stt", stt_terminal, self._stt_attempt)
                shutdown_error = self._retain_failed_terminal(stt_terminal, shutdown_error)

            try:
                await self._stage_queue.join()
            except BaseException as error:
                if not (
                    isinstance(error, asyncio.CancelledError) and self._cancel_requested.is_set()
                ):
                    shutdown_error = shutdown_error or error
                    self._record_failure(error)
            await self._cancel_and_await_task(self._stage_task)

            # Graceful shutdown terminalizes TTS only after all queued final
            # synthesis has completed.
            if self._tts:
                returned_tts = await self._tts.finish()
                tts_terminal = await self._drain_tts_terminal(returned_tts)
                await self._report_terminal("tts", tts_terminal, 1)
                shutdown_error = self._retain_failed_terminal(tts_terminal, shutdown_error)
            await self._checkpoint_sink(self._last_input, self._last_output, True)
            self._state = Lifecycle.TERMINAL
            if shutdown_error is not None:
                raise shutdown_error

    async def cancel(self) -> None:
        # Interrupt provider work before lifecycle/finish serialization: those
        # locks may be held by work that cancellation itself must release.
        self._stop_requested.set()
        self._cancel_requested.set()
        self._resolve_replacement_waiter(None)
        await self._cancel_replacement_open()
        await self._cancel_replacement_candidate()
        self._cancel_task(self._boundary_task)
        active_attempt = self._active_synthesis_attempt
        self._cancel_task(self._stage_task)

        cancellations: list[Awaitable[SessionTerminal | OperationTerminal]] = []
        if active_attempt is not None:
            cancellations.append(active_attempt.cancel())
        if self._stt is not None:
            cancellations.append(self._stt.cancel())
        if self._tts is not None:
            cancellations.append(self._tts.cancel())
        results = await asyncio.gather(*cancellations, return_exceptions=True)
        async with self._lifecycle_lock:
            if self._state is Lifecycle.TERMINAL:
                return
            self._state = Lifecycle.DRAINING

        shutdown_error: BaseException | None = self._shutdown_failure
        task_results = await asyncio.gather(
            *(
                task
                for task in (self._boundary_task, self._events_task, self._stage_task)
                if task is not None
            ),
            return_exceptions=True,
        )
        for task_result in task_results:
            if isinstance(task_result, BaseException) and not isinstance(
                task_result, asyncio.CancelledError
            ):
                if not self._is_missing_terminal_error(task_result):
                    shutdown_error = shutdown_error or task_result
                    self._record_failure(task_result)

        for result in results:
            if isinstance(result, BaseException):
                if not isinstance(result, asyncio.CancelledError):
                    shutdown_error = shutdown_error or result
                    self._record_failure(result)
            elif isinstance(result, SessionTerminal):
                await self._emit_normalized(SessionTerminalEvent(result))
                await self._report_terminal(
                    "stt" if result.session_id in self._stt_session_ids else "tts",
                    result,
                    self._stt_attempt if result.session_id in self._stt_session_ids else 1,
                )
                shutdown_error = self._retain_failed_terminal(result, shutdown_error)
            else:
                await self._emit_normalized(OperationTerminalEvent(result))
                await self._report_terminal("tts", result, 1)
                shutdown_error = self._retain_failed_terminal(result, shutdown_error)
        try:
            await self._stage_queue.join()
        except BaseException as error:
            if not isinstance(error, asyncio.CancelledError):
                shutdown_error = shutdown_error or error
                self._record_failure(error)

        async with self._finish_lock:
            if self._state is Lifecycle.TERMINAL:
                return
            await self._checkpoint_sink(self._last_input, self._last_output, True)
            self._state = Lifecycle.TERMINAL
            if shutdown_error is not None:
                raise shutdown_error

    @staticmethod
    def _cancel_task(task: asyncio.Task[None] | None) -> None:
        if task is not None and not task.done():
            task.cancel()

    async def _cancel_and_await_task(self, task: asyncio.Task[None] | None) -> None:
        self._cancel_task(task)
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)

    @staticmethod
    def _is_missing_terminal_error(error: BaseException) -> bool:
        return isinstance(error, RuntimeError) and "stream ended without terminal" in str(error)

    def _retain_failed_terminal(
        self,
        terminal: SessionTerminal | OperationTerminal,
        current: BaseException | None,
    ) -> BaseException | None:
        if terminal.outcome is not Outcome.FAILED:
            return current
        error = RuntimeError(
            f"provider {terminal.transport.value} operation ended with failed outcome"
        )
        self._record_failure(error)
        if self._shutdown_failure is None:
            self._shutdown_failure = error
        return current or error

    async def _drain_tts_terminal(self, fallback: SessionTerminal) -> SessionTerminal:
        if self._tts is None or self._cancel_requested.is_set():
            await self._emit_normalized(SessionTerminalEvent(fallback))
            return fallback
        observed: SessionTerminal | None = None
        async for event in self._tts.session_events():
            await self._emit_normalized(event)
            observed = event.terminal
            break
        if observed is None:
            await self._emit_normalized(SessionTerminalEvent(fallback))
            return fallback
        return observed

    def _record_failure(self, error: BaseException) -> None:
        if not self._failure.done():
            self._failure.set_result(error)

    def _resolve_replacement_waiter(self, replacement: SttSession | None) -> None:
        if not self._replacement_ready.done():
            self._replacement_ready.set_result(replacement)

    async def _cancel_replacement_open(self) -> None:
        task = self._replacement_open_task
        if task is None:
            return
        if not task.done():
            task.cancel()
        result = (await asyncio.gather(task, return_exceptions=True))[0]
        if isinstance(result, BaseException):
            if not isinstance(result, asyncio.CancelledError):
                self._record_failure(result)
                if self._shutdown_failure is None:
                    self._shutdown_failure = result
            return
        self._replacement_candidate = result
        await self._cancel_replacement_candidate()

    async def _cancel_replacement_candidate(self) -> None:
        replacement = self._replacement_candidate
        if replacement is None:
            return
        try:
            terminal = await replacement.cancel()
            await self._emit_normalized(SessionTerminalEvent(terminal))
            self._retain_failed_terminal(terminal, self._shutdown_failure)
        finally:
            if self._replacement_candidate is replacement:
                self._replacement_candidate = None

    async def _consume_stt(self) -> None:
        retry_policy = FrozenRetryPolicy(3, 100, 2000)
        try:
            consume_replacement = True
            while consume_replacement:
                consume_replacement = await self._consume_current_stt_stream(retry_policy)
        finally:
            self._resolve_replacement_waiter(None)

    async def _consume_current_stt_stream(
        self,
        retry_policy: FrozenRetryPolicy,
    ) -> bool:
        assert self._stt is not None
        async for event in self._stt.events():
            if isinstance(event, SessionTerminalEvent):
                self._last_stt_terminal = event.terminal
            await self._emit_normalized(event)
            if isinstance(event, TranscriptEvent):
                await self._handle_transcript_event(event)
            elif isinstance(event, BoundaryEvent):
                await self._handle_boundary_event(event)
            elif isinstance(event, SessionTerminalEvent):
                await self._stage_queue.join()
                if event.terminal.outcome is not Outcome.FAILED:
                    await self._report_terminal("stt", event.terminal, self._stt_attempt)
                    return False
                return await self._recover_failed_stt(
                    event.terminal,
                    retry_policy,
                )
        raise RuntimeError("STT stream ended without terminal")

    async def _report_terminal(
        self,
        stage: str,
        terminal: SessionTerminal | OperationTerminal,
        attempt_number: int,
        retry_of_attempt_id: UUID | None = None,
        decision: RetryDecision | None = None,
        occurred_at_ms: int | None = None,
    ) -> None:
        if terminal.terminal_id in self._reported_terminal_ids:
            return
        if self._terminal_sink is None:
            self._reported_terminal_ids.add(terminal.terminal_id)
            return
        if decision is None:
            decision = (
                terminal.retry
                if isinstance(terminal, OperationTerminal)
                else RetryDecision(
                    RetryAction.STOP, None, "provider terminal is authoritative", None
                )
            )
        identity = (
            terminal.attempt_id if isinstance(terminal, OperationTerminal) else terminal.session_id
        )
        started_at_ms = self._operation_started_at_ms.get(
            identity,
            self._session_started_at_ms.get(identity, occurred_at_ms or int(time.time() * 1000)),
        )
        try:
            await self._terminal_sink(
                stage,
                terminal,
                attempt_number,
                retry_of_attempt_id,
                decision,
                started_at_ms,
                occurred_at_ms or int(time.time() * 1000),
            )
        except BaseException as reporting_error:
            if terminal.outcome is Outcome.FAILED:
                provider_failure = RuntimeError(
                    f"provider {terminal.transport.value} operation ended with failed outcome"
                )
                provider_failure.add_note(f"terminal reporting also failed: {reporting_error}")
                raise provider_failure from reporting_error
            raise
        self._reported_terminal_ids.add(terminal.terminal_id)

    async def _emit_normalized(self, event: object) -> None:
        async with self._normalized_lock:
            terminal_id: UUID | None = None
            if isinstance(event, SessionTerminalEvent | OperationTerminalEvent):
                terminal_id = event.terminal.terminal_id
                if terminal_id in self._emitted_terminal_ids:
                    return
            if self._normalized_sink is not None:
                await self._normalized_sink(event)
            if terminal_id is not None:
                self._emitted_terminal_ids.add(terminal_id)

    async def _handle_transcript_event(self, event: TranscriptEvent) -> None:
        if self._spec.same_language:
            if event.finality is Finality.SPAN_FINAL:
                upsert_final_span(self._same_language_spans, event)
            return

        async def translate_provisional(
            transcript_event: TranscriptEvent = event,
        ) -> None:
            provisional = await self._assembler.transcript(
                transcript_event,
                time.monotonic_ns() // 1_000_000,
            )
            if provisional is not None:
                await self._caption_sink(provisional)

        await self._stage_queue.submit(
            event.finality is Finality.SPAN_FINAL,
            translate_provisional,
        )

    async def _handle_boundary_event(self, event: BoundaryEvent) -> None:
        self._commit_input_through(event.committed_through)
        if self._spec.same_language:
            await self._emit_same_language_boundary(event.committed_through)
            return

        async def translate_boundary(boundary_event: BoundaryEvent = event) -> None:
            revisions = await self._assembler.boundary(boundary_event)
            for revision in revisions:
                await self._caption_sink(revision)
                await self._synthesize(revision)

        await self._stage_queue.submit(True, translate_boundary)

    def _commit_input_through(self, committed_through: int) -> None:
        self._committed_input = max(self._committed_input, committed_through)
        self._recent_audio = [
            chunk for chunk in self._recent_audio if chunk.samples.end > self._committed_input
        ]

    async def _emit_same_language_boundary(self, committed_through: int) -> None:
        ready_spans = [
            span for span in self._same_language_spans if span.samples.end <= committed_through
        ]
        self._same_language_spans = [
            span for span in self._same_language_spans if span.samples.end > committed_through
        ]
        if not ready_spans:
            return
        text = " ".join(span.text.strip() for span in ready_spans).strip()
        revision = CaptionRevision(
            self._same_language_utterance,
            1,
            True,
            text,
            text,
            SampleRange(
                ready_spans[0].samples.start,
                ready_spans[-1].samples.end,
            ),
        )
        self._same_language_utterance = uuid4()
        await self._caption_sink(revision)

    async def _recover_failed_stt(
        self,
        terminal: SessionTerminal,
        retry_policy: FrozenRetryPolicy,
    ) -> bool:
        if terminal.error is None:
            raise RuntimeError("STT failed without normalized error")
        decision = retry_policy.decide(
            terminal.error,
            self._stt_attempt,
            0,
            0,
            0,
            random.SystemRandom().random(),
        )
        await self._emit_normalized(decision)
        await self._report_terminal(
            "stt",
            terminal,
            self._stt_attempt,
            decision=decision,
        )
        if decision.action is not RetryAction.RETRY:
            raise RuntimeError(f"STT degraded: {decision.reason}")
        if not await self._wait_for_retry(decision.delay_ms or 0):
            return False
        self._stt_attempt += 1
        async with self._lifecycle_lock:
            if self._state is not Lifecycle.OPEN or self._stop_requested.is_set():
                return False
            if self._replacement_ready.done():
                self._replacement_ready = asyncio.get_running_loop().create_future()
        self._replacement_open_task = asyncio.create_task(self._open_replacement_stt())
        try:
            replacement = await self._replacement_open_task
        except asyncio.CancelledError:
            if self._stop_requested.is_set():
                self._resolve_replacement_waiter(None)
                return False
            raise
        finally:
            self._replacement_open_task = None
        return await self._install_and_replay_stt(replacement)

    async def _wait_for_retry(self, delay_ms: int) -> bool:
        if self._stop_requested.is_set():
            return False
        try:
            await asyncio.wait_for(
                self._stop_requested.wait(),
                delay_ms / 1000,
            )
        except TimeoutError:
            pass
        return not self._stop_requested.is_set()

    async def _open_replacement_stt(self) -> SttSession:
        if self._stage_gate is not None:
            await self._stage_gate("stt_start", 1)
        replay_audio = tuple(
            clipped
            for chunk in self._recent_audio
            if (clipped := _audio_after_watermark(chunk, self._committed_input)) is not None
        )
        resume_at_sample = replay_audio[0].samples.start if replay_audio else self._committed_input
        session_id = uuid4()
        self._stt_session_ids.add(session_id)
        self._session_started_at_ms[session_id] = int(time.time() * 1000)
        return await self._stt_provider.open(
            session_id,
            self._spec.source_language,
            resume_at_sample=resume_at_sample,
            commit_watermark=self._committed_input,
        )

    async def _install_and_replay_stt(self, replacement: SttSession) -> bool:
        install_error: BaseException | None = None
        installed = False
        self._replacement_candidate = replacement
        try:
            async with self._lifecycle_lock:
                if self._state is Lifecycle.OPEN and not self._stop_requested.is_set():
                    self._stt = replacement
                    try:
                        replay_audio = tuple(
                            clipped
                            for chunk in self._recent_audio
                            if (clipped := _audio_after_watermark(chunk, self._committed_input))
                            is not None
                        )
                        for chunk in replay_audio:
                            if self._stage_gate is not None:
                                await self._stage_gate("stt", chunk.samples.length)
                            await replacement.send_audio(chunk)
                    except BaseException as error:
                        install_error = error
                    else:
                        if not self._stop_requested.is_set():
                            installed = True
                            self._resolve_replacement_waiter(replacement)
        finally:
            if self._replacement_candidate is replacement:
                self._replacement_candidate = None
        if installed:
            return True
        terminal = await replacement.cancel()
        await self._emit_normalized(SessionTerminalEvent(terminal))
        self._retain_failed_terminal(terminal, self._shutdown_failure)
        self._resolve_replacement_waiter(None)
        if install_error is not None and not self._stop_requested.is_set():
            raise install_error
        return False

    async def _synthesize(self, revision: CaptionRevision) -> None:
        assert self._tts is not None and self._spec.voice is not None
        operation_id = uuid4()
        retry_policy = FrozenRetryPolicy(3, 100, 2000)
        previous_attempt_id: UUID | None = None
        for attempt_number in range(1, retry_policy.maximum_attempts + 1):
            terminal, pending_pcm, occurred_at_ms = await self._run_synthesis_attempt(
                operation_id,
                revision,
            )
            if terminal.outcome is Outcome.SUCCEEDED:
                await self._report_terminal(
                    "tts",
                    terminal,
                    attempt_number,
                    previous_attempt_id,
                    occurred_at_ms=occurred_at_ms,
                )
                await self._flush_final_pcm_frame(pending_pcm)
                await self._checkpoint_sink(
                    self._last_input,
                    self._last_output,
                    False,
                )
                return
            decision = await self._handle_failed_synthesis(
                terminal,
                attempt_number,
                retry_policy,
            )
            await self._report_terminal(
                "tts",
                terminal,
                attempt_number,
                previous_attempt_id,
                decision,
                occurred_at_ms,
            )
            previous_attempt_id = terminal.attempt_id
            if decision.action is not RetryAction.RETRY:
                raise RuntimeError(f"synthesis degraded: {decision.reason}")
            await asyncio.sleep((decision.delay_ms or 0) / 1000)
        raise RuntimeError("synthesis retry policy exhausted")

    async def _run_synthesis_attempt(
        self,
        operation_id: UUID,
        revision: CaptionRevision,
    ) -> tuple[OperationTerminal, bytearray, int]:
        assert self._tts is not None and self._spec.voice is not None
        utterance = SynthesisUtterance(
            operation_id,
            uuid4(),
            revision.translated_text,
            self._spec.target_language,
            self._spec.voice,
            revision.samples,
        )
        self._operation_started_at_ms[utterance.attempt_id] = int(time.time() * 1000)
        if self._stage_gate is not None:
            await self._stage_gate("tts", len(revision.translated_text))
        attempt = await self._tts.start(utterance)
        self._active_synthesis_attempt = attempt
        terminal: OperationTerminal | None = None
        pending_pcm = bytearray()
        try:
            async for event in attempt.events():
                await self._emit_normalized(event)
                if isinstance(event, AudioEvent):
                    pending_pcm.extend(event.pcm)
                    await self._publish_complete_pcm_frames(pending_pcm)
                elif isinstance(event, OperationTerminalEvent):
                    terminal = event.terminal
        finally:
            if self._active_synthesis_attempt is attempt:
                self._active_synthesis_attempt = None
        if terminal is None:
            raise RuntimeError("synthesis stream ended without terminal")
        return terminal, pending_pcm, int(time.time() * 1000)

    async def _publish_complete_pcm_frames(self, pending_pcm: bytearray) -> None:
        pending_before = len(pending_pcm)
        try:
            published_samples = await publish_complete_pcm_frames(
                pending_pcm,
                self._audio_sink,
                1_920,
            )
        except Exception:
            delivered_samples = (pending_before - len(pending_pcm)) // 2
            self._last_output += delivered_samples
            await self._checkpoint_published_output_after_failure()
            raise
        self._last_output += published_samples

    async def _checkpoint_published_output_after_failure(self) -> None:
        if self._last_output:
            await self._checkpoint_sink(
                self._last_input,
                self._last_output,
                False,
            )

    async def _flush_final_pcm_frame(self, pending_pcm: bytearray) -> None:
        if not pending_pcm:
            return
        pending_pcm.extend(b"\0" * (1_920 - len(pending_pcm)))
        try:
            await self._audio_sink(bytes(pending_pcm))
        except Exception:
            await self._checkpoint_published_output_after_failure()
            raise
        self._last_output += 960

    async def _handle_failed_synthesis(
        self,
        terminal: OperationTerminal,
        attempt_number: int,
        retry_policy: FrozenRetryPolicy,
    ) -> RetryDecision:
        if terminal.error is None:
            raise RuntimeError("synthesis failed without normalized error")
        decision = retry_policy.decide(
            terminal.error,
            attempt_number,
            terminal.accepted_input,
            terminal.received_output,
            terminal.emitted_output,
            random.SystemRandom().random(),
        )
        await self._emit_normalized(decision)
        return decision
