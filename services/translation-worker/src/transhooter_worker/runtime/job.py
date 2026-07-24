from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4, uuid5

from livekit import agents, rtc
from opentelemetry import metrics, trace
from opentelemetry.trace import Status, StatusCode
from transhooter_spool import (
    ConsultationProducerAuthority,
    EncryptedSpool,
    SpoolUnavailable,
    deterministic_roomy_capacity,
    statvfs_capacity,
)

from transhooter_worker.adapters.scoped_journal import ScopedExchangeJournal
from transhooter_worker.application.session import DirectionResult, DirectionSession, DirectionSpec
from transhooter_worker.domain.models import (
    OperationTerminal,
    Outcome,
    ProviderHealth,
    RetryAction,
    RetryDecision,
    SessionTerminal,
    StageCapabilities,
)
from transhooter_worker.runtime.checkpoints import (
    ZERO_CHECKPOINT_WATERMARK,
    CheckpointWatermark,
    FinalDirectionCheckpoint,
    _persist_checkpoint,
    _restore_checkpoint_state,
    seal_terminal_checkpoints,
)
from transhooter_worker.runtime.consultation import (
    SourceTrackTimeline,
    _build_direction_sinks,
    _install_room_handlers,
)
from transhooter_worker.runtime.control_client import ControlClient
from transhooter_worker.runtime.job_metadata import (
    DirectionMetadata,
    FrozenStage,
    JobMetadata,
    SttStage,
    TranslationStage,
    TtsStage,
    _validated_job_metadata,
)
from transhooter_worker.runtime.provider_registry import (
    ProviderRegistry,
    Providers,
    credential_fingerprint,
)
from transhooter_worker.runtime.publisher import (
    PreservedAudioPublisher,
    publish_private_interpretation_tracks,
)
from transhooter_worker.runtime.redis_quota import RedisQuotaGate
from transhooter_worker.telemetry import bounded_error_kind

_tracer = trace.get_tracer(__name__)
_meter = metrics.get_meter(__name__)
_active_jobs = _meter.create_up_down_counter(
    "transhooter.worker.jobs.active",
    description="Currently active translation worker consultation jobs.",
    unit="{job}",
)
_jobs_total = _meter.create_counter(
    "transhooter.worker.jobs.total",
    description="Translation worker consultation jobs by terminal result.",
    unit="{job}",
)
_job_duration = _meter.create_histogram(
    "transhooter.worker.job.duration",
    description="Translation worker consultation job duration.",
    unit="s",
)
_provider_operation_duration = _meter.create_histogram(
    "transhooter.worker.provider.operation.duration",
    description="Provider operation duration reported at its deduplicated terminal.",
    unit="s",
)
_spool_utilization = _meter.create_histogram(
    "transhooter.worker.spool.utilization",
    description="Encrypted spool utilization ratio observed by worker heartbeats.",
    unit="1",
)
_preflight_duration = _meter.create_histogram(
    "transhooter.worker.preflight.duration",
    description="Frozen provider preflight duration.",
    unit="s",
)


def _add_metric(instrument: Any, value: int, attributes: dict[str, str]) -> None:
    try:
        instrument.add(value, attributes)
    except Exception:
        pass


def _record_metric(instrument: Any, value: float, attributes: dict[str, str]) -> None:
    try:
        instrument.record(value, attributes)
    except Exception:
        pass


def _start_span(name: str) -> Any | None:
    try:
        return _tracer.start_span(name)
    except Exception:
        return None


def _set_ok_status(span: Any | None) -> None:
    if span is None:
        return
    try:
        span.set_status(Status(StatusCode.OK))
    except Exception:
        pass


def _end_span(span: Any | None) -> None:
    if span is None:
        return
    try:
        span.end()
    except Exception:
        pass


def _job_failure_phase(error: BaseException) -> str:
    if isinstance(error, SpoolUnavailable):
        return "preservation"
    error_kind = bounded_error_kind(error)
    if error_kind == "aborted":
        return "cancellation"
    if error_kind == "validation":
        return "admission"
    return "runtime"


def _set_error_status(span: Any | None, error: BaseException, phase: str) -> None:
    if span is None:
        return
    try:
        span.set_attribute("error.kind", bounded_error_kind(error))
        span.set_attribute("failure.phase", phase)
        span.set_status(Status(StatusCode.ERROR))
    except Exception:
        pass


def _spool() -> EncryptedSpool:
    root = Path(os.environ["SPOOL_PATH"])
    database = Path(os.environ.get("SPOOL_DATABASE", str(root / "journal.sqlite3")))
    capacity_probe = (
        deterministic_roomy_capacity if os.environ.get("APP_ENV") == "test" else statvfs_capacity
    )
    return EncryptedSpool.from_keyring(
        root,
        database,
        Path(os.environ["SPOOL_KEYRING_FILE"]),
        capacity_probe=capacity_probe,
    )


async def _reported_preflight(
    operation: Callable[[], Awaitable[Any]],
) -> Any:
    started = time.monotonic()
    result = "succeeded"
    error_kind: str | None = None
    span = _start_span("transhooter.worker.provider.preflight")
    try:
        value = await operation()
        _set_ok_status(span)
        return value
    except BaseException as error:
        result = "failed"
        error_kind = bounded_error_kind(error)
        _set_error_status(span, error, "preflight")
        raise
    finally:
        attributes = {"result": result}
        if error_kind is not None:
            attributes["error.kind"] = error_kind
        _record_metric(
            _preflight_duration,
            time.monotonic() - started,
            attributes,
        )
        _end_span(span)


def _selection_scope(metadata: JobMetadata) -> tuple[tuple[str, str], ...]:
    return (
        ("generation", str(metadata.generation)),
        ("workerId", str(metadata.worker_identity)),
        ("profileId", metadata.selection.profile_id),
        ("profileRevision", str(metadata.selection.profile_revision)),
        ("capabilityHash", metadata.selection.capability_hash),
        ("snapshotHash", metadata.snapshot_hash),
        ("workerEpoch", str(metadata.worker_epoch)),
        ("writeEpoch", str(metadata.write_epoch)),
        (
            "expectedParticipantIds",
            json.dumps(
                [str(value) for value in metadata.expected_participant_ids],
                separators=(",", ":"),
            ),
        ),
        (
            "frozenDirections",
            json.dumps(
                [
                    direction.model_dump(mode="json", by_alias=True, exclude_none=True)
                    for direction in metadata.selection.directions
                ],
                separators=(",", ":"),
                sort_keys=True,
            ),
        ),
    )


def _validate_frozen_directions(metadata: JobMetadata) -> None:
    selected_ids = {direction.source_participant_id for direction in metadata.selection.directions}
    if selected_ids != set(metadata.expected_participant_ids):
        raise RuntimeError("provider selection participants do not match the exact frozen pair")
    if any(
        direction.source_participant_id == direction.destination_participant_id
        for direction in metadata.selection.directions
    ):
        raise RuntimeError("provider selection direction must cross distinct identities")


def _selected_locales(metadata: JobMetadata) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            locale
            for direction in metadata.selection.directions
            for locale in (
                direction.stt.locale,
                direction.tts.locale if direction.tts is not None else direction.stt.locale,
            )
        )
    )


def _expected_credential_version(profile_id: str, capability: StageCapabilities) -> str:
    if profile_id in {"google-eu", "google-speech-eu"}:
        return credential_fingerprint(
            Path(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]),
            "Google ADC",
        )
    if profile_id == "deepgram-deepl-eu":
        credential_file = (
            os.environ["DEEPL_API_KEY_FILE"]
            if capability.stage == "translation"
            else os.environ["DEEPGRAM_API_KEY_FILE"]
        )
        return credential_fingerprint(Path(credential_file), capability.provider)
    return "fixture"


def _expected_credential_reference(profile_id: str, stage: str) -> str:
    if profile_id in {"google-eu", "google-speech-eu"}:
        return "google-adc"
    if profile_id == "deepgram-deepl-eu":
        return "deepl-api-key" if stage == "translation" else "deepgram-api-key"
    return "fixture"


def _validate_frozen_stage(
    selected: FrozenStage | None,
    capability: StageCapabilities,
    profile_id: str,
) -> None:
    if selected is None:
        return
    if (
        selected.provider != capability.provider
        or selected.endpoint != capability.endpoint
        or selected.region not in capability.regions
        or selected.model not in capability.models
    ):
        raise RuntimeError("frozen provider stage does not match admitted capability")
    if selected.adapter_build != "transhooter-worker@0.1.0":
        raise RuntimeError("frozen provider adapter build differs from runtime")
    if selected.policy != "provider-profile-v1":
        raise RuntimeError("frozen provider policy differs from runtime")
    if selected.credential.reference != _expected_credential_reference(
        profile_id, capability.stage
    ):
        raise RuntimeError("frozen provider credential reference differs from admitted profile")
    if isinstance(selected, SttStage) and selected.locale not in capability.languages:
        raise RuntimeError("frozen STT locale is not capability-approved")
    if isinstance(selected, SttStage) and selected.encoding != "linear16":
        raise RuntimeError("frozen STT encoding is not runtime-approved")
    if isinstance(selected, TranslationStage) and (
        selected.source_code not in capability.languages
        or selected.target_code not in capability.languages
    ):
        raise RuntimeError("frozen Translation code is not capability-approved")
    if isinstance(selected, TtsStage) and (
        selected.locale not in capability.languages
        or selected.voice not in capability.voices
        or selected.sample_rate != dict(capability.limits).get("sample_rate")
    ):
        raise RuntimeError("frozen TTS format is not capability-approved")
    if isinstance(selected, TtsStage) and selected.encoding != "linear16":
        raise RuntimeError("frozen TTS encoding is not runtime-approved")
    required_limits = dict(capability.limits)
    if selected.limits != required_limits:
        raise RuntimeError("frozen provider limits differ from admitted capability")
    if selected.credential.version != _expected_credential_version(profile_id, capability):
        raise RuntimeError("frozen provider credential version differs from admitted profile")


async def _settled_fanout(*operations: Awaitable[Any]) -> tuple[Any, ...]:
    tasks = tuple(asyncio.ensure_future(operation) for operation in operations)
    try:
        return tuple(await asyncio.gather(*tasks))
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


async def _provider_preflight(
    metadata: JobMetadata,
    providers: Any,
) -> tuple[StageCapabilities, StageCapabilities, StageCapabilities, tuple[ProviderHealth, ...]]:
    stt_capabilities, translation_capabilities, tts_capabilities = await _settled_fanout(
        providers.stt.capabilities(),
        providers.translation.capabilities(),
        providers.tts.capabilities(),
    )
    health_providers = (
        (providers.stt,)
        if all(direction.mode == "same_language" for direction in metadata.selection.directions)
        else (providers.stt, providers.translation, providers.tts)
    )
    provider_health = tuple(
        await _settled_fanout(
            *(provider.health(metadata.selection.capability_hash) for provider in health_providers)
        )
    )
    if not all(item.healthy for item in provider_health):
        raise RuntimeError("frozen provider health preflight failed")
    for direction in metadata.selection.directions:
        for selected, capability in (
            (direction.stt, stt_capabilities),
            (direction.translation, translation_capabilities),
            (direction.tts, tts_capabilities),
        ):
            _validate_frozen_stage(selected, capability, metadata.selection.profile_id)
    return (
        stt_capabilities,
        translation_capabilities,
        tts_capabilities,
        provider_health,
    )


async def _heartbeat_loop(
    metadata: JobMetadata,
    spool: EncryptedSpool,
    control: ControlClient,
    provider_health: tuple[ProviderHealth, ...],
    quota_leases: list[tuple[RedisQuotaGate, str, str]],
    drain_requested: asyncio.Event,
) -> None:
    while True:
        usage = spool.usage_ratio()
        _record_metric(_spool_utilization, usage, {"role": "heartbeat"})
        if usage >= 0.80:
            raise RuntimeError("encrypted spool reached fail-closed 80% capacity")
        await asyncio.gather(
            *(gate.reserve_active(stage, reservation) for gate, stage, reservation in quota_leases)
        )
        accepted = await control.heartbeat(
            {
                "writeEpoch": metadata.write_epoch,
                "snapshotHash": metadata.snapshot_hash,
                "providersOk": all(item.healthy for item in provider_health),
                "archiveOk": usage < 0.80,
                "acceptingLoad": usage < 0.70,
            }
        )
        if not accepted:
            drain_requested.set()
            return
        await asyncio.sleep(5)


async def _cancel_stream_tasks(stream_tasks: set[asyncio.Task[None]]) -> None:
    for stream_task in stream_tasks:
        stream_task.cancel()
    await asyncio.gather(*stream_tasks, return_exceptions=True)


async def _drain_runtime(
    stream_tasks: set[asyncio.Task[None]],
    sessions: dict[UUID, DirectionSession],
    publishers: dict[UUID, PreservedAudioPublisher],
    provider_sets: list[Providers],
) -> dict[UUID, DirectionResult]:
    await _cancel_stream_tasks(stream_tasks)
    session_results = await asyncio.gather(
        *(session.finish() for session in sessions.values()),
        return_exceptions=True,
    )
    publisher_results = await asyncio.gather(
        *(publisher.drain() for publisher in publishers.values()),
        return_exceptions=True,
    )
    provider_results = await asyncio.gather(
        *(providers.aclose() for providers in provider_sets),
        return_exceptions=True,
    )
    direction_results = {
        source_id: (
            result
            if isinstance(result, DirectionResult)
            else DirectionResult(session._last_input, session._last_output)
        )
        for (source_id, session), result in zip(sessions.items(), session_results, strict=True)
    }
    errors = [
        result
        for result in (*session_results, *publisher_results, *provider_results)
        if isinstance(result, BaseException)
    ]
    if len(errors) == 1:
        error = errors[0]
        error.direction_results = direction_results  # type: ignore[attr-defined]
        raise error
    if errors:
        error_group = BaseExceptionGroup("translation runtime drain failed", errors)
        error_group.direction_results = direction_results  # type: ignore[attr-defined]
        raise error_group
    return direction_results


async def _unpublish_interpretation_tracks(
    ctx: agents.JobContext,
    interpretation_tracks: dict[str, tuple[rtc.AudioSource, rtc.LocalTrackPublication]],
) -> None:
    await asyncio.gather(
        *(
            ctx.room.local_participant.unpublish_track(publication.sid)
            for _, publication in interpretation_tracks.values()
        ),
        return_exceptions=True,
    )


def _completed_runtime_error(
    completed: set[asyncio.Future[Any]],
    heartbeat_task: asyncio.Task[None],
    stream_failure: asyncio.Future[BaseException],
    sessions: dict[UUID, DirectionSession],
) -> BaseException | None:
    if stream_failure in completed:
        return stream_failure.result()
    if heartbeat_task in completed and not heartbeat_task.cancelled():
        return heartbeat_task.exception()
    return next(
        (session.failure.result() for session in sessions.values() if session.failure in completed),
        None,
    )


class _MemoizedShutdown:
    def __init__(self, factory: Callable[[], Coroutine[Any, Any, None]]) -> None:
        self._factory = factory
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None

    @property
    def started(self) -> bool:
        return self._task is not None

    async def run(self) -> None:
        async with self._lock:
            if self._task is None:
                self._task = asyncio.create_task(self._factory())
            task = self._task
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise


@dataclass(slots=True)
class QuotaGateLifecycle:
    gates: list[RedisQuotaGate] = field(default_factory=list)
    leases: list[tuple[RedisQuotaGate, str, str]] = field(default_factory=list)
    _cleanup_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    _cleanup_task: asyncio.Task[None] | None = field(default=None, init=False)

    def add_gate(self, gate: RedisQuotaGate) -> None:
        if all(registered is not gate for registered in self.gates):
            self.gates.append(gate)

    def add_lease(self, gate: RedisQuotaGate, stage: str, reservation: str) -> None:
        self.leases.append((gate, stage, reservation))

    @staticmethod
    async def _settle(awaitables: tuple[Awaitable[Any], ...]) -> tuple[BaseException, ...]:
        if not awaitables:
            return ()
        results = await asyncio.gather(*awaitables, return_exceptions=True)
        return tuple(result for result in results if isinstance(result, BaseException))

    @staticmethod
    def _raise_cleanup_errors(errors: tuple[BaseException, ...]) -> None:
        if not errors:
            return
        if len(errors) == 1:
            raise errors[0]
        raise BaseExceptionGroup("Redis quota cleanup failed", list(errors))

    async def cleanup(self) -> None:
        async with self._cleanup_lock:
            if self._cleanup_task is None:
                leases = tuple(self.leases)
                gates = tuple(self.gates)

                async def cleanup_resources() -> None:
                    release_errors = await self._settle(
                        tuple(
                            gate.release_active(stage, reservation)
                            for gate, stage, reservation in leases
                        )
                    )
                    close_errors = await self._settle(tuple(gate.aclose() for gate in gates))
                    self._raise_cleanup_errors((*release_errors, *close_errors))

                self._cleanup_task = asyncio.create_task(cleanup_resources())
            cleanup_task = self._cleanup_task
        try:
            await asyncio.shield(cleanup_task)
        except asyncio.CancelledError:
            await cleanup_task
            raise


async def _supervise_runtime(
    ctx: agents.JobContext,
    metadata: JobMetadata,
    spool: EncryptedSpool,
    control: ControlClient,
    provider_health: tuple[ProviderHealth, ...],
    sessions: dict[UUID, DirectionSession],
    publishers: dict[UUID, PreservedAudioPublisher],
    quota_lifecycle: QuotaGateLifecycle,
    stream_tasks: set[asyncio.Task[None]],
    drain_runtime: Callable[[], Awaitable[dict[UUID, DirectionResult]]],
    stream_failure: asyncio.Future[BaseException],
    disconnected: asyncio.Event,
    drain_requested: asyncio.Event,
    checkpoint_hashes: dict[UUID, str],
    interpretation_tracks: dict[str, tuple[rtc.AudioSource, rtc.LocalTrackPublication]],
) -> None:
    heartbeat_task = asyncio.create_task(
        _heartbeat_loop(
            metadata,
            spool,
            control,
            provider_health,
            quota_lifecycle.leases,
            drain_requested,
        )
    )
    disconnect_task = asyncio.create_task(disconnected.wait())
    drain_task = asyncio.create_task(drain_requested.wait())
    waiters: set[asyncio.Future[Any]] = {
        heartbeat_task,
        disconnect_task,
        drain_task,
        stream_failure,
        *(session.failure for session in sessions.values()),
    }
    try:
        completed, _ = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        runtime_failure = _completed_runtime_error(
            completed,
            heartbeat_task,
            stream_failure,
            sessions,
        )
        if runtime_failure is not None:
            raise runtime_failure
        await drain_runtime()
    except BaseException:
        await _cancel_stream_tasks(stream_tasks)
        await _unpublish_interpretation_tracks(ctx, interpretation_tracks)
        raise
    finally:
        heartbeat_task.cancel()
        disconnect_task.cancel()
        drain_task.cancel()
        await asyncio.gather(
            heartbeat_task,
            disconnect_task,
            drain_task,
            return_exceptions=True,
        )
        await quota_lifecycle.cleanup()


@dataclass(slots=True)
class InitializationContext:
    ctx: agents.JobContext
    control: ControlClient
    sessions: dict[UUID, DirectionSession]
    interpretation_tracks: dict[str, tuple[rtc.AudioSource, rtc.LocalTrackPublication]]
    quota_lifecycle: QuotaGateLifecycle
    checkpoint_hashes: dict[UUID, str]
    provider_sets: list[Providers] = field(default_factory=list)
    _cleanup_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    _cleanup_task: asyncio.Task[None] | None = field(default=None, init=False)

    @staticmethod
    async def _settle(
        factory: Callable[[], tuple[Awaitable[Any], ...]],
    ) -> tuple[BaseException, ...]:
        try:
            awaitables = factory()
            if not awaitables:
                return ()
            results = await asyncio.gather(*awaitables, return_exceptions=True)
            return tuple(result for result in results if isinstance(result, BaseException))
        except BaseException as error:
            return (error,)

    async def _cleanup_resources(self, error: BaseException) -> None:
        errors = list(
            await self._settle(
                lambda: tuple(session.cancel() for session in self.sessions.values())
            )
        )
        errors.extend(
            await self._settle(
                lambda: tuple(
                    self.ctx.room.local_participant.unpublish_track(publication.sid)
                    for _, publication in self.interpretation_tracks.values()
                )
            )
        )
        errors.extend(
            await self._settle(
                lambda: tuple(providers.aclose() for providers in self.provider_sets)
            )
        )
        try:
            await self.quota_lifecycle.cleanup()
        except BaseException as cleanup_error:
            errors.append(cleanup_error)
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup("translation initialization cleanup failed", errors)

    async def cleanup(self, error: BaseException) -> None:
        async with self._cleanup_lock:
            if self._cleanup_task is None:
                self._cleanup_task = asyncio.create_task(self._cleanup_resources(error))
            cleanup_task = self._cleanup_task
        try:
            await asyncio.shield(cleanup_task)
        except asyncio.CancelledError:
            await cleanup_task
            raise

    async def _cleanup_failed_initialization(self, error: BaseException) -> None:
        try:
            await self.cleanup(error)
        except BaseException as cleanup_error:
            raise BaseExceptionGroup(
                "translation initialization failed during cleanup",
                [error, cleanup_error],
            ) from None

    async def await_effect(self, effect: Awaitable[Any]) -> Any:
        try:
            return await effect
        except BaseException as error:
            await self._cleanup_failed_initialization(error)
            raise

    async def construct(
        self,
        factory: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        try:
            return factory(*args, **kwargs)
        except BaseException as error:
            await self._cleanup_failed_initialization(error)
            raise


def _provider_error_payload(terminal: OperationTerminal | SessionTerminal) -> dict[str, Any] | None:
    error = terminal.error
    if terminal.outcome is not Outcome.FAILED or error is None:
        return None
    return {
        "kind": error.kind.value,
        "scope": error.scope,
        "providerRetryAdvice": error.provider_retry_advice.value,
        "providerCode": error.provider_code,
        "providerRequestId": error.provider_request_id,
        "retryDelayMs": error.retry_delay_ms,
        "attemptId": str(error.attempt_id),
        "rawObjectIds": [str(reference.object_id) for reference in error.raw_refs],
    }


def _raw_reference_payload(terminal: OperationTerminal | SessionTerminal) -> list[dict[str, Any]]:
    return [
        {
            "objectId": str(reference.object_id),
            "ordinal": reference.ordinal,
            "sha256": reference.sha256,
            "size": reference.size,
            "mediaType": reference.media_type,
        }
        for reference in terminal.raw_refs
    ]


def _build_provider_terminal_sink(
    metadata: JobMetadata,
    direction: DirectionMetadata,
    authority: ConsultationProducerAuthority,
    spool: EncryptedSpool,
    control: ControlClient,
) -> Callable[
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
]:
    reported_terminal_ids: set[UUID] = set()
    lock = asyncio.Lock()

    async def report(
        stage: str,
        terminal: OperationTerminal | SessionTerminal,
        attempt_number: int,
        retry_of_attempt_id: UUID | None,
        decision: RetryDecision,
        started_at_ms: int,
        occurred_at_ms: int,
    ) -> None:
        async with lock:
            if terminal.terminal_id in reported_terminal_ids:
                return
            selected = {
                "stt": direction.stt,
                "translation": direction.translation,
                "tts": direction.tts,
            }.get(stage)
            if selected is None:
                raise RuntimeError(f"provider terminal uses unavailable stage {stage}")
            operation_id = (
                terminal.operation_id
                if isinstance(terminal, OperationTerminal)
                else terminal.session_id
            )
            attempt_id = (
                terminal.attempt_id
                if isinstance(terminal, OperationTerminal)
                else terminal.session_id
            )
            retry_at_ms = (
                occurred_at_ms + (decision.delay_ms or 0)
                if decision.action is RetryAction.RETRY
                else None
            )
            fingerprint = (
                terminal.credential_fingerprint
                if isinstance(terminal, OperationTerminal)
                else (
                    "fixture"
                    if metadata.selection.profile_id == "fixture"
                    else selected.credential.version
                )
            )
            payload: dict[str, Any] = {
                "directionId": str(direction.capability_row_id),
                "stage": stage,
                "terminalId": str(terminal.terminal_id),
                "operationId": str(operation_id),
                "attemptId": str(attempt_id),
                "attemptNumber": attempt_number,
                "retryOfAttemptId": (
                    str(retry_of_attempt_id) if retry_of_attempt_id is not None else None
                ),
                "outcome": terminal.outcome.value,
                "error": _provider_error_payload(terminal),
                "retryDecision": {
                    "action": (
                        "do_not_retry"
                        if decision.action is RetryAction.STOP
                        else decision.action.value
                    ),
                    "reason": decision.reason,
                    "retryAtMs": retry_at_ms,
                    "previousAttemptId": (
                        str(decision.previous_attempt_id)
                        if decision.previous_attempt_id is not None
                        else None
                    ),
                },
                "watermarks": {
                    "acceptedInputSequence": None,
                    "acceptedInputSampleEnd": terminal.accepted_input,
                    "receivedOutputSequence": None,
                    "receivedOutputSampleEnd": terminal.received_output,
                    "emittedOutputSequence": None,
                    "emittedOutputSampleEnd": terminal.emitted_output,
                },
                "credentialVersion": selected.credential.version,
                "credentialFingerprint": fingerprint,
                "transport": terminal.transport.value,
                "rawReferences": _raw_reference_payload(terminal),
                "startedAtMs": started_at_ms,
                "occurredAtMs": max(started_at_ms, occurred_at_ms),
            }
            terminal_bytes_payload = json.dumps(
                payload, separators=(",", ":"), sort_keys=True
            ).encode()
            payload["terminalHash"] = hashlib.sha256(terminal_bytes_payload).hexdigest()
            for evidence_stage in (stage, "terminal"):
                spool.append(
                    authority,
                    meeting_id=metadata.consultation_id,
                    attempt_id=attempt_id,
                    stage=evidence_stage,
                    transport=terminal.transport.value,
                    direction=str(direction.capability_row_id),
                    media_type="application/json",
                    payload=terminal_bytes_payload,
                )
            reported_terminal_ids.add(terminal.terminal_id)
            retry_action = (
                "do_not_retry" if decision.action is RetryAction.STOP else decision.action.value
            )
            _record_metric(
                _provider_operation_duration,
                max(0, occurred_at_ms - started_at_ms) / 1000,
                {
                    "stage": stage,
                    "transport": terminal.transport.value,
                    "outcome": terminal.outcome.value,
                    "retry.action": retry_action,
                },
            )
            await control.provider_attempt(payload, event_id=terminal.terminal_id)

    return report


def _direction_scope(
    metadata: JobMetadata,
    direction: DirectionMetadata,
) -> tuple[tuple[str, str], ...]:
    return (
        ("generation", str(metadata.generation)),
        ("workerId", str(metadata.worker_identity)),
        ("workerEpoch", str(metadata.worker_epoch)),
        ("writeEpoch", str(metadata.write_epoch)),
        ("profileId", metadata.selection.profile_id),
        ("profileRevision", str(metadata.selection.profile_revision)),
        ("capabilityHash", metadata.selection.capability_hash),
        ("snapshotHash", metadata.snapshot_hash),
        ("sourceParticipantId", str(direction.source_participant_id)),
        ("destinationParticipantId", str(direction.destination_participant_id)),
        ("sourceLanguage", direction.source_language),
        ("targetLanguage", direction.target_language),
        (
            "frozenDirection",
            json.dumps(
                direction.model_dump(mode="json", by_alias=True, exclude_none=True),
                separators=(",", ":"),
                sort_keys=True,
            ),
        ),
    )


async def _reserve_direction_quota(
    metadata: JobMetadata,
    direction: DirectionMetadata,
    capabilities: tuple[StageCapabilities, StageCapabilities, StageCapabilities],
    initialization: InitializationContext,
) -> Callable[[str, int], Awaitable[None]] | None:
    if metadata.selection.profile_id == "fixture":
        return None
    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        error = RuntimeError("REDIS_URL is required for provider quota enforcement")
        await initialization._cleanup_failed_initialization(error)
        raise error
    gates: dict[str, RedisQuotaGate] = {}
    for selected, capability in zip(
        (direction.stt, direction.translation, direction.tts),
        capabilities,
        strict=True,
    ):
        if selected is None:
            continue
        gate = await initialization.construct(
            RedisQuotaGate,
            redis_url,
            capability.provider,
            selected.credential.reference,
            selected.region,
            {capability.stage: selected.limits},
        )
        initialization.quota_lifecycle.add_gate(gate)
        gates[capability.stage] = gate
        reservation = (
            f"{metadata.consultation_id}:{direction.source_participant_id}:{capability.stage}"
        )
        await initialization.await_effect(gate.reserve_active(capability.stage, reservation))
        initialization.quota_lifecycle.add_lease(gate, capability.stage, reservation)

    async def enforce_stage_quota(stage: str, amount: int) -> None:
        await gates[stage.removesuffix("_start")](stage, amount)

    return enforce_stage_quota


async def _run_consultation(ctx: agents.JobContext) -> None:
    metadata = _validated_job_metadata(ctx.job.metadata)
    spool = _spool()
    try:
        authority = spool.open_consultation_producer(
            meeting_id=metadata.consultation_id,
            generation=metadata.generation,
            worker_id=metadata.worker_identity,
            worker_epoch=metadata.worker_epoch,
            write_epoch=metadata.write_epoch,
        )
        try:
            await _run_owned_consultation(ctx, metadata, spool, authority)
        finally:
            authority.close()
    finally:
        spool.close()


async def _run_owned_consultation(
    ctx: agents.JobContext,
    metadata: JobMetadata,
    spool: EncryptedSpool,
    authority: ConsultationProducerAuthority,
) -> None:
    crash_before_seal = False
    if os.environ.get("APP_ENV") == "test":
        from transhooter_worker.adapters.fixture.scenario import FixtureScenario

        spool_scenario = FixtureScenario.configured(metadata.consultation_id).section("spool")
        if spool_scenario.get("preservationFail") is True:
            authority.close()
            raise SpoolUnavailable("injected preservation failure")
        crash_before_seal = spool_scenario.get("crashBeforeSeal") is True
    livekit_by_participant = dict(
        zip(metadata.expected_participant_ids, metadata.expected_livekit_identities, strict=True)
    )
    _validate_frozen_directions(metadata)
    providers = ProviderRegistry.resolve(
        metadata.selection.profile_id,
        metadata.consultation_id,
        ScopedExchangeJournal(spool, authority, _selection_scope(metadata)),
        _selected_locales(metadata),
    )
    if spool.usage_ratio() >= 0.70:
        authority.close()
        await providers.aclose()
        raise RuntimeError("encrypted spool admission requires capacity below 70%")
    control = ControlClient(
        os.environ["CONTROL_INTERNAL_URL"],
        Path(os.environ["INTERNAL_TOKEN_FILE"]),
        metadata.consultation_id,
        metadata.generation,
        metadata.worker_identity,
        metadata.worker_epoch,
        spool,
        authority,
    )
    sessions: dict[UUID, DirectionSession] = {}
    sessions_by_identity: dict[UUID, DirectionSession] = {}
    publishers: dict[UUID, PreservedAudioPublisher] = {}
    stream_tasks: set[asyncio.Task[None]] = set()
    quota_lifecycle = QuotaGateLifecycle()
    provider_sets: list[Providers] = [providers]
    checkpoint_state = _restore_checkpoint_state(spool, metadata)
    completed_sources = {
        source_id
        for source_id, watermark in checkpoint_state.watermarks.items()
        if watermark.terminal
    }
    if completed_sources:
        authority.close()
        await providers.aclose()
        await control.aclose()
        return

    final_results: dict[UUID, DirectionResult] = {}
    drain_task: asyncio.Task[dict[UUID, DirectionResult]] | None = None
    terminal_error: BaseException | None = None
    handoff_complete = False
    timelines: dict[UUID, SourceTrackTimeline] = {}
    interpretation_tracks: dict[str, tuple[rtc.AudioSource, rtc.LocalTrackPublication]] = {}
    completion_event_id = uuid5(
        metadata.consultation_id,
        f"worker-completion:{metadata.generation}:{metadata.worker_identity}:{metadata.worker_epoch}:{metadata.write_epoch}",
    )

    async def drain_runtime_once() -> dict[UUID, DirectionResult]:
        nonlocal drain_task
        if drain_task is None:
            drain_task = asyncio.create_task(
                _drain_runtime(stream_tasks, sessions, publishers, provider_sets)
            )
        try:
            return await asyncio.shield(drain_task)
        except asyncio.CancelledError:
            await drain_task
            raise

    async def seal_once() -> None:
        nonlocal final_results, terminal_error, handoff_complete
        if crash_before_seal:
            _request_supervisor_shutdown()
            raise RuntimeError("fatal pre-seal worker shutdown returned")
        spool.begin_consultation_settlement(authority)
        try:
            try:
                final_results = await drain_runtime_once()
            except BaseException as drain_error:
                recovered_results = getattr(drain_error, "direction_results", None)
                if isinstance(recovered_results, dict):
                    final_results = recovered_results
                terminal_error = terminal_error or drain_error
            try:
                await _unpublish_interpretation_tracks(ctx, interpretation_tracks)
            except BaseException as unpublish_error:
                terminal_error = terminal_error or unpublish_error
            for direction in metadata.selection.directions:
                source_id = direction.source_participant_id
                if source_id not in final_results:
                    session = sessions.get(source_id)
                    watermark = checkpoint_state.watermarks.get(
                        source_id, ZERO_CHECKPOINT_WATERMARK
                    )
                    final_results[source_id] = DirectionResult(
                        session._last_input if session is not None else watermark.input_sample,
                        session._last_output if session is not None else watermark.output_sample,
                    )
            final_directions = tuple(
                FinalDirectionCheckpoint(
                    source_id=direction.source_participant_id,
                    destination_id=direction.destination_participant_id,
                    watermark=CheckpointWatermark(
                        input_sequence=timelines.get(
                            livekit_by_participant[direction.source_participant_id],
                            SourceTrackTimeline(
                                cursor=checkpoint_state.watermarks.get(
                                    direction.source_participant_id, ZERO_CHECKPOINT_WATERMARK
                                ).input_sample,
                                sequence=checkpoint_state.watermarks.get(
                                    direction.source_participant_id, ZERO_CHECKPOINT_WATERMARK
                                ).input_sequence,
                            ),
                        ).sequence,
                        input_sample=final_results[direction.source_participant_id].input_sample,
                        provider_output_sample=final_results[
                            direction.source_participant_id
                        ].output_sample,
                        output_sample=final_results[direction.source_participant_id].output_sample,
                        terminal=True,
                    ),
                )
                for direction in metadata.selection.directions
            )
            assert len(final_directions) == 2
            seal_occurred_at_ms = int(time.time() * 1000)
            failure_payload: dict[str, object] | None
            failure_payload = (
                {
                    "kind": type(terminal_error).__name__,
                    "message": str(terminal_error)[:512],
                }
                if terminal_error is not None
                else None
            )
            deadline = time.monotonic() + 15
            delay = 0.05
            while True:
                try:
                    seal_terminal_checkpoints(
                        metadata,
                        spool,
                        authority,
                        checkpoint_state,
                        final_directions,
                        terminal_outcome="failed" if terminal_error is not None else "clean",
                        completion_event_id=completion_event_id,
                        failure=failure_payload,
                        occurred_at_ms=seal_occurred_at_ms,
                    )
                    handoff_complete = True
                    return
                except BaseException as error:
                    handoff = spool.consultation_handoff(
                        meeting_id=metadata.consultation_id,
                        generation=metadata.generation,
                        worker_id=metadata.worker_identity,
                        worker_epoch=metadata.worker_epoch,
                        write_epoch=metadata.write_epoch,
                    )
                    if handoff == "sealed":
                        handoff_complete = True
                        return
                    if time.monotonic() >= deadline:
                        spool.relinquish_consultation(
                            authority, "terminal seal retry deadline exhausted"
                        )
                        handoff_complete = True
                        raise SpoolUnavailable(
                            "terminal checkpoint seal could not be committed"
                        ) from error
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 1)
        except BaseException:
            if not handoff_complete:
                spool.relinquish_consultation(authority, "terminal checkpoint sealing failed")
                handoff_complete = True
            raise
        finally:
            authority.close()
            await quota_lifecycle.cleanup()
            await control.aclose()

    shutdown = _MemoizedShutdown(seal_once)
    ctx.add_shutdown_callback(shutdown.run)

    try:
        stt_caps, translation_caps, tts_caps, provider_health = await _reported_preflight(
            lambda: _provider_preflight(metadata, providers)
        )
        await providers.aclose()
        sessions_ready = asyncio.Event()
        disconnected = asyncio.Event()
        drain_requested = asyncio.Event()
        failure: asyncio.Future[BaseException] = asyncio.get_running_loop().create_future()
        for participant_id in metadata.expected_participant_ids:
            watermark = checkpoint_state.watermarks.get(participant_id, ZERO_CHECKPOINT_WATERMARK)
            timelines[livekit_by_participant[participant_id]] = SourceTrackTimeline(
                cursor=watermark.input_sample,
                sequence=watermark.input_sequence,
            )
        subscribe_if_allowed = _install_room_handlers(
            ctx,
            metadata,
            authority,
            spool,
            sessions_by_identity,
            timelines,
            sessions_ready,
            stream_tasks,
            failure,
            disconnected,
            drain_requested,
        )
        initialization = InitializationContext(
            ctx,
            control,
            sessions,
            interpretation_tracks,
            quota_lifecycle,
            checkpoint_state.hashes,
            provider_sets,
        )
        initialize = initialization.await_effect
        initialize_value = initialization.construct
        await initialize(ctx.connect(auto_subscribe=agents.AutoSubscribe.SUBSCRIBE_NONE))

        async def adopt_existing_publications() -> None:
            for participant in ctx.room.remote_participants.values():
                for publication in participant.track_publications.values():
                    subscribe_if_allowed(publication, participant)

        await initialize(adopt_existing_publications())

        async def checkpoint(
            source_id: UUID,
            destination_id: UUID,
            input_sample: int,
            output_sample: int,
            terminal: bool,
            input_sequence: int,
            provider_output_sample: int,
        ) -> None:
            if terminal:
                return
            await _persist_checkpoint(
                metadata,
                spool,
                authority,
                checkpoint_state,
                source_id,
                destination_id,
                input_sample,
                output_sample,
                input_sequence=input_sequence,
                provider_output_sample=provider_output_sample,
            )

        translated_targets = await initialize_value(
            lambda: tuple(
                str(livekit_by_participant[direction.destination_participant_id])
                for direction in metadata.selection.directions
                if direction.mode == "translated"
            )
        )
        if translated_targets:
            interpretation_tracks.update(
                await initialize(
                    publish_private_interpretation_tracks(ctx.room, translated_targets)
                )
            )
        for direction in metadata.selection.directions:
            publisher = None
            watermark = checkpoint_state.watermarks.get(
                direction.source_participant_id, ZERO_CHECKPOINT_WATERMARK
            )
            if direction.mode == "translated":
                source, _ = await initialize_value(
                    interpretation_tracks.__getitem__,
                    str(livekit_by_participant[direction.destination_participant_id]),
                )
                publisher = await initialize_value(
                    PreservedAudioPublisher,
                    metadata.consultation_id,
                    authority,
                    uuid4(),
                    direction.destination_participant_id,
                    source,
                    spool,
                )
                if watermark.output_sample % PreservedAudioPublisher.FRAME_SAMPLES:
                    raise RuntimeError("checkpoint output is not aligned to published frames")
                publisher._sample = watermark.output_sample
                publisher._sequence = (
                    watermark.output_sample // PreservedAudioPublisher.FRAME_SAMPLES
                )
                publishers[direction.destination_participant_id] = publisher
            sinks = _build_direction_sinks(
                ctx,
                metadata,
                direction,
                livekit_by_participant,
                authority,
                spool,
                publisher,
                checkpoint,
                watermark.provider_output_sample,
            )
            stage_gate = await _reserve_direction_quota(
                metadata,
                direction,
                (stt_caps, translation_caps, tts_caps),
                initialization,
            )
            direction_locales = (
                direction.stt.locale,
                direction.tts.locale if direction.tts is not None else direction.stt.locale,
            )
            direction_providers = ProviderRegistry.resolve(
                metadata.selection.profile_id,
                metadata.consultation_id,
                ScopedExchangeJournal(spool, authority, _direction_scope(metadata, direction)),
                tuple(dict.fromkeys(direction_locales)),
            )
            provider_sets.append(direction_providers)
            spec = DirectionSpec(
                direction.source_participant_id,
                direction.destination_participant_id,
                direction.source_language,
                direction.target_language,
                direction.voice,
                direction.mode == "same_language",
            )
            session = await initialize_value(
                DirectionSession,
                spec,
                direction_providers.stt,
                direction_providers.translation,
                direction_providers.tts,
                sinks.caption,
                sinks.audio,
                sinks.checkpoint,
                sinks.normalized,
                stage_gate,
                _build_provider_terminal_sink(metadata, direction, authority, spool, control),
            )
            session._last_input = watermark.input_sample
            sessions[direction.source_participant_id] = session
            try:
                await initialize(session.start())
            except BaseException:
                provider_sets.remove(direction_providers)
                raise
            sessions_by_identity[livekit_by_participant[direction.source_participant_id]] = session
        sessions_ready.set()
        try:
            await _supervise_runtime(
                ctx,
                metadata,
                spool,
                control,
                provider_health,
                sessions,
                publishers,
                quota_lifecycle,
                stream_tasks,
                drain_runtime_once,
                failure,
                disconnected,
                drain_requested,
                checkpoint_state.hashes,
                interpretation_tracks,
            )
        except BaseException as error:
            terminal_error = error
            raise
        finally:
            await shutdown.run()
    except BaseException as error:
        terminal_error = terminal_error or error
        if not handoff_complete and not shutdown.started and sessions:
            await shutdown.run()
        elif not handoff_complete and not shutdown.started:
            spool.relinquish_consultation(authority, "worker initialization failed before sealing")
            handoff_complete = True
            authority.close()
            await providers.aclose()
            await control.aclose()
        raise


def _contains_spool_unavailable(error: BaseException) -> bool:
    if isinstance(error, SpoolUnavailable):
        return True
    if isinstance(error, BaseExceptionGroup):
        return any(_contains_spool_unavailable(nested) for nested in error.exceptions)
    return False


def _request_supervisor_shutdown() -> None:
    os._exit(74)


async def consultation_entrypoint(ctx: agents.JobContext) -> None:
    started = time.monotonic()
    result = "succeeded"
    failure_phase: str | None = None
    error_kind: str | None = None
    span = _start_span("transhooter.worker.job")
    _add_metric(_active_jobs, 1, {})
    try:
        try:
            await _run_consultation(ctx)
        except BaseException as error:
            result = "cancelled" if bounded_error_kind(error) == "aborted" else "failed"
            failure_phase = _job_failure_phase(error)
            error_kind = bounded_error_kind(error)
            _set_error_status(span, error, failure_phase)
            raise
        else:
            _set_ok_status(span)
    except BaseException as error:
        if _contains_spool_unavailable(error):
            _request_supervisor_shutdown()
        raise
    finally:
        attributes = {"result": result}
        if failure_phase is not None:
            attributes["failure.phase"] = failure_phase
        if error_kind is not None:
            attributes["error.kind"] = error_kind
        _add_metric(_active_jobs, -1, {})
        _add_metric(_jobs_total, 1, attributes)
        _record_metric(_job_duration, time.monotonic() - started, attributes)
        _end_span(span)


def _worker_options(api_key: str, api_secret: str) -> agents.WorkerOptions:
    return agents.WorkerOptions(
        entrypoint_fnc=consultation_entrypoint,
        agent_name="translation-worker",
        ws_url=os.environ.get("LIVEKIT_URL", "ws://localhost:7880"),
        api_key=api_key,
        api_secret=api_secret,
        job_executor_type=agents.JobExecutorType.THREAD,
        shutdown_process_timeout=30.0,
    )


def run_worker() -> None:
    credentials_file = os.environ.get("LIVEKIT_CREDENTIALS_FILE")
    if not credentials_file:
        raise RuntimeError("LIVEKIT_CREDENTIALS_FILE is required")
    credentials = json.loads(Path(credentials_file).read_text("utf-8"))
    api_key = credentials.get("apiKey")
    api_secret = credentials.get("apiSecret")
    if (
        not isinstance(api_key, str)
        or not api_key
        or not isinstance(api_secret, str)
        or not api_secret
    ):
        raise RuntimeError("LiveKit credential file is invalid")
    agents.cli.run_app(_worker_options(api_key, api_secret))
