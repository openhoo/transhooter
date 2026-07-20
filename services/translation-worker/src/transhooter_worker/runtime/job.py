from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Literal
from uuid import UUID, uuid4, uuid5

from jsonschema import Draft202012Validator, FormatChecker  # type: ignore[import-untyped]
from livekit import agents, rtc
from opentelemetry import metrics, trace
from opentelemetry.trace import Status, StatusCode
from pydantic import BaseModel, ConfigDict, Field, model_validator

from transhooter_worker.adapters.s3_archive import S3Archive
from transhooter_worker.adapters.scoped_journal import ScopedExchangeJournal
from transhooter_worker.adapters.spool import (
    EncryptedSpool,
    SpoolCheckpointDelivery,
    SpoolUnavailable,
    deterministic_roomy_capacity,
    statvfs_capacity,
)
from transhooter_worker.adapters.terminal import terminal_bytes
from transhooter_worker.application.compactor import CompactedPcm, PcmCompactor
from transhooter_worker.application.pipeline import CaptionRevision
from transhooter_worker.application.session import DirectionSession, DirectionSpec
from transhooter_worker.domain.models import (
    AudioChunk,
    AudioEvent,
    OperationTerminal,
    Outcome,
    ProviderHealth,
    RetryAction,
    RetryDecision,
    SampleRange,
    SessionTerminal,
    StageCapabilities,
)
from transhooter_worker.ports.archive import ObjectRecord
from transhooter_worker.runtime.control_client import ControlClient
from transhooter_worker.runtime.provider_registry import (
    ProviderRegistry,
    credential_fingerprint,
)
from transhooter_worker.runtime.publisher import (
    PreservedAudioPublisher,
    publish_private_interpretation_tracks,
)
from transhooter_worker.runtime.redis_quota import RedisQuotaGate
from transhooter_worker.runtime.spool_drainer import (
    build_archive,
    upload_committed_objects_async,
)
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


@dataclass(slots=True)
class SourceTrackTimeline:
    FRAME_SAMPLES: ClassVar[int] = 4_000
    cursor: int = 0
    sequence: int = 0
    generation: int = 0

    def replace(self) -> int:
        self.generation += 1
        return self.generation

    def claim(self, generation: int, samples: int) -> tuple[int, SampleRange] | None:
        if generation != self.generation:
            return None
        if samples != self.FRAME_SAMPLES:
            raise RuntimeError("source audio frame is not 250 ms at 16 kHz")
        sequence = self.sequence
        span = SampleRange(self.cursor, self.cursor + samples)
        self.cursor = span.end
        self.sequence += 1
        return sequence, span


class WireModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=lambda name: "".join(
            [name.split("_")[0], *(part.title() for part in name.split("_")[1:])]
        ),
        populate_by_name=True,
        extra="forbid",
    )


class CredentialReference(WireModel):
    reference: str = Field(min_length=1)
    version: str = Field(min_length=1)


class FrozenStage(WireModel):
    provider: str = Field(min_length=1)
    endpoint: str = Field(min_length=1)
    region: str = Field(min_length=1)
    model: str = Field(min_length=1)
    adapter_build: str = Field(min_length=1)
    policy: str = Field(min_length=1)
    credential: CredentialReference
    limits: dict[str, int]


class SttStage(FrozenStage):
    locale: str = Field(min_length=1)
    encoding: str = Field(min_length=1)


class TranslationStage(FrozenStage):
    source_code: str = Field(min_length=1)
    target_code: str = Field(min_length=1)


class TtsStage(FrozenStage):
    locale: str = Field(min_length=1)
    voice: str = Field(min_length=1)
    encoding: str = Field(min_length=1)
    sample_rate: int = Field(gt=0)


class DirectionMetadata(WireModel):
    mode: Literal["translated", "same_language"]
    source_participant_id: UUID
    destination_participant_id: UUID
    capability_row_id: UUID
    stt: SttStage
    bypass: Literal[True] | None = None
    target_code: str | None = None
    translation: TranslationStage | None = None
    tts: TtsStage | None = None

    @model_validator(mode="after")
    def validate_mode(self) -> DirectionMetadata:
        translated = self.mode == "translated"
        if translated != (
            self.translation is not None and self.tts is not None and self.target_code is not None
        ):
            raise ValueError("translated direction stages are incomplete")
        if translated == (self.bypass is True):
            raise ValueError("same-language bypass shape is inconsistent")
        return self

    @property
    def source_language(self) -> str:
        return self.stt.locale

    @property
    def target_language(self) -> str:
        return self.tts.locale if self.tts is not None else self.stt.locale

    @property
    def voice(self) -> str | None:
        return self.tts.voice if self.tts is not None else None


class RoomProviderSelectionMetadata(WireModel):
    profile_id: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
    profile_revision: int = Field(ge=1)
    capability_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    participant_ids: tuple[UUID, UUID]
    directions: tuple[DirectionMetadata, DirectionMetadata]


class JobMetadata(WireModel):
    schema_version: Literal[1]
    consultation_id: UUID
    generation: int = Field(ge=1)
    room_name: UUID
    worker_identity: UUID
    worker_epoch: int = Field(ge=1)
    write_epoch: int = Field(ge=0)
    expected_participant_ids: tuple[UUID, UUID]
    expected_livekit_identities: tuple[UUID, UUID]
    selection: RoomProviderSelectionMetadata = Field(alias="providerSelection")
    snapshot_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    adoption_id: UUID | None = None

    @model_validator(mode="after")
    def validate_bindings(self) -> JobMetadata:
        if self.expected_participant_ids != self.selection.participant_ids:
            raise ValueError("worker participant order must match provider selection")
        canonical = json.dumps(
            self.selection.model_dump(mode="json", by_alias=True, exclude_none=True),
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        if hashlib.sha256(canonical).hexdigest() != self.snapshot_hash:
            raise ValueError("provider selection snapshot hash mismatch")
        return self


class CaptionPacket(BaseModel):
    schemaVersion: Literal[1]
    consultationId: UUID
    destinationParticipantId: UUID
    sourceParticipantId: UUID
    utteranceId: UUID
    revision: int = Field(ge=1)
    finality: Literal["provisional", "final"]
    sourceLanguage: str
    targetLanguage: str
    sourceText: str
    translatedText: str
    sourceSampleStart: int = Field(ge=0)
    sourceSampleEnd: int = Field(gt=0)
    occurredAtMs: int = Field(ge=0)


def _spool() -> EncryptedSpool:
    root = Path(os.environ.get("SPOOL_PATH") or os.environ["SPOOL_DIR"])
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


def _validated_job_metadata(payload: str) -> JobMetadata:
    path = Path(
        os.environ.get("CONTRACTS_SCHEMA_FILE", "/workspace/contracts/contracts.schema.json")
    )
    try:
        schema = json.loads(path.read_text("utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("generated contracts schema is required") from exc
    definitions = schema.get("schemas")
    if not isinstance(definitions, dict) or not isinstance(
        definitions.get("WorkerJobMetadata"), dict
    ):
        raise RuntimeError("generated WorkerJobMetadata schema is absent")
    candidate = json.loads(payload)
    Draft202012Validator(definitions["WorkerJobMetadata"], format_checker=FormatChecker()).validate(
        candidate
    )
    return JobMetadata.model_validate(candidate)


async def _reported_preflight(
    operation: Callable[[], Awaitable[Any]],
    report: Callable[[dict[str, object]], Awaitable[None]],
    snapshot_hash: str,
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
        try:
            await report(
                {
                    "kind": type(error).__name__,
                    "message": str(error),
                    "phase": "provider-preflight",
                    "snapshotHash": snapshot_hash,
                    "lastCheckpointHashes": {},
                }
            )
        except Exception:
            pass
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
    if profile_id == "google-eu":
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
    if profile_id == "google-eu":
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
) -> None:
    while True:
        usage = spool.usage_ratio()
        _record_metric(_spool_utilization, usage, {"role": "heartbeat"})
        if usage >= 0.80:
            raise RuntimeError("encrypted spool reached fail-closed 80% capacity")
        await asyncio.gather(
            *(gate.reserve_active(stage, reservation) for gate, stage, reservation in quota_leases)
        )
        await control.heartbeat(
            {
                "writeEpoch": metadata.write_epoch,
                "snapshotHash": metadata.snapshot_hash,
                "providersOk": all(item.healthy for item in provider_health),
                "archiveOk": usage < 0.80,
                "acceptingLoad": usage < 0.70,
            }
        )
        await asyncio.sleep(5)


async def _cancel_stream_tasks(stream_tasks: set[asyncio.Task[None]]) -> None:
    for stream_task in stream_tasks:
        stream_task.cancel()
    await asyncio.gather(*stream_tasks, return_exceptions=True)


async def _drain_runtime(
    stream_tasks: set[asyncio.Task[None]],
    sessions: dict[UUID, DirectionSession],
    publishers: dict[UUID, PreservedAudioPublisher],
) -> None:
    await _cancel_stream_tasks(stream_tasks)
    await asyncio.gather(*(session.finish() for session in sessions.values()))
    await asyncio.gather(*(publisher.drain() for publisher in publishers.values()))


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


async def _report_runtime_failure(
    control: ControlClient,
    error: BaseException,
    checkpoint_hashes: dict[UUID, str],
) -> None:
    try:
        await control.report_failure(
            {
                "kind": type(error).__name__,
                "message": str(error),
                "lastCheckpointHashes": {
                    str(source_id): digest for source_id, digest in checkpoint_hashes.items()
                },
            }
        )
    except Exception:
        pass


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


async def _supervise_runtime(
    ctx: agents.JobContext,
    metadata: JobMetadata,
    spool: EncryptedSpool,
    control: ControlClient,
    provider_health: tuple[ProviderHealth, ...],
    sessions: dict[UUID, DirectionSession],
    publishers: dict[UUID, PreservedAudioPublisher],
    quota_leases: list[tuple[RedisQuotaGate, str, str]],
    stream_tasks: set[asyncio.Task[None]],
    drain_runtime: Callable[[], Awaitable[None]],
    stream_failure: asyncio.Future[BaseException],
    disconnected: asyncio.Event,
    drain_requested: asyncio.Event,
    checkpoint_hashes: dict[UUID, str],
    interpretation_tracks: dict[str, tuple[rtc.AudioSource, rtc.LocalTrackPublication]],
) -> None:
    heartbeat_task = asyncio.create_task(
        _heartbeat_loop(metadata, spool, control, provider_health, quota_leases)
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
    except BaseException as shutdown_error:
        await _cancel_stream_tasks(stream_tasks)
        await asyncio.gather(
            *(session.cancel() for session in sessions.values()),
            return_exceptions=True,
        )
        await _unpublish_interpretation_tracks(ctx, interpretation_tracks)
        await _report_runtime_failure(control, shutdown_error, checkpoint_hashes)
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
        await asyncio.gather(
            *(gate.release_active(stage, reservation) for gate, stage, reservation in quota_leases),
            return_exceptions=True,
        )


@dataclass(slots=True)
class InitializationContext:
    ctx: agents.JobContext
    control: ControlClient
    sessions: dict[UUID, DirectionSession]
    interpretation_tracks: dict[str, tuple[rtc.AudioSource, rtc.LocalTrackPublication]]
    quota_leases: list[tuple[RedisQuotaGate, str, str]]
    checkpoint_hashes: dict[UUID, str]
    cleaned: bool = False

    async def cleanup(self, error: BaseException) -> None:
        if self.cleaned:
            return
        self.cleaned = True
        await asyncio.gather(
            *(session.cancel() for session in self.sessions.values()),
            return_exceptions=True,
        )
        await _unpublish_interpretation_tracks(self.ctx, self.interpretation_tracks)
        await asyncio.gather(
            *(
                gate.release_active(stage, reservation)
                for gate, stage, reservation in self.quota_leases
            ),
            return_exceptions=True,
        )
        await _report_runtime_failure(self.control, error, self.checkpoint_hashes)

    async def await_effect(self, effect: Awaitable[Any]) -> Any:
        try:
            return await effect
        except BaseException as error:
            await self.cleanup(error)
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
            await self.cleanup(error)
            raise


@dataclass(frozen=True, slots=True)
class PendingCheckpoint:
    checkpoint_id: UUID
    control_event_id: UUID
    checkpoint: dict[str, Any]
    body: bytes
    digest: str
    input_sample: int
    input_sequence: int
    provider_output_sample: int
    output_sample: int
    terminal: bool


@dataclass(slots=True)
class CheckpointChainState:
    hashes: dict[UUID, str]
    watermarks: dict[UUID, tuple[int, int, int, int, bool]]
    pending: dict[UUID, PendingCheckpoint]
    locks: dict[UUID, asyncio.Lock]

    @classmethod
    def empty(cls) -> CheckpointChainState:
        return cls({}, {}, {}, {})

    def lock_for(self, source_id: UUID) -> asyncio.Lock:
        lock = self.locks.get(source_id)
        if lock is None:
            lock = asyncio.Lock()
            self.locks[source_id] = lock
        return lock


async def _persist_checkpoint(
    metadata: JobMetadata,
    spool: EncryptedSpool,
    archive: S3Archive,
    control: ControlClient,
    state: CheckpointChainState,
    source_id: UUID,
    destination_id: UUID,
    input_sample: int,
    output_sample: int,
    terminal: bool,
    *,
    input_sequence: int | None = None,
    provider_output_sample: int | None = None,
) -> None:
    input_sequence = (
        input_sample // SourceTrackTimeline.FRAME_SAMPLES
        if input_sequence is None
        else input_sequence
    )
    provider_output_sample = (
        output_sample if provider_output_sample is None else provider_output_sample
    )
    async with state.lock_for(source_id):
        pending = state.pending.get(source_id)
        requested = (
            input_sequence,
            input_sample,
            provider_output_sample,
            output_sample,
            terminal,
        )
        if pending is not None:
            await _deliver_checkpoint(metadata, spool, archive, control, pending)
            state.hashes[source_id] = pending.digest
            del state.pending[source_id]
            state.watermarks[source_id] = (
                pending.input_sequence,
                pending.input_sample,
                pending.provider_output_sample,
                pending.output_sample,
                pending.terminal,
            )
            if requested == (
                pending.input_sequence,
                pending.input_sample,
                pending.provider_output_sample,
                pending.output_sample,
                pending.terminal,
            ):
                return

        predecessor = state.watermarks.get(source_id)
        if requested == predecessor:
            return
        if predecessor is not None:
            (
                previous_sequence,
                previous_input,
                previous_provider_output,
                previous_output,
                previous_terminal,
            ) = predecessor
            if previous_terminal:
                raise RuntimeError("cannot append after a terminal checkpoint")
            if (
                input_sequence < previous_sequence
                or input_sample < previous_input
                or provider_output_sample < previous_provider_output
                or output_sample < previous_output
            ):
                raise RuntimeError("checkpoint watermarks cannot regress")

        previous_hash = state.hashes.get(source_id)
        checkpoint_id = uuid4()
        checkpoint = {
            "checkpointId": str(checkpoint_id),
            "sourceParticipantId": str(source_id),
            "destinationParticipantId": str(destination_id),
            "acceptedInputSequence": input_sequence,
            "acceptedInput": input_sample,
            "receivedOutput": provider_output_sample,
            "emittedOutput": output_sample,
            "workerEpoch": metadata.worker_epoch,
            "previousCheckpointSha256": previous_hash,
            "expectedObjectIds": [],
            "observedObjectIds": [],
            "gaps": [],
            "terminal": terminal,
            "occurredAtMs": int(time.time() * 1000),
        }
        encoded = json.dumps(checkpoint, separators=(",", ":"), sort_keys=True).encode()
        digest = hashlib.sha256((previous_hash or "").encode() + encoded).hexdigest()
        checkpoint["highWatermarkSha256"] = digest
        body = json.dumps(checkpoint, separators=(",", ":"), sort_keys=True).encode()
        delivery = spool.register_checkpoint_delivery(
            checkpoint_id=checkpoint_id,
            meeting_id=metadata.consultation_id,
            source_id=source_id,
            worker_epoch=metadata.worker_epoch,
            checkpoint_hash=digest,
            previous_hash=previous_hash,
            control_event_id=uuid4(),
            body=body,
        )
        pending = _pending_checkpoint(delivery)
        state.pending[source_id] = pending
        await _deliver_checkpoint(metadata, spool, archive, control, pending)
        state.hashes[source_id] = digest
        state.watermarks[source_id] = requested
        del state.pending[source_id]


def _pending_checkpoint(delivery: SpoolCheckpointDelivery) -> PendingCheckpoint:
    try:
        checkpoint = json.loads(delivery.body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError("checkpoint delivery body is malformed") from error
    if not isinstance(checkpoint, dict):
        raise RuntimeError("checkpoint delivery body is not an object")
    expected = {
        "checkpointId": str(delivery.checkpoint_id),
        "sourceParticipantId": str(delivery.source_id),
        "workerEpoch": delivery.worker_epoch,
        "previousCheckpointSha256": delivery.previous_hash,
        "highWatermarkSha256": delivery.checkpoint_hash,
    }
    if any(checkpoint.get(key) != value for key, value in expected.items()):
        raise RuntimeError("checkpoint delivery body does not match its durable identity")
    if json.dumps(checkpoint, separators=(",", ":"), sort_keys=True).encode() != delivery.body:
        raise RuntimeError("checkpoint delivery body is not canonical")
    hash_input = dict(checkpoint)
    hash_input.pop("highWatermarkSha256", None)
    computed_hash = hashlib.sha256(
        (delivery.previous_hash or "").encode()
        + json.dumps(hash_input, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    if computed_hash != delivery.checkpoint_hash:
        raise RuntimeError("checkpoint delivery hash is invalid")
    destination_id = checkpoint.get("destinationParticipantId")
    if not isinstance(destination_id, str):
        raise RuntimeError("checkpoint delivery is missing destination participant")
    try:
        UUID(destination_id)
        input_sequence_value = checkpoint["acceptedInputSequence"]
        input_value = checkpoint["acceptedInput"]
        provider_output_value = checkpoint["receivedOutput"]
        output_value = checkpoint["emittedOutput"]
        values = (
            input_sequence_value,
            input_value,
            provider_output_value,
            output_value,
        )
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in values
        ):
            raise ValueError
        input_sequence = input_sequence_value
        input_sample = input_value
        provider_output_sample = provider_output_value
        output_sample = output_value
        if input_sample != input_sequence * SourceTrackTimeline.FRAME_SAMPLES:
            raise ValueError
        if output_sample % PreservedAudioPublisher.FRAME_SAMPLES:
            raise ValueError
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("checkpoint delivery watermarks are malformed") from error
    terminal = checkpoint.get("terminal")
    if not isinstance(terminal, bool):
        raise RuntimeError("checkpoint delivery terminal flag is malformed")
    return PendingCheckpoint(
        checkpoint_id=delivery.checkpoint_id,
        control_event_id=delivery.control_event_id,
        checkpoint=checkpoint,
        body=delivery.body,
        digest=delivery.checkpoint_hash,
        input_sample=input_sample,
        input_sequence=input_sequence,
        provider_output_sample=provider_output_sample,
        output_sample=output_sample,
        terminal=terminal,
    )


def _restore_checkpoint_state(
    spool: EncryptedSpool,
    meeting_id: UUID,
    worker_epoch: int,
    destinations: dict[UUID, UUID] | None = None,
) -> CheckpointChainState:
    state = CheckpointChainState.empty()
    recovered_destinations: dict[UUID, str] = {}
    for delivery in spool.list_checkpoint_deliveries(meeting_id, worker_epoch):
        pending = _pending_checkpoint(delivery)
        destination_id = str(pending.checkpoint["destinationParticipantId"])
        recovered_destination = recovered_destinations.setdefault(
            delivery.source_id, destination_id
        )
        if recovered_destination != destination_id:
            raise RuntimeError("checkpoint delivery direction changes within its chain")
        if destinations is not None:
            expected_destination = destinations.get(delivery.source_id)
            if expected_destination is None or destination_id != str(expected_destination):
                raise RuntimeError("checkpoint delivery does not match the frozen direction")
        current_hash = state.hashes.get(delivery.source_id)
        if delivery.previous_hash != current_hash:
            raise RuntimeError("checkpoint delivery chain is discontinuous")
        predecessor_watermarks = state.watermarks.get(delivery.source_id)
        if predecessor_watermarks is not None:
            (
                previous_sequence,
                previous_input,
                previous_provider_output,
                previous_output,
                previous_terminal,
            ) = predecessor_watermarks
            if previous_terminal:
                raise RuntimeError("checkpoint delivery follows a terminal checkpoint")
            if (
                pending.input_sequence < previous_sequence
                or pending.input_sample < previous_input
                or pending.provider_output_sample < previous_provider_output
                or pending.output_sample < previous_output
            ):
                raise RuntimeError("checkpoint delivery watermarks regress")
        if delivery.acknowledged:
            if delivery.source_id in state.pending:
                raise RuntimeError("acknowledged checkpoint follows a pending delivery")
            state.hashes[delivery.source_id] = pending.digest
            state.watermarks[delivery.source_id] = (
                pending.input_sequence,
                pending.input_sample,
                pending.provider_output_sample,
                pending.output_sample,
                pending.terminal,
            )
            continue
        if delivery.source_id in state.pending:
            raise RuntimeError("multiple pending checkpoints exist for one source")
        state.pending[delivery.source_id] = pending
    return state


async def _replay_pending_checkpoints(
    metadata: JobMetadata,
    spool: EncryptedSpool,
    archive: S3Archive,
    control: ControlClient,
    state: CheckpointChainState,
) -> None:
    for source_id in tuple(state.pending):
        async with state.lock_for(source_id):
            pending = state.pending[source_id]
            await _deliver_checkpoint(metadata, spool, archive, control, pending)
            state.hashes[source_id] = pending.digest
            state.watermarks[source_id] = (
                pending.input_sequence,
                pending.input_sample,
                pending.provider_output_sample,
                pending.output_sample,
                pending.terminal,
            )
            del state.pending[source_id]


_PCM_OBJECT_CLASSES = {
    "stt-input": "stt_input_pcm",
    "tts-output": "tts_output_pcm",
    "livekit-output": "livekit_output_pcm",
}


async def _record_compacted_pcm(
    metadata: JobMetadata,
    control: ControlClient,
    compacted: CompactedPcm,
) -> None:
    sample_range = {
        "start": compacted.samples.start,
        "end": compacted.samples.end,
    }
    for record, object_class in (
        (compacted.pcm, _PCM_OBJECT_CLASSES[compacted.stage]),
        (compacted.sidecar, "pcm_sidecar"),
    ):
        object_id = uuid5(
            metadata.consultation_id,
            f"archive-object:{record.key}:{record.version_id}",
        )
        await control.record_object(
            {
                "writerEpoch": metadata.write_epoch,
                "causalKey": record.key,
                "object": {
                    "objectId": str(object_id),
                    "class": object_class,
                    "key": record.key,
                    "versionId": record.version_id,
                    "size": record.size,
                    "sha256": record.sha256,
                    "s3Checksum": record.s3_checksum,
                    "contentType": record.content_type,
                    "sampleRange": sample_range,
                    "attempt": None,
                    "sequence": None,
                },
            },
            event_id=object_id,
        )


async def _record_terminal_pcm(
    metadata: JobMetadata,
    spool: EncryptedSpool,
    archive: S3Archive,
    control: ControlClient,
) -> None:
    for meeting_id, stage, direction in spool.pcm_scopes(include_uploaded=True):
        if meeting_id != metadata.consultation_id:
            continue
        terminal_checkpoint = spool.covering_checkpoint(
            meeting_id,
            stage,
            direction,
            0,
            terminal_only=True,
        )
        if terminal_checkpoint is None:
            continue
        compactor = PcmCompactor(
            spool,
            archive,
            meeting_id,
            16_000 if stage == "stt-input" else 48_000,
        )
        closed_objects = compactor.compact(
            stage,
            direction,
            drain=True,
            include_uploaded=True,
        )
        for closed_object in closed_objects:
            if not spool.checkpoint_covers(
                terminal_checkpoint,
                stage,
                direction,
                closed_object.samples.end,
            ):
                raise RuntimeError("terminal checkpoint does not cover compacted PCM")
            await _record_compacted_pcm(metadata, control, closed_object)
            compactor.acknowledge_covering_checkpoint(
                closed_object,
                str(terminal_checkpoint),
            )


async def _register_uploaded_evidence(
    metadata: JobMetadata,
    control: ControlClient,
    _meeting_id: UUID,
    spool_object_id: UUID,
    object_class: str,
    record: ObjectRecord,
) -> None:
    await control.record_object(
        {
            "writerEpoch": metadata.write_epoch,
            "causalKey": str(spool_object_id),
            "object": {
                "objectId": str(spool_object_id),
                "class": object_class,
                "key": record.key,
                "versionId": record.version_id,
                "size": record.size,
                "sha256": record.sha256,
                "s3Checksum": record.s3_checksum,
                "contentType": record.content_type,
                "sampleRange": None,
                "attempt": None,
                "sequence": None,
            },
        },
        event_id=spool_object_id,
    )


async def _deliver_checkpoint(
    metadata: JobMetadata,
    spool: EncryptedSpool,
    archive: S3Archive,
    control: ControlClient,
    pending: PendingCheckpoint,
) -> None:
    checkpoint_id = pending.checkpoint_id
    object_key = (
        f"v1/meetings/{metadata.consultation_id}/inventory/checkpoints/{checkpoint_id}.json"
    )
    object_sha256 = hashlib.sha256(pending.body).hexdigest()
    spool.register_checkpoint_delivery(
        checkpoint_id=pending.checkpoint_id,
        meeting_id=metadata.consultation_id,
        source_id=UUID(str(pending.checkpoint["sourceParticipantId"])),
        worker_epoch=metadata.worker_epoch,
        checkpoint_hash=pending.digest,
        previous_hash=(
            str(pending.checkpoint["previousCheckpointSha256"])
            if pending.checkpoint["previousCheckpointSha256"] is not None
            else None
        ),
        control_event_id=pending.control_event_id,
        body=pending.body,
    )
    archived = archive.put_create_once(
        object_key,
        pending.body,
        "application/json",
        object_sha256,
    )
    await control.record_object(
        {
            "writerEpoch": metadata.write_epoch,
            "causalKey": str(checkpoint_id),
            "object": {
                "objectId": str(checkpoint_id),
                "class": "checkpoint",
                "key": archived.key,
                "versionId": archived.version_id,
                "size": archived.size,
                "sha256": archived.sha256,
                "s3Checksum": archived.s3_checksum,
                "contentType": archived.content_type,
                "sampleRange": None,
                "attempt": None,
                "sequence": None,
            },
        },
        event_id=pending.checkpoint_id,
    )
    if pending.terminal:
        await upload_committed_objects_async(
            spool,
            archive,
            lambda meeting_id, object_id, object_class, record: _register_uploaded_evidence(
                metadata,
                control,
                meeting_id,
                object_id,
                object_class,
                record,
            ),
            metadata.consultation_id,
        )
        await _record_terminal_pcm(metadata, spool, archive, control)
    await control.checkpoint(
        {
            "writeEpoch": metadata.write_epoch,
            "objectKey": object_key,
            "checkpoint": pending.checkpoint,
        },
        event_id=pending.control_event_id,
    )
    spool.mark_checkpoint_delivery_acknowledged(pending.control_event_id)


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


@dataclass(frozen=True, slots=True)
class DirectionSinks:
    caption: Callable[[CaptionRevision], Awaitable[None]]
    audio: Callable[[bytes], Awaitable[None]]
    checkpoint: Callable[[int, int, bool], Awaitable[None]]
    normalized: Callable[[object], Awaitable[None]]


def _build_direction_sinks(
    ctx: agents.JobContext,
    metadata: JobMetadata,
    direction: DirectionMetadata,
    livekit_by_participant: dict[UUID, UUID],
    spool: EncryptedSpool,
    publisher: PreservedAudioPublisher | None,
    persist_checkpoint: Callable[[UUID, UUID, int, int, bool, int, int], Awaitable[None]],
    initial_output_sample: int = 0,
) -> DirectionSinks:
    checkpoint_lock = asyncio.Lock()

    async def caption_sink(revision: CaptionRevision) -> None:
        packet = CaptionPacket(
            schemaVersion=1,
            consultationId=metadata.consultation_id,
            destinationParticipantId=direction.destination_participant_id,
            sourceParticipantId=direction.source_participant_id,
            utteranceId=revision.utterance_id,
            revision=revision.revision,
            finality="final" if revision.final else "provisional",
            sourceLanguage=direction.source_language,
            targetLanguage=direction.target_language,
            sourceText=revision.source_text,
            translatedText=revision.translated_text,
            sourceSampleStart=revision.samples.start,
            sourceSampleEnd=revision.samples.end,
            occurredAtMs=int(time.time() * 1000),
        )
        payload = packet.model_dump_json().encode()
        spool.append(
            meeting_id=metadata.consultation_id,
            attempt_id=revision.utterance_id,
            stage="caption",
            transport="websocket",
            direction="publish",
            media_type="application/json",
            payload=payload,
            sample_range=revision.samples,
        )
        await ctx.room.local_participant.publish_data(
            payload,
            reliable=True,
            destination_identities=[
                str(livekit_by_participant[direction.destination_participant_id])
            ],
            topic="consultation.translation.v1",
        )

    async def audio_sink(pcm: bytes) -> None:
        if publisher is None:
            raise RuntimeError("same-language direction attempted interpretation publication")
        await publisher.publish(pcm)

    tts_raw_cursor = initial_output_sample

    async def normalized_sink(event: object) -> None:
        nonlocal tts_raw_cursor
        samples = getattr(event, "samples", None)
        sample_range = samples if isinstance(samples, SampleRange) else None
        if isinstance(event, AudioEvent):
            raw_range = SampleRange(tts_raw_cursor, tts_raw_cursor + len(event.pcm) // 2)
            spool.append(
                meeting_id=metadata.consultation_id,
                attempt_id=event.operation_id,
                stage="tts-output",
                transport=(
                    "grpc" if direction.tts and direction.tts.provider == "google" else "websocket"
                ),
                direction=str(direction.destination_participant_id),
                media_type="audio/L16",
                payload=event.pcm,
                sample_range=raw_range,
                metadata=(
                    ("providerSampleStart", str(event.samples.start)),
                    ("providerSampleEnd", str(event.samples.end)),
                ),
            )
            tts_raw_cursor = raw_range.end
        spool.append(
            meeting_id=metadata.consultation_id,
            attempt_id=direction.source_participant_id,
            stage="normalized",
            transport="internal",
            direction=str(direction.source_participant_id),
            media_type="application/json",
            payload=terminal_bytes(event),
            sample_range=sample_range,
        )

    async def checkpoint_sink(
        input_sample: int,
        output_sample: int,
        terminal: bool,
    ) -> None:
        async with checkpoint_lock:
            await persist_checkpoint(
                direction.source_participant_id,
                direction.destination_participant_id,
                input_sample,
                output_sample,
                terminal,
                input_sample // SourceTrackTimeline.FRAME_SAMPLES,
                tts_raw_cursor,
            )

    return DirectionSinks(caption_sink, audio_sink, checkpoint_sink, normalized_sink)


def _direction_scope(
    metadata: JobMetadata,
    direction: DirectionMetadata,
) -> tuple[tuple[str, str], ...]:
    return (
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
        await initialization.cleanup(error)
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
        gates[capability.stage] = gate
        reservation = (
            f"{metadata.consultation_id}:{direction.source_participant_id}:{capability.stage}"
        )
        await initialization.await_effect(gate.reserve_active(capability.stage, reservation))
        initialization.quota_leases.append((gate, capability.stage, reservation))

    async def enforce_stage_quota(stage: str, amount: int) -> None:
        await gates[stage.removesuffix("_start")](stage, amount)

    return enforce_stage_quota


def _install_room_handlers(
    ctx: agents.JobContext,
    metadata: JobMetadata,
    spool: EncryptedSpool,
    sessions_by_identity: dict[UUID, DirectionSession],
    timelines: dict[UUID, SourceTrackTimeline],
    sessions_ready: asyncio.Event,
    stream_tasks: set[asyncio.Task[None]],
    stream_failure: asyncio.Future[BaseException],
    disconnected: asyncio.Event,
    drain_requested: asyncio.Event,
) -> Callable[[rtc.RemoteTrackPublication, rtc.RemoteParticipant], None]:
    active_stream_tasks: dict[UUID, asyncio.Task[None]] = {}

    async def consume_track(
        track: rtc.Track,
        participant: rtc.RemoteParticipant,
        generation: int,
    ) -> None:
        await sessions_ready.wait()
        try:
            source_identity = UUID(participant.identity)
        except ValueError:
            return
        session = sessions_by_identity.get(source_identity)
        if session is None or track.kind != rtc.TrackKind.KIND_AUDIO:
            return
        stream = rtc.AudioStream(
            track,
            sample_rate=16000,
            num_channels=1,
            frame_size_ms=250,
        )
        timeline = timelines[source_identity]
        try:
            async for event in stream:
                claimed_frame = timeline.claim(generation, event.frame.samples_per_channel)
                if claimed_frame is None:
                    return
                sequence, sample_range = claimed_frame
                pcm = bytes(event.frame.data)
                spool.append(
                    meeting_id=metadata.consultation_id,
                    attempt_id=source_identity,
                    stage="stt-input",
                    transport="grpc",
                    direction=str(source_identity),
                    media_type="audio/L16",
                    payload=pcm,
                    sample_range=sample_range,
                    metadata=(("sequence", str(sequence)),),
                )
                await session.send_audio(
                    AudioChunk(
                        source_identity,
                        sequence,
                        sample_range,
                        pcm,
                        16000,
                        1,
                        "LINEAR16",
                    )
                )
            if timeline.generation == generation:
                await session.boundary()
        finally:
            await stream.aclose()

    def track_done(source_identity: UUID, task: asyncio.Task[None]) -> None:
        stream_tasks.discard(task)
        if active_stream_tasks.get(source_identity) is task:
            del active_stream_tasks[source_identity]
        if (
            not task.cancelled()
            and (error := task.exception()) is not None
            and not stream_failure.done()
        ):
            stream_failure.set_result(error)

    def subscribe_if_allowed(
        publication: rtc.RemoteTrackPublication,
        participant: rtc.RemoteParticipant,
    ) -> None:
        try:
            identity = UUID(participant.identity)
        except ValueError:
            return
        if identity in timelines and publication.source == rtc.TrackSource.SOURCE_MICROPHONE:
            publication.set_subscribed(True)

    @ctx.room.on("track_published")
    def on_track_published(
        publication: rtc.RemoteTrackPublication,
        participant: rtc.RemoteParticipant,
    ) -> None:
        subscribe_if_allowed(publication, participant)

    @ctx.room.on("track_subscribed")
    def on_track_subscribed(
        track: rtc.Track,
        publication: rtc.RemoteTrackPublication,
        participant: rtc.RemoteParticipant,
    ) -> None:
        if publication.source != rtc.TrackSource.SOURCE_MICROPHONE:
            return
        try:
            source_identity = UUID(participant.identity)
        except ValueError:
            return
        if source_identity not in timelines:
            return
        generation = timelines[source_identity].replace()
        previous_task = active_stream_tasks.get(source_identity)
        if previous_task is not None:
            previous_task.cancel()
        stream_task = asyncio.create_task(consume_track(track, participant, generation))
        active_stream_tasks[source_identity] = stream_task
        stream_tasks.add(stream_task)
        stream_task.add_done_callback(lambda task: track_done(source_identity, task))

    @ctx.room.on("data_received")
    def on_data_received(packet: rtc.DataPacket) -> None:
        if packet.participant is not None or packet.topic != "consultation.status.v1":
            return
        try:
            message = json.loads(packet.data)
        except (ValueError, TypeError):
            return
        if (
            message.get("consultationId") == str(metadata.consultation_id)
            and message.get("generation") == metadata.generation
            and message.get("reasonCode") == "SHUTDOWN"
        ):
            drain_requested.set()

    @ctx.room.on("disconnected")
    def on_disconnected(*_: object) -> None:
        disconnected.set()

    return subscribe_if_allowed


async def _run_consultation(ctx: agents.JobContext) -> None:
    metadata = _validated_job_metadata(ctx.job.metadata)
    spool = _spool()
    archive = build_archive(Path(os.environ.get("SPOOL_PATH") or os.environ["SPOOL_DIR"]))
    livekit_by_participant = dict(
        zip(metadata.expected_participant_ids, metadata.expected_livekit_identities, strict=True)
    )
    _validate_frozen_directions(metadata)
    providers = ProviderRegistry.resolve(
        metadata.selection.profile_id,
        metadata.consultation_id,
        ScopedExchangeJournal(spool, _selection_scope(metadata)),
        _selected_locales(metadata),
    )
    if spool.usage_ratio() >= 0.70:
        raise RuntimeError("encrypted spool admission requires capacity below 70%")
    control = ControlClient(
        os.environ["CONTROL_INTERNAL_URL"],
        Path(os.environ["WORKER_INTERNAL_BEARER_FILE"]),
        metadata.consultation_id,
        metadata.generation,
        metadata.worker_identity,
        metadata.worker_epoch,
        spool,
    )
    sessions: dict[UUID, DirectionSession] = {}
    sessions_by_identity: dict[UUID, DirectionSession] = {}
    publishers: dict[UUID, PreservedAudioPublisher] = {}
    quota_leases: list[tuple[RedisQuotaGate, str, str]] = []
    stream_tasks: set[asyncio.Task[None]] = set()

    drain_task: asyncio.Task[None] | None = None

    async def drain_runtime_once() -> None:
        nonlocal drain_task
        if drain_task is None:
            drain_task = asyncio.create_task(_drain_runtime(stream_tasks, sessions, publishers))
        await asyncio.shield(drain_task)

    async def shutdown_runtime() -> None:
        try:
            await drain_runtime_once()
        finally:
            await control.aclose()

    ctx.add_shutdown_callback(shutdown_runtime)

    checkpoint_state = _restore_checkpoint_state(
        spool,
        metadata.consultation_id,
        metadata.worker_epoch,
        {
            direction.source_participant_id: direction.destination_participant_id
            for direction in metadata.selection.directions
        },
    )
    await _replay_pending_checkpoints(metadata, spool, archive, control, checkpoint_state)
    completed_sources = {
        source_id
        for source_id, (_, _, _, _, terminal) in checkpoint_state.watermarks.items()
        if terminal
    }
    if completed_sources == {
        direction.source_participant_id for direction in metadata.selection.directions
    }:
        await control.aclose()
        return

    stt_caps, translation_caps, tts_caps, provider_health = await _reported_preflight(
        lambda: _provider_preflight(metadata, providers),
        control.report_failure,
        metadata.snapshot_hash,
    )
    sessions_ready = asyncio.Event()
    disconnected = asyncio.Event()
    drain_requested = asyncio.Event()
    failure: asyncio.Future[BaseException] = asyncio.get_running_loop().create_future()

    timelines: dict[UUID, SourceTrackTimeline] = {}
    for participant_id in metadata.expected_participant_ids:
        accepted_sequence, accepted_input, _, _, _ = checkpoint_state.watermarks.get(
            participant_id, (0, 0, 0, 0, False)
        )
        timelines[livekit_by_participant[participant_id]] = SourceTrackTimeline(
            cursor=accepted_input,
            sequence=accepted_sequence,
        )

    subscribe_if_allowed = _install_room_handlers(
        ctx,
        metadata,
        spool,
        sessions_by_identity,
        timelines,
        sessions_ready,
        stream_tasks,
        failure,
        disconnected,
        drain_requested,
    )

    interpretation_tracks: dict[str, tuple[rtc.AudioSource, rtc.LocalTrackPublication]] = {}
    initialization = InitializationContext(
        ctx,
        control,
        sessions,
        interpretation_tracks,
        quota_leases,
        checkpoint_state.hashes,
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
        await _persist_checkpoint(
            metadata,
            spool,
            archive,
            control,
            checkpoint_state,
            source_id,
            destination_id,
            input_sample,
            output_sample,
            terminal,
            input_sequence=input_sequence,
            provider_output_sample=provider_output_sample,
        )

    translated_targets = await initialize_value(
        lambda: tuple(
            str(livekit_by_participant[direction.destination_participant_id])
            for direction in metadata.selection.directions
            if direction.mode == "translated"
            and direction.source_participant_id not in completed_sources
        )
    )
    if translated_targets:
        interpretation_tracks.update(
            await initialize(publish_private_interpretation_tracks(ctx.room, translated_targets))
        )
    for direction in metadata.selection.directions:
        publisher = None
        (
            _resumed_sequence,
            resumed_input,
            resumed_provider_output,
            resumed_output,
            resumed_terminal,
        ) = checkpoint_state.watermarks.get(direction.source_participant_id, (0, 0, 0, 0, False))
        if resumed_terminal:
            continue
        if direction.source_participant_id in completed_sources:
            continue
        if direction.mode == "translated":
            source, _ = await initialize_value(
                interpretation_tracks.__getitem__,
                str(livekit_by_participant[direction.destination_participant_id]),
            )
            publisher = await initialize_value(
                PreservedAudioPublisher,
                metadata.consultation_id,
                uuid4(),
                direction.destination_participant_id,
                source,
                spool,
            )
            if resumed_output % PreservedAudioPublisher.FRAME_SAMPLES:
                raise RuntimeError("checkpoint output is not aligned to published frames")
            publisher._sample = resumed_output
            publisher._sequence = resumed_output // PreservedAudioPublisher.FRAME_SAMPLES
            publishers[direction.destination_participant_id] = publisher
        sinks = _build_direction_sinks(
            ctx,
            metadata,
            direction,
            livekit_by_participant,
            spool,
            publisher,
            checkpoint,
            resumed_provider_output,
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
            ScopedExchangeJournal(spool, _direction_scope(metadata, direction)),
            tuple(dict.fromkeys(direction_locales)),
        )
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
            _build_provider_terminal_sink(metadata, direction, spool, control),
        )
        session._last_input = resumed_input
        session._committed_input = resumed_input
        session._last_output = resumed_output
        sessions[direction.source_participant_id] = session
        await initialize(session.start())
        sessions_by_identity[livekit_by_participant[direction.source_participant_id]] = session
    sessions_ready.set()

    await _supervise_runtime(
        ctx,
        metadata,
        spool,
        control,
        provider_health,
        sessions,
        publishers,
        quota_leases,
        stream_tasks,
        drain_runtime_once,
        failure,
        disconnected,
        drain_requested,
        checkpoint_state.hashes,
        interpretation_tracks,
    )


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
    except SpoolUnavailable:
        supervisor_pid = os.environ.get("TRANSHOOTER_WORKER_SUPERVISOR_PID")
        if supervisor_pid is not None:
            os.kill(int(supervisor_pid), signal.SIGTERM)
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
    os.environ["TRANSHOOTER_WORKER_SUPERVISOR_PID"] = str(os.getpid())
    agents.cli.run_app(
        agents.WorkerOptions(
            entrypoint_fnc=consultation_entrypoint,
            agent_name="translation-worker",
            ws_url=os.environ.get("LIVEKIT_URL", "ws://localhost:7880"),
            api_key=api_key,
            api_secret=api_secret,
        )
    )
