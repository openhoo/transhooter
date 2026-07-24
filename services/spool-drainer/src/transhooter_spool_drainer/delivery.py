from __future__ import annotations

import hashlib
import json
import logging
import os
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid5

import boto3  # type: ignore[import-untyped]
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from opentelemetry import metrics
from transhooter_spool import (
    ObjectRecord,
    SampleRange,
    SpoolCheckpointDelivery,
    SpoolConsultationSeal,
    SpoolDrainer,
    SpoolRecordDelivery,
)

from .clients import (
    CompleteWorkerEpoch,
    DrainerControlClient,
    PermanentControlRequestError,
    TerminalCheckpointIdentity,
    WorkerTuple,
)
from .s3_archive import ArchiveConflict, S3Archive
from .scenario import ScenarioCrashHooks
from .telemetry import bounded_error_kind

logger = logging.getLogger(__name__)
_METER = metrics.get_meter(__name__)
_OBJECTS = _METER.create_counter("transhooter.spool.drainer.objects.total", unit="{object}")
_CHECKPOINTS = _METER.create_counter(
    "transhooter.spool.drainer.checkpoints.total", unit="{checkpoint}"
)

_RECORD_EXCLUDED_STAGES = frozenset(
    {"stt-input", "tts-output", "livekit-output", "checkpoint", "checkpoint-seal"}
)
_PCM_OBJECT_CLASSES = {
    "stt-input": "stt_input_pcm",
    "tts-output": "tts_output_pcm",
    "livekit-output": "livekit_output_pcm",
}


class ArchiveStore(Protocol):
    def put_create_once(
        self, key: str, body: bytes, content_type: str, sha256: str
    ) -> ObjectRecord: ...

    def verify(self, record: ObjectRecord) -> bool: ...


class DeliveryRetryable(RuntimeError):
    pass


class DeliveryPermanent(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class DeliveryStats:
    uploaded_records: int = 0
    permanent_records: int = 0
    acknowledged_checkpoints: int = 0
    permanent_checkpoints: int = 0
    completed_workers: int = 0

    def plus(self, other: DeliveryStats) -> DeliveryStats:
        return DeliveryStats(
            self.uploaded_records + other.uploaded_records,
            self.permanent_records + other.permanent_records,
            self.acknowledged_checkpoints + other.acknowledged_checkpoints,
            self.permanent_checkpoints + other.permanent_checkpoints,
            self.completed_workers + other.completed_workers,
        )


@dataclass(frozen=True, slots=True)
class CompactedPcm:
    pcm: ObjectRecord
    sidecar: ObjectRecord
    samples: SampleRange
    source_ids: tuple[UUID, ...]
    stage: str
    direction: str
    worker: WorkerTuple
    permanent_source_ids: tuple[UUID, ...]


class PcmCompactor:
    def __init__(
        self,
        spool: SpoolDrainer,
        archive: ArchiveStore,
        meeting_id: UUID,
        sample_rate: int = 48_000,
    ) -> None:
        self._spool = spool
        self._archive = archive
        self._meeting_id = meeting_id
        self._rate = sample_rate

    def compact(
        self,
        stage: str,
        direction: str,
        *,
        drain: bool = False,
        include_uploaded: bool = False,
    ) -> tuple[CompactedPcm, ...]:
        records = [
            delivery
            for delivery in self._spool.list_record_deliveries(
                meeting_id=self._meeting_id,
                states={"committed", "uploaded"} if include_uploaded else {"committed"},
            )
            if delivery.context.stage == stage
            and delivery.context.direction == direction
            and delivery.sample_range is not None
        ]
        records.sort(key=lambda item: item.sample_range.start if item.sample_range else -1)
        output: list[CompactedPcm] = []
        batch: list[SpoolRecordDelivery] = []
        samples = 0
        expected: int | None = None
        for delivery in records:
            span = delivery.sample_range
            assert span is not None
            if expected is not None and span.start != expected:
                if batch and (drain or samples >= self._rate * 10):
                    output.append(self._flush(stage, direction, batch))
                batch, samples = [], 0
            batch.append(delivery)
            samples += span.length
            expected = span.end
            if samples >= self._rate * 10:
                output.append(self._flush(stage, direction, batch))
                batch, samples, expected = [], 0, None
        if drain and batch:
            output.append(self._flush(stage, direction, batch))
        return tuple(output)

    def acknowledge_covering_checkpoint(self, compacted: CompactedPcm, checkpoint_id: UUID) -> None:
        if not self._spool.checkpoint_covers(
            checkpoint_id, compacted.stage, compacted.direction, compacted.samples.end
        ):
            raise ValueError("checkpoint is absent or does not durably cover compacted samples")
        for object_id in compacted.source_ids:
            self._spool.mark_record_uploaded(
                object_id, compacted.pcm.version_id, compacted.pcm.s3_checksum
            )

    def _flush(
        self,
        stage: str,
        direction: str,
        batch: list[SpoolRecordDelivery],
    ) -> CompactedPcm:
        first_span = batch[0].sample_range
        last_span = batch[-1].sample_range
        assert first_span is not None and last_span is not None
        worker = _record_worker(batch[0])
        if any(_record_worker(item) != worker for item in batch):
            raise DeliveryPermanent("PCM compaction crossed a producer tuple")
        start, end = first_span.start, last_span.end
        pcm = b"".join(self._spool.read(item.raw_ref.object_id) for item in batch)
        prefix = f"v1/meetings/{self._meeting_id}/audio/{stage}/{direction}/{start:020d}-{end:020d}"
        digest = hashlib.sha256(pcm).hexdigest()
        pcm_record = self._archive.put_create_once(prefix + ".pcm", pcm, "audio/L16", digest)
        sidecar_body = json.dumps(
            {
                "encoding": "LINEAR16",
                "rate": self._rate,
                "channels": 1,
                "format": "raw",
                "sampleStart": start,
                "sampleEnd": end,
                "sha256": digest,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        sidecar = self._archive.put_create_once(
            prefix + ".json",
            sidecar_body,
            "application/json",
            hashlib.sha256(sidecar_body).hexdigest(),
        )
        return CompactedPcm(
            pcm_record,
            sidecar,
            SampleRange(start, end),
            tuple(item.raw_ref.object_id for item in batch),
            stage,
            direction,
            worker,
            tuple(item.raw_ref.object_id for item in batch if item.state == "committed"),
        )


def build_archive(root: Path) -> S3Archive:
    credentials_file = os.environ.get("S3_CREDENTIALS_FILE")
    if not credentials_file:
        raise RuntimeError("S3_CREDENTIALS_FILE is required")
    credentials = json.loads(Path(credentials_file).read_text("utf-8"))
    access_key = credentials.get("accessKeyId")
    secret_key = credentials.get("secretAccessKey")
    if (
        not isinstance(access_key, str)
        or not access_key
        or not isinstance(secret_key, str)
        or not secret_key
    ):
        raise RuntimeError("S3 credential file is invalid")
    client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT"),
        region_name=os.environ.get("S3_REGION", "eu-central-1"),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    bucket = os.environ.get("S3_BUCKET", "").strip()
    if not bucket:
        raise RuntimeError("S3_BUCKET is required")
    return S3Archive(
        client,
        bucket,
        os.environ.get("S3_KMS_KEY_ID"),
        os.environ.get("ARCHIVE_REQUIRE_KMS", "true").lower() == "true",
        root / "multipart.sqlite3",
    )


def drain_delivery_cycle(
    spool: SpoolDrainer,
    archive: ArchiveStore,
    control: DrainerControlClient,
    *,
    before_operation: Callable[[], bool] | None = None,
    crash_hooks: ScenarioCrashHooks | None = None,
) -> DeliveryStats:
    can_start = before_operation or (lambda: True)
    hooks = crash_hooks or ScenarioCrashHooks.from_environment()
    stats = _deliver_records(spool, archive, control, can_start, hooks)
    if can_start():
        stats = stats.plus(_deliver_pcm(spool, archive, control, can_start, hooks))
    if can_start():
        stats = stats.plus(_deliver_checkpoints(spool, archive, control, can_start, hooks))
    return stats


def _deliver_records(
    spool: SpoolDrainer,
    archive: ArchiveStore,
    control: DrainerControlClient,
    can_start: Callable[[], bool],
    crash_hooks: ScenarioCrashHooks,
) -> DeliveryStats:
    retryable: list[BaseException] = []
    uploaded = permanent = 0
    for delivery in spool.list_record_deliveries(states={"committed"}):
        if not can_start():
            break
        context = delivery.context
        if context.stage in _RECORD_EXCLUDED_STAGES:
            continue
        object_class = _object_class(context.stage)
        key = _raw_key(delivery)
        try:
            record = archive.put_create_once(
                key,
                spool.read(delivery.raw_ref.object_id),
                context.media_type,
                delivery.raw_ref.sha256,
            )
            crash_hooks.trigger(context.meeting_id, "s3-put")
            control.archive_object(
                _record_worker(delivery),
                causal_key=str(delivery.raw_ref.object_id),
                object_id=delivery.raw_ref.object_id,
                object_class=object_class,
                record=record,
                sample_range=delivery.sample_range,
                sequence=context.ordinal,
            )
            crash_hooks.trigger(context.meeting_id, "archive-registration")
        except (ArchiveConflict, PermanentControlRequestError, DeliveryPermanent) as error:
            spool.mark_record_delivery_permanent(
                delivery.raw_ref.object_id,
                _error_kind(error),
                datetime.now(UTC),
            )
            permanent += 1
            _record_metric(_OBJECTS, "permanent", object_class, error)
            continue
        except Exception as error:
            retryable.append(error)
            _record_metric(_OBJECTS, "retryable", object_class, error)
            continue
        spool.mark_record_uploaded(
            delivery.raw_ref.object_id, record.version_id, record.s3_checksum
        )
        uploaded += 1
        _record_metric(_OBJECTS, "uploaded", object_class)
    spool.compact_uploaded_envelopes()
    _raise_retryable(retryable)
    return DeliveryStats(uploaded_records=uploaded, permanent_records=permanent)


def _deliver_pcm(
    spool: SpoolDrainer,
    archive: ArchiveStore,
    control: DrainerControlClient,
    can_start: Callable[[], bool],
    crash_hooks: ScenarioCrashHooks,
) -> DeliveryStats:
    registered = permanent = 0
    retryable: list[BaseException] = []
    for meeting_id, stage, direction in spool.pcm_scopes(include_uploaded=True):
        if not can_start():
            break
        terminal_checkpoint = spool.covering_checkpoint(
            meeting_id, stage, direction, 0, terminal_only=True
        )
        compactor = PcmCompactor(
            spool, archive, meeting_id, 16_000 if stage == "stt-input" else 48_000
        )
        for compacted in compactor.compact(
            stage,
            direction,
            drain=terminal_checkpoint is not None,
            include_uploaded=terminal_checkpoint is not None,
        ):
            if not can_start():
                _raise_retryable(retryable)
                return DeliveryStats(uploaded_records=registered, permanent_records=permanent)
            pcm_class = _PCM_OBJECT_CLASSES[stage]
            registration_class = pcm_class
            try:
                for record, current_class in (
                    (compacted.pcm, pcm_class),
                    (compacted.sidecar, "pcm_sidecar"),
                ):
                    registration_class = current_class
                    object_id = uuid5(
                        meeting_id, f"archive-object:{record.key}:{record.version_id}"
                    )
                    crash_hooks.trigger(meeting_id, "s3-put")
                    control.archive_object(
                        compacted.worker,
                        causal_key=record.key,
                        object_id=object_id,
                        object_class=registration_class,
                        record=record,
                        sample_range=compacted.samples,
                    )
                    crash_hooks.trigger(meeting_id, "archive-registration")
                    registered += 1
            except (ArchiveConflict, PermanentControlRequestError, DeliveryPermanent) as error:
                failed_at = datetime.now(UTC)
                error_kind = _error_kind(error)
                for source_id in compacted.permanent_source_ids:
                    spool.mark_record_delivery_permanent(source_id, error_kind, failed_at)
                permanent += len(compacted.permanent_source_ids)
                _record_metric(_OBJECTS, "permanent", registration_class, error)
                continue
            except Exception as error:
                retryable.append(error)
                _record_metric(_OBJECTS, "retryable", registration_class, error)
                continue
            checkpoint_id = spool.covering_checkpoint(
                meeting_id, stage, direction, compacted.samples.end
            )
            if checkpoint_id is not None:
                compactor.acknowledge_covering_checkpoint(compacted, checkpoint_id)
    _raise_retryable(retryable)
    return DeliveryStats(uploaded_records=registered, permanent_records=permanent)


def _deliver_checkpoints(
    spool: SpoolDrainer,
    archive: ArchiveStore,
    control: DrainerControlClient,
    can_start: Callable[[], bool],
    crash_hooks: ScenarioCrashHooks,
) -> DeliveryStats:
    deliveries = tuple(spool.list_checkpoint_deliveries())
    by_chain: dict[tuple[UUID, int, UUID, int, int, UUID], list[SpoolCheckpointDelivery]] = {}
    for delivery in deliveries:
        key = (
            delivery.meeting_id,
            delivery.generation,
            delivery.worker_id,
            delivery.worker_epoch,
            delivery.write_epoch,
            delivery.source_id,
        )
        by_chain.setdefault(key, []).append(delivery)
    for chain in by_chain.values():
        chain.sort(key=lambda item: item.raw_ref.ordinal)

    acknowledged = permanent = completed = 0
    retryable: list[BaseException] = []
    for chain in by_chain.values():
        predecessor_acknowledged = True
        for delivery in chain:
            if delivery.delivery_state == "acknowledged":
                predecessor_acknowledged = True
                continue
            if delivery.delivery_state == "permanent":
                predecessor_acknowledged = False
                break
            if not predecessor_acknowledged or not can_start():
                break
            try:
                checkpoint = _validated_checkpoint(delivery)
                if _terminal_delivery_blocked(spool, delivery, checkpoint):
                    predecessor_acknowledged = False
                    break
                record = archive.put_create_once(
                    delivery.object_key,
                    delivery.body,
                    "application/json",
                    hashlib.sha256(delivery.body).hexdigest(),
                )
                crash_hooks.trigger(delivery.meeting_id, "s3-put")
                worker = crash_hooks.fence_checkpoint_worker(_checkpoint_worker(delivery))
                control.archive_object(
                    worker,
                    causal_key=str(delivery.checkpoint_id),
                    object_id=delivery.checkpoint_id,
                    object_class="checkpoint",
                    record=record,
                )
                crash_hooks.trigger(delivery.meeting_id, "archive-registration")
                control.checkpoint(
                    worker,
                    object_key=delivery.object_key,
                    checkpoint=checkpoint,
                    event_id=delivery.control_event_id,
                )
                crash_hooks.trigger(delivery.meeting_id, "checkpoint-acceptance")
            except (
                ArchiveConflict,
                PermanentControlRequestError,
                DeliveryPermanent,
                ValueError,
            ) as error:
                spool.mark_checkpoint_delivery_permanent(
                    delivery.control_event_id,
                    _error_kind(error),
                    datetime.now(UTC),
                )
                permanent += 1
                predecessor_acknowledged = False
                _record_metric(_CHECKPOINTS, "permanent", "checkpoint", error)
                break
            except Exception as error:
                retryable.append(error)
                _record_metric(_CHECKPOINTS, "retryable", "checkpoint", error)
                break
            spool.mark_checkpoint_delivery_acknowledged(delivery.control_event_id)
            acknowledged += 1
            predecessor_acknowledged = True
            _record_metric(_CHECKPOINTS, "acknowledged", "checkpoint")

    for seal in spool.list_consultation_seals(completion_states={"pending"}):
        if not can_start():
            break
        terminal = {
            delivery.checkpoint_id: delivery
            for delivery in spool.list_checkpoint_deliveries(
                meeting_id=seal.meeting_id,
                generation=seal.generation,
                worker_id=seal.worker_id,
                worker_epoch=seal.worker_epoch,
                write_epoch=seal.write_epoch,
            )
            if delivery.checkpoint_id in {seal.first_checkpoint_id, seal.second_checkpoint_id}
        }
        if len(terminal) != 2 or any(
            item.delivery_state != "acknowledged" for item in terminal.values()
        ):
            continue
        terminal_pair = (
            TerminalCheckpointIdentity(
                seal.first_checkpoint_id,
                terminal[seal.first_checkpoint_id].checkpoint_hash,
            ),
            TerminalCheckpointIdentity(
                seal.second_checkpoint_id,
                terminal[seal.second_checkpoint_id].checkpoint_hash,
            ),
        )
        outcome = "clean" if seal.terminal_outcome in {"clean", "succeeded"} else "failed"
        failure = seal.failure
        if outcome == "failed" and failure is None:
            raise DeliveryPermanent("failed terminal seal omitted its bounded failure payload")
        request = CompleteWorkerEpoch(
            worker=_seal_worker(seal),
            completion_event_id=seal.completion_event_id,
            outcome=outcome,
            terminal_checkpoints=terminal_pair,
            failure=failure,
        )
        try:
            control.complete_worker_epoch(request)
        except PermanentControlRequestError as error:
            raise DeliveryPermanent(str(error)) from error
        crash_hooks.trigger(seal.meeting_id, "completion-acceptance")
        control_status = spool.mark_consultation_completion_acknowledged(seal.seal_id)
        if control_status is not None:
            completed += 1
    _raise_retryable(retryable)
    return DeliveryStats(
        acknowledged_checkpoints=acknowledged,
        permanent_checkpoints=permanent,
        completed_workers=completed,
    )


def _terminal_delivery_blocked(
    spool: SpoolDrainer,
    delivery: SpoolCheckpointDelivery,
    checkpoint: dict[str, object],
) -> bool:
    if checkpoint.get("terminal") is not True:
        return False
    seals = tuple(
        spool.list_consultation_seals(
            meeting_id=delivery.meeting_id,
            generation=delivery.generation,
            worker_id=delivery.worker_id,
            worker_epoch=delivery.worker_epoch,
            write_epoch=delivery.write_epoch,
        )
    )
    matches = tuple(
        seal
        for seal in seals
        if delivery.checkpoint_id in {seal.first_checkpoint_id, seal.second_checkpoint_id}
    )
    if len(matches) != 1:
        raise DeliveryRetryable("terminal checkpoint seal is not durably available")
    seal = matches[0]
    handoff = spool.consultation_handoff(
        meeting_id=delivery.meeting_id,
        generation=delivery.generation,
        worker_id=delivery.worker_id,
        worker_epoch=delivery.worker_epoch,
        write_epoch=delivery.write_epoch,
    )
    if handoff != "sealed":
        raise DeliveryRetryable("terminal checkpoint handoff is not sealed")
    blocked_records = any(
        record.raw_ref.ordinal <= seal.evidence_ordinal
        and record.context.stage not in {"checkpoint", "checkpoint-seal"}
        and (
            record.context.generation,
            record.context.worker_id,
            record.context.worker_epoch,
            record.context.write_epoch,
        )
        == (
            delivery.generation,
            delivery.worker_id,
            delivery.worker_epoch,
            delivery.write_epoch,
        )
        for record in spool.list_record_deliveries(
            meeting_id=delivery.meeting_id,
            states={"permanent", "quarantined"},
        )
    )
    blocked_checkpoints = any(
        candidate.delivery_state == "permanent"
        for candidate in spool.list_checkpoint_deliveries(
            meeting_id=delivery.meeting_id,
            generation=delivery.generation,
            worker_id=delivery.worker_id,
            worker_epoch=delivery.worker_epoch,
            write_epoch=delivery.write_epoch,
        )
    )
    return blocked_records or blocked_checkpoints


def _validated_checkpoint(delivery: SpoolCheckpointDelivery) -> dict[str, object]:
    try:
        value = json.loads(delivery.body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("checkpoint delivery body is malformed") from error
    if not isinstance(value, dict):
        raise ValueError("checkpoint delivery body is not an object")
    if json.dumps(value, separators=(",", ":"), sort_keys=True).encode() != delivery.body:
        raise ValueError("checkpoint delivery body is not canonical")
    expected = {
        "checkpointId": str(delivery.checkpoint_id),
        "sourceParticipantId": str(delivery.source_id),
        "workerEpoch": delivery.worker_epoch,
        "previousCheckpointSha256": delivery.previous_hash,
        "highWatermarkSha256": delivery.checkpoint_hash,
    }
    if any(value.get(key) != expected_value for key, expected_value in expected.items()):
        raise ValueError("checkpoint body does not match durable identity")
    hash_input = dict(value)
    hash_input.pop("highWatermarkSha256", None)
    computed = hashlib.sha256(
        (delivery.previous_hash or "").encode()
        + json.dumps(hash_input, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    if computed != delivery.checkpoint_hash:
        raise ValueError("checkpoint hash is invalid")
    try:
        _checkpoint_validator(_contracts_schema_path()).validate(value)
    except JsonSchemaValidationError as error:
        raise ValueError("checkpoint delivery violates WorkerCheckpoint schema") from error
    if value["sourceParticipantId"] == value["destinationParticipantId"]:
        raise ValueError("checkpoint source and destination must differ")
    return value


@cache
def _checkpoint_validator(path: Path) -> Draft202012Validator:
    try:
        document = json.loads(path.read_text("utf-8"))
        schemas = document["schemas"]
        schema = schemas["WorkerCheckpoint"]
    except (OSError, json.JSONDecodeError, KeyError, TypeError) as error:
        raise RuntimeError("generated WorkerCheckpoint schema is unavailable") from error
    return Draft202012Validator(schema, format_checker=FormatChecker())


def _contracts_schema_path() -> Path:
    return Path(
        os.environ.get("CONTRACTS_SCHEMA_FILE", "/workspace/contracts/contracts.schema.json")
    ).resolve()


def _raw_key(delivery: SpoolRecordDelivery) -> str:
    context = delivery.context
    suffix = "json" if "json" in context.media_type else "bin"
    return (
        f"v1/meetings/{context.meeting_id}/pipeline/{context.stage}/raw/"
        f"{context.attempt_id}/{context.ordinal:020d}.{suffix}"
    )


def _object_class(stage: str) -> str:
    if stage == "terminal" or stage.endswith("-terminal"):
        return "provider_terminal"
    if stage == "caption":
        return "caption_ledger"
    return "pipeline_exchange"


def _record_worker(delivery: SpoolRecordDelivery) -> WorkerTuple:
    context = delivery.context
    return WorkerTuple(
        context.meeting_id,
        context.generation,
        context.worker_id,
        context.worker_epoch,
        context.write_epoch,
    )


def _checkpoint_worker(delivery: SpoolCheckpointDelivery) -> WorkerTuple:
    return WorkerTuple(
        delivery.meeting_id,
        delivery.generation,
        delivery.worker_id,
        delivery.worker_epoch,
        delivery.write_epoch,
    )


def _seal_worker(seal: SpoolConsultationSeal) -> WorkerTuple:
    return WorkerTuple(
        seal.meeting_id,
        seal.generation,
        seal.worker_id,
        seal.worker_epoch,
        seal.write_epoch,
    )


def _secret(name: str) -> str:
    path = os.environ.get(name + "_FILE")
    value = Path(path).read_text("utf-8").strip() if path else ""
    if not value:
        raise RuntimeError(f"{name}_FILE is required")
    return value


def _error_kind(error: BaseException) -> str:
    if isinstance(error, ArchiveConflict):
        return "s3_identity_conflict"
    if isinstance(error, PermanentControlRequestError):
        return error.code[:100]
    return bounded_error_kind(error)


def _record_metric(
    counter: Any,
    result: str,
    object_class: str,
    error: BaseException | None = None,
) -> None:
    attributes = {"result": result, "object.class": object_class}
    if error is not None:
        attributes["error.kind"] = bounded_error_kind(error)
    try:
        counter.add(1, attributes)
    except Exception:
        pass


def _raise_retryable(errors: Iterable[BaseException]) -> None:
    collected = tuple(errors)
    if not collected:
        return
    if len(collected) == 1:
        raise DeliveryRetryable(str(collected[0])) from collected[0]
    raise DeliveryRetryable(
        f"{len(collected)} delivery operations remain retryable"
    ) from collected[0]
