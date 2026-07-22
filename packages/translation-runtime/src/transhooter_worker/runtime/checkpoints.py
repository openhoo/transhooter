from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import dataclass
from uuid import UUID, uuid4, uuid5

from transhooter_worker.adapters.archive_delivery import upload_committed_objects_async
from transhooter_worker.adapters.s3_archive import S3Archive
from transhooter_worker.adapters.spool import EncryptedSpool, SpoolCheckpointDelivery
from transhooter_worker.application.compactor import CompactedPcm, PcmCompactor
from transhooter_worker.ports.archive import ObjectRecord
from transhooter_worker.runtime.consultation import SourceTrackTimeline
from transhooter_worker.runtime.control_client import ControlClient
from transhooter_worker.runtime.job_metadata import JobMetadata
from transhooter_worker.runtime.publisher import PreservedAudioPublisher


@dataclass(frozen=True, slots=True)
class PendingCheckpoint:
    checkpoint_id: UUID
    control_event_id: UUID
    checkpoint: dict[str, object]
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
        requested = (input_sequence, input_sample, provider_output_sample, output_sample, terminal)
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
            if requested == state.watermarks[source_id]:
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
        checkpoint: dict[str, object] = {
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
        values = (input_sequence_value, input_value, provider_output_value, output_value)
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
    metadata: JobMetadata, control: ControlClient, compacted: CompactedPcm
) -> None:
    sample_range = {"start": compacted.samples.start, "end": compacted.samples.end}
    for record, object_class in (
        (compacted.pcm, _PCM_OBJECT_CLASSES[compacted.stage]),
        (compacted.sidecar, "pcm_sidecar"),
    ):
        object_id = uuid5(
            metadata.consultation_id, f"archive-object:{record.key}:{record.version_id}"
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
    metadata: JobMetadata, spool: EncryptedSpool, archive: S3Archive, control: ControlClient
) -> None:
    for meeting_id, stage, direction in spool.pcm_scopes(include_uploaded=True):
        if meeting_id != metadata.consultation_id:
            continue
        terminal_checkpoint = spool.covering_checkpoint(
            meeting_id, stage, direction, 0, terminal_only=True
        )
        if terminal_checkpoint is None:
            continue
        compactor = PcmCompactor(
            spool, archive, meeting_id, 16_000 if stage == "stt-input" else 48_000
        )
        closed_objects = compactor.compact(stage, direction, drain=True, include_uploaded=True)
        for closed_object in closed_objects:
            if not spool.checkpoint_covers(
                terminal_checkpoint, stage, direction, closed_object.samples.end
            ):
                raise RuntimeError("terminal checkpoint does not cover compacted PCM")
            await _record_compacted_pcm(metadata, control, closed_object)
            compactor.acknowledge_covering_checkpoint(closed_object, str(terminal_checkpoint))


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
    archived = archive.put_create_once(object_key, pending.body, "application/json", object_sha256)
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
                metadata, control, meeting_id, object_id, object_class, record
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
