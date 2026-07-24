from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import dataclass
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from transhooter_spool import (
    ConsultationProducerAuthority,
    EncryptedSpool,
    SpoolCheckpointDelivery,
    TerminalCheckpointIntent,
)

from transhooter_worker.runtime.consultation import SourceTrackTimeline
from transhooter_worker.runtime.job_metadata import JobMetadata
from transhooter_worker.runtime.publisher import PreservedAudioPublisher


@dataclass(frozen=True, slots=True)
class CheckpointWatermark:
    input_sequence: int
    input_sample: int
    provider_output_sample: int
    output_sample: int
    terminal: bool


ZERO_CHECKPOINT_WATERMARK = CheckpointWatermark(0, 0, 0, 0, False)


@dataclass(frozen=True, slots=True)
class FinalDirectionCheckpoint:
    source_id: UUID
    destination_id: UUID
    watermark: CheckpointWatermark


@dataclass(slots=True)
class CheckpointChainState:
    hashes: dict[UUID, str]
    watermarks: dict[UUID, CheckpointWatermark]
    locks: dict[UUID, asyncio.Lock]

    @classmethod
    def empty(cls) -> CheckpointChainState:
        return cls({}, {}, {})

    def lock_for(self, source_id: UUID) -> asyncio.Lock:
        lock = self.locks.get(source_id)
        if lock is None:
            lock = asyncio.Lock()
            self.locks[source_id] = lock
        return lock


def _object_key(meeting_id: UUID, checkpoint_id: UUID) -> str:
    return f"v1/meetings/{meeting_id}/inventory/checkpoints/{checkpoint_id}.json"


def _canonical_checkpoint(
    metadata: JobMetadata,
    source_id: UUID,
    destination_id: UUID,
    watermark: CheckpointWatermark,
    checkpoint_id: UUID,
    previous_hash: str | None,
    *,
    occurred_at_ms: int,
) -> tuple[dict[str, object], bytes, str]:
    checkpoint: dict[str, object] = {
        "checkpointId": str(checkpoint_id),
        "sourceParticipantId": str(source_id),
        "destinationParticipantId": str(destination_id),
        "acceptedInputSequence": watermark.input_sequence,
        "acceptedInput": watermark.input_sample,
        "receivedOutput": watermark.provider_output_sample,
        "emittedOutput": watermark.output_sample,
        "workerEpoch": metadata.worker_epoch,
        "previousCheckpointSha256": previous_hash,
        "expectedObjectIds": [],
        "observedObjectIds": [],
        "gaps": [],
        "terminal": watermark.terminal,
        "occurredAtMs": occurred_at_ms,
    }
    encoded = json.dumps(checkpoint, separators=(",", ":"), sort_keys=True).encode()
    digest = hashlib.sha256((previous_hash or "").encode() + encoded).hexdigest()
    checkpoint["highWatermarkSha256"] = digest
    body = json.dumps(checkpoint, separators=(",", ":"), sort_keys=True).encode()
    return checkpoint, body, digest


def _watermark_from_checkpoint(checkpoint: dict[str, object]) -> CheckpointWatermark:
    try:
        values = tuple(
            checkpoint[name]
            for name in ("acceptedInputSequence", "acceptedInput", "receivedOutput", "emittedOutput")
        )
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in values
        ):
            raise ValueError
        input_sequence, input_sample, provider_output_sample, output_sample = values
        assert isinstance(input_sequence, int)
        assert isinstance(input_sample, int)
        assert isinstance(provider_output_sample, int)
        assert isinstance(output_sample, int)
        if input_sample != input_sequence * SourceTrackTimeline.FRAME_SAMPLES:
            raise ValueError
        if output_sample % PreservedAudioPublisher.FRAME_SAMPLES:
            raise ValueError
        terminal = checkpoint["terminal"]
        if not isinstance(terminal, bool):
            raise ValueError
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("checkpoint delivery watermarks are malformed") from error
    return CheckpointWatermark(
        input_sequence=input_sequence,
        input_sample=input_sample,
        provider_output_sample=provider_output_sample,
        output_sample=output_sample,
        terminal=terminal,
    )


def _delivery_body(_spool: EncryptedSpool, delivery: SpoolCheckpointDelivery) -> bytes:
    return delivery.body


def _validated_delivery(
    spool: EncryptedSpool, delivery: SpoolCheckpointDelivery
) -> tuple[dict[str, object], CheckpointWatermark]:
    body = _delivery_body(spool, delivery)
    try:
        checkpoint = json.loads(body)
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
    if json.dumps(checkpoint, separators=(",", ":"), sort_keys=True).encode() != body:
        raise RuntimeError("checkpoint delivery body is not canonical")
    hash_input = dict(checkpoint)
    hash_input.pop("highWatermarkSha256", None)
    computed_hash = hashlib.sha256(
        (delivery.previous_hash or "").encode()
        + json.dumps(hash_input, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    if computed_hash != delivery.checkpoint_hash:
        raise RuntimeError("checkpoint delivery hash is invalid")
    return checkpoint, _watermark_from_checkpoint(checkpoint)


def _all_checkpoint_deliveries(
    spool: EncryptedSpool, metadata: JobMetadata
) -> tuple[SpoolCheckpointDelivery, ...]:
    return spool.list_checkpoint_deliveries(meeting_id=metadata.consultation_id)


def _delivery_tuple(delivery: SpoolCheckpointDelivery) -> tuple[UUID, int, UUID, int, int]:
    return (
        delivery.meeting_id,
        delivery.generation,
        delivery.worker_id,
        delivery.worker_epoch,
        delivery.write_epoch,
    )


def _metadata_tuple(metadata: JobMetadata) -> tuple[UUID, int, UUID, int, int]:
    return (
        metadata.consultation_id,
        metadata.generation,
        metadata.worker_identity,
        metadata.worker_epoch,
        metadata.write_epoch,
    )


def _restore_checkpoint_state(spool: EncryptedSpool, metadata: JobMetadata) -> CheckpointChainState:
    state = CheckpointChainState.empty()
    destinations = {
        direction.source_participant_id: direction.destination_participant_id
        for direction in metadata.selection.directions
    }
    expected_tuple = _metadata_tuple(metadata)
    deliveries = _all_checkpoint_deliveries(spool, metadata)
    for delivery in deliveries:
        if delivery.meeting_id != metadata.consultation_id:
            continue
        if delivery.source_id not in destinations:
            raise RuntimeError("checkpoint delivery source does not match the frozen directions")
        if _delivery_tuple(delivery) != expected_tuple:
            raise RuntimeError("checkpoint delivery does not match the admitted worker tuple")
        checkpoint, watermark = _validated_delivery(spool, delivery)
        destination_id = checkpoint.get("destinationParticipantId")
        if destination_id != str(destinations[delivery.source_id]):
            raise RuntimeError("checkpoint delivery does not match the frozen direction")
        current_hash = state.hashes.get(delivery.source_id)
        if delivery.previous_hash != current_hash:
            raise RuntimeError("checkpoint delivery chain is discontinuous")
        predecessor = state.watermarks.get(delivery.source_id)
        if predecessor is not None:
            if predecessor.terminal:
                raise RuntimeError("checkpoint delivery follows a terminal checkpoint")
            if (
                watermark.input_sequence < predecessor.input_sequence
                or watermark.input_sample < predecessor.input_sample
                or watermark.provider_output_sample < predecessor.provider_output_sample
                or watermark.output_sample < predecessor.output_sample
            ):
                raise RuntimeError("checkpoint delivery watermarks regress")
        state.hashes[delivery.source_id] = delivery.checkpoint_hash
        state.watermarks[delivery.source_id] = watermark
    terminal_sources = {
        source_id for source_id, watermark in state.watermarks.items() if watermark.terminal
    }
    if terminal_sources and terminal_sources != set(destinations):
        raise RuntimeError("completed consultation must contain exactly two terminal sources")
    return state


async def _persist_checkpoint(
    metadata: JobMetadata,
    spool: EncryptedSpool,
    authority: ConsultationProducerAuthority,
    state: CheckpointChainState,
    source_id: UUID,
    destination_id: UUID,
    input_sample: int,
    output_sample: int,
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
    requested = CheckpointWatermark(
        input_sequence=input_sequence,
        input_sample=input_sample,
        provider_output_sample=provider_output_sample,
        output_sample=output_sample,
        terminal=False,
    )
    async with state.lock_for(source_id):
        predecessor = state.watermarks.get(source_id)
        if requested == predecessor:
            return
        if predecessor is not None:
            if predecessor.terminal:
                raise RuntimeError("cannot append after a terminal checkpoint")
            if (
                input_sequence < predecessor.input_sequence
                or input_sample < predecessor.input_sample
                or provider_output_sample < predecessor.provider_output_sample
                or output_sample < predecessor.output_sample
            ):
                raise RuntimeError("checkpoint watermarks cannot regress")
        previous_hash = state.hashes.get(source_id)
        checkpoint_id = uuid4()
        _, body, digest = _canonical_checkpoint(
            metadata,
            source_id,
            destination_id,
            requested,
            checkpoint_id,
            previous_hash,
            occurred_at_ms=int(time.time() * 1000),
        )
        control_event_id = uuid4()
        spool.register_checkpoint_delivery(
            authority,
            checkpoint_id=checkpoint_id,
            meeting_id=metadata.consultation_id,
            generation=metadata.generation,
            worker_id=metadata.worker_identity,
            worker_epoch=metadata.worker_epoch,
            write_epoch=metadata.write_epoch,
            source_id=source_id,
            checkpoint_hash=digest,
            previous_hash=previous_hash,
            control_event_id=control_event_id,
            object_key=_object_key(metadata.consultation_id, checkpoint_id),
            body=body,
        )
        state.hashes[source_id] = digest
        state.watermarks[source_id] = requested


def _terminal_intent(
    metadata: JobMetadata,
    state: CheckpointChainState,
    final: FinalDirectionCheckpoint,
    completion_event_id: UUID,
    occurred_at_ms: int,
) -> TerminalCheckpointIntent:
    watermark = CheckpointWatermark(
        final.watermark.input_sequence,
        final.watermark.input_sample,
        final.watermark.provider_output_sample,
        final.watermark.output_sample,
        True,
    )
    previous_hash = state.hashes.get(final.source_id)
    checkpoint_id = uuid5(
        NAMESPACE_URL,
        f"https://transhooter.local/checkpoints/{completion_event_id}/{final.source_id}",
    )
    _, body, digest = _canonical_checkpoint(
        metadata,
        final.source_id,
        final.destination_id,
        watermark,
        checkpoint_id,
        previous_hash,
        occurred_at_ms=occurred_at_ms,
    )
    control_event_id = uuid5(checkpoint_id, "control-event")
    return TerminalCheckpointIntent(
        checkpoint_id=checkpoint_id,
        source_id=final.source_id,
        checkpoint_hash=digest,
        previous_hash=previous_hash,
        control_event_id=control_event_id,
        object_key=_object_key(metadata.consultation_id, checkpoint_id),
        body=body,
    )


def seal_terminal_checkpoints(
    metadata: JobMetadata,
    spool: EncryptedSpool,
    authority: ConsultationProducerAuthority,
    state: CheckpointChainState,
    final_directions: tuple[FinalDirectionCheckpoint, FinalDirectionCheckpoint],
    *,
    terminal_outcome: str,
    completion_event_id: UUID,
    failure: dict[str, object] | None,
    occurred_at_ms: int | None = None,
) -> object:
    if len(final_directions) != 2 or len({item.source_id for item in final_directions}) != 2:
        raise RuntimeError("terminal seal requires exactly two distinct source intents")
    stable_occurred_at_ms = int(time.time() * 1000) if occurred_at_ms is None else occurred_at_ms
    ordered = sorted(final_directions, key=lambda item: str(item.source_id))
    intents = (
        _terminal_intent(metadata, state, ordered[0], completion_event_id, stable_occurred_at_ms),
        _terminal_intent(metadata, state, ordered[1], completion_event_id, stable_occurred_at_ms),
    )
    failure_payload = (
        None
        if failure is None
        else json.dumps(failure, separators=(",", ":"), sort_keys=True).encode()
    )
    return spool.seal_terminal_checkpoints(
        authority,
        terminal_outcome=terminal_outcome,
        completion_event_id=completion_event_id,
        failure_payload=failure_payload,
        intents=intents,
    )
