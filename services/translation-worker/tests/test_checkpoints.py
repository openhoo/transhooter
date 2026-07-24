import hashlib
import json
from dataclasses import dataclass, replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from transhooter_worker.runtime.checkpoints import (
    CheckpointChainState,
    CheckpointWatermark,
    FinalDirectionCheckpoint,
    _persist_checkpoint,
    _restore_checkpoint_state,
    seal_terminal_checkpoints,
)


@dataclass(frozen=True)
class Delivery:
    meeting_id: UUID
    generation: int
    worker_id: UUID
    worker_epoch: int
    write_epoch: int
    source_id: UUID
    checkpoint_id: UUID
    checkpoint_hash: str
    previous_hash: str | None
    body: bytes
    delivery_state: str = "pending"
    error_kind: str | None = None
    failed_at: object | None = None


class RecordingSpool:
    def __init__(self, deliveries: tuple[Delivery, ...] = ()) -> None:
        self.deliveries = deliveries
        self.registered: list[dict[str, object]] = []
        self.seals: list[dict[str, object]] = []
        self.network_effects: list[object] = []

    def list_checkpoint_deliveries(self, **_: object) -> tuple[Delivery, ...]:
        return self.deliveries

    def register_checkpoint_delivery(self, authority: object, **values: object) -> None:
        self.registered.append({"authority": authority, **values})

    def seal_terminal_checkpoints(self, authority: object, **values: object) -> object:
        self.seals.append({"authority": authority, **values})
        return object()


def metadata() -> SimpleNamespace:
    first = uuid4()
    second = uuid4()
    directions = (
        SimpleNamespace(source_participant_id=first, destination_participant_id=second),
        SimpleNamespace(source_participant_id=second, destination_participant_id=first),
    )
    return SimpleNamespace(
        consultation_id=uuid4(),
        generation=4,
        worker_identity=uuid4(),
        worker_epoch=5,
        write_epoch=6,
        selection=SimpleNamespace(directions=directions),
    )


def delivery(
    meta: SimpleNamespace,
    source_id: UUID,
    destination_id: UUID,
    sequence: int,
    output: int,
    *,
    previous_hash: str | None = None,
    generation: int | None = None,
) -> Delivery:
    checkpoint_id = uuid4()
    payload: dict[str, object] = {
        "checkpointId": str(checkpoint_id),
        "sourceParticipantId": str(source_id),
        "destinationParticipantId": str(destination_id),
        "acceptedInputSequence": sequence,
        "acceptedInput": sequence * 4_000,
        "receivedOutput": output,
        "emittedOutput": output,
        "workerEpoch": meta.worker_epoch,
        "previousCheckpointSha256": previous_hash,
        "expectedObjectIds": [],
        "observedObjectIds": [],
        "gaps": [],
        "terminal": False,
        "occurredAtMs": 1,
    }
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    digest = hashlib.sha256((previous_hash or "").encode() + encoded).hexdigest()
    payload["highWatermarkSha256"] = digest
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return Delivery(
        meta.consultation_id,
        meta.generation if generation is None else generation,
        meta.worker_identity,
        meta.worker_epoch,
        meta.write_epoch,
        source_id,
        checkpoint_id,
        digest,
        previous_hash,
        body,
    )


def test_restore_consumes_pending_and_permanent_chain_members_in_record_order() -> None:
    meta = metadata()
    direction = meta.selection.directions[0]
    first = delivery(
        meta, direction.source_participant_id, direction.destination_participant_id, 1, 960
    )
    second = delivery(
        meta,
        direction.source_participant_id,
        direction.destination_participant_id,
        2,
        1_920,
        previous_hash=first.checkpoint_hash,
    )
    permanent = replace(second, delivery_state="permanent", error_kind="conflict")
    state = _restore_checkpoint_state(RecordingSpool((first, permanent)), meta)
    assert state.hashes[direction.source_participant_id] == permanent.checkpoint_hash
    assert state.watermarks[direction.source_participant_id] == CheckpointWatermark(
        2, 8_000, 1_920, 1_920, False
    )


def test_restore_rejects_same_consultation_checkpoint_outside_exact_tuple() -> None:
    meta = metadata()
    direction = meta.selection.directions[0]
    wrong = delivery(
        meta,
        direction.source_participant_id,
        direction.destination_participant_id,
        1,
        960,
        generation=meta.generation + 1,
    )
    with pytest.raises(RuntimeError, match="admitted worker tuple"):
        _restore_checkpoint_state(RecordingSpool((wrong,)), meta)


@pytest.mark.asyncio
async def test_nonterminal_checkpoint_is_durable_only_and_has_no_network_effect() -> None:
    meta = metadata()
    direction = meta.selection.directions[0]
    spool = RecordingSpool()
    authority = object()
    state = CheckpointChainState.empty()
    await _persist_checkpoint(
        meta,
        spool,
        authority,
        state,
        direction.source_participant_id,
        direction.destination_participant_id,
        4_000,
        960,
        input_sequence=1,
        provider_output_sample=1_200,
    )
    assert len(spool.registered) == 1
    assert spool.registered[0]["authority"] is authority
    assert json.loads(spool.registered[0]["body"])["terminal"] is False
    assert spool.network_effects == []


def test_terminal_shutdown_builds_exact_pair_and_one_atomic_seal() -> None:
    meta = metadata()
    spool = RecordingSpool()
    authority = object()
    state = CheckpointChainState.empty()
    finals = tuple(
        FinalDirectionCheckpoint(
            direction.source_participant_id,
            direction.destination_participant_id,
            CheckpointWatermark(1, 4_000, 960, 960, True),
        )
        for direction in meta.selection.directions
    )
    completion_event_id = uuid4()
    seal_terminal_checkpoints(
        meta,
        spool,
        authority,
        state,
        finals,
        terminal_outcome="failed",
        completion_event_id=completion_event_id,
        failure={"kind": "RuntimeError"},
    )
    assert len(spool.seals) == 1
    seal = spool.seals[0]
    assert seal["authority"] is authority
    assert seal["terminal_outcome"] == "failed"
    assert seal["completion_event_id"] == completion_event_id
    assert len(seal["intents"]) == 2
    assert {intent.source_id for intent in seal["intents"]} == {
        direction.source_participant_id for direction in meta.selection.directions
    }
    assert json.loads(seal["failure_payload"]) == {"kind": "RuntimeError"}
    assert spool.network_effects == []
