from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from transhooter_spool import (
    EncryptedSpool,
    SampleRange,
    SpoolUnavailable,
    TerminalCheckpointIntent,
    deterministic_roomy_capacity,
)

KEYS = {"v1": b"k" * 32}


def make_spool(tmp_path: Path) -> EncryptedSpool:
    return EncryptedSpool(
        tmp_path / "payloads",
        tmp_path / "journal.sqlite3",
        KEYS,
        "v1",
        capacity_probe=deterministic_roomy_capacity,
    )


def tuple_values() -> tuple[UUID, int, UUID, int, int]:
    return uuid4(), 3, uuid4(), 7, 2


def open_authority(spool: EncryptedSpool, values: tuple[UUID, int, UUID, int, int]):
    meeting_id, generation, worker_id, worker_epoch, write_epoch = values
    return spool.open_consultation_producer(
        meeting_id=meeting_id,
        generation=generation,
        worker_id=worker_id,
        worker_epoch=worker_epoch,
        write_epoch=write_epoch,
    )


def checkpoint_intent(
    values: tuple[UUID, int, UUID, int, int], source_id: UUID, previous: str | None = None
) -> TerminalCheckpointIntent:
    checkpoint_id = uuid4()
    body_without_hash: dict[str, object] = {
        "acceptedInput": 10,
        "checkpointId": str(checkpoint_id),
        "destinationParticipantId": str(uuid4()),
        "emittedOutput": 20,
        "previousCheckpointSha256": previous,
        "sourceParticipantId": str(source_id),
        "terminal": True,
        "workerEpoch": values[3],
    }
    import hashlib

    digest = hashlib.sha256(
        (previous or "").encode()
        + json.dumps(body_without_hash, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    body_without_hash["highWatermarkSha256"] = digest
    return TerminalCheckpointIntent(
        checkpoint_id=checkpoint_id,
        source_id=source_id,
        checkpoint_hash=digest,
        previous_hash=previous,
        control_event_id=uuid4(),
        object_key=f"checkpoints/{checkpoint_id}.json",
        body=json.dumps(body_without_hash, separators=(",", ":"), sort_keys=True).encode(),
    )


def test_fixture_append_faults_fail_preservation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    spool = make_spool(tmp_path)
    values = tuple_values()
    authority = open_authority(spool, values)
    scenario = tmp_path / "scenario.json"
    scenario.write_text(
        json.dumps(
            {
                "consultations": {
                    str(values[0]): {"spool": {"walFailAfterAppends": 1}}
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("FIXTURE_SCENARIO_FILE", str(scenario))

    with pytest.raises(SpoolUnavailable, match="injected WAL fsync failure"):
        spool.append(
            authority,
            meeting_id=values[0],
            attempt_id=uuid4(),
            stage="translation",
            transport="http",
            direction="in",
            media_type="application/json",
            payload=b"payload",
        )


def test_schema_v2_creation_and_old_schema_exact_rejection(tmp_path: Path) -> None:
    spool = make_spool(tmp_path)
    assert spool._db.execute("PRAGMA user_version").fetchone() == (2,)
    tables = {
        row[0]
        for row in spool._db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }
    assert tables == {
        "checkpoint_deliveries",
        "compacted_envelopes",
        "consultation_handoffs",
        "consultation_seals",
        "records",
        "spool_meta",
    }
    spool.close()

    old_root = tmp_path / "old"
    old_root.mkdir()
    database = tmp_path / "old.sqlite3"
    connection = sqlite3.connect(database)
    connection.execute(
        "CREATE TABLE checkpoint_deliveries(checkpoint_id TEXT PRIMARY KEY, acknowledged INTEGER)"
    )
    connection.commit()
    connection.close()
    with pytest.raises(
        SpoolUnavailable,
        match="^unsupported spool schema; drain and recreate the spool before service cutover$",
    ):
        EncryptedSpool(
            old_root,
            database,
            KEYS,
            "v1",
            capacity_probe=deterministic_roomy_capacity,
        )


def test_authenticated_tuple_append_tamper_and_permanent_state(tmp_path: Path) -> None:
    spool = make_spool(tmp_path)
    values = tuple_values()
    authority = open_authority(spool, values)
    ref = spool.append(
        authority,
        meeting_id=values[0],
        attempt_id=uuid4(),
        stage="stt-input",
        transport="grpc",
        direction="in",
        media_type="audio/L16",
        payload=b"\0\1" * 10,
        sample_range=SampleRange(0, 10),
        metadata=(("codec", "pcm"),),
    )
    delivery = spool.list_record_deliveries()[0]
    assert delivery.raw_ref == ref
    assert (delivery.context.generation, delivery.context.worker_id) == (values[1], values[2])
    assert spool.read(ref.object_id) == b"\0\1" * 10
    permanent = spool.mark_record_delivery_permanent(
        ref.object_id, "archive-rejected", datetime.now(UTC)
    )
    assert permanent.state == "permanent"
    assert spool.list_record_deliveries(states={"permanent"}) == (permanent,)
    authority.close()


def test_producer_and_recovery_authority_exclusion_and_handoffs(tmp_path: Path) -> None:
    spool = make_spool(tmp_path)
    values = tuple_values()
    authority = open_authority(spool, values)
    assert (
        spool.acquire_consultation_recovery(
            meeting_id=values[0],
            generation=values[1],
            worker_id=values[2],
            worker_epoch=values[3],
            write_epoch=values[4],
        )
        is None
    )
    spool.begin_consultation_settlement(authority)
    assert (
        spool.consultation_handoff(
            meeting_id=values[0],
            generation=values[1],
            worker_id=values[2],
            worker_epoch=values[3],
            write_epoch=values[4],
        )
        == "settling"
    )
    authority.close()
    recovery = spool.acquire_consultation_recovery(
        meeting_id=values[0],
        generation=values[1],
        worker_id=values[2],
        worker_epoch=values[3],
        write_epoch=values[4],
    )
    assert recovery is not None
    spool.relinquish_expired_consultation(recovery, "worker lease expired")
    recovery.close()
    assert (
        spool.consultation_relinquishment_reason(
            meeting_id=values[0],
            generation=values[1],
            worker_id=values[2],
            worker_epoch=values[3],
            write_epoch=values[4],
        )
        == "worker lease expired"
    )


def test_nonterminal_checkpoint_and_atomic_terminal_seal(tmp_path: Path) -> None:
    spool = make_spool(tmp_path)
    values = tuple_values()
    authority = open_authority(spool, values)
    source = uuid4()
    nonterminal = checkpoint_intent(values, source)
    pending = spool.register_checkpoint_delivery(
        authority,
        checkpoint_id=nonterminal.checkpoint_id,
        meeting_id=values[0],
        generation=values[1],
        worker_id=values[2],
        worker_epoch=values[3],
        write_epoch=values[4],
        source_id=source,
        checkpoint_hash=nonterminal.checkpoint_hash,
        previous_hash=nonterminal.previous_hash,
        control_event_id=nonterminal.control_event_id,
        object_key=nonterminal.object_key,
        body=nonterminal.body,
    )
    assert pending.evidence_ordinal is None
    assert pending.record_id == pending.checkpoint_id
    assert (
        spool.mark_checkpoint_delivery_acknowledged(pending.control_event_id).delivery_state
        == "acknowledged"
    )

    spool.append(
        authority,
        meeting_id=values[0],
        attempt_id=uuid4(),
        stage="translation",
        transport="http",
        direction="out",
        media_type="application/json",
        payload=b"{}",
    )
    spool.begin_consultation_settlement(authority)
    intents = (checkpoint_intent(values, uuid4()), checkpoint_intent(values, uuid4()))
    seal = spool.seal_terminal_checkpoints(
        authority,
        terminal_outcome="succeeded",
        completion_event_id=uuid4(),
        intents=intents,
    )
    terminal = spool.list_checkpoint_deliveries(meeting_id=values[0], states={"pending"})
    assert len(terminal) == 2
    assert {item.record_id for item in terminal} == {seal.seal_id}
    assert all(item.evidence_ordinal == seal.evidence_ordinal for item in terminal)
    assert (
        spool.consultation_handoff(
            meeting_id=values[0],
            generation=values[1],
            worker_id=values[2],
            worker_epoch=values[3],
            write_epoch=values[4],
        )
        == "sealed"
    )
    with pytest.raises(SpoolUnavailable, match="sealed"):
        spool.append(
            authority,
            meeting_id=values[0],
            attempt_id=uuid4(),
            stage="translation",
            transport="http",
            direction="out",
            media_type="application/json",
            payload=b"{}",
        )
    authority.close()
