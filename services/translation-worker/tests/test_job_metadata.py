import asyncio
import hashlib
import json
import signal
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from jsonschema.exceptions import ValidationError

from transhooter_worker.adapters.spool import SpoolUnavailable
from transhooter_worker.domain.models import StageCapabilities
from transhooter_worker.runtime.job import (
    SourceTrackTimeline,
    SttStage,
    _heartbeat_loop,
    _reported_preflight,
    _validate_frozen_stage,
    _validated_job_metadata,
    consultation_entrypoint,
)


def same_language_direction(
    source_participant_id: UUID,
    destination_participant_id: UUID,
) -> dict[str, object]:
    return {
        "mode": "same_language",
        "sourceParticipantId": str(source_participant_id),
        "destinationParticipantId": str(destination_participant_id),
        "capabilityRowId": str(uuid4()),
        "stt": {
            "provider": "fixture",
            "endpoint": "fixture://stt",
            "region": "test",
            "model": "deterministic",
            "adapterBuild": "fixture-v1",
            "policy": "frozen-v1",
            "credential": {"reference": "fixture", "version": "v1"},
            "limits": {},
            "locale": "en-US",
            "encoding": "LINEAR16",
        },
        "bypass": True,
    }


def job_metadata_payload() -> dict[str, object]:
    first_participant_id = uuid4()
    second_participant_id = uuid4()
    first_direction = same_language_direction(
        first_participant_id,
        second_participant_id,
    )
    second_direction = same_language_direction(
        second_participant_id,
        first_participant_id,
    )
    provider_selection = {
        "profileId": "fixture",
        "profileRevision": 1,
        "capabilityHash": "a" * 64,
        "participantIds": [
            str(first_participant_id),
            str(second_participant_id),
        ],
        "directions": [first_direction, second_direction],
    }
    canonical_selection = json.dumps(
        provider_selection,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    snapshot_hash = hashlib.sha256(canonical_selection).hexdigest()

    return {
        "schemaVersion": 1,
        "consultationId": str(uuid4()),
        "generation": 1,
        "roomName": str(uuid4()),
        "workerIdentity": str(uuid4()),
        "workerEpoch": 1,
        "writeEpoch": 0,
        "expectedParticipantIds": [
            str(first_participant_id),
            str(second_participant_id),
        ],
        "expectedLivekitIdentities": [str(uuid4()), str(uuid4())],
        "providerSelection": provider_selection,
        "snapshotHash": snapshot_hash,
    }


def configure_contract_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    schema_path = Path(__file__).parents[3] / "packages/contracts/generated/contracts.schema.json"
    monkeypatch.setenv("CONTRACTS_SCHEMA_FILE", str(schema_path))


@pytest.mark.asyncio
async def test_rejected_heartbeat_requests_graceful_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_contract_schema(monkeypatch)
    metadata = _validated_job_metadata(json.dumps(job_metadata_payload()))
    drain_requested = asyncio.Event()

    class HealthySpool:
        @staticmethod
        def usage_ratio() -> float:
            return 0.1

    class RejectingControl:
        calls = 0

        async def heartbeat(self, health: dict[str, object]) -> bool:
            self.calls += 1
            return False

    control = RejectingControl()
    await _heartbeat_loop(
        metadata,
        HealthySpool(),  # type: ignore[arg-type]
        control,  # type: ignore[arg-type]
        (),
        [],
        drain_requested,
    )

    assert drain_requested.is_set()
    assert control.calls == 1


def test_metadata_is_validated_against_generated_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_contract_schema(monkeypatch)
    metadata = _validated_job_metadata(json.dumps(job_metadata_payload()))

    assert metadata.selection.profile_id == "fixture"
    assert metadata.write_epoch == 0


def test_generated_contract_rejects_extra_dispatch_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_contract_schema(monkeypatch)
    metadata_payload = job_metadata_payload()
    metadata_payload["profileId"] = "fixture"

    with pytest.raises(ValidationError):
        _validated_job_metadata(json.dumps(metadata_payload))


def test_republished_track_keeps_absolute_cursor_and_fences_old_track() -> None:
    timeline = SourceTrackTimeline()
    first_generation = timeline.replace()
    first_claim = timeline.claim(first_generation, 4_000)
    assert first_claim is not None
    first_sequence, first_span = first_claim
    assert first_sequence == 0
    assert first_span is not None

    second_generation = timeline.replace()
    assert timeline.claim(first_generation, 4_000) is None

    second_claim = timeline.claim(second_generation, 4_000)
    assert second_claim is not None
    second_sequence, second_span = second_claim
    assert second_sequence == 1
    assert second_span is not None
    assert (second_span.start, second_span.end) == (4_000, 8_000)


@pytest.mark.asyncio
async def test_provider_preflight_failure_is_reported_before_propagation() -> None:
    reports: list[dict[str, object]] = []

    async def fail_preflight() -> None:
        raise RuntimeError("capability mismatch")

    async def capture_report(report: dict[str, object]) -> None:
        reports.append(report)

    with pytest.raises(RuntimeError, match="capability mismatch"):
        await _reported_preflight(
            fail_preflight,
            capture_report,
            "a" * 64,
        )

    expected_report = {
        "kind": "RuntimeError",
        "message": "capability mismatch",
        "phase": "provider-preflight",
        "snapshotHash": "a" * 64,
        "lastCheckpointHashes": {},
    }
    assert reports == [expected_report]


@pytest.mark.asyncio
async def test_spool_failure_requests_graceful_supervisor_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    killed: list[tuple[int, int]] = []

    async def fail_consultation(_ctx: object) -> None:
        raise SpoolUnavailable("spool unavailable")

    monkeypatch.setenv("TRANSHOOTER_WORKER_SUPERVISOR_PID", "1234")
    monkeypatch.setattr("transhooter_worker.runtime.job._run_consultation", fail_consultation)
    monkeypatch.setattr("os.kill", lambda pid, sig: killed.append((pid, sig)))

    with pytest.raises(SpoolUnavailable, match="spool unavailable"):
        await consultation_entrypoint(object())  # type: ignore[arg-type]

    assert killed == [(1234, signal.SIGTERM)]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("adapterBuild", "stale-build", "adapter build"),
        ("policy", "unfrozen-policy", "policy"),
        ("encoding", "opus", "encoding"),
        (
            "credential",
            {"reference": "caller-secret", "version": "fixture"},
            "credential reference",
        ),
        ("limits", {"message_bytes": 8_000, "extra": 1}, "limits"),
    ],
)
def test_frozen_stage_preflight_rejects_every_non_capability_field_mismatch(
    field: str,
    value: object,
    message: str,
) -> None:
    selected = {
        "provider": "fixture",
        "endpoint": "fixture://stt",
        "region": "test",
        "model": "deterministic",
        "adapterBuild": "transhooter-worker@0.1.0",
        "policy": "provider-profile-v1",
        "credential": {"reference": "fixture", "version": "fixture"},
        "limits": {"message_bytes": 8_000},
        "locale": "en-US",
        "encoding": "linear16",
    }
    selected[field] = value
    capability = StageCapabilities(
        provider="fixture",
        stage="stt",
        endpoint="fixture://stt",
        regions=("test",),
        languages=("en-US",),
        models=("deterministic",),
        limits=(("message_bytes", 8_000),),
        evidence=None,
    )

    with pytest.raises(RuntimeError, match=message):
        _validate_frozen_stage(SttStage.model_validate(selected), capability, "fixture")
