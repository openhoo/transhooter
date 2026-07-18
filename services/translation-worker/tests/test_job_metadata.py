import hashlib
import json
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from jsonschema.exceptions import ValidationError

from transhooter_worker.runtime.job import (
    SourceTrackTimeline,
    _reported_preflight,
    _validated_job_metadata,
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
