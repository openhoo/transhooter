import asyncio
import hashlib
import json
import signal
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from jsonschema.exceptions import ValidationError

from transhooter_worker.adapters.spool import SpoolUnavailable
from transhooter_worker.domain.models import StageCapabilities
from transhooter_worker.runtime.consultation import SourceTrackTimeline
from transhooter_worker.runtime.job import (
    InitializationContext,
    QuotaGateLifecycle,
    _heartbeat_loop,
    _reported_preflight,
    _supervise_runtime,
    _validate_frozen_stage,
    consultation_entrypoint,
)
from transhooter_worker.runtime.job_metadata import (
    SttStage,
    _validated_job_metadata,
    _worker_job_metadata_validator,
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


@pytest.mark.asyncio
async def test_quota_gate_cleanup_releases_before_closing_each_unique_gate_once() -> None:
    events: list[str] = []

    class Gate:
        def __init__(self, name: str) -> None:
            self.name = name

        async def release_active(self, stage: str, reservation: str) -> None:
            events.append(f"release:{self.name}:{stage}:{reservation}")

        async def aclose(self) -> None:
            assert "release:active:stt:reservation" in events
            events.append(f"close:{self.name}")

    active_gate = Gate("active")
    window_only_gate = Gate("window-only")
    lifecycle = QuotaGateLifecycle()
    lifecycle.add_gate(active_gate)  # type: ignore[arg-type]
    lifecycle.add_gate(active_gate)  # type: ignore[arg-type]
    lifecycle.add_gate(window_only_gate)  # type: ignore[arg-type]
    lifecycle.add_lease(active_gate, "stt", "reservation")  # type: ignore[arg-type]

    await asyncio.gather(lifecycle.cleanup(), lifecycle.cleanup())

    assert events == [
        "release:active:stt:reservation",
        "close:active",
        "close:window-only",
    ]


@pytest.mark.asyncio
async def test_quota_gate_cleanup_finishes_before_propagating_cancellation() -> None:
    release_started = asyncio.Event()
    allow_release = asyncio.Event()
    events: list[str] = []

    class Gate:
        async def release_active(self, _stage: str, _reservation: str) -> None:
            release_started.set()
            await allow_release.wait()
            events.append("gate:release")

        async def aclose(self) -> None:
            events.append("gate:close")

    gate = Gate()
    lifecycle = QuotaGateLifecycle()
    lifecycle.add_gate(gate)  # type: ignore[arg-type]
    lifecycle.add_lease(gate, "stt", "reservation")  # type: ignore[arg-type]
    cleanup_task = asyncio.create_task(lifecycle.cleanup())
    await release_started.wait()
    cleanup_task.cancel()
    allow_release.set()

    with pytest.raises(asyncio.CancelledError):
        await cleanup_task

    assert events == ["gate:release", "gate:close"]


@pytest.mark.asyncio
async def test_quota_gate_cleanup_closes_all_gates_and_aggregates_errors() -> None:
    events: list[str] = []

    class FailingGate:
        def __init__(self, name: str) -> None:
            self.name = name

        async def release_active(self, _stage: str, _reservation: str) -> None:
            events.append(f"release:{self.name}")
            raise RuntimeError(f"release {self.name}")

        async def aclose(self) -> None:
            events.append(f"close:{self.name}")
            raise RuntimeError(f"close {self.name}")

    first = FailingGate("first")
    second = FailingGate("second")
    lifecycle = QuotaGateLifecycle()
    for gate in (first, second):
        lifecycle.add_gate(gate)  # type: ignore[arg-type]
        lifecycle.add_lease(gate, "stt", gate.name)  # type: ignore[arg-type]

    with pytest.raises(BaseExceptionGroup) as raised:
        await lifecycle.cleanup()

    assert events == [
        "release:first",
        "release:second",
        "close:first",
        "close:second",
    ]
    assert [str(error) for error in raised.value.exceptions] == [
        "release first",
        "release second",
        "close first",
        "close second",
    ]


@pytest.mark.asyncio
async def test_runtime_supervision_stops_gate_users_before_quota_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_contract_schema(monkeypatch)
    metadata = _validated_job_metadata(json.dumps(job_metadata_payload()))
    events: list[str] = []

    class Spool:
        @staticmethod
        def usage_ratio() -> float:
            return 0.1

    class Control:
        async def heartbeat(self, _health: dict[str, object]) -> bool:
            return True

        async def report_failure(self, _report: dict[str, object]) -> None:
            raise AssertionError("graceful shutdown must not report a failure")

    stream_task = asyncio.create_task(asyncio.Event().wait())
    stream_tasks = {stream_task}

    async def drain_runtime() -> None:
        stream_task.cancel()
        await asyncio.gather(stream_task, return_exceptions=True)
        events.append("tasks:stopped")

    class Gate:
        async def reserve_active(self, _stage: str, _reservation: str) -> None:
            return None

        async def release_active(self, _stage: str, _reservation: str) -> None:
            assert stream_task.done()
            events.append("gate:release")

        async def aclose(self) -> None:
            assert events == ["tasks:stopped", "gate:release"]
            events.append("gate:close")

    gate = Gate()
    lifecycle = QuotaGateLifecycle()
    lifecycle.add_gate(gate)  # type: ignore[arg-type]
    lifecycle.add_lease(gate, "stt", "reservation")  # type: ignore[arg-type]
    disconnected = asyncio.Event()
    disconnected.set()

    await _supervise_runtime(
        SimpleNamespace(room=SimpleNamespace()),  # type: ignore[arg-type]
        metadata,
        Spool(),  # type: ignore[arg-type]
        Control(),  # type: ignore[arg-type]
        (),
        {},
        {},
        lifecycle,
        stream_tasks,  # type: ignore[arg-type]
        drain_runtime,
        asyncio.get_running_loop().create_future(),
        disconnected,
        asyncio.Event(),
        {},
        {},
    )

    assert events == ["tasks:stopped", "gate:release", "gate:close"]


@pytest.mark.asyncio
async def test_initialization_cleanup_stops_sessions_before_releasing_and_closing_gates() -> None:
    events: list[str] = []

    class Session:
        async def cancel(self) -> None:
            events.append("session:cancel")

    class Gate:
        async def release_active(self, _stage: str, _reservation: str) -> None:
            assert events == ["session:cancel"]
            events.append("gate:release")

        async def aclose(self) -> None:
            assert events == ["session:cancel", "gate:release"]
            events.append("gate:close")

    class Control:
        async def report_failure(self, _report: dict[str, object]) -> None:
            events.append("control:report")

    gate = Gate()
    lifecycle = QuotaGateLifecycle()
    lifecycle.add_gate(gate)  # type: ignore[arg-type]
    lifecycle.add_lease(gate, "translation", "reservation")  # type: ignore[arg-type]
    initialization = InitializationContext(
        SimpleNamespace(room=SimpleNamespace()),  # type: ignore[arg-type]
        Control(),  # type: ignore[arg-type]
        {uuid4(): Session()},  # type: ignore[dict-item]
        {},
        lifecycle,
        {},
    )

    await initialization.cleanup(RuntimeError("initialization failed"))
    await initialization.cleanup(RuntimeError("duplicate cleanup"))

    assert events == [
        "session:cancel",
        "gate:release",
        "gate:close",
        "control:report",
    ]


@pytest.mark.asyncio
async def test_initialization_failure_reports_once_and_preserves_cleanup_failures() -> None:
    events: list[str] = []
    initialization_error = RuntimeError("initialization failed")

    class Session:
        async def cancel(self) -> None:
            events.append("session:cancel")
            raise RuntimeError("session cleanup failed")

    class ProviderSet:
        async def aclose(self) -> None:
            events.append("provider:close")
            raise RuntimeError("provider cleanup failed")

    class QuotaLifecycle:
        async def cleanup(self) -> None:
            events.append("quota:cleanup")
            raise RuntimeError("quota cleanup failed")

    class Control:
        async def report_failure(self, report: dict[str, object]) -> None:
            events.append("control:report")
            assert report["kind"] == "RuntimeError"
            assert report["message"] == "initialization failed"
            raise RuntimeError("report failed")

    initialization = InitializationContext(
        SimpleNamespace(room=SimpleNamespace()),  # type: ignore[arg-type]
        Control(),  # type: ignore[arg-type]
        {uuid4(): Session()},  # type: ignore[dict-item]
        {},
        QuotaLifecycle(),  # type: ignore[arg-type]
        {},
        [ProviderSet()],  # type: ignore[list-item]
    )

    async def fail_initialization() -> None:
        raise initialization_error

    with pytest.raises(BaseExceptionGroup) as raised:
        await initialization.await_effect(fail_initialization())

    assert raised.value.exceptions[0] is initialization_error
    cleanup_group = raised.value.exceptions[1]
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert [str(error) for error in cleanup_group.exceptions] == [
        "session cleanup failed",
        "provider cleanup failed",
        "quota cleanup failed",
        "report failed",
    ]
    assert events == [
        "session:cancel",
        "provider:close",
        "quota:cleanup",
        "control:report",
    ]

    repeated = await asyncio.gather(
        initialization.cleanup(RuntimeError("ignored")),
        initialization.cleanup(RuntimeError("also ignored")),
        return_exceptions=True,
    )
    assert all(isinstance(result, BaseExceptionGroup) for result in repeated)
    assert events.count("control:report") == 1


def test_metadata_validator_is_cached_per_resolved_schema_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    schema_path = Path(__file__).parents[3] / "packages/contracts/generated/contracts.schema.json"
    first_path = tmp_path / "first.json"
    second_path = tmp_path / "second.json"
    schema = schema_path.read_text("utf-8")
    first_path.write_text(schema, "utf-8")
    second_path.write_text(schema, "utf-8")
    _worker_job_metadata_validator.cache_clear()

    monkeypatch.setenv("CONTRACTS_SCHEMA_FILE", str(first_path))
    _validated_job_metadata(json.dumps(job_metadata_payload()))
    _validated_job_metadata(json.dumps(job_metadata_payload()))
    first_cache = _worker_job_metadata_validator.cache_info()

    monkeypatch.setenv("CONTRACTS_SCHEMA_FILE", str(second_path))
    _validated_job_metadata(json.dumps(job_metadata_payload()))
    second_cache = _worker_job_metadata_validator.cache_info()

    assert first_cache.hits == 1
    assert first_cache.misses == 1
    assert second_cache.misses == 2
    assert second_cache.currsize == 2


@pytest.mark.asyncio
async def test_initialization_cleanup_reports_after_unpublish_setup_failure() -> None:
    events: list[str] = []

    class Participant:
        def unpublish_track(self, _sid: str) -> object:
            events.append("track:unpublish")
            raise RuntimeError("unpublish setup failed")

    class Control:
        async def report_failure(self, _report: dict[str, object]) -> None:
            events.append("control:report")

    initialization = InitializationContext(
        SimpleNamespace(room=SimpleNamespace(local_participant=Participant())),  # type: ignore[arg-type]
        Control(),  # type: ignore[arg-type]
        {},
        {"track": (object(), SimpleNamespace(sid="publication"))},  # type: ignore[dict-item]
        QuotaGateLifecycle(),
        {},
    )

    with pytest.raises(RuntimeError, match="unpublish setup failed"):
        await initialization.cleanup(RuntimeError("initialization failed"))

    assert events == ["track:unpublish", "control:report"]


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


@pytest.mark.asyncio
async def test_nested_spool_failure_requests_graceful_supervisor_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    killed: list[tuple[int, int]] = []

    async def fail_consultation(_ctx: object) -> None:
        raise BaseExceptionGroup(
            "translation runtime drain failed",
            [RuntimeError("provider cleanup failed"), SpoolUnavailable("spool unavailable")],
        )

    monkeypatch.setenv("TRANSHOOTER_WORKER_SUPERVISOR_PID", "1234")
    monkeypatch.setattr("transhooter_worker.runtime.job._run_consultation", fail_consultation)
    monkeypatch.setattr("os.kill", lambda pid, sig: killed.append((pid, sig)))

    with pytest.raises(BaseExceptionGroup, match="translation runtime drain failed"):
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
