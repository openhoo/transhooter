from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pytest

from transhooter_spool_drainer.clients import WorkerTuple
from transhooter_spool_drainer.scenario import ScenarioCrashHooks


def scenario_file(tmp_path: Path, meeting_id: object, point: str) -> Path:
    path = tmp_path / "spool-drainer-scenarios.json"
    path.write_text(
        json.dumps(
            {"consultations": {str(meeting_id): {"crashPoint": point}}},
            separators=(",", ":"),
            sort_keys=True,
        ),
        "utf-8",
    )
    return path


def test_matching_crash_point_is_consumed_before_process_exit(tmp_path: Path) -> None:
    meeting_id = uuid4()
    path = scenario_file(tmp_path, meeting_id, "archive-registration")
    exits: list[int] = []
    hooks = ScenarioCrashHooks(path, exit_process=lambda code: exits.append(code))

    with pytest.raises(RuntimeError, match="returned unexpectedly"):
        hooks.trigger(meeting_id, "archive-registration")

    assert exits == [86]
    assert json.loads(path.read_text("utf-8")) == {"consultations": {}}


def test_nonmatching_or_other_consultation_does_not_consume_hook(tmp_path: Path) -> None:
    meeting_id = uuid4()
    path = scenario_file(tmp_path, meeting_id, "s3-put")
    hooks = ScenarioCrashHooks(path, exit_process=lambda _code: pytest.fail("unexpected exit"))

    hooks.trigger(meeting_id, "archive-registration")
    hooks.trigger(uuid4(), "s3-put")

    assert json.loads(path.read_text("utf-8"))["consultations"][str(meeting_id)] == {
        "crashPoint": "s3-put"
    }



def test_historical_write_epoch_fences_only_matching_checkpoint_worker(tmp_path: Path) -> None:
    meeting_id = uuid4()
    worker = WorkerTuple(meeting_id, 3, uuid4(), 4, 0)
    path = tmp_path / "spool-drainer-scenarios.json"
    path.write_text(
        json.dumps(
            {"consultations": {str(meeting_id): {"historicalFence": "write-epoch"}}},
            separators=(",", ":"),
            sort_keys=True,
        ),
        "utf-8",
    )
    hooks = ScenarioCrashHooks(path, exit_process=lambda _code: pytest.fail("unexpected exit"))

    hooks.trigger(meeting_id, "s3-put")

    assert hooks.fence_checkpoint_worker(worker) == WorkerTuple(
        meeting_id,
        worker.generation,
        worker.worker_id,
        worker.worker_epoch,
        1,
    )
    assert json.loads(path.read_text("utf-8"))["consultations"] == {}
    assert hooks.fence_checkpoint_worker(worker) == worker
    other = WorkerTuple(uuid4(), 3, worker.worker_id, 4, 0)
    assert hooks.fence_checkpoint_worker(other) == other

def test_environment_scenarios_are_test_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = scenario_file(tmp_path, uuid4(), "s3-put")
    monkeypatch.setenv("SPOOL_DRAINER_SCENARIO_FILE", str(path))
    monkeypatch.setenv("APP_ENV", "production")

    with pytest.raises(RuntimeError, match="require APP_ENV=test"):
        ScenarioCrashHooks.from_environment()


def test_invalid_shape_or_point_fails_closed(tmp_path: Path) -> None:
    meeting_id = uuid4()
    path = scenario_file(tmp_path, meeting_id, "invalid")
    hooks = ScenarioCrashHooks(path, exit_process=lambda _code: pytest.fail("unexpected exit"))

    with pytest.raises(RuntimeError, match="crashPoint is invalid"):
        hooks.trigger(meeting_id, "s3-put")
