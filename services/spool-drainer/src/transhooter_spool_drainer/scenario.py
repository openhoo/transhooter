from __future__ import annotations

import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import Final, Literal, cast
from uuid import UUID, uuid4

from .clients import WorkerTuple

CrashPoint = Literal[
    "s3-put",
    "archive-registration",
    "checkpoint-acceptance",
    "completion-acceptance",
]

_CRASH_POINTS: Final = frozenset(
    {
        "s3-put",
        "archive-registration",
        "checkpoint-acceptance",
        "completion-acceptance",
    }
)
_EXIT_CODE = 86


class ScenarioCrashHooks:
    def __init__(
        self,
        path: Path | None,
        *,
        exit_process: Callable[[int], object] = os._exit,
    ) -> None:
        self._path = path
        self._exit_process = exit_process

    @classmethod
    def from_environment(cls) -> ScenarioCrashHooks:
        value = os.environ.get("SPOOL_DRAINER_SCENARIO_FILE", "").strip()
        if not value:
            return cls(None)
        if os.environ.get("APP_ENV", "").strip() != "test":
            raise RuntimeError("spool drainer scenarios require APP_ENV=test")
        return cls(Path(value))

    def trigger(self, meeting_id: UUID, point: CrashPoint) -> None:
        if point not in _CRASH_POINTS:
            raise ValueError("unknown spool drainer crash point")
        scenario = self._scenario(meeting_id)
        if scenario is None or "crashPoint" not in scenario:
            return
        if scenario["crashPoint"] != point:
            return

        document = self._read_document()
        consultations = document["consultations"]
        if consultations.get(str(meeting_id)) != scenario:
            return
        del consultations[str(meeting_id)]
        self._write_document(document)
        self._exit_process(_EXIT_CODE)
        raise RuntimeError("spool drainer crash hook returned unexpectedly")

    def fence_checkpoint_worker(self, worker: WorkerTuple) -> WorkerTuple:
        scenario = self._scenario(worker.consultation_id)
        if scenario is None or "historicalFence" not in scenario:
            return worker
        document = self._read_document()
        consultations = document["consultations"]
        if consultations.get(str(worker.consultation_id)) != scenario:
            return worker
        del consultations[str(worker.consultation_id)]
        self._write_document(document)
        return WorkerTuple(
            consultation_id=worker.consultation_id,
            generation=worker.generation,
            worker_id=worker.worker_id,
            worker_epoch=worker.worker_epoch,
            write_epoch=worker.write_epoch + 1,
        )

    def _scenario(self, meeting_id: UUID) -> dict[str, str] | None:
        if self._path is None:
            return None
        scenario = self._read_document()["consultations"].get(str(meeting_id))
        if scenario is None:
            return None
        if not isinstance(scenario, dict):
            raise RuntimeError("spool drainer scenario entry is malformed")
        if set(scenario) == {"crashPoint"}:
            configured = scenario["crashPoint"]
            if not isinstance(configured, str) or configured not in _CRASH_POINTS:
                raise RuntimeError("spool drainer crashPoint is invalid")
        elif scenario != {"historicalFence": "write-epoch"}:
            raise RuntimeError("spool drainer scenario entry is malformed")
        return scenario

    def _read_document(self) -> dict[str, dict[str, dict[str, str]]]:
        assert self._path is not None
        try:
            value = json.loads(self._path.read_text("utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError("spool drainer scenario file is unavailable or malformed") from error
        if not isinstance(value, dict) or set(value) != {"consultations"}:
            raise RuntimeError("spool drainer scenario file has an invalid shape")
        consultations = value["consultations"]
        if not isinstance(consultations, dict) or any(
            not isinstance(key, str) for key in consultations
        ):
            raise RuntimeError("spool drainer scenario consultation map is invalid")
        return cast(dict[str, dict[str, dict[str, str]]], value)

    def _write_document(self, value: dict[str, dict[str, dict[str, str]]]) -> None:
        assert self._path is not None
        body = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
        temporary = self._path.with_name(f".{self._path.name}.{uuid4()}.tmp")
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as output:
                output.write(body)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, self._path)
            directory = os.open(self._path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            temporary.unlink(missing_ok=True)
