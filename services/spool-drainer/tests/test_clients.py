from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from transhooter_spool import ObjectRecord

from transhooter_spool_drainer.clients import (
    AbandonWorkerEpoch,
    CompleteWorkerEpoch,
    DrainerControlClient,
    PermanentControlRequestError,
    RetryableControlRequestError,
    TerminalCheckpointIdentity,
    WorkerTuple,
)


def client_for(tmp_path: Path, handler: httpx.MockTransport) -> DrainerControlClient:
    bearer = tmp_path / "bearer"
    bearer.write_text("token-one", "utf-8")
    return DrainerControlClient("http://web:3000", bearer, httpx.Client(transport=handler))


@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (401, RetryableControlRequestError),
        (403, RetryableControlRequestError),
        (408, RetryableControlRequestError),
        (425, RetryableControlRequestError),
        (429, RetryableControlRequestError),
        (503, RetryableControlRequestError),
        (400, PermanentControlRequestError),
        (409, PermanentControlRequestError),
        (422, PermanentControlRequestError),
    ],
)
def test_http_classification(tmp_path: Path, status: int, error_type: type[RuntimeError]) -> None:
    client = client_for(
        tmp_path,
        httpx.MockTransport(lambda _request: httpx.Response(status, json={"code": "FENCED"})),
    )
    with pytest.raises(error_type) as caught:
        client.expired_worker_epochs()
    assert type(caught.value) is error_type
    assert caught.value.code == "FENCED"


def test_bearer_reloads_and_expired_tuples_are_exact(tmp_path: Path) -> None:
    bearer = tmp_path / "bearer"
    bearer.write_text("first", "utf-8")
    authorizations: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        authorizations.append(request.headers["Authorization"])
        return httpx.Response(
            200,
            json=[
                {
                    "consultationId": str(UUID(int=1)),
                    "generation": 4,
                    "workerId": str(UUID(int=2)),
                    "epoch": 7,
                    "writeEpoch": 9,
                }
            ],
        )

    client = DrainerControlClient(
        "http://web:3000", bearer, httpx.Client(transport=httpx.MockTransport(respond))
    )
    first = client.expired_worker_epochs()
    bearer.write_text("second", "utf-8")
    second = client.expired_worker_epochs()
    assert first == second
    assert first[0].worker == WorkerTuple(UUID(int=1), 4, UUID(int=2), 7, 9)
    assert authorizations == ["Bearer first", "Bearer second"]


def test_archive_checkpoint_completion_and_abandonment_wire_shapes(tmp_path: Path) -> None:
    requests: list[tuple[str, dict[str, object]]] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append((request.url.path, json.loads(request.content)))
        return httpx.Response(200, json=True)

    client = client_for(tmp_path, httpx.MockTransport(respond))
    worker = WorkerTuple(UUID(int=1), 3, UUID(int=2), 5, 8)
    record = ObjectRecord("ignored", "v1/meetings/00000000-0000-0000-0000-000000000001/pipeline/x/raw/y/0.json", "v1", 2, "a" * 64, "crc", "application/json")
    client.archive_object(
        worker,
        causal_key="causal",
        object_id=UUID(int=3),
        object_class="pipeline_exchange",
        record=record,
    )
    checkpoint = {
        "checkpointId": str(UUID(int=4)),
        "workerEpoch": 5,
        "sourceParticipantId": str(UUID(int=6)),
        "destinationParticipantId": str(UUID(int=7)),
        "acceptedInputSequence": 0,
        "acceptedInput": 0,
        "receivedOutput": 0,
        "emittedOutput": 0,
        "previousCheckpointSha256": None,
        "highWatermarkSha256": "b" * 64,
        "expectedObjectIds": [],
        "observedObjectIds": [],
        "gaps": [],
        "terminal": True,
        "occurredAtMs": 1,
    }
    client.checkpoint(worker, object_key="checkpoint-key", checkpoint=checkpoint, event_id=UUID(int=8))
    pair = (
        TerminalCheckpointIdentity(UUID(int=4), "b" * 64),
        TerminalCheckpointIdentity(UUID(int=5), "c" * 64),
    )
    client.complete_worker_epoch(
        CompleteWorkerEpoch(worker, UUID(int=9), "clean", pair, None)
    )
    client.abandon_worker_epoch(
        AbandonWorkerEpoch(worker, UUID(int=10), "missing seal", "d" * 64, "e" * 64)
    )

    assert [path for path, _ in requests] == [
        "/api/internal/archive-object",
        "/api/internal/checkpoint",
        "/api/internal/worker-epochs/complete",
        "/api/internal/worker-epochs/abandon",
    ]
    archive = requests[0][1]
    assert archive | {} == archive
    assert archive["generation"] == 3
    assert archive["workerId"] == str(UUID(int=2))
    assert archive["epoch"] == 5
    assert archive["writerEpoch"] == 8
    object_payload = archive["object"]
    assert isinstance(object_payload, dict)
    assert object_payload["attempt"] is None
    assert object_payload["sequence"] is None
    complete = requests[2][1]
    assert complete["terminalCheckpoints"] == [
        {"checkpointId": str(UUID(int=4)), "checkpointHash": "b" * 64},
        {"checkpointId": str(UUID(int=5)), "checkpointHash": "c" * 64},
    ]
    abandon = requests[3][1]
    assert abandon["handoffDigest"] == "d" * 64
    assert abandon["permanentOutcomeDigest"] == "e" * 64
    assert "sealId" not in abandon
    assert "completionEventId" not in abandon


def test_recovery_requires_exact_boolean_acceptance(tmp_path: Path) -> None:
    client = client_for(
        tmp_path,
        httpx.MockTransport(lambda _request: httpx.Response(200, json={"accepted": True})),
    )
    worker = WorkerTuple(UUID(int=1), 3, UUID(int=2), 5, 8)
    pair = (
        TerminalCheckpointIdentity(UUID(int=4), "b" * 64),
        TerminalCheckpointIdentity(UUID(int=5), "c" * 64),
    )
    with pytest.raises(ValueError, match="non-boolean acceptance"):
        client.complete_worker_epoch(CompleteWorkerEpoch(worker, UUID(int=9), "clean", pair, None))


def test_completion_rejects_invalid_outcome_failure_pair(tmp_path: Path) -> None:
    client = client_for(
        tmp_path,
        httpx.MockTransport(lambda _request: httpx.Response(200, json=True)),
    )
    worker = WorkerTuple(UUID(int=1), 3, UUID(int=2), 5, 8)
    pair = (
        TerminalCheckpointIdentity(UUID(int=4), "b" * 64),
        TerminalCheckpointIdentity(UUID(int=5), "c" * 64),
    )
    with pytest.raises(ValueError, match="clean or failed"):
        client.complete_worker_epoch(CompleteWorkerEpoch(worker, UUID(int=9), "succeeded", pair, None))
    with pytest.raises(ValueError, match="failure must be null"):
        client.complete_worker_epoch(CompleteWorkerEpoch(worker, UUID(int=9), "failed", pair, None))
