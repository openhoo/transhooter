"""Tuple-fenced internal HTTP clients.

Wire assumptions: archive-object/checkpoint accept JSON bodies defined by the canonical plan;
expired worker epochs returns a bare tuple array; complete/abandon return the bare boolean true
for first acceptance and exact replay. Non-2xx responses carry an optional stable ``code``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

import httpx
from transhooter_spool import ObjectRecord, SampleRange

_RETRYABLE_STATUSES = frozenset({401, 403, 408, 425, 429})


class ControlRequestError(RuntimeError):
    def __init__(self, operation: str, status: int | None, code: str) -> None:
        self.operation = operation
        self.status = status
        self.code = code
        suffix = f"HTTP {status}" if status is not None else "transport failure"
        super().__init__(f"internal {operation} rejected with {suffix} ({code})")


class RetryableControlRequestError(ControlRequestError):
    pass


class PermanentControlRequestError(ControlRequestError):
    pass


@dataclass(frozen=True, slots=True)
class WorkerTuple:
    consultation_id: UUID
    generation: int
    worker_id: UUID
    worker_epoch: int
    write_epoch: int

    def __post_init__(self) -> None:
        if self.generation < 0 or self.worker_epoch < 1 or self.write_epoch < 0:
            raise ValueError("worker tuple epochs are invalid")


@dataclass(frozen=True, slots=True)
class ExpiredWorkerEpoch:
    worker: WorkerTuple


@dataclass(frozen=True, slots=True)
class TerminalCheckpointIdentity:
    checkpoint_id: UUID
    checkpoint_hash: str


@dataclass(frozen=True, slots=True)
class CompleteWorkerEpoch:
    worker: WorkerTuple
    completion_event_id: UUID
    outcome: str
    terminal_checkpoints: tuple[TerminalCheckpointIdentity, TerminalCheckpointIdentity]
    failure: dict[str, object] | None


@dataclass(frozen=True, slots=True)
class AbandonWorkerEpoch:
    worker: WorkerTuple
    abandonment_event_id: UUID
    reason: str
    handoff_digest: str
    permanent_outcome_digest: str
    seal_id: UUID | None = None
    completion_event_id: UUID | None = None


class DrainerControlClient:
    def __init__(
        self,
        base_url: str,
        bearer_file: Path,
        client: httpx.Client | None = None,
        *,
        timeout_seconds: float = 10,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._bearer_file = bearer_file
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=timeout_seconds, follow_redirects=False)
        self._read_bearer()

    def __enter__(self) -> DrainerControlClient:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def archive_object(
        self,
        worker: WorkerTuple,
        *,
        causal_key: str,
        object_id: UUID,
        object_class: str,
        record: ObjectRecord,
        sample_range: SampleRange | None = None,
        attempt: int | None = None,
        sequence: int | None = None,
    ) -> None:
        self._request(
            "POST",
            "archive-object",
            {
                **self._tuple_body(worker),
                "epoch": worker.worker_epoch,
                "writerEpoch": worker.write_epoch,
                "causalKey": causal_key,
                "object": {
                    "objectId": str(object_id),
                    "class": object_class,
                    "key": record.key,
                    "versionId": record.version_id,
                    "size": record.size,
                    "sha256": record.sha256,
                    "s3Checksum": record.s3_checksum,
                    "contentType": record.content_type,
                    "sampleRange": (
                        None
                        if sample_range is None
                        else {"start": sample_range.start, "end": sample_range.end}
                    ),
                    "attempt": attempt,
                    "sequence": sequence,
                },
            },
        )

    def checkpoint(
        self,
        worker: WorkerTuple,
        *,
        object_key: str,
        checkpoint: dict[str, object],
        event_id: UUID,
    ) -> None:
        self._request(
            "POST",
            "checkpoint",
            {
                **self._tuple_body(worker),
                "writeEpoch": worker.write_epoch,
                "objectKey": object_key,
                "checkpoint": checkpoint,
                "eventId": str(event_id),
            },
        )

    def expired_worker_epochs(self) -> tuple[ExpiredWorkerEpoch, ...]:
        payload = self._request("GET", "worker-epochs/expired")
        if isinstance(payload, dict):
            payload = payload.get("workerEpochs", payload.get("expiredWorkerEpochs"))
        if not isinstance(payload, list):
            raise ValueError("expired worker epochs response must be an array")
        return tuple(ExpiredWorkerEpoch(self._parse_tuple(item)) for item in payload)

    def complete_worker_epoch(self, request: CompleteWorkerEpoch) -> None:
        if len(request.terminal_checkpoints) != 2:
            raise ValueError("completion requires exactly two terminal checkpoints")
        if request.outcome not in {"clean", "failed"}:
            raise ValueError("completion outcome must be clean or failed")
        if (request.outcome == "clean") != (request.failure is None):
            raise ValueError("completion failure must be null exactly for clean outcomes")
        response = self._request(
            "POST",
            "worker-epochs/complete",
            {
                **self._tuple_body(request.worker),
                "epoch": request.worker.worker_epoch,
                "writeEpoch": request.worker.write_epoch,
                "completionEventId": str(request.completion_event_id),
                "outcome": request.outcome,
                "terminalCheckpoints": [
                    {
                        "checkpointId": str(checkpoint.checkpoint_id),
                        "checkpointHash": checkpoint.checkpoint_hash,
                    }
                    for checkpoint in request.terminal_checkpoints
                ],
                "failure": request.failure,
            },
        )
        self._require_acceptance("worker-epochs/complete", response)

    def abandon_worker_epoch(self, request: AbandonWorkerEpoch) -> None:
        body: dict[str, object] = {
            **self._tuple_body(request.worker),
            "epoch": request.worker.worker_epoch,
            "writeEpoch": request.worker.write_epoch,
            "abandonmentEventId": str(request.abandonment_event_id),
            "reason": request.reason,
            "handoffDigest": request.handoff_digest,
            "permanentOutcomeDigest": request.permanent_outcome_digest,
        }
        if request.seal_id is not None:
            body["sealId"] = str(request.seal_id)
        if request.completion_event_id is not None:
            body["completionEventId"] = str(request.completion_event_id)
        response = self._request("POST", "worker-epochs/abandon", body)
        self._require_acceptance("worker-epochs/abandon", response)

    @staticmethod
    def _require_acceptance(operation: str, response: object) -> None:
        if response is not True:
            raise ValueError(f"internal {operation} returned a non-boolean acceptance")

    def _read_bearer(self) -> str:
        try:
            bearer = self._bearer_file.read_text("utf-8").strip()
        except OSError as error:
            raise RuntimeError("spool drainer internal bearer is unavailable") from error
        if not bearer:
            raise RuntimeError("spool drainer internal bearer is empty")
        return bearer

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> object:
        request = self._client.build_request(
            method,
            f"{self._base}/api/internal/{path}",
            headers={
                "Authorization": f"Bearer {self._read_bearer()}",
                "Content-Type": "application/json",
            },
            content=(
                None
                if payload is None
                else json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
            ),
        )
        try:
            response = self._client.send(request)
        except httpx.TransportError as error:
            raise RetryableControlRequestError(path, None, type(error).__name__) from error
        if response.status_code // 100 != 2:
            code = "unknown"
            try:
                body = response.json()
                if isinstance(body, dict) and isinstance(body.get("code"), str):
                    code = body["code"]
            except ValueError:
                pass
            retryable = response.status_code in _RETRYABLE_STATUSES or response.status_code >= 500
            error_type = RetryableControlRequestError if retryable else PermanentControlRequestError
            raise error_type(path, response.status_code, code)
        if not response.content:
            return None
        try:
            return response.json()
        except ValueError as error:
            raise ValueError(f"internal {path} returned malformed JSON") from error

    @staticmethod
    def _tuple_body(worker: WorkerTuple) -> dict[str, object]:
        return {
            "consultationId": str(worker.consultation_id),
            "generation": worker.generation,
            "workerId": str(worker.worker_id),
        }

    @staticmethod
    def _parse_tuple(value: object) -> WorkerTuple:
        if not isinstance(value, dict):
            raise ValueError("expired worker epoch entry must be an object")
        try:
            consultation_id = UUID(str(value["consultationId"]))
            worker_id = UUID(str(value["workerId"]))
            generation = value["generation"]
            epoch = value["epoch"]
            write_epoch = value["writeEpoch"]
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("expired worker epoch entry has an invalid identity") from error
        if any(not isinstance(item, int) or isinstance(item, bool) for item in (generation, epoch, write_epoch)):
            raise ValueError("expired worker epoch entry has invalid epochs")
        return WorkerTuple(consultation_id, generation, worker_id, epoch, write_epoch)
