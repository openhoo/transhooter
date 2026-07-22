import asyncio
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

from transhooter_worker.adapters.spool import EncryptedSpool, deterministic_roomy_capacity
from transhooter_worker.domain.models import SampleRange
from transhooter_worker.ports.archive import ObjectRecord
from transhooter_worker.runtime.control_client import (
    ControlClient,
    PermanentControlRequestError,
    RetryableControlRequestError,
)
from transhooter_worker.runtime.job import (
    CheckpointChainState,
    _persist_checkpoint,
    _replay_pending_checkpoints,
    _restore_checkpoint_state,
)


class RecordingSpool:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def append(self, **record: Any) -> None:
        self.records.append(record)


def make_client(
    bearer_file: Path,
    transport: httpx.AsyncBaseTransport,
    spool: RecordingSpool,
) -> ControlClient:
    return ControlClient(
        "http://control.test",
        bearer_file,
        UUID(int=1),
        1,
        UUID(int=2),
        1,
        spool,  # type: ignore[arg-type]
        httpx.AsyncClient(transport=transport),
        retry_delays=(0, 0),
    )


@pytest.mark.asyncio
async def test_control_client_closes_internally_created_http_client(tmp_path: Path) -> None:
    bearer_file = tmp_path / "bearer"
    bearer_file.write_text("secret", "utf-8")
    client = ControlClient(
        "http://control.test",
        bearer_file,
        UUID(int=1),
        1,
        UUID(int=2),
        1,
        RecordingSpool(),  # type: ignore[arg-type]
    )
    owned_client = client._client

    with pytest.raises(RuntimeError, match="preflight failed"):
        async with client:
            assert not owned_client.is_closed
            raise RuntimeError("preflight failed")

    assert owned_client.is_closed


@pytest.mark.asyncio
async def test_control_client_preserves_injected_http_client_ownership(tmp_path: Path) -> None:
    bearer_file = tmp_path / "bearer"
    bearer_file.write_text("secret", "utf-8")
    injected_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(204))
    )
    client = ControlClient(
        "http://control.test",
        bearer_file,
        UUID(int=1),
        1,
        UUID(int=2),
        1,
        RecordingSpool(),  # type: ignore[arg-type]
        injected_client,
    )

    with pytest.raises(RuntimeError, match="request failed"):
        async with client:
            raise RuntimeError("request failed")

    assert not injected_client.is_closed
    await injected_client.aclose()


@pytest.mark.asyncio
async def test_provider_attempt_posts_shared_envelope_and_body(tmp_path: Path) -> None:
    bearer_file = tmp_path / "bearer"
    bearer_file.write_text("secret", "utf-8")
    bodies: list[dict[str, Any]] = []
    paths: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        bodies.append(json.loads(request.content))
        return httpx.Response(204, request=request)

    event_id = uuid4()
    spool = RecordingSpool()
    client = make_client(bearer_file, httpx.MockTransport(handler), spool)
    await client.provider_attempt(
        {"terminalId": str(event_id), "stage": "stt"},
        event_id=event_id,
    )

    assert paths == ["/api/internal/provider-attempt"]
    assert bodies == [
        {
            "consultationId": str(UUID(int=1)),
            "epoch": 1,
            "eventId": str(event_id),
            "generation": 1,
            "report": {
                "stage": "stt",
                "terminalId": str(event_id),
            },
            "workerId": str(UUID(int=2)),
        }
    ]
    assert spool.records[0]["stage"] == "control-provider-attempt"


@pytest.mark.asyncio
async def test_archive_object_registration_does_not_recursively_journal_itself(
    tmp_path: Path,
) -> None:
    bearer_file = tmp_path / "bearer"
    bearer_file.write_text("secret", "utf-8")
    paths: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(204, request=request)

    spool = RecordingSpool()
    client = make_client(bearer_file, httpx.MockTransport(handler), spool)
    await client.record_object({"objectId": str(uuid4())})

    assert paths == ["/api/internal/archive-object"]
    assert spool.records == []


@pytest.mark.asyncio
async def test_control_client_reloads_projected_bearer_for_each_request(tmp_path: Path) -> None:
    bearer_file = tmp_path / "bearer"
    bearer_file.write_text("first", "utf-8")
    authorizations: list[str | None] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        authorizations.append(request.headers.get("authorization"))
        return httpx.Response(200, json=True, request=request)

    client = make_client(
        bearer_file,
        httpx.MockTransport(handler),
        RecordingSpool(),
    )
    await client.heartbeat({})
    bearer_file.write_text("second", "utf-8")
    await client.heartbeat({})

    assert authorizations == ["Bearer first", "Bearer second"]


@pytest.mark.asyncio
async def test_heartbeat_returns_false_response_without_retrying(tmp_path: Path) -> None:
    bearer_file = tmp_path / "bearer"
    bearer_file.write_text("secret", "utf-8")
    requests = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(200, json=False, request=request)

    client = make_client(
        bearer_file,
        httpx.MockTransport(handler),
        RecordingSpool(),
    )

    assert await client.heartbeat({}) is False
    assert requests == 1


@pytest.mark.asyncio
async def test_control_post_retries_transport_failure_with_stable_event_id(tmp_path: Path) -> None:
    bearer_file = tmp_path / "bearer"
    bearer_file.write_text("secret", "utf-8")
    bodies: list[dict[str, Any]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        if len(bodies) == 1:
            raise httpx.ReadError("connection closed before response", request=request)
        return httpx.Response(204, request=request)

    spool = RecordingSpool()
    client = make_client(bearer_file, httpx.MockTransport(handler), spool)
    await client.checkpoint({"terminal": True})

    assert len(bodies) == 2
    assert bodies[0] == bodies[1]
    assert bodies[0]["eventId"]
    assert [record["direction"] for record in spool.records] == ["out", "out", "in"]


@pytest.mark.asyncio
async def test_control_post_retries_server_failure_with_stable_event_id(tmp_path: Path) -> None:
    bearer_file = tmp_path / "bearer"
    bearer_file.write_text("secret", "utf-8")
    bodies: list[dict[str, Any]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        status = 503 if len(bodies) == 1 else 200
        return httpx.Response(status, request=request)

    spool = RecordingSpool()
    client = make_client(bearer_file, httpx.MockTransport(handler), spool)
    await client.checkpoint({"terminal": True})

    assert len(bodies) == 2
    assert bodies[0] == bodies[1]
    assert bodies[0]["eventId"]
    assert [record["direction"] for record in spool.records] == ["out", "in", "out", "in"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "error_type", "expected_requests"),
    [
        (409, PermanentControlRequestError, 1),
        (429, RetryableControlRequestError, 3),
        (503, RetryableControlRequestError, 3),
    ],
)
async def test_control_post_exposes_permanent_and_retryable_rejections(
    tmp_path: Path,
    status: int,
    error_type: type[RuntimeError],
    expected_requests: int,
) -> None:
    bearer_file = tmp_path / "bearer"
    bearer_file.write_text("secret", "utf-8")
    requests = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(status, request=request)

    client = make_client(
        bearer_file,
        httpx.MockTransport(handler),
        RecordingSpool(),
    )

    with pytest.raises(error_type, match=f"HTTP {status}") as caught:
        await client.record_object({"objectId": str(uuid4())})

    assert type(caught.value) is error_type
    assert requests == expected_requests


@pytest.mark.asyncio
async def test_control_post_survives_extended_transient_server_failure(tmp_path: Path) -> None:
    bearer_file = tmp_path / "bearer"
    bearer_file.write_text("secret", "utf-8")
    requests = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(503 if requests < 6 else 204, request=request)

    client = ControlClient(
        "http://control.test",
        bearer_file,
        UUID(int=1),
        1,
        UUID(int=2),
        1,
        RecordingSpool(),  # type: ignore[arg-type]
        httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        retry_delays=(0, 0, 0, 0, 0),
    )
    await client.checkpoint({"terminal": True})

    assert requests == 6


@pytest.mark.asyncio
async def test_control_post_does_not_retry_client_rejection(tmp_path: Path) -> None:
    bearer_file = tmp_path / "bearer"
    bearer_file.write_text("secret", "utf-8")
    bodies: list[bytes] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content)
        return httpx.Response(409, request=request)

    spool = RecordingSpool()
    client = make_client(bearer_file, httpx.MockTransport(handler), spool)

    with pytest.raises(RuntimeError, match="HTTP 409"):
        await client.checkpoint({"terminal": True})

    assert len(bodies) == 1
    assert [record["direction"] for record in spool.records] == ["out", "in"]


@pytest.mark.asyncio
async def test_control_post_cancellation_is_not_retried(tmp_path: Path) -> None:
    bearer_file = tmp_path / "bearer"
    bearer_file.write_text("secret", "utf-8")
    entered = asyncio.Event()
    requests = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    client = make_client(
        bearer_file,
        httpx.MockTransport(handler),
        RecordingSpool(),
    )
    posting = asyncio.create_task(client.checkpoint({"terminal": True}))
    await entered.wait()
    posting.cancel()

    with pytest.raises(asyncio.CancelledError):
        await posting

    assert requests == 1


@pytest.mark.asyncio
async def test_caller_owned_event_id_survives_exhausted_ack_retries(
    tmp_path: Path,
) -> None:
    bearer_file = tmp_path / "bearer"
    bearer_file.write_text("secret", "utf-8")
    bodies: list[dict[str, Any]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        if len(bodies) <= 3:
            raise httpx.ReadError("ack lost", request=request)
        return httpx.Response(204, request=request)

    event_id = uuid4()
    client = make_client(
        bearer_file,
        httpx.MockTransport(handler),
        RecordingSpool(),
    )
    with pytest.raises(httpx.ReadError):
        await client.checkpoint(
            {
                "terminal": True,
                "consultationId": "payload-must-not-win",
                "eventId": "payload-must-not-win",
            },
            event_id=event_id,
        )
    await client.checkpoint(
        {
            "terminal": True,
            "consultationId": "payload-must-not-win",
            "eventId": "payload-must-not-win",
        },
        event_id=event_id,
    )

    assert len(bodies) == 4
    assert all(body == bodies[0] for body in bodies)
    assert bodies[0]["eventId"] == str(event_id)
    assert bodies[0]["consultationId"] == str(UUID(int=1))


class RecordingArchive:
    def __init__(self) -> None:
        self.bodies: list[bytes] = []

    def put_create_once(
        self,
        key: str,
        body: bytes,
        content_type: str,
        sha256: str,
    ) -> ObjectRecord:
        self.bodies.append(body)
        return ObjectRecord(
            object_id=key,
            key=key,
            version_id="version",
            size=len(body),
            sha256=sha256,
            s3_checksum="checksum",
            content_type=content_type,
        )


class BlockingCheckpointControl:
    def __init__(self) -> None:
        self.first_checkpoint_entered = asyncio.Event()
        self.release_first_checkpoint = asyncio.Event()
        self.checkpoints: list[dict[str, Any]] = []

    async def record_object(self, _: dict[str, Any], *, event_id: UUID | None = None) -> None:
        assert event_id is not None

    async def checkpoint(self, payload: dict[str, Any], *, event_id: UUID | None = None) -> None:
        assert event_id is not None
        self.checkpoints.append(payload)
        if len(self.checkpoints) == 1:
            self.first_checkpoint_entered.set()
            await self.release_first_checkpoint.wait()


def checkpoint_metadata(meeting_id: UUID) -> Any:
    return type(
        "Metadata",
        (),
        {"consultation_id": meeting_id, "worker_epoch": 3, "write_epoch": 4},
    )()


def encrypted_spool(root: Path, database: Path) -> EncryptedSpool:
    return EncryptedSpool(
        root,
        database,
        {"v1": b"k" * 32},
        "v1",
        capacity_probe=deterministic_roomy_capacity,
    )


@pytest.mark.asyncio
async def test_concurrent_checkpoints_for_one_source_form_one_serial_chain(
    tmp_path: Path,
) -> None:
    meeting_id = UUID(int=30)
    source_id = UUID(int=31)
    destination_id = UUID(int=32)
    spool = encrypted_spool(tmp_path / "payloads", tmp_path / "journal.sqlite3")
    archive = RecordingArchive()
    control = BlockingCheckpointControl()
    state = CheckpointChainState.empty()
    metadata = checkpoint_metadata(meeting_id)

    first = asyncio.create_task(
        _persist_checkpoint(
            metadata,
            spool,
            archive,  # type: ignore[arg-type]
            control,  # type: ignore[arg-type]
            state,
            source_id,
            destination_id,
            4_000,
            1_920,
            False,
            input_sequence=1,
            provider_output_sample=2_400,
        )
    )
    await control.first_checkpoint_entered.wait()

    second = asyncio.create_task(
        _persist_checkpoint(
            metadata,
            spool,
            archive,  # type: ignore[arg-type]
            control,  # type: ignore[arg-type]
            state,
            source_id,
            destination_id,
            8_000,
            3_840,
            False,
            input_sequence=2,
            provider_output_sample=4_800,
        )
    )
    second_had_turn = asyncio.Event()
    asyncio.get_running_loop().call_soon(second_had_turn.set)
    await second_had_turn.wait()

    assert len(control.checkpoints) == 1
    assert len(spool.list_checkpoint_deliveries(meeting_id, 3)) == 1

    control.release_first_checkpoint.set()
    await asyncio.gather(first, second)

    deliveries = spool.list_checkpoint_deliveries(meeting_id, 3)
    assert len(deliveries) == 2
    assert deliveries[0].previous_hash is None
    assert deliveries[1].previous_hash == deliveries[0].checkpoint_hash
    assert all(delivery.acknowledged for delivery in deliveries)
    assert state.watermarks[source_id] == (2, 8_000, 4_800, 3_840, False)


@pytest.mark.asyncio
async def test_restart_replays_exact_checkpoint_before_advancing_chain(
    tmp_path: Path,
) -> None:
    meeting_id = UUID(int=10)
    source_id = UUID(int=11)
    destination_id = UUID(int=12)
    worker_id = UUID(int=13)
    bearer_file = tmp_path / "bearer"
    bearer_file.write_text("secret", "utf-8")
    database = tmp_path / "journal.sqlite3"
    root = tmp_path / "payloads"
    first_spool = encrypted_spool(root, database)
    archive = RecordingArchive()
    first_spool.append(
        meeting_id=meeting_id,
        attempt_id=source_id,
        stage="stt-input",
        transport="grpc",
        direction=str(source_id),
        media_type="audio/L16",
        payload=b"\x00\x00" * 4_000,
        sample_range=SampleRange(24_000, 28_000),
    )
    first_spool.append(
        meeting_id=meeting_id,
        attempt_id=destination_id,
        stage="tts-output",
        transport="grpc",
        direction=str(destination_id),
        media_type="audio/L16",
        payload=b"\x00\x00" * 24_000,
        sample_range=SampleRange(0, 24_000),
    )
    first_spool.append(
        meeting_id=meeting_id,
        attempt_id=destination_id,
        stage="livekit-output",
        transport="webrtc",
        direction=str(destination_id),
        media_type="audio/L16",
        payload=b"\x00\x00" * 19_200,
        sample_range=SampleRange(0, 19_200),
    )

    committed_checkpoint_requests: list[bytes] = []

    async def lose_checkpoint_acks(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/checkpoint"):
            committed_checkpoint_requests.append(request.content)
            raise httpx.ReadError("ack lost after server commit", request=request)
        return httpx.Response(204, request=request)

    first_client = ControlClient(
        "http://control.test",
        bearer_file,
        meeting_id,
        1,
        worker_id,
        3,
        first_spool,
        httpx.AsyncClient(transport=httpx.MockTransport(lose_checkpoint_acks)),
        retry_delays=(0, 0),
    )
    first_state = CheckpointChainState.empty()
    with pytest.raises(httpx.ReadError, match="ack lost"):
        await _persist_checkpoint(
            checkpoint_metadata(meeting_id),
            first_spool,
            archive,  # type: ignore[arg-type]
            first_client,
            first_state,
            source_id,
            destination_id,
            28_000,
            19_200,
            False,
            input_sequence=7,
            provider_output_sample=24_000,
        )

    pending_delivery = first_spool.list_checkpoint_deliveries(meeting_id, 3)[0]
    assert not pending_delivery.acknowledged
    assert len(committed_checkpoint_requests) == 3
    assert all(body == committed_checkpoint_requests[0] for body in committed_checkpoint_requests)
    first_request = json.loads(committed_checkpoint_requests[0])
    assert first_request["eventId"] == str(pending_delivery.control_event_id)
    assert first_request["checkpoint"] == json.loads(pending_delivery.body)
    assert first_request["checkpoint"]["acceptedInputSequence"] == 7
    assert first_request["checkpoint"]["receivedOutput"] == 24_000

    replayed_checkpoint_requests: list[bytes] = []

    async def acknowledge_replay(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/checkpoint"):
            replayed_checkpoint_requests.append(request.content)
        return httpx.Response(204, request=request)

    restarted_spool = encrypted_spool(root, database)
    restarted_client = ControlClient(
        "http://control.test",
        bearer_file,
        meeting_id,
        1,
        worker_id,
        3,
        restarted_spool,
        httpx.AsyncClient(transport=httpx.MockTransport(acknowledge_replay)),
    )
    restarted_state = _restore_checkpoint_state(restarted_spool, meeting_id, 3)
    assert restarted_state.hashes == {}
    assert restarted_state.pending[source_id].body == pending_delivery.body
    await _replay_pending_checkpoints(
        checkpoint_metadata(meeting_id),
        restarted_spool,
        archive,  # type: ignore[arg-type]
        restarted_client,
        restarted_state,
    )

    assert replayed_checkpoint_requests == [committed_checkpoint_requests[0]]
    assert archive.bodies == [pending_delivery.body, pending_delivery.body]
    acknowledged = restarted_spool.list_checkpoint_deliveries(meeting_id, 3)[0]
    assert acknowledged.acknowledged
    assert restarted_state.hashes[source_id] == acknowledged.checkpoint_hash
    assert restarted_state.watermarks[source_id] == (7, 28_000, 24_000, 19_200, False)
    await _persist_checkpoint(
        checkpoint_metadata(meeting_id),
        restarted_spool,
        archive,  # type: ignore[arg-type]
        restarted_client,
        restarted_state,
        source_id,
        destination_id,
        28_000,
        19_200,
        False,
        input_sequence=7,
        provider_output_sample=24_000,
    )
    assert len(restarted_spool.list_checkpoint_deliveries(meeting_id, 3)) == 1
    assert replayed_checkpoint_requests == [committed_checkpoint_requests[0]]

    with pytest.raises(RuntimeError, match="cannot regress"):
        await _persist_checkpoint(
            checkpoint_metadata(meeting_id),
            restarted_spool,
            archive,  # type: ignore[arg-type]
            restarted_client,
            restarted_state,
            source_id,
            destination_id,
            27_999,
            19_200,
            False,
            input_sequence=6,
            provider_output_sample=24_000,
        )
    first_checkpoint = json.loads(acknowledged.body)
    assert first_checkpoint["sourceParticipantId"] == str(source_id)
    assert first_checkpoint["destinationParticipantId"] == str(destination_id)
    assert first_checkpoint["acceptedInputSequence"] == 7
    assert first_checkpoint["acceptedInput"] == 28_000
    assert first_checkpoint["receivedOutput"] == 24_000
    assert first_checkpoint["emittedOutput"] == 19_200
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM records WHERE stage = 'checkpoint'"
        ).fetchone() == (1,)

    await _persist_checkpoint(
        checkpoint_metadata(meeting_id),
        restarted_spool,
        archive,  # type: ignore[arg-type]
        restarted_client,
        restarted_state,
        source_id,
        destination_id,
        32_000,
        38_400,
        True,
        input_sequence=8,
        provider_output_sample=48_000,
    )
    deliveries = restarted_spool.list_checkpoint_deliveries(meeting_id, 3)
    assert len(deliveries) == 2
    second_checkpoint = json.loads(deliveries[1].body)
    assert second_checkpoint["previousCheckpointSha256"] == acknowledged.checkpoint_hash
    assert second_checkpoint["acceptedInputSequence"] == 8
    assert second_checkpoint["acceptedInput"] == 32_000
    assert second_checkpoint["receivedOutput"] == 48_000
    assert second_checkpoint["emittedOutput"] == 38_400
    assert second_checkpoint["destinationParticipantId"] == str(destination_id)
    restored_after_ack = _restore_checkpoint_state(restarted_spool, meeting_id, 3)
    assert restored_after_ack.watermarks[source_id] == (
        8,
        32_000,
        48_000,
        38_400,
        True,
    )


def register_persisted_checkpoint(
    spool: EncryptedSpool,
    meeting_id: UUID,
    source_id: UUID,
    destination_id: UUID,
    previous_hash: str | None,
    sequence: int,
    accepted_input: int,
    received_output: int,
    emitted_output: int,
) -> str:
    checkpoint_id = uuid4()
    checkpoint = {
        "checkpointId": str(checkpoint_id),
        "sourceParticipantId": str(source_id),
        "destinationParticipantId": str(destination_id),
        "acceptedInputSequence": sequence,
        "acceptedInput": accepted_input,
        "receivedOutput": received_output,
        "emittedOutput": emitted_output,
        "workerEpoch": 3,
        "previousCheckpointSha256": previous_hash,
        "expectedObjectIds": [],
        "observedObjectIds": [],
        "gaps": [],
        "terminal": False,
        "occurredAtMs": sequence,
    }
    encoded = json.dumps(checkpoint, separators=(",", ":"), sort_keys=True).encode()
    digest = hashlib.sha256((previous_hash or "").encode() + encoded).hexdigest()
    checkpoint["highWatermarkSha256"] = digest
    body = json.dumps(checkpoint, separators=(",", ":"), sort_keys=True).encode()
    event_id = uuid4()
    spool.register_checkpoint_delivery(
        checkpoint_id=checkpoint_id,
        meeting_id=meeting_id,
        source_id=source_id,
        worker_epoch=3,
        checkpoint_hash=digest,
        previous_hash=previous_hash,
        control_event_id=event_id,
        body=body,
    )
    spool.mark_checkpoint_delivery_acknowledged(event_id)
    return digest


def test_restore_rejects_real_persisted_regressing_chain(tmp_path: Path) -> None:
    meeting_id = UUID(int=20)
    source_id = UUID(int=21)
    destination_id = UUID(int=22)
    spool = encrypted_spool(tmp_path / "payloads", tmp_path / "journal.sqlite3")
    predecessor = register_persisted_checkpoint(
        spool, meeting_id, source_id, destination_id, None, 2, 8_000, 2_400, 1_920
    )
    register_persisted_checkpoint(
        spool,
        meeting_id,
        source_id,
        destination_id,
        predecessor,
        1,
        4_000,
        1_200,
        960,
    )

    with pytest.raises(RuntimeError, match="watermarks regress"):
        _restore_checkpoint_state(spool, meeting_id, 3)


@pytest.mark.asyncio
async def test_concurrent_source_callbacks_form_one_chain_without_roots(
    tmp_path: Path,
) -> None:
    meeting_id = UUID(int=30)
    source_id = UUID(int=31)
    destination_id = UUID(int=32)
    spool = encrypted_spool(tmp_path / "payloads", tmp_path / "journal.sqlite3")
    state = CheckpointChainState.empty()

    class Control:
        async def record_object(
            self, payload: dict[str, Any], *, event_id: UUID | None = None
        ) -> None:
            await asyncio.sleep(0)

        async def checkpoint(
            self, payload: dict[str, Any], *, event_id: UUID | None = None
        ) -> None:
            await asyncio.sleep(0)

    arguments = (
        checkpoint_metadata(meeting_id),
        spool,
        RecordingArchive(),
        Control(),
        state,
        source_id,
        destination_id,
    )
    await asyncio.gather(
        _persist_checkpoint(*arguments, 4_000, 960, False),  # type: ignore[arg-type]
        _persist_checkpoint(*arguments, 8_000, 1_920, True),  # type: ignore[arg-type]
    )

    deliveries = spool.list_checkpoint_deliveries(meeting_id, 3)
    assert len(deliveries) == 2
    assert deliveries[0].acknowledged
    assert deliveries[1].acknowledged
    assert deliveries[0].previous_hash is None
    assert deliveries[1].previous_hash == deliveries[0].checkpoint_hash
