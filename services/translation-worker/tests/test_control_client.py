import asyncio
import json
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

from transhooter_worker.runtime.control_client import (
    ControlClient,
    PermanentControlRequestError,
    RetryableControlRequestError,
)


class RecordingSpool:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def append(self, _authority: object, **record: Any) -> None:
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
        object(),  # producer authority
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
        object(),  # producer authority
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
        object(),  # producer authority
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
    await client.provider_attempt({"terminal": True})

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
    await client.provider_attempt({"terminal": True})

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
        await client.provider_attempt({"terminalId": str(uuid4())})

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
        object(),  # producer authority
        httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        retry_delays=(0, 0, 0, 0, 0),
    )
    await client.provider_attempt({"terminal": True})

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
        await client.provider_attempt({"terminal": True})

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
    posting = asyncio.create_task(client.provider_attempt({"terminal": True}))
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
        await client.provider_attempt(
            {
                "terminal": True,
                "consultationId": "payload-must-not-win",
                "eventId": "payload-must-not-win",
            },
            event_id=event_id,
        )
    await client.provider_attempt(
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
