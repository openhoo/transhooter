from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from websockets.frames import Frame, Opcode

from transhooter_worker.adapters.deepgram.sansio import SansIoWebSocket
from transhooter_worker.adapters.deepl.provider import DeepLConfig, DeepLProvider
from transhooter_worker.adapters.google.provider import (
    GoogleConfig,
    GoogleSttSession,
    GoogleTranslationProvider,
    translation_service,
)
from transhooter_worker.domain.models import (
    AudioChunk,
    BoundaryEvent,
    Outcome,
    RawRef,
    SampleRange,
    TranslationRequest,
)
from transhooter_worker.runtime.redis_quota import RedisQuotaGate


class MemoryJournal:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.terminals: dict[UUID, bytes] = {}

    def append(self, **row: Any) -> RawRef:
        self.rows.append(row)
        payload = bytes(row.get("payload", b""))
        return RawRef(
            uuid4(),
            len(self.rows),
            hashlib.sha256(payload).hexdigest(),
            len(payload),
            str(row.get("media_type", "application/octet-stream")),
        )

    def terminal(self, attempt_id: UUID, payload: bytes) -> RawRef:
        self.terminals.setdefault(attempt_id, payload)
        return self.append(payload=self.terminals[attempt_id], media_type="application/json")


def google_config() -> GoogleConfig:
    return GoogleConfig(
        project="project",
        quota_project="quota",
        meeting_id=uuid4(),
        credential_fingerprint="credential",
        probe_voice="de-DE-Chirp3-HD-Achernar",
    )


class BlockingUnaryCall:
    def __init__(self, raw: bytes, deserializer: Any) -> None:
        self.raw = raw
        self.deserializer = deserializer
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    def __await__(self):
        async def wait() -> Any:
            self.entered.set()
            await self.release.wait()
            return self.deserializer(self.raw)

        return wait().__await__()

    async def initial_metadata(self) -> tuple[tuple[str, str], ...]:
        return ()

    async def trailing_metadata(self) -> tuple[tuple[str, str], ...]:
        return ()

    async def code(self) -> str:
        return "OK"

    async def details(self) -> str:
        return ""


class BlockingTranslationChannel:
    def __init__(self) -> None:
        self.calls = 0
        self.call: BlockingUnaryCall | None = None

        self.created = asyncio.Event()

    def unary_unary(self, _: str, request_serializer: Any, response_deserializer: Any):
        response = translation_service.TranslateTextResponse(
            translations=[translation_service.Translation(translated_text="Guten Tag")]
        )
        raw = bytes(translation_service.TranslateTextResponse.pb(response).SerializeToString())

        def invoke(request: Any, **__: Any) -> BlockingUnaryCall:
            self.calls += 1
            request_serializer(request)
            self.call = BlockingUnaryCall(raw, response_deserializer)
            self.created.set()
            return self.call

        return invoke


@pytest.mark.asyncio
async def test_google_translation_caller_cancellation_does_not_restart_provider_call() -> None:
    channel = BlockingTranslationChannel()
    journal = MemoryJournal()
    request = TranslationRequest(uuid4(), uuid4(), "final", "en", "de", "hello", SampleRange(0, 1))
    attempt = await GoogleTranslationProvider(google_config(), journal, channel).start(request)

    first_waiter = asyncio.create_task(attempt.result())
    await channel.created.wait()
    call = channel.call
    assert call is not None
    await call.entered.wait()
    first_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first_waiter

    second_waiter = asyncio.create_task(attempt.result())
    call.release.set()
    outcome = await second_waiter

    assert channel.calls == 1
    assert outcome.terminal.outcome is Outcome.SUCCEEDED
    assert outcome.result is not None and outcome.result.text == "Guten Tag"


@pytest.mark.asyncio
async def test_google_translation_explicit_cancel_is_journaled_once() -> None:
    channel = BlockingTranslationChannel()
    journal = MemoryJournal()
    request = TranslationRequest(uuid4(), uuid4(), "final", "en", "de", "hello", SampleRange(0, 1))
    attempt = await GoogleTranslationProvider(google_config(), journal, channel).start(request)
    waiter = asyncio.create_task(attempt.result())
    await channel.created.wait()
    call = channel.call
    assert call is not None
    await call.entered.wait()

    terminal = await attempt.cancel()
    settled = await waiter

    cancellation_rows = [
        row
        for row in journal.rows
        if row.get("direction") == "status-in" and row.get("payload") == b'{"code":"CANCELLED"}'
    ]
    assert terminal.outcome is Outcome.CANCELLED
    assert settled.terminal.terminal_id == terminal.terminal_id
    assert channel.calls == 1
    assert len(cancellation_rows) == 1


class CancellationSettlingUnaryCall(BlockingUnaryCall):
    def __init__(self, raw: bytes, deserializer: Any) -> None:
        super().__init__(raw, deserializer)
        self.cancelled = asyncio.Event()
        self.release_after_cancel = asyncio.Event()

    def __await__(self):
        async def wait() -> Any:
            self.entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled.set()
                await self.release_after_cancel.wait()
                return self.deserializer(self.raw)
            raise AssertionError("unreachable")

        return wait().__await__()


class CancellationSettlingTranslationChannel(BlockingTranslationChannel):
    def unary_unary(self, _: str, request_serializer: Any, response_deserializer: Any):
        response = translation_service.TranslateTextResponse(
            translations=[translation_service.Translation(translated_text="Guten Tag")]
        )
        raw = bytes(translation_service.TranslateTextResponse.pb(response).SerializeToString())

        def invoke(request: Any, **__: Any) -> CancellationSettlingUnaryCall:
            self.calls += 1
            request_serializer(request)
            call = CancellationSettlingUnaryCall(raw, response_deserializer)
            self.call = call
            self.created.set()
            return call

        return invoke


@pytest.mark.asyncio
async def test_google_cancel_survives_cancelling_its_caller_and_terminalizes_once() -> None:
    channel = CancellationSettlingTranslationChannel()
    journal = MemoryJournal()
    request = TranslationRequest(uuid4(), uuid4(), "final", "en", "de", "hello", SampleRange(0, 1))
    attempt = await GoogleTranslationProvider(google_config(), journal, channel).start(request)
    result_waiter = asyncio.create_task(attempt.result())
    await channel.created.wait()
    call = channel.call
    assert isinstance(call, CancellationSettlingUnaryCall)
    await call.entered.wait()

    cancel_waiter = asyncio.create_task(attempt.cancel())
    await call.cancelled.wait()
    cancel_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancel_waiter
    call.release_after_cancel.set()

    terminal = await attempt.cancel()
    settled = await result_waiter
    cancellation_rows = [
        row
        for row in journal.rows
        if row.get("direction") == "status-in" and row.get("payload") == b'{"code":"CANCELLED"}'
    ]
    assert terminal.outcome is Outcome.CANCELLED
    assert settled.terminal.terminal_id == terminal.terminal_id
    assert len(journal.terminals) == 1
    assert len(cancellation_rows) == 1


class HangingStreamCall:
    def __aiter__(self) -> AsyncIterator[Any]:
        async def stream() -> AsyncIterator[Any]:
            await asyncio.Event().wait()
            if False:
                yield None

        return stream()


class HangingSttChannel:
    def stream_stream(self, *_: Any, **__: Any):
        return lambda *args, **kwargs: HangingStreamCall()


@pytest.mark.asyncio
async def test_google_stt_queues_apply_backpressure_and_reserve_terminal_slot() -> None:
    journal = MemoryJournal()
    session = GoogleSttSession(google_config(), journal, HangingSttChannel(), uuid4(), "en-US")
    raw_ref = journal.append(payload=b"event", media_type="application/json")
    for _ in range(session._EVENT_QUEUE_SIZE):
        await session._emit_event(BoundaryEvent(uuid4(), 0, raw_ref))

    for sequence in range(session._INPUT_QUEUE_SIZE):
        start = sequence * 4_000
        await session.send_audio(
            AudioChunk(uuid4(), sequence, SampleRange(start, start + 4_000), b"\0" * 8_000)
        )
    blocked = asyncio.create_task(
        session.send_audio(
            AudioChunk(
                uuid4(), session._INPUT_QUEUE_SIZE, SampleRange(128_000, 132_000), b"\0" * 8_000
            )
        )
    )
    await asyncio.sleep(0)
    assert not blocked.done()

    terminal = await asyncio.wait_for(session.cancel(), 1)
    with pytest.raises(RuntimeError, match="session terminal"):
        await blocked
    assert terminal.outcome is Outcome.CANCELLED
    assert session._events.qsize() == session._EVENT_QUEUE_SIZE + 1


@pytest.mark.asyncio
async def test_google_stt_cancelled_blocked_send_does_not_leave_queue_put_tasks() -> None:
    session = GoogleSttSession(
        google_config(), MemoryJournal(), HangingSttChannel(), uuid4(), "en-US"
    )
    queued = object()
    for _ in range(session._INPUT_QUEUE_SIZE):
        session._input.put_nowait(queued)
    blocked = asyncio.create_task(session._put_input(object()))
    await asyncio.sleep(0)
    assert not blocked.done()

    blocked.cancel()
    with pytest.raises(asyncio.CancelledError):
        await blocked
    session._input.get_nowait()
    await asyncio.sleep(0)

    assert session._input.qsize() == session._INPUT_QUEUE_SIZE - 1
    await session.cancel()


class RecordingWriter:
    def __init__(self) -> None:
        self.closed = False
        self.waited = False
        self.writes: list[bytes] = []

    def write(self, payload: bytes) -> None:
        self.writes.append(payload)

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        self.waited = True


@pytest.mark.asyncio
async def test_deepgram_failed_upgrade_closes_tcp_writer(monkeypatch: pytest.MonkeyPatch) -> None:
    writer = RecordingWriter()

    async def open_stream(*_: Any) -> tuple[object, RecordingWriter]:
        return object(), writer

    async def fail_upgrade(*_: Any) -> None:
        raise RuntimeError("upgrade rejected")

    monkeypatch.setattr(SansIoWebSocket, "_open_tls_stream", open_stream)
    monkeypatch.setattr(SansIoWebSocket, "_send_upgrade_request", fail_upgrade)

    with pytest.raises(RuntimeError, match="upgrade rejected"):
        await SansIoWebSocket.connect(
            "wss://api.eu.deepgram.com/v1/listen", "Token secret", lambda *args: None
        )
    assert writer.closed and writer.waited


class BlockingReader:
    async def read(self, _: int) -> bytes:
        await asyncio.Event().wait()
        return b""


class IdleProtocol:
    state = type("State", (), {"name": "OPEN"})()

    def data_to_send(self) -> list[bytes]:
        return []


@pytest.mark.asyncio
async def test_deepgram_inbound_queue_is_bounded_with_backpressure() -> None:
    writer = RecordingWriter()
    socket = SansIoWebSocket(BlockingReader(), writer, IdleProtocol(), lambda *args: None)
    frame = Frame(Opcode.BINARY, b"audio", fin=True)
    for _ in range(socket._INBOUND_QUEUE_SIZE):
        await socket._append_message_fragment(frame)
    blocked = asyncio.create_task(socket._append_message_fragment(frame))
    await asyncio.sleep(0)
    assert not blocked.done()

    iterator = socket.__aiter__()
    assert await anext(iterator) == b"audio"
    await asyncio.wait_for(blocked, 1)
    socket._reader_task.cancel()
    await asyncio.gather(socket._reader_task, return_exceptions=True)
    assert writer.closed and writer.waited


class OneReadReader:
    def __init__(self) -> None:
        self.reads = 0

    async def read(self, _: int) -> bytes:
        self.reads += 1
        return b"server-close"


class ClosingProtocol:
    state = type("State", (), {"name": "CLOSING"})()
    close_code = 1000
    close_reason = "done"

    def __init__(self) -> None:
        self.delivered = False
        self.flushed = False

    def receive_data(self, _: bytes) -> None:
        return None

    def events_received(self) -> list[Frame]:
        if self.delivered:
            return []
        self.delivered = True
        return [Frame(Opcode.CLOSE, b"\x03\xe8done", fin=True)]

    def data_to_send(self) -> list[bytes]:
        if self.delivered and not self.flushed:
            self.flushed = True
            return [b"close-reply"]
        return []


@pytest.mark.asyncio
async def test_deepgram_flushes_close_reply_before_socket_shutdown() -> None:
    writer = RecordingWriter()
    protocol = ClosingProtocol()
    socket = SansIoWebSocket(OneReadReader(), writer, protocol, lambda *args: None)

    await asyncio.wait_for(socket._reader_task, 1)

    assert writer.writes == [b"close-reply"]
    assert writer.closed and writer.waited


class SettlingTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.entered = asyncio.Event()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return httpx.Response(
                200,
                json={"translations": [{"text": "Guten Tag"}]},
                request=request,
            )
        raise AssertionError("unreachable")


@pytest.mark.asyncio
async def test_deepl_cancel_settles_http_task_and_retains_inbound_evidence() -> None:
    transport = SettlingTransport()
    journal = MemoryJournal()
    client = httpx.AsyncClient(transport=transport)
    request = TranslationRequest(uuid4(), uuid4(), "final", "en", "de", "hello", SampleRange(0, 1))
    attempt = await DeepLProvider(DeepLConfig("secret", uuid4()), journal, client).start(request)
    result_waiter = asyncio.create_task(attempt.result())
    await transport.entered.wait()

    terminal = await attempt.cancel()
    late = await result_waiter
    await client.aclose()

    assert terminal.outcome is Outcome.CANCELLED
    assert terminal.received_output == 1
    assert len(terminal.raw_refs) == 2
    assert late.result is None and late.terminal.terminal_id == terminal.terminal_id


class CancellationSettlingTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.release_after_cancel = asyncio.Event()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            await self.release_after_cancel.wait()
            return httpx.Response(
                200,
                json={"translations": [{"text": "Guten Tag"}]},
                request=request,
            )
        raise AssertionError("unreachable")


@pytest.mark.asyncio
async def test_deepl_cancel_survives_cancelling_its_caller_and_terminalizes_once() -> None:
    transport = CancellationSettlingTransport()
    journal = MemoryJournal()
    client = httpx.AsyncClient(transport=transport)
    request = TranslationRequest(uuid4(), uuid4(), "final", "en", "de", "hello", SampleRange(0, 1))
    attempt = await DeepLProvider(DeepLConfig("secret", uuid4()), journal, client).start(request)
    result_waiter = asyncio.create_task(attempt.result())
    await transport.entered.wait()

    cancel_waiter = asyncio.create_task(attempt.cancel())
    await transport.cancelled.wait()
    cancel_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancel_waiter
    transport.release_after_cancel.set()

    terminal = await attempt.cancel()
    settled = await result_waiter
    await client.aclose()
    assert terminal.outcome is Outcome.CANCELLED
    assert settled.terminal.terminal_id == terminal.terminal_id
    assert len(journal.terminals) == 1


class RecordingQuotaGate(RedisQuotaGate):
    def __init__(self, *, accepted: bool = True) -> None:
        super().__init__(
            "redis://redis:6379",
            "provider",
            "account",
            "eu",
            {"stt": {"streams": 10, "sessions": 10}},
        )
        self.evals: list[tuple[str, list[str], list[str]]] = []
        self.accepted = accepted

    async def _eval(self, script: str, keys: list[str], arguments: list[str]) -> bool:
        self.evals.append((script, keys, arguments))
        return self.accepted


@pytest.mark.asyncio
async def test_active_quota_dimensions_reserve_and_release_atomically() -> None:
    gate = RecordingQuotaGate()
    await gate.reserve_active("stt", "reservation")
    await gate.release_active("stt", "reservation")

    reserve, release = gate.evals
    assert len(reserve[1]) == 2
    assert reserve[2][-2:] == ["8", "8"]
    assert len(release[1]) == 2
    assert release[2] == ["reservation"]


@pytest.mark.asyncio
async def test_active_quota_rejection_cannot_leave_a_partial_dimension() -> None:
    gate = RecordingQuotaGate(accepted=False)

    with pytest.raises(RuntimeError, match="provider active quota rejected"):
        await gate.reserve_active("stt", "reservation")

    assert len(gate.evals) == 1
    assert len(gate.evals[0][1]) == 2
