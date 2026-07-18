from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncIterator, Callable

from websockets.client import ClientProtocol
from websockets.frames import Frame, Opcode
from websockets.http11 import Response
from websockets.uri import parse_uri

JournalFrame = Callable[[str, str, bytes, tuple[tuple[str, str], ...]], None]


class SansIoWebSocket:
    """Adapter-owned TLS transport exposing every HTTP and WebSocket protocol boundary."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        protocol: ClientProtocol,
        journal: JournalFrame,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._protocol = protocol
        self._journal = journal
        self._queue: asyncio.Queue[str | bytes | None] = asyncio.Queue()
        self._fragments = bytearray()
        self._fragment_opcode: Opcode | None = None
        self._reader_task = asyncio.create_task(self._read())
        self.close_code: int | None = None
        self.close_reason: str | None = None

    @classmethod
    async def connect(
        cls,
        url: str,
        authorization: str,
        journal: JournalFrame,
    ) -> SansIoWebSocket:
        uri = parse_uri(url)
        reader, writer = await cls._open_tls_stream(uri.host, uri.port)
        protocol = ClientProtocol(uri, max_size=2**24)
        await cls._send_upgrade_request(protocol, writer, authorization, journal)
        await cls._receive_upgrade_response(protocol, reader, journal)
        return cls(reader, writer, protocol, journal)

    @staticmethod
    async def _open_tls_stream(
        host: str,
        port: int | None,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return await asyncio.open_connection(
            host,
            port or 443,
            ssl=ssl.create_default_context(),
            server_hostname=host,
        )

    @staticmethod
    async def _send_upgrade_request(
        protocol: ClientProtocol,
        writer: asyncio.StreamWriter,
        authorization: str,
        journal: JournalFrame,
    ) -> None:
        request = protocol.connect()
        request.headers["Authorization"] = authorization
        request.headers["User-Agent"] = "transhooter-worker/0.1"
        protocol.send_request(request)
        payloads = protocol.data_to_send()
        raw_request = b"".join(payloads)
        redacted_request = raw_request.replace(
            authorization.encode(),
            b"[REDACTED:deepgram-api-key]",
        )
        redacted_headers = tuple(
            (
                name,
                "[REDACTED:deepgram-api-key]" if name.lower() == "authorization" else value,
            )
            for name, value in request.headers.raw_items()
        )
        journal("upgrade-out", "http", redacted_request, redacted_headers)
        for payload in payloads:
            writer.write(payload)
        await writer.drain()

    @staticmethod
    async def _receive_upgrade_response(
        protocol: ClientProtocol,
        reader: asyncio.StreamReader,
        journal: JournalFrame,
    ) -> None:
        while protocol.state.name == "CONNECTING":
            data = await reader.read(65536)
            if not data:
                raise ConnectionError("WebSocket EOF during upgrade")
            protocol.receive_data(data)
            for event in protocol.events_received():
                if isinstance(event, Response):
                    journal(
                        "upgrade-in",
                        "http",
                        event.serialize(),
                        ((":status", str(event.status_code)), *tuple(event.headers.raw_items())),
                    )
                elif isinstance(event, Frame):
                    raise ConnectionError("WebSocket frame arrived before completed upgrade")

    async def send(self, message: str | bytes) -> None:
        body = message.encode() if isinstance(message, str) else message
        opcode = "text" if isinstance(message, str) else "binary"
        self._journal("frame-out", opcode, body, (("fin", "true"),))
        if isinstance(message, str):
            self._protocol.send_text(body)
        else:
            self._protocol.send_binary(body)
        await self._flush()

    async def close(self, code: int = 1000, reason: str = "") -> None:
        if self._protocol.state.name not in {"CLOSING", "CLOSED"}:
            self._journal(
                "frame-out", "close", reason.encode(), (("code", str(code)), ("fin", "true"))
            )
            self._protocol.send_close(code, reason)
            await self._flush()
        try:
            await asyncio.wait_for(self._reader_task, 5)
        except TimeoutError:
            self._writer.close()
            await self._writer.wait_closed()

    def __aiter__(self) -> AsyncIterator[str | bytes]:
        async def iterator() -> AsyncIterator[str | bytes]:
            while True:
                value = await self._queue.get()
                if value is None:
                    return
                yield value

        return iterator()

    async def _read(self) -> None:
        try:
            while True:
                data = await self._reader.read(65536)
                if data:
                    self._protocol.receive_data(data)
                else:
                    self._protocol.receive_eof()

                for event in self._protocol.events_received():
                    if isinstance(event, Frame) and await self._handle_frame(event):
                        return

                await self._flush()
                if not data:
                    await self._queue.put(None)
                    return
        finally:
            self._writer.close()
            await self._writer.wait_closed()

    async def _handle_frame(self, frame: Frame) -> bool:
        opcode_name = frame.opcode.name.lower()
        payload = bytes(frame.data)
        metadata = (
            ("opcode", opcode_name),
            ("fin", str(frame.fin).lower()),
        )
        self._journal("frame-in", opcode_name, payload, metadata)

        if frame.opcode is Opcode.PING:
            await self._reply_to_ping(payload)
            return False
        if frame.opcode is Opcode.PONG:
            return False
        if frame.opcode is Opcode.CLOSE:
            self.close_code = self._protocol.close_code
            self.close_reason = self._protocol.close_reason
            await self._queue.put(None)
            return True

        await self._append_message_fragment(frame)
        return False

    async def _reply_to_ping(self, payload: bytes) -> None:
        self._journal("frame-out", "pong", payload, (("fin", "true"),))
        self._protocol.send_pong(payload)
        await self._flush()

    async def _append_message_fragment(self, frame: Frame) -> None:
        if frame.opcode in {Opcode.TEXT, Opcode.BINARY}:
            self._fragment_opcode = frame.opcode
            self._fragments = bytearray(frame.data)
        elif frame.opcode is Opcode.CONT:
            self._fragments.extend(frame.data)

        if not frame.fin or self._fragment_opcode is None:
            return

        payload = bytes(self._fragments)
        message: str | bytes
        if self._fragment_opcode is Opcode.TEXT:
            message = payload.decode()
        else:
            message = payload
        await self._queue.put(message)
        self._fragment_opcode = None
        self._fragments.clear()

    async def _flush(self) -> None:
        for payload in self._protocol.data_to_send():
            self._writer.write(payload)
        await self._writer.drain()
