import asyncio
import json
from typing import Any
from uuid import UUID, uuid4

import pytest
from websockets.client import ClientProtocol
from websockets.exceptions import InvalidStatus
from websockets.uri import parse_uri

from transhooter_worker.adapters.deepgram.provider import DeepgramConfig, DeepgramSttSession
from transhooter_worker.adapters.deepgram.sansio import SansIoWebSocket
from transhooter_worker.application.session import _audio_after_watermark
from transhooter_worker.domain.models import (
    AudioChunk,
    BoundaryEvent,
    Outcome,
    RawRef,
    SampleRange,
    SessionTerminalEvent,
    TranscriptEvent,
)


class Journal:
    def __init__(self) -> None:
        self.ordinal = 0

    def append(self, **kwargs: Any) -> RawRef:
        self.ordinal += 1
        return RawRef(
            uuid4(),
            self.ordinal,
            "0" * 64,
            len(kwargs.get("payload", b"")),
            str(kwargs.get("media_type", "application/json")),
        )

    def terminal(self, *_: object) -> RawRef:
        return self.append(payload=b"terminal")


class Socket:
    close_code = 1000
    close_reason = "complete"

    def __aiter__(self):
        async def messages():
            yield json.dumps(
                {
                    "type": "Results",
                    "start": 0.0,
                    "duration": 0.25,
                    "is_final": True,
                    "speech_final": False,
                    "channel": {"alternatives": [{"transcript": "resumed", "words": []}]},
                }
            )

        return messages()

    async def send(self, _: object) -> None:
        pass

    async def close(self, **_: object) -> None:
        pass


class FailingSocket:
    close_code = 1006
    close_reason = "transport lost"

    def __init__(self) -> None:
        self.fail = asyncio.Event()

    def __aiter__(self):
        async def messages():
            await self.fail.wait()
            raise ConnectionError("transport lost")
            yield  # pragma: no cover

        return messages()

    async def send(self, _: object) -> None:
        pass

    async def close(self, **_: object) -> None:
        pass


class ReplaySocket(Socket):
    def __aiter__(self):
        async def messages():
            yield json.dumps(
                {
                    "type": "Results",
                    "start": 0.0,
                    "duration": 1.0,
                    "is_final": True,
                    "speech_final": False,
                    "channel": {
                        "alternatives": [
                            {
                                "transcript": "committed suffix",
                                "words": [
                                    {"word": "committed", "start": 0.0, "end": 0.5},
                                    {"word": "suffix", "start": 0.5, "end": 1.0},
                                ],
                            }
                        ]
                    },
                }
            )

        return messages()


def deepgram_config() -> DeepgramConfig:
    return DeepgramConfig(
        api_key="secret",
        meeting_id=UUID(int=1),
        language="en-US",
        voice="voice",
        approved_voices=("voice",),
        credential_fingerprint="fingerprint",
    )


@pytest.mark.asyncio
async def test_deepgram_retry_offsets_provider_time_to_absolute_samples() -> None:
    retry_base_sample = 32_000
    commit_watermark = 31_000
    provider_span_samples = 4_000
    config = DeepgramConfig(
        api_key="secret",
        meeting_id=UUID(int=1),
        language="en-US",
        voice="voice",
        approved_voices=("voice",),
        credential_fingerprint="fingerprint",
    )
    session = DeepgramSttSession(
        config=config,
        journal=Journal(),
        session_id=uuid4(),
        websocket=Socket(),
        references=[RawRef(uuid4(), 0, "0" * 64, 1, "application/http")],
        resume_at_sample=retry_base_sample,
        commit_watermark=commit_watermark,
    )
    events = [event async for event in session.events()]
    transcript = next(event for event in events if isinstance(event, TranscriptEvent))
    assert transcript.samples.start == retry_base_sample
    assert transcript.samples.end == retry_base_sample + provider_span_samples


@pytest.mark.asyncio
async def test_transport_failure_does_not_commit_requested_boundary() -> None:
    socket = FailingSocket()
    session = DeepgramSttSession(
        config=deepgram_config(),
        journal=Journal(),
        session_id=uuid4(),
        websocket=socket,
        references=[RawRef(uuid4(), 0, "0" * 64, 1, "application/http")],
    )
    chunk = AudioChunk(uuid4(), 0, SampleRange(0, 4), b"\0" * 8)
    await session.send_audio(chunk)
    await session.request_boundary(uuid4())

    socket.fail.set()
    events = [event async for event in session.events()]

    assert not any(isinstance(event, BoundaryEvent) for event in events)
    terminal = next(event.terminal for event in events if isinstance(event, SessionTerminalEvent))
    assert terminal.outcome is Outcome.FAILED
    assert terminal.accepted_input == chunk.samples.end


@pytest.mark.asyncio
async def test_spanning_replay_final_emits_only_post_watermark_words() -> None:
    session = DeepgramSttSession(
        config=deepgram_config(),
        journal=Journal(),
        session_id=uuid4(),
        websocket=ReplaySocket(),
        references=[RawRef(uuid4(), 0, "0" * 64, 1, "application/http")],
        commit_watermark=8_000,
    )

    events = [event async for event in session.events()]
    transcript = next(event for event in events if isinstance(event, TranscriptEvent))

    assert transcript.text == "suffix"
    assert transcript.samples == SampleRange(8_000, 16_000)
    assert tuple(word.text for word in transcript.words) == ("suffix",)
    assert transcript.words[0].samples == transcript.samples


@pytest.mark.asyncio
async def test_crossing_replay_without_word_timing_fails_for_clipped_retry() -> None:
    session = DeepgramSttSession(
        config=deepgram_config(),
        journal=Journal(),
        session_id=uuid4(),
        websocket=Socket(),
        references=[RawRef(uuid4(), 0, "0" * 64, 1, "application/http")],
        commit_watermark=2_000,
    )

    events = [event async for event in session.events()]

    assert not any(isinstance(event, TranscriptEvent) for event in events)
    terminal = next(event.terminal for event in events if isinstance(event, SessionTerminalEvent))
    assert terminal.outcome is Outcome.FAILED
    assert terminal.error is not None
    assert "sufficient word timing" in terminal.error.message


def test_replay_audio_is_clipped_exactly_at_committed_watermark() -> None:
    chunk = AudioChunk(
        uuid4(),
        7,
        SampleRange(4, 12),
        b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f",
    )

    clipped = _audio_after_watermark(chunk, 8)

    assert clipped is not None
    assert clipped.samples == SampleRange(8, 12)
    assert clipped.pcm == chunk.pcm[8:]
    assert clipped.operation_id == chunk.operation_id
    assert clipped.sequence == chunk.sequence


@pytest.mark.asyncio
async def test_rejected_websocket_upgrade_raises_handshake_exception() -> None:
    protocol = ClientProtocol(parse_uri("wss://api.eu.deepgram.com/v1/listen"))
    protocol.send_request(protocol.connect())
    protocol.data_to_send()
    reader = asyncio.StreamReader()
    reader.feed_data(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
    reader.feed_eof()

    with pytest.raises(InvalidStatus) as raised:
        await SansIoWebSocket._receive_upgrade_response(
            protocol,
            reader,
            lambda *_: None,
        )

    assert raised.value is protocol.handshake_exc
