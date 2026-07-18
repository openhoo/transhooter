import json
from typing import Any
from uuid import UUID, uuid4

import pytest

from transhooter_worker.adapters.deepgram.provider import DeepgramConfig, DeepgramSttSession
from transhooter_worker.domain.models import RawRef, TranscriptEvent


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
