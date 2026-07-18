import io
import wave
from typing import Any
from uuid import UUID

import pytest
from google.cloud.location import locations_pb2
from google.cloud.speech_v2.types import locations_metadata
from google.protobuf.any_pb2 import Any as ProtobufAny

from transhooter_worker.adapters.google import provider as google_provider
from transhooter_worker.domain.models import (
    AudioEvent,
    OperationTerminal,
    OperationTerminalEvent,
    Outcome,
    RawRef,
    RetryAction,
    RetryDecision,
    SampleRange,
    Transport,
)


class Journal:
    def append(self, **_: Any) -> RawRef:
        raise AssertionError("fake RPC owns evidence")

    def terminal(self, *_: object) -> RawRef:
        return RawRef(UUID(int=9), 9, "9" * 64, 1, "application/json")


@pytest.mark.asyncio
async def test_speech_capabilities_decode_nested_language_model_maps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata = locations_metadata.LocationsMetadata(
        languages=locations_metadata.LanguageMetadata(
            models={
                "de-DE": locations_metadata.ModelMetadata(
                    model_features={"long": locations_metadata.ModelFeatures()}
                ),
                "en-US": locations_metadata.ModelMetadata(
                    model_features={"short": locations_metadata.ModelFeatures()}
                ),
            }
        )
    )
    packed = ProtobufAny()
    packed.Pack(locations_metadata.LocationsMetadata.pb(metadata))
    location = locations_pb2.Location(name="projects/project/locations/eu", metadata=packed)
    evidence = RawRef(UUID(int=1), 0, "0" * 64, 1, "application/protobuf")

    async def rpc(*_: object, **__: object) -> tuple[bytes, RawRef]:
        return location.SerializeToString(), evidence

    monkeypatch.setattr(google_provider, "_capability_rpc", rpc)
    config = google_provider.GoogleConfig(
        "project",
        "quota",
        UUID(int=2),
        "credential",
    )
    provider = google_provider.GoogleSttProvider(
        config,
        Journal(),
        object(),  # type: ignore[arg-type]
    )
    capabilities = await provider.capabilities()

    assert capabilities.languages == ("de-DE", "en-US")
    assert capabilities.models == ("long", "short")
    assert capabilities.evidence == evidence


@pytest.mark.asyncio
@pytest.mark.parametrize(("sample_rate", "healthy"), [(48000, True), (24000, False)])
async def test_tts_health_runs_streaming_synthesis_and_validates_returned_format(
    monkeypatch: pytest.MonkeyPatch,
    sample_rate: int,
    healthy: bool,
) -> None:
    evidence = RawRef(UUID(int=3), 1, "1" * 64, 2, "application/protobuf")
    operation_id = UUID(int=4)
    attempt_id = UUID(int=5)
    terminal = OperationTerminal(
        UUID(int=6),
        operation_id,
        attempt_id,
        Outcome.SUCCEEDED,
        None,
        RetryDecision(RetryAction.STOP, None, "done", None),
        1,
        1,
        2,
        Transport.GRPC,
        (evidence,),
        "credential",
    )

    class Attempt:
        def events(self):
            async def stream():
                yield AudioEvent(
                    operation_id,
                    0,
                    SampleRange(0, 2),
                    b"\0\0\0\0",
                    sample_rate,
                    1,
                    evidence,
                )
                yield OperationTerminalEvent(terminal)

            return stream()

    captured: dict[str, object] = {}

    def attempt_factory(*args: object) -> Attempt:
        captured["utterance"] = args[3]
        captured["language"] = args[4]
        captured["voice"] = args[5]
        return Attempt()

    monkeypatch.setattr(google_provider, "GoogleTtsAttempt", attempt_factory)
    config = google_provider.GoogleConfig(
        project="project",
        quota_project="quota",
        meeting_id=UUID(int=2),
        credential_fingerprint="credential",
        probe_voice="de-DE-Chirp3-HD-Algenib",
        probe_voice_locale="de-DE",
    )
    provider = google_provider.GoogleTtsProvider(
        config,
        Journal(),
        object(),  # type: ignore[arg-type]
    )
    result = await provider.health("snapshot")

    assert result.healthy is healthy
    if healthy:
        assert result.evidence == evidence
    else:
        assert result.evidence is None
    utterance = captured["utterance"]
    assert isinstance(utterance, google_provider.SynthesisUtterance)
    assert utterance.language == "de-DE"
    assert captured["language"] == "de-DE"
    assert captured["voice"] == "de-DE-Chirp3-HD-Algenib"


def test_linear16_decoder_strips_split_wav_header_and_preserves_pcm() -> None:
    pcm = b"\x01\x00\x02\x00\x03\x00"
    encoded = io.BytesIO()
    with wave.open(encoded, "wb") as target:
        target.setnchannels(1)
        target.setsampwidth(2)
        target.setframerate(48000)
        target.writeframes(pcm)
    payload = encoded.getvalue()
    decoder = google_provider.Linear16StreamDecoder()
    first_chunk = decoder.feed(payload[:17])
    second_chunk = decoder.feed(payload[17:43])
    final_chunk = decoder.feed(payload[43:])
    decoded = first_chunk + second_chunk + final_chunk
    decoder.finish()

    assert decoded == pcm
    assert not decoded.startswith(b"RIFF")


def test_linear16_decoder_rejects_wrong_wav_sample_rate() -> None:
    encoded = io.BytesIO()
    with wave.open(encoded, "wb") as target:
        target.setnchannels(1)
        target.setsampwidth(2)
        target.setframerate(24000)
        target.writeframes(b"\0\0")
    with pytest.raises(RuntimeError):
        google_provider.Linear16StreamDecoder().feed(encoded.getvalue())
