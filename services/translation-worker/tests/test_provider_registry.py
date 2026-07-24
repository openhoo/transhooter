from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from transhooter_spool import RawRef, SampleRange

from transhooter_worker.domain.models import TranslationRequest
from transhooter_worker.runtime.provider_registry import (
    AlternateProfile,
    GoogleSpeechProfile,
    ProviderRegistry,
    resolve_profile_config,
)

APPEND_REF = RawRef(
    UUID(int=1),
    1,
    "0" * 64,
    0,
    "application/octet-stream",
)
TERMINAL_REF = RawRef(
    UUID(int=2),
    2,
    "1" * 64,
    0,
    "application/json",
)


class DeterministicJournal:
    def append(self, **kwargs: Any) -> RawRef:
        return RawRef(
            APPEND_REF.object_id,
            APPEND_REF.ordinal,
            APPEND_REF.sha256,
            len(kwargs.get("payload", b"")),
            str(kwargs.get("media_type", APPEND_REF.media_type)),
        )

    def terminal(self, attempt_id: UUID, payload: bytes) -> RawRef:
        return RawRef(
            TERMINAL_REF.object_id,
            TERMINAL_REF.ordinal,
            TERMINAL_REF.sha256,
            len(payload),
            TERMINAL_REF.media_type,
        )


def alternate_profile(tmp_path: Path) -> AlternateProfile:
    deepgram_key_file = tmp_path / "deepgram"
    deepl_key_file = tmp_path / "deepl"
    deepgram_key_file.write_text("dg")
    deepl_key_file.write_text("dl")
    return AlternateProfile(
        kind="deepgram-deepl-eu",
        deepgram_key_file=deepgram_key_file,
        deepl_key_file=deepl_key_file,
        deepgram_api_key="dg",
        deepl_api_key="dl",
        language="en-US",
        languages=("en-US", "de-DE"),
        voice="aura-2-test",
        approved_voices=("aura-2-test",),
        deepgram_credential_fingerprint="dg-v1",
        deepl_credential_fingerprint="dl-v1",
        deepgram_streams=2,
        deepgram_audio_seconds_minute=120,
        deepl_requests_minute=100,
        deepl_characters_minute=100000,
    )


def test_alternate_profile_equality_ignores_secret_bytes_but_compares_fingerprints(
    tmp_path: Path,
) -> None:
    original = alternate_profile(tmp_path)
    different_secrets = original.model_copy(
        update={"deepgram_api_key": "other-dg", "deepl_api_key": "other-dl"}
    )
    different_fingerprint = original.model_copy(update={"deepgram_credential_fingerprint": "dg-v2"})

    assert original == different_secrets
    assert original != different_fingerprint
    assert "deepgram_api_key" not in original.model_dump()
    assert "other-dg" not in repr(different_secrets)


def test_environment_resolver_preserves_alternate_profile_fingerprints(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    deepgram_key_file = tmp_path / "deepgram"
    deepl_key_file = tmp_path / "deepl"
    deepgram_key_file.write_text("dg")
    deepl_key_file.write_text("dl")
    environment = {
        "DEEPGRAM_API_KEY_FILE": str(deepgram_key_file),
        "DEEPL_API_KEY_FILE": str(deepl_key_file),
        "DEEPGRAM_VOICE": "aura-2-test",
        "DEEPGRAM_APPROVED_VOICES": "aura-2-test",
        "DEEPGRAM_STREAMS": "2",
        "DEEPGRAM_AUDIO_SECONDS_MINUTE": "120",
        "DEEPL_REQUESTS_MINUTE": "100",
        "DEEPL_CHARACTERS_MINUTE": "100000",
    }
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    profile = resolve_profile_config(
        "deepgram-deepl-eu",
        ("en-US", "de-DE", "en-US"),
    )

    assert isinstance(profile, AlternateProfile)
    assert profile.languages == ("en-US", "de-DE")
    assert profile.deepgram_credential_fingerprint == hashlib.sha256(b"dg").hexdigest()
    assert profile.deepl_credential_fingerprint == hashlib.sha256(b"dl").hexdigest()


def test_google_speech_profile_resolves_example_pipeline_configuration(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    credentials = tmp_path / "google-adc.json"
    credentials.write_text('{"type":"service_account"}')
    environment = {
        "GOOGLE_APPLICATION_CREDENTIALS": str(credentials),
        "GOOGLE_CLOUD_PROJECT": "project",
        "GOOGLE_QUOTA_PROJECT": "quota",
        "GOOGLE_TTS_VOICE": "de-DE-Chirp3-HD-Achernar",
        "GOOGLE_TTS_VOICE_LOCALE": "de-DE",
        "GOOGLE_SPEECH_LOCATION": "europe-west3",
        "GOOGLE_SPEECH_MODEL": "long",
        "GOOGLE_TRANSLATION_LOCATION": "europe-west1",
        "GOOGLE_TRANSLATION_MODEL": "general/base",
        "GOOGLE_TTS_LOCATION": "eu",
    }
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    profile = resolve_profile_config("google-speech-eu", ("en-US", "de-DE"))

    assert isinstance(profile, GoogleSpeechProfile)
    assert profile.speech_location == "europe-west3"
    assert profile.speech_model == "long"
    assert profile.translation_location == "europe-west1"
    assert profile.translation_model == "general/base"
    assert profile.tts_location == "eu"
    assert profile.credential_fingerprint == hashlib.sha256(credentials.read_bytes()).hexdigest()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("GOOGLE_SPEECH_LOCATION", "eu"),
        ("GOOGLE_SPEECH_MODEL", "chirp_3"),
        ("GOOGLE_TRANSLATION_LOCATION", "us-central1"),
        ("GOOGLE_TRANSLATION_MODEL", "general/nmt"),
        ("GOOGLE_TTS_LOCATION", "us"),
    ],
)
def test_google_speech_profile_rejects_unsupported_configuration(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    name: str,
    value: str,
) -> None:
    credentials = tmp_path / "google-adc.json"
    credentials.write_text('{"type":"service_account"}')
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(credentials))
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "project")
    monkeypatch.setenv("GOOGLE_QUOTA_PROJECT", "quota")
    monkeypatch.setenv("GOOGLE_TTS_VOICE", "en-US-Chirp3-HD-Achernar")
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match="unsupported configuration"):
        resolve_profile_config("google-speech-eu", ("en-US", "de-DE"))


@pytest.mark.asyncio
async def test_alternate_capabilities_cover_bilingual_directions(
    tmp_path: Path,
) -> None:
    profile = alternate_profile(tmp_path)
    providers = ProviderRegistry.construct(
        profile,
        UUID(int=3),
        DeterministicJournal(),
    )
    stt = await providers.stt.capabilities()
    tts = await providers.tts.capabilities()

    assert stt.languages == ("en-US", "de-DE")
    assert tts.languages == ("en-US", "de-DE")


def test_alternate_credentials_are_read_once_and_bound_to_fingerprints(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    deepgram_key_file = tmp_path / "deepgram-once"
    deepl_key_file = tmp_path / "deepl-once"
    deepgram_key_file.write_text("dg-original")
    deepl_key_file.write_text("dl-original")
    environment = {
        "DEEPGRAM_API_KEY_FILE": str(deepgram_key_file),
        "DEEPL_API_KEY_FILE": str(deepl_key_file),
        "DEEPGRAM_VOICE": "aura-2-thalia-en",
        "DEEPGRAM_APPROVED_VOICES": "aura-2-thalia-en",
        "DEEPGRAM_STREAMS": "2",
        "DEEPGRAM_AUDIO_SECONDS_MINUTE": "120",
        "DEEPL_REQUESTS_MINUTE": "100",
        "DEEPL_CHARACTERS_MINUTE": "100000",
    }
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    reads: dict[Path, int] = {}
    original_read_bytes = Path.read_bytes

    def counted_read_bytes(path: Path) -> bytes:
        reads[path] = reads.get(path, 0) + 1
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", counted_read_bytes)
    profile = resolve_profile_config("deepgram-deepl-eu", ("en-US", "de-DE"))
    assert isinstance(profile, AlternateProfile)
    deepgram_key_file.write_text("dg-rotated")
    deepl_key_file.write_text("dl-rotated")

    providers = ProviderRegistry.construct(profile, UUID(int=4), DeterministicJournal())

    assert reads == {deepgram_key_file: 1, deepl_key_file: 1}
    assert providers.stt._config.api_key == "dg-original"
    assert providers.translation._config.api_key == "dl-original"
    assert profile.deepgram_credential_fingerprint == hashlib.sha256(b"dg-original").hexdigest()
    assert profile.deepl_credential_fingerprint == hashlib.sha256(b"dl-original").hexdigest()


@pytest.mark.asyncio
async def test_google_speech_registry_uses_configured_pipeline_endpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from transhooter_worker.adapters.google import provider as google_provider

    class Channel:
        def __init__(self, endpoint: str) -> None:
            self.endpoint = endpoint

        async def close(self) -> None:
            return None

    created: list[Channel] = []

    def create_channel(endpoint: str, _: str) -> Channel:
        channel = Channel(endpoint)
        created.append(channel)
        return channel

    monkeypatch.setattr(google_provider, "authenticated_channel", create_channel)
    providers = ProviderRegistry.construct(
        GoogleSpeechProfile(
            kind="google-speech-eu",
            project="project",
            quota_project="quota",
            credential_fingerprint="credential",
            probe_voice="de-DE-Chirp3-HD-Achernar",
        ),
        UUID(int=7),
        DeterministicJournal(),
    )

    providers.stt._channels.channel()
    request = TranslationRequest(
        UUID(int=31),
        UUID(int=32),
        "final",
        "en-US",
        "de-DE",
        "hello",
        SampleRange(0, 1),
    )
    await providers.translation.start(request)
    await providers.tts.open(UUID(int=33), "de-DE", "de-DE-Chirp3-HD-Achernar")

    assert [channel.endpoint for channel in created] == [
        "europe-west3-speech.googleapis.com:443",
        "translate-eu.googleapis.com:443",
        "eu-texttospeech.googleapis.com:443",
    ]
    assert providers.stt._config.recognizer == (
        "projects/project/locations/europe-west3/recognizers/_"
    )
    assert providers.stt._config.speech_model == "long"
    assert providers.translation._config.parent == "projects/project/locations/europe-west1"
    assert providers.translation._config.model.endswith("/models/general/base")
    assert providers.tts._config.tts_location == "eu"
    await providers.aclose()


@pytest.mark.asyncio
async def test_google_registry_constructs_lazily_reuses_endpoint_channels_and_closes_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from transhooter_worker.adapters.google import provider as google_provider
    from transhooter_worker.runtime.provider_registry import GoogleProfile

    class Channel:
        def __init__(self, endpoint: str) -> None:
            self.endpoint = endpoint
            self.closes = 0

        async def close(self) -> None:
            self.closes += 1

    created: list[Channel] = []

    def create_channel(endpoint: str, _: str) -> Channel:
        channel = Channel(endpoint)
        created.append(channel)
        return channel

    monkeypatch.setattr(google_provider, "authenticated_channel", create_channel)
    providers = ProviderRegistry.construct(
        GoogleProfile(
            kind="google-eu",
            project="project",
            quota_project="quota",
            credential_fingerprint="credential",
            probe_voice="voice",
        ),
        UUID(int=5),
        DeterministicJournal(),
    )

    assert created == []

    stt_channel = providers.stt._channels.channel()
    assert providers.stt._channels.channel() is stt_channel
    assert [channel.endpoint for channel in created] == ["eu-speech.googleapis.com:443"]

    request = TranslationRequest(
        UUID(int=21),
        UUID(int=22),
        "final",
        "en-US",
        "de-DE",
        "hello",
        SampleRange(0, 1),
    )
    first_attempt = await providers.translation.start(request)
    second_attempt = await providers.translation.start(request)
    assert first_attempt._channel is second_attempt._channel

    first_tts_session = await providers.tts.open(UUID(int=23), "de-DE", "voice")
    second_tts_session = await providers.tts.open(UUID(int=24), "de-DE", "voice")
    assert first_tts_session._channel is second_tts_session._channel
    await asyncio.gather(first_tts_session.cancel(), second_tts_session.cancel())

    assert [channel.endpoint for channel in created] == [
        "eu-speech.googleapis.com:443",
        "translate-eu.googleapis.com:443",
        "eu-texttospeech.googleapis.com:443",
    ]

    await asyncio.gather(providers.aclose(), providers.aclose())

    assert [channel.closes for channel in created] == [1, 1, 1]


@pytest.mark.asyncio
async def test_google_registry_close_before_first_use_does_not_create_channels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from transhooter_worker.adapters.google import provider as google_provider
    from transhooter_worker.runtime.provider_registry import GoogleProfile

    creations = 0

    def create_channel(*_: object) -> object:
        nonlocal creations
        creations += 1
        return object()

    monkeypatch.setattr(google_provider, "authenticated_channel", create_channel)
    providers = ProviderRegistry.construct(
        GoogleProfile(
            kind="google-eu",
            project="project",
            quota_project="quota",
            credential_fingerprint="credential",
            probe_voice="voice",
        ),
        UUID(int=6),
        DeterministicJournal(),
    )

    await providers.aclose()

    assert creations == 0


@pytest.mark.asyncio
async def test_alternate_registry_closes_owned_deepl_client_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    closes = 0

    async def counted_close(_client: object) -> None:
        nonlocal closes
        closes += 1

    monkeypatch.setattr("httpx.AsyncClient.aclose", counted_close)
    providers = ProviderRegistry.construct(
        alternate_profile(tmp_path),
        UUID(int=8),
        DeterministicJournal(),
    )

    await asyncio.gather(providers.aclose(), providers.aclose())

    assert closes == 1
