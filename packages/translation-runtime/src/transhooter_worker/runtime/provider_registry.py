from __future__ import annotations

import asyncio
import hashlib
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

from transhooter_worker.adapters.deepgram.provider import (
    DeepgramConfig,
    DeepgramSttProvider,
    DeepgramTtsProvider,
)
from transhooter_worker.adapters.deepl.provider import DeepLConfig, DeepLProvider
from transhooter_worker.adapters.fixture.provider import (
    FixtureSttProvider,
    FixtureTranslationProvider,
    FixtureTtsProvider,
)
from transhooter_worker.adapters.fixture.scenario import FixtureScenario
from transhooter_worker.adapters.google.provider import (
    GoogleChannelPool,
    GoogleConfig,
    GoogleSttProvider,
    GoogleTranslationProvider,
    GoogleTtsProvider,
)
from transhooter_worker.ports.exchange_journal import ExchangeJournal
from transhooter_worker.ports.providers import (
    StreamingSttProvider,
    StreamingTtsProvider,
    TextTranslationProvider,
)


def _credential(path: Path, name: str) -> tuple[str, str]:
    try:
        value = path.read_bytes()
    except OSError as exc:
        raise RuntimeError(f"{name} credential file is required") from exc
    stripped = value.strip()
    if not stripped:
        raise RuntimeError(f"{name} credential file is empty")
    try:
        secret = stripped.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RuntimeError(f"{name} credential file must be UTF-8") from exc
    return secret, hashlib.sha256(value).hexdigest()


def credential_fingerprint(path: Path, name: str) -> str:
    return _credential(path, name)[1]


class FixtureProfile(BaseModel):
    kind: Literal["fixture"]


class GoogleProfile(BaseModel):
    kind: Literal["google-eu"]
    project: str
    quota_project: str
    credential_fingerprint: str
    probe_voice: str
    probe_voice_locale: str = "en-US"


class GoogleSpeechProfile(BaseModel):
    kind: Literal["google-speech-eu"]
    project: str
    quota_project: str
    credential_fingerprint: str
    probe_voice: str
    probe_voice_locale: str = "en-US"
    speech_location: str = "europe-west3"
    speech_model: str = "long"
    translation_location: str = "europe-west1"
    translation_model: str = "general/base"
    tts_location: str = "eu"

    @model_validator(mode="after")
    def validate_supported_configuration(self) -> GoogleSpeechProfile:
        expected = {
            "speech_location": (self.speech_location, "europe-west3"),
            "speech_model": (self.speech_model, "long"),
            "translation_location": (self.translation_location, "europe-west1"),
            "translation_model": (self.translation_model, "general/base"),
            "tts_location": (self.tts_location, "eu"),
        }
        invalid = [name for name, (value, required) in expected.items() if value != required]
        if invalid:
            raise ValueError(
                "google-speech-eu has unsupported configuration: " + ", ".join(invalid)
            )
        return self


class AlternateProfile(BaseModel):
    kind: Literal["deepgram-deepl-eu"]
    deepgram_key_file: Path
    deepl_key_file: Path
    deepgram_api_key: str = Field(exclude=True, repr=False)
    deepl_api_key: str = Field(exclude=True, repr=False)
    languages: tuple[str, ...] = ()
    language: str
    voice: str
    approved_voices: tuple[str, ...]
    deepgram_credential_fingerprint: str
    deepl_credential_fingerprint: str
    deepgram_streams: int = Field(gt=0)
    deepgram_audio_seconds_minute: int = Field(gt=0)
    deepl_requests_minute: int = Field(gt=0)
    deepl_characters_minute: int = Field(gt=0)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, AlternateProfile) and self.model_dump() == other.model_dump()


ProfileConfig = Annotated[
    FixtureProfile | GoogleProfile | GoogleSpeechProfile | AlternateProfile,
    Field(discriminator="kind"),
]


async def _run_close(close: Callable[[], Awaitable[None]]) -> None:
    await close()


@dataclass(frozen=True, slots=True)
class Providers:
    stt: StreamingSttProvider
    translation: TextTranslationProvider
    tts: StreamingTtsProvider
    _close: Callable[[], Awaitable[None]] | None = None
    _close_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)
    _close_task: asyncio.Task[None] | None = field(default=None, init=False, repr=False)

    async def aclose(self) -> None:
        if self._close is None:
            return
        async with self._close_lock:
            close = self._close
            assert close is not None
            if self._close_task is None:
                object.__setattr__(self, "_close_task", asyncio.create_task(_run_close(close)))
            close_task = self._close_task
        assert close_task is not None
        try:
            await asyncio.shield(close_task)
        except asyncio.CancelledError:
            await close_task
            raise


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _speech_endpoint(location: str) -> str:
    return f"{location}-speech.googleapis.com:443"


def _translation_endpoint(location: str) -> str:
    endpoints = {"europe-west1": "translate-eu.googleapis.com:443"}
    try:
        return endpoints[location]
    except KeyError as exc:
        raise RuntimeError(f"unsupported Google Translation location: {location}") from exc


def _tts_endpoint(location: str) -> str:
    return f"{location}-texttospeech.googleapis.com:443"


def _fixture_config() -> FixtureProfile:
    return FixtureProfile(kind="fixture")


def _google_config() -> GoogleProfile:
    credentials_path = Path(_required_env("GOOGLE_APPLICATION_CREDENTIALS"))
    return GoogleProfile(
        kind="google-eu",
        project=_required_env("GOOGLE_CLOUD_PROJECT"),
        quota_project=_required_env("GOOGLE_QUOTA_PROJECT"),
        credential_fingerprint=credential_fingerprint(credentials_path, "Google ADC"),
        probe_voice=_required_env("GOOGLE_TTS_VOICE"),
    )


def _google_speech_config() -> GoogleSpeechProfile:
    credentials_path = Path(_required_env("GOOGLE_APPLICATION_CREDENTIALS"))
    return GoogleSpeechProfile(
        kind="google-speech-eu",
        project=_required_env("GOOGLE_CLOUD_PROJECT"),
        quota_project=_required_env("GOOGLE_QUOTA_PROJECT"),
        credential_fingerprint=credential_fingerprint(credentials_path, "Google ADC"),
        probe_voice=_required_env("GOOGLE_TTS_VOICE"),
        probe_voice_locale=os.environ.get("GOOGLE_TTS_VOICE_LOCALE", "en-US").strip() or "en-US",
        speech_location=os.environ.get("GOOGLE_SPEECH_LOCATION", "europe-west3").strip()
        or "europe-west3",
        speech_model=os.environ.get("GOOGLE_SPEECH_MODEL", "long").strip() or "long",
        translation_location=os.environ.get("GOOGLE_TRANSLATION_LOCATION", "europe-west1").strip()
        or "europe-west1",
        translation_model=os.environ.get("GOOGLE_TRANSLATION_MODEL", "general/base").strip()
        or "general/base",
        tts_location=os.environ.get("GOOGLE_TTS_LOCATION", "eu").strip() or "eu",
    )


def _alternate_config(languages: tuple[str, ...]) -> AlternateProfile:
    unique_languages = tuple(dict.fromkeys(languages))
    deepgram_key_file = Path(_required_env("DEEPGRAM_API_KEY_FILE"))
    deepl_key_file = Path(_required_env("DEEPL_API_KEY_FILE"))
    deepgram_api_key, deepgram_fingerprint = _credential(deepgram_key_file, "Deepgram")
    deepl_api_key, deepl_fingerprint = _credential(deepl_key_file, "DeepL")
    return AlternateProfile(
        kind="deepgram-deepl-eu",
        deepgram_key_file=deepgram_key_file,
        deepl_key_file=deepl_key_file,
        deepgram_api_key=deepgram_api_key,
        deepl_api_key=deepl_api_key,
        language=languages[0],
        languages=unique_languages,
        voice=_required_env("DEEPGRAM_VOICE"),
        approved_voices=tuple(_required_env("DEEPGRAM_APPROVED_VOICES").split(",")),
        deepgram_credential_fingerprint=deepgram_fingerprint,
        deepl_credential_fingerprint=deepl_fingerprint,
        deepgram_streams=int(_required_env("DEEPGRAM_STREAMS")),
        deepgram_audio_seconds_minute=int(_required_env("DEEPGRAM_AUDIO_SECONDS_MINUTE")),
        deepl_requests_minute=int(_required_env("DEEPL_REQUESTS_MINUTE")),
        deepl_characters_minute=int(_required_env("DEEPL_CHARACTERS_MINUTE")),
    )


def resolve_profile_config(
    profile_id: str,
    languages: tuple[str, ...],
) -> ProfileConfig:
    if profile_id == "fixture":
        return _fixture_config()
    if profile_id == "google-eu":
        return _google_config()
    if profile_id == "google-speech-eu":
        return _google_speech_config()
    if profile_id == "deepgram-deepl-eu":
        return _alternate_config(languages)
    raise RuntimeError(f"unregistered provider profile: {profile_id}")


class ProviderRegistry:
    @staticmethod
    def resolve(
        profile_id: str,
        meeting_id: UUID,
        journal: ExchangeJournal,
        languages: tuple[str, ...],
    ) -> Providers:
        config = resolve_profile_config(profile_id, languages)
        return ProviderRegistry.construct(config, meeting_id, journal)

    @staticmethod
    def construct(
        config: ProfileConfig,
        meeting_id: UUID,
        journal: ExchangeJournal,
    ) -> Providers:
        if isinstance(config, FixtureProfile):
            return ProviderRegistry._construct_fixture(meeting_id)
        if isinstance(config, GoogleSpeechProfile):
            return ProviderRegistry._construct_google_speech(config, meeting_id, journal)
        if isinstance(config, GoogleProfile):
            return ProviderRegistry._construct_google(config, meeting_id, journal)
        return ProviderRegistry._construct_alternate(config, meeting_id, journal)

    @staticmethod
    def _construct_fixture(meeting_id: UUID) -> Providers:
        scenario = FixtureScenario.configured(meeting_id)
        return Providers(
            stt=FixtureSttProvider(scenario),
            translation=FixtureTranslationProvider(scenario),
            tts=FixtureTtsProvider(scenario),
        )

    @staticmethod
    def _construct_google(
        config: GoogleProfile, meeting_id: UUID, journal: ExchangeJournal
    ) -> Providers:
        google_config = GoogleConfig(
            project=config.project,
            quota_project=config.quota_project,
            meeting_id=meeting_id,
            credential_fingerprint=config.credential_fingerprint,
            probe_voice=config.probe_voice,
            probe_voice_locale=config.probe_voice_locale,
        )
        return ProviderRegistry._construct_google_pipeline(google_config, journal)

    @staticmethod
    def _construct_google_speech(
        config: GoogleSpeechProfile, meeting_id: UUID, journal: ExchangeJournal
    ) -> Providers:
        google_config = GoogleConfig(
            project=config.project,
            quota_project=config.quota_project,
            meeting_id=meeting_id,
            credential_fingerprint=config.credential_fingerprint,
            probe_voice=config.probe_voice,
            probe_voice_locale=config.probe_voice_locale,
            speech_location=config.speech_location,
            speech_endpoint=_speech_endpoint(config.speech_location),
            speech_model=config.speech_model,
            translation_location=config.translation_location,
            translation_endpoint=_translation_endpoint(config.translation_location),
            translation_model=config.translation_model,
            tts_location=config.tts_location,
            tts_endpoint=_tts_endpoint(config.tts_location),
        )
        return ProviderRegistry._construct_google_pipeline(google_config, journal)

    @staticmethod
    def _construct_google_pipeline(
        google_config: GoogleConfig, journal: ExchangeJournal
    ) -> Providers:
        channels = GoogleChannelPool(google_config.quota_project)
        return Providers(
            stt=GoogleSttProvider(google_config, journal, channel_pool=channels),
            translation=GoogleTranslationProvider(google_config, journal, channel_pool=channels),
            tts=GoogleTtsProvider(google_config, journal, channel_pool=channels),
            _close=channels.aclose,
        )

    @staticmethod
    def _construct_alternate(
        config: AlternateProfile, meeting_id: UUID, journal: ExchangeJournal
    ) -> Providers:
        deepgram_api_key = config.deepgram_api_key
        deepl_api_key = config.deepl_api_key
        deepgram_config = DeepgramConfig(
            deepgram_api_key,
            meeting_id,
            config.language,
            config.voice,
            config.approved_voices,
            config.deepgram_credential_fingerprint,
            "api.eu.deepgram.com",
            config.deepgram_streams,
            config.deepgram_audio_seconds_minute,
            config.languages or (config.language,),
        )
        deepl_config = DeepLConfig(
            deepl_api_key,
            meeting_id,
            config.deepl_credential_fingerprint,
            "https://api.deepl.com",
            "latency_optimized",
            config.deepl_requests_minute,
            config.deepl_characters_minute,
        )
        deepl = DeepLProvider(deepl_config, journal)
        return Providers(
            stt=DeepgramSttProvider(deepgram_config, journal),
            translation=deepl,
            tts=DeepgramTtsProvider(deepgram_config, journal),
            _close=deepl.aclose,
        )
