from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, Field

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
    FixtureProfile | GoogleProfile | AlternateProfile, Field(discriminator="kind")
]


@dataclass(frozen=True, slots=True)
class Providers:
    stt: StreamingSttProvider
    translation: TextTranslationProvider
    tts: StreamingTtsProvider


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


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
        return Providers(
            stt=GoogleSttProvider(google_config, journal),
            translation=GoogleTranslationProvider(google_config, journal),
            tts=GoogleTtsProvider(google_config, journal),
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
        return Providers(
            stt=DeepgramSttProvider(deepgram_config, journal),
            translation=DeepLProvider(deepl_config, journal),
            tts=DeepgramTtsProvider(deepgram_config, journal),
        )
