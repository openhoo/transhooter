from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID

import httpx
import pytest

from transhooter_worker.domain.models import RawRef, StageCapabilities
from transhooter_worker.provider_cli import (
    _approved_voice,
    _capability_refresh,
    _configure_journal_context,
    _effective_voice,
    _profile_id,
    _profile_voice,
    _publish_capabilities,
    _supports_capability_locale,
)
from transhooter_worker.runtime.provider_registry import (
    FixtureProfile,
    GoogleProfile,
    GoogleSpeechProfile,
    ProviderRegistry,
)


def fixture_stage_capabilities(
    name: str,
    languages: tuple[str, ...],
    voices: tuple[str, ...] = (),
) -> StageCapabilities:
    return StageCapabilities(
        provider="fixture",
        stage=name,
        endpoint=f"fixture://{name}",
        regions=("local",),
        languages=languages,
        models=("deterministic",),
        limits=(("sample_rate", 48_000),) if name == "tts" else (),
        evidence=RawRef(
            UUID(int=len(name)),
            1,
            name.rjust(64, "0"),
            1,
            "application/json",
        ),
        voices=voices,
    )


def fixture_capabilities() -> tuple[StageCapabilities, ...]:
    return (
        fixture_stage_capabilities("stt", ("en-US", "de-DE")),
        fixture_stage_capabilities("translation", ("en", "de")),
        fixture_stage_capabilities(
            "tts",
            ("en", "de"),
            ("fixture-thalia-en", "fixture-viktoria-de"),
        ),
    )


class RecordingAsyncClient:
    def __init__(
        self,
        status_code: int = 204,
        response_body: object | None = None,
    ) -> None:
        self.url: str | None = None
        self.headers: dict[str, str] | None = None
        self.payload: dict[str, object] | None = None
        self.status_code = status_code
        self.response_body = response_body

    async def __aenter__(self) -> RecordingAsyncClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def post(
        self,
        url: str,
        *,
        headers: dict[str, str],
        json: dict[str, object],
    ) -> httpx.Response:
        self.url = url
        self.headers = headers
        self.payload = json
        request = httpx.Request("POST", url)
        if self.response_body is None:
            return httpx.Response(self.status_code, request=request)
        return httpx.Response(self.status_code, request=request, json=self.response_body)


def test_refresh_contains_complete_bidirectional_frozen_selection() -> None:
    first = _capability_refresh(
        "fixture",
        FixtureProfile(kind="fixture"),
        "en-US",
        "de-DE",
        fixture_capabilities(),
    )
    second = _capability_refresh(
        "fixture",
        FixtureProfile(kind="fixture"),
        "en-US",
        "de-DE",
        fixture_capabilities(),
    )

    expected_profile_id = str(_profile_id("fixture"))
    assert first["profileId"] == expected_profile_id
    assert second["profileId"] == expected_profile_id
    rows = first["rows"]
    assert isinstance(rows, list)
    assert {(row["sourceLocale"], row["targetLocale"], row["mode"]) for row in rows} == {
        ("en-US", "en-US", "same_language"),
        ("de-DE", "de-DE", "same_language"),
        ("en-US", "de-DE", "translated"),
        ("de-DE", "en-US", "translated"),
    }
    translated = [row for row in rows if row["mode"] == "translated"]
    assert [row["snapshot"]["tts"]["voice"] for row in translated] == [
        "fixture-viktoria-de",
        "fixture-thalia-en",
    ]
    assert all(row["capabilityHash"] == first["capabilityHash"] for row in rows)
    assert "api-key" not in json.dumps(first)


@pytest.mark.asyncio
async def test_publish_uses_bearer_without_putting_token_in_payload(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    token = "private-control-token"
    token_file = tmp_path / "token"
    token_file.write_text(token)
    client = RecordingAsyncClient()

    monkeypatch.setenv("CAPABILITY_PUBLISH_URL", "http://web/api/internal/capabilities")
    monkeypatch.setenv("INTERNAL_TOKEN_FILE", str(token_file))
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_: client)
    payload = {"profileName": "fixture"}

    await _publish_capabilities(payload)

    assert client.url == "http://web/api/internal/capabilities"
    assert client.headers == {"authorization": f"Bearer {token}"}
    assert client.payload == payload
    assert token not in json.dumps(client.payload)


@pytest.mark.asyncio
async def test_publish_rejection_reports_only_bounded_status_and_server_code(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    token = "private-control-token"
    token_file = tmp_path / "token"
    token_file.write_text(token)
    client = RecordingAsyncClient(
        400,
        {
            "code": "STALE_CAPABILITY_REFRESH",
            "message": "sensitive server detail",
            "payload": "sensitive request echo",
            "issues": [
                {
                    "code": "invalid_type",
                    "path": ["rows", 0, "snapshot"],
                    "message": "sensitive validation detail",
                }
            ],
        },
    )
    monkeypatch.setenv("CAPABILITY_PUBLISH_URL", "http://web/api/internal/capabilities")
    monkeypatch.setenv("INTERNAL_TOKEN_FILE", str(token_file))
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_: client)

    with pytest.raises(
        RuntimeError,
        match=(
            r"^capability publication rejected with HTTP 400 "
            r"\(STALE_CAPABILITY_REFRESH\) \[rows\.0\.snapshot:invalid_type\]$"
        ),
    ) as raised:
        await _publish_capabilities({"profileName": "fixture"})

    diagnostic = str(raised.value)
    assert token not in diagnostic
    assert "sensitive server detail" not in diagnostic
    assert "sensitive request echo" not in diagnostic
    assert "sensitive validation detail" not in diagnostic
    assert "http://web" not in diagnostic


@pytest.mark.parametrize("locale", ["fr-CA", "fr-FR", "pt-BR", "pt-PT", "zh-CN", "zh-TW"])
def test_google_translation_generic_languages_admit_preserved_variants(locale: str) -> None:
    language = locale.split("-", 1)[0]
    capability = StageCapabilities(
        provider="google",
        stage="translation",
        endpoint="translate-eu.googleapis.com",
        regions=("europe-west1",),
        languages=(language,),
        models=("general/base",),
        limits=(),
        evidence=None,
    )

    assert _supports_capability_locale(capability, locale, language_only=True)


def google_tts_capabilities(voices: tuple[str, ...]) -> StageCapabilities:
    return StageCapabilities(
        provider="google",
        stage="tts",
        endpoint="eu-texttospeech.googleapis.com",
        regions=("eu",),
        languages=("de-DE", "en-US"),
        models=("Chirp3-HD",),
        limits=(("sample_rate", 48_000),),
        evidence=None,
        voices=voices,
    )


def test_google_voice_selection_preserves_locale_association() -> None:
    capabilities = google_tts_capabilities(
        (
            "en-US-Chirp3-HD-Achernar",
            "de-DE-Chirp3-HD-Algenib",
            "de-DE-Chirp3-HD-Achernar",
        )
    )

    assert _approved_voice(capabilities, "de-DE") == "de-DE-Chirp3-HD-Achernar"


@pytest.mark.parametrize(
    ("voices", "requested"),
    [
        (("en-US-Chirp3-HD-Achernar",), None),
        (("de-DE-Chirp3-HD-Achernar",), "en-US-Chirp3-HD-Achernar"),
        (("de-DE-Chirp3-HD-Achernar",), "de-DE-unapproved"),
    ],
)
def test_google_voice_selection_rejects_missing_wrong_locale_and_unapproved_request(
    voices: tuple[str, ...],
    requested: str | None,
) -> None:
    with pytest.raises(RuntimeError, match="capability-approved voice|no approved voice"):
        _approved_voice(google_tts_capabilities(voices), "de-DE", requested)


def test_google_smoke_voice_does_not_replace_discovered_target_catalog() -> None:
    profile = GoogleProfile(
        kind="google-eu",
        project="project",
        quota_project="quota",
        credential_fingerprint="fingerprint",
        probe_voice="en-US-Chirp3-HD-Achernar",
    )
    discovered = google_tts_capabilities(
        (
            "en-US-Chirp3-HD-Achernar",
            "de-DE-Chirp3-HD-Algenib",
        )
    )

    voice = _effective_voice(profile, "de-DE", None, discovered)

    assert voice == "de-DE-Chirp3-HD-Algenib"


def test_google_speech_profile_selects_and_binds_target_voice() -> None:
    profile = GoogleSpeechProfile(
        kind="google-speech-eu",
        project="project",
        quota_project="quota",
        credential_fingerprint="fingerprint",
        probe_voice="en-US-Chirp3-HD-Achernar",
    )
    discovered = google_tts_capabilities(("de-DE-Chirp3-HD-Algenib",))

    voice = _effective_voice(profile, "de-DE", None, discovered)
    execution_profile = _profile_voice(profile, voice, "de-DE")

    assert voice == "de-DE-Chirp3-HD-Algenib"
    assert isinstance(execution_profile, GoogleSpeechProfile)
    assert execution_profile.probe_voice == voice
    assert execution_profile.probe_voice_locale == "de-DE"


def test_google_missing_discovered_target_voice_fails_closed() -> None:
    profile = GoogleProfile(
        kind="google-eu",
        project="project",
        quota_project="quota",
        credential_fingerprint="fingerprint",
        probe_voice="en-US-Chirp3-HD-Achernar",
    )

    with pytest.raises(RuntimeError, match="no approved voice"):
        _effective_voice(
            profile,
            "de-DE",
            None,
            google_tts_capabilities(("en-US-Chirp3-HD-Achernar",)),
        )


def test_effective_voice_is_identical_in_journal_and_provider_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from transhooter_worker.adapters.google import provider as google_provider

    profile = GoogleProfile(
        kind="google-eu",
        project="project",
        quota_project="quota",
        credential_fingerprint="fingerprint",
        probe_voice="de-DE-Chirp3-HD-Achernar",
    )
    voice = _effective_voice(
        profile,
        "de-DE",
        None,
        google_tts_capabilities(("de-DE-Chirp3-HD-Achernar",)),
    )
    execution_profile = _profile_voice(profile, voice, "de-DE")

    class Journal:
        context: dict[str, object]

        def set_context(self, context: dict[str, object]) -> None:
            self.context = context

    def reject_channel_creation(*_: object) -> object:
        raise AssertionError("configuration construction must not load ADC or create channels")

    monkeypatch.setattr(google_provider, "authenticated_channel", reject_channel_creation)
    journal = Journal()
    _configure_journal_context(
        journal,  # type: ignore[arg-type]
        UUID(int=90),
        "google-eu",
        "en-US",
        "de-DE",
        voice,
    )

    providers = ProviderRegistry.construct(
        execution_profile,
        UUID(int=90),
        journal,  # type: ignore[arg-type]
    )

    assert journal.context["voice"] == voice
    assert isinstance(execution_profile, GoogleProfile)
    assert execution_profile.probe_voice == voice
    assert execution_profile.probe_voice_locale == "de-DE"
    assert providers.tts._config.probe_voice == journal.context["voice"]
    assert providers.tts._config.probe_voice_locale == "de-DE"
