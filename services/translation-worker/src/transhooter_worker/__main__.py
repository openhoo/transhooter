from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import statistics
import sys
import tempfile
import time
import wave
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import boto3  # type: ignore[import-untyped]
import httpx

from transhooter_worker.adapters.s3_archive import S3Archive
from transhooter_worker.adapters.spool import (
    CapacityProbe,
    EncryptedSpool,
    deterministic_roomy_capacity,
    statvfs_capacity,
)
from transhooter_worker.domain.models import RawRef, StageCapabilities
from transhooter_worker.runtime.capability_store import CapabilityStore
from transhooter_worker.runtime.job import run_worker
from transhooter_worker.runtime.probe import execute_probe
from transhooter_worker.runtime.provider_registry import (
    AlternateProfile,
    FixtureProfile,
    GoogleProfile,
    ProfileConfig,
    ProviderRegistry,
    resolve_profile_config,
)
from transhooter_worker.telemetry import configure_telemetry


def _add_provider_command_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument(
        "--profile",
        default=os.environ.get("PROVIDER_PROFILE", "google-eu"),
    )
    command.add_argument("--source", default="en-US")
    command.add_argument("--target", default="de-DE")
    command.add_argument("--voice")
    command.add_argument("--audio", type=Path)
    command.add_argument("--profiles")
    command.add_argument("--fixtures", type=Path)
    command.add_argument("--new-run", action="store_true")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="transhooter-worker")
    commands = root.add_subparsers(dest="group", required=True)
    providers = commands.add_parser("providers").add_subparsers(dest="command", required=True)
    for command_name in ("preflight", "sync", "probe", "benchmark"):
        _add_provider_command_arguments(providers.add_parser(command_name))
    return root


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _capacity_probe() -> CapacityProbe:
    if os.environ.get("APP_ENV") == "test":
        return deterministic_roomy_capacity
    return statvfs_capacity


def _spool() -> EncryptedSpool:
    root = Path(os.environ.get("SPOOL_DIR", tempfile.mkdtemp(prefix="transhooter-spool-")))
    capacity_probe = _capacity_probe()
    keyring_file = os.environ.get("SPOOL_KEYRING_FILE")
    if keyring_file:
        return EncryptedSpool.from_keyring(
            root,
            root / "journal.sqlite3",
            Path(keyring_file),
            capacity_probe=capacity_probe,
        )
    encoded = os.environ.get("SPOOL_KEY_B64")
    if encoded:
        key = base64.b64decode(encoded, validate=True)
    elif os.environ.get("APP_ENV") == "test":
        key = b"x" * 32
    else:
        raise RuntimeError("SPOOL_KEYRING_FILE is required")
    return EncryptedSpool(
        root,
        root / "journal.sqlite3",
        {"cli-v1": key},
        "cli-v1",
        capacity_probe=capacity_probe,
    )


def _profile_id(name: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"https://transhooter.local/provider-profiles/{name}")


def _credential(profile: ProfileConfig, stage: str) -> dict[str, str]:
    if isinstance(profile, GoogleProfile):
        return {"reference": "google-adc", "version": profile.credential_fingerprint}
    if isinstance(profile, AlternateProfile):
        if stage == "translation":
            return {"reference": "deepl-api-key", "version": profile.deepl_credential_fingerprint}
        return {"reference": "deepgram-api-key", "version": profile.deepgram_credential_fingerprint}
    return {"reference": "fixture", "version": "fixture"}


def _language_code(provider: str, locale: str) -> str:
    code = locale.split("-", 1)[0]
    return code.upper() if provider == "deepl" else code.lower()


def _voice_matches_locale(capability: StageCapabilities, voice: str, locale: str) -> bool:
    normalized_voice = voice.casefold()
    normalized_locale = locale.casefold()
    language = normalized_locale.split("-", 1)[0]
    if capability.provider == "fixture" and voice == "fixture-voice":
        return normalized_locale in {language.casefold() for language in capability.languages}
    if capability.provider == "google":
        return normalized_voice.startswith(f"{normalized_locale}-")
    return normalized_voice.endswith(f"-{normalized_locale}") or normalized_voice.endswith(
        f"-{language}"
    )


def _approved_voice(
    capability: StageCapabilities,
    locale: str,
    requested: str | None = None,
) -> str:
    matching = tuple(
        sorted(
            voice for voice in capability.voices if _voice_matches_locale(capability, voice, locale)
        )
    )
    if requested is not None:
        if requested not in matching:
            raise RuntimeError(
                f"{requested} is not a capability-approved voice for target locale {locale}"
            )
        return requested
    if not matching:
        raise RuntimeError(
            f"{capability.provider} TTS capability has no approved voice for target locale {locale}"
        )
    return matching[0]


def _frozen_stage(
    profile: FixtureProfile | GoogleProfile | AlternateProfile,
    capability: StageCapabilities,
) -> dict[str, object]:
    if not capability.models or not capability.regions:
        raise RuntimeError(f"{capability.provider} {capability.stage} capability is incomplete")
    return {
        "provider": capability.provider,
        "endpoint": capability.endpoint,
        "region": capability.regions[0],
        "model": capability.models[0],
        "adapterBuild": "transhooter-worker@0.1.0",
        "policy": "provider-profile-v1",
        "credential": _credential(profile, capability.stage),
        "limits": dict(capability.limits),
    }


def _stage_capabilities(
    capabilities: tuple[StageCapabilities, ...],
) -> dict[str, StageCapabilities]:
    stages = {capability.stage: capability for capability in capabilities}
    if set(stages) != {"stt", "translation", "tts"}:
        raise RuntimeError("provider refresh requires exactly STT, translation, and TTS")
    return stages


def _stt_snapshot(
    profile: FixtureProfile | GoogleProfile | AlternateProfile,
    capability: StageCapabilities,
    locale: str,
) -> dict[str, object]:
    return {
        **_frozen_stage(profile, capability),
        "locale": locale,
        "encoding": "linear16",
    }


def _same_language_row(
    profile: FixtureProfile | GoogleProfile | AlternateProfile,
    stt: StageCapabilities,
    locale: str,
) -> dict[str, object]:
    return {
        "sourceLocale": locale,
        "targetLocale": locale,
        "mode": "same_language",
        "enabled": True,
        "snapshot": {
            "mode": "same_language",
            "stt": _stt_snapshot(profile, stt, locale),
            "bypass": True,
        },
    }


def _translated_row(
    profile: FixtureProfile | GoogleProfile | AlternateProfile,
    stt: StageCapabilities,
    translation: StageCapabilities,
    tts: StageCapabilities,
    source_locale: str,
    target_locale: str,
) -> dict[str, object]:
    source_code = _language_code(translation.provider, source_locale)
    target_code = _language_code(translation.provider, target_locale)
    translation_snapshot = {
        **_frozen_stage(profile, translation),
        "sourceCode": source_code,
        "targetCode": target_code,
    }
    tts_snapshot = {
        **_frozen_stage(profile, tts),
        "locale": target_locale,
        "voice": _approved_voice(tts, target_locale),
        "encoding": "linear16",
        "sampleRate": dict(tts.limits).get("sample_rate", 48_000),
    }
    return {
        "sourceLocale": source_locale,
        "targetLocale": target_locale,
        "mode": "translated",
        "enabled": True,
        "snapshot": {
            "mode": "translated",
            "stt": _stt_snapshot(profile, stt, source_locale),
            "targetCode": target_code,
            "translation": translation_snapshot,
            "tts": tts_snapshot,
        },
    }


def _supports_capability_locale(
    capability: StageCapabilities,
    locale: str,
    *,
    language_only: bool,
) -> bool:
    normalized_locale = locale.casefold()
    normalized_language = (
        _language_code(capability.provider, locale).casefold()
        if language_only
        else normalized_locale.split("-", 1)[0]
    )
    return any(
        normalized == normalized_locale if "-" in normalized else normalized == normalized_language
        for normalized in (language.casefold() for language in capability.languages)
    )


def _require_capability_locale(
    capability: StageCapabilities,
    locale: str,
    *,
    role: str,
    language_only: bool = False,
) -> None:
    if not _supports_capability_locale(capability, locale, language_only=language_only):
        raise RuntimeError(
            f"configured {role} locale {locale} is not supported by "
            f"{capability.provider} {capability.stage} capabilities"
        )


def _validate_capability_locales(
    source: str,
    target: str,
    stages: dict[str, StageCapabilities],
) -> None:
    stt = stages["stt"]
    _require_capability_locale(stt, source, role="source")
    if source == target:
        return

    translation = stages["translation"]
    tts = stages["tts"]
    _require_capability_locale(
        translation,
        source,
        role="source",
        language_only=True,
    )
    _require_capability_locale(
        translation,
        target,
        role="target",
        language_only=True,
    )
    _require_capability_locale(tts, target, role="target")

    # The catalog is bidirectional, including a same-language row for each
    # configured locale, so the reverse direction must be fully supported too.
    _require_capability_locale(stt, target, role="target")
    _require_capability_locale(tts, source, role="source")


def _capability_rows(
    profile: FixtureProfile | GoogleProfile | AlternateProfile,
    source: str,
    target: str,
    stages: dict[str, StageCapabilities],
) -> list[dict[str, object]]:
    _validate_capability_locales(source, target, stages)
    locales = tuple(dict.fromkeys((source, target)))
    rows = [_same_language_row(profile, stages["stt"], locale) for locale in locales]
    if source != target:
        rows.extend(
            _translated_row(
                profile,
                stages["stt"],
                stages["translation"],
                stages["tts"],
                source_locale,
                target_locale,
            )
            for source_locale, target_locale in ((source, target), (target, source))
        )
    return rows


def _capability_evidence(
    capabilities: tuple[StageCapabilities, ...],
) -> list[dict[str, object]]:
    return [
        {
            "stage": capability.stage,
            "objectId": (str(capability.evidence.object_id) if capability.evidence else None),
            "sha256": capability.evidence.sha256 if capability.evidence else None,
        }
        for capability in capabilities
    ]


def _capability_hash(
    profile_name: str,
    rows: list[dict[str, object]],
    evidence: list[dict[str, object]],
) -> str:
    canonical_payload = json.dumps(
        {"profile": profile_name, "rows": rows, "evidence": evidence},
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical_payload.encode()).hexdigest()


def _credential_references(
    profile: FixtureProfile | GoogleProfile | AlternateProfile,
) -> list[dict[str, str]]:
    credentials = sorted(
        {
            (credential["reference"], credential["version"])
            for stage in ("stt", "translation", "tts")
            for credential in (_credential(profile, stage),)
        }
    )
    return [{"reference": reference, "version": version} for reference, version in credentials]


def _capability_refresh(
    profile_name: str,
    profile: FixtureProfile | GoogleProfile | AlternateProfile,
    source: str,
    target: str,
    capabilities: tuple[StageCapabilities, ...],
) -> dict[str, object]:
    stages = _stage_capabilities(capabilities)
    raw_rows = _capability_rows(profile, source, target, stages)
    capability_hash = _capability_hash(
        profile_name,
        raw_rows,
        _capability_evidence(capabilities),
    )
    fresh_until = (
        datetime.now(UTC)
        + timedelta(seconds=int(os.environ.get("PROFILE_CAPABILITY_TTL_SECONDS", "3600")))
    ).isoformat()
    rows = [
        {
            **row,
            "capabilityHash": capability_hash,
            "freshUntil": fresh_until,
        }
        for row in raw_rows
    ]
    return {
        "profileId": str(_profile_id(profile_name)),
        "profileName": profile_name,
        "revision": 1,
        "capabilityHash": capability_hash,
        "adapterBuilds": {"translationWorker": "0.1.0"},
        "policy": {"name": "provider-profile-v1", "region": "eu"},
        "credentialReferences": _credential_references(profile),
        "complete": True,
        "rows": rows,
    }


async def _publish_capabilities(payload: dict[str, object]) -> None:
    url = os.environ.get("CAPABILITY_PUBLISH_URL")
    if not url:
        return
    token_file = os.environ.get("INTERNAL_TOKEN_FILE")
    if not token_file:
        raise RuntimeError("INTERNAL_TOKEN_FILE is required for capability publication")
    token = Path(token_file).read_text("utf-8").strip()
    if not token:
        raise RuntimeError("capability publication token is empty")
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            url,
            headers={"authorization": f"Bearer {token}"},
            json=payload,
        )
    response.raise_for_status()


def _wav_seconds(path: Path) -> float:
    try:
        with wave.open(str(path), "rb") as source:
            return source.getnframes() / source.getframerate()
    except (wave.Error, OSError, ZeroDivisionError) as exc:
        raise RuntimeError(f"benchmark audio fixture is not a valid WAV: {path}") from exc


def _effective_voice(
    profile: ProfileConfig,
    target: str,
    requested: str | None,
    capability: StageCapabilities,
) -> str:
    expected_provider = (
        "fixture"
        if isinstance(profile, FixtureProfile)
        else "google"
        if isinstance(profile, GoogleProfile)
        else "deepgram"
    )
    if capability.provider != expected_provider or capability.stage != "tts":
        raise RuntimeError("selected profile returned an unexpected TTS capability catalog")
    return _approved_voice(capability, target, requested)


def _profile_voice(profile: ProfileConfig, voice: str, locale: str) -> ProfileConfig:
    if isinstance(profile, GoogleProfile):
        return profile.model_copy(update={"probe_voice": voice, "probe_voice_locale": locale})
    if isinstance(profile, AlternateProfile):
        return profile.model_copy(update={"voice": voice})
    return profile


def _configure_journal_context(
    journal: EncryptedSpool,
    run_id: UUID,
    profile_name: str,
    source: str,
    target: str,
    voice: str | None,
) -> None:
    journal.set_context(
        {
            "schemaVersion": 1,
            "runId": str(run_id),
            "runEpoch": 1,
            "writeEpoch": 0,
            "profileId": profile_name,
            "sourceLanguage": source,
            "targetLanguage": target,
            "voice": voice,
        }
    )


def _probe_archive_secret(name: str) -> str:
    secret_file = os.environ.get(name + "_FILE")
    value = (
        Path(secret_file).read_text("utf-8").strip()
        if secret_file
        else os.environ.get(name, "").strip()
    )
    if not value:
        raise RuntimeError(f"{name}_FILE is required for probe evidence")
    return value


def _probe_archive() -> S3Archive:
    client = boto3.client(
        "s3",
        endpoint_url=_required("S3_ENDPOINT"),
        region_name=os.environ.get("S3_REGION", "eu-central-1"),
        aws_access_key_id=_probe_archive_secret("S3_ACCESS_KEY"),
        aws_secret_access_key=_probe_archive_secret("S3_SECRET_KEY"),
    )
    multipart_database = (
        Path(os.environ.get("SPOOL_DIR", tempfile.mkdtemp())) / "probe-multipart.sqlite3"
    )
    return S3Archive(
        client,
        _probe_archive_secret("S3_BUCKET"),
        os.environ.get("S3_KMS_KEY_ID"),
        os.environ.get("ARCHIVE_REQUIRE_KMS", "true").lower() == "true",
        multipart_database,
    )


def _archive_probe_evidence(
    meeting_id: UUID,
    journal: EncryptedSpool,
    evidence: list[RawRef],
) -> list[dict[str, str]]:
    archive = _probe_archive()
    archived: list[dict[str, str]] = []
    for reference in evidence:
        owner, attempt, stage, ordinal, media_type = journal.context(reference.object_id)
        if owner != meeting_id:
            raise RuntimeError("stale probe evidence owner mismatch")
        body = journal.read(reference.object_id)
        record = archive.put_create_once(
            (f"v1/meetings/{meeting_id}/pipeline/{stage}/probe/{attempt}/{ordinal:020d}.bin"),
            body,
            media_type,
            hashlib.sha256(body).hexdigest(),
        )
        archived.append(
            {
                "key": record.key,
                "versionId": record.version_id,
                "checksum": record.s3_checksum,
            }
        )
    return archived


async def _one_probe(
    profile_name: str,
    source: str,
    target: str,
    voice: str | None,
    audio: Path,
) -> dict[str, object]:
    meeting_id = uuid4()
    profile = resolve_profile_config(profile_name, (source, target))
    journal = _spool()
    _configure_journal_context(
        journal,
        meeting_id,
        profile_name,
        source,
        target,
        None,
    )
    before_ids = {ref.object_id for ref, _ in journal.committed()}
    bootstrap_providers = ProviderRegistry.construct(profile, meeting_id, journal)
    capabilities = await asyncio.gather(
        bootstrap_providers.stt.capabilities(),
        bootstrap_providers.translation.capabilities(),
        bootstrap_providers.tts.capabilities(),
    )
    effective_voice = _effective_voice(
        profile,
        target,
        voice,
        _stage_capabilities(tuple(capabilities))["tts"],
    )
    profile = _profile_voice(profile, effective_voice, target)
    _configure_journal_context(
        journal,
        meeting_id,
        profile_name,
        source,
        target,
        effective_voice,
    )
    providers = ProviderRegistry.construct(profile, meeting_id, journal)
    probe_input = audio.read_bytes()
    journal.append(
        meeting_id=meeting_id,
        attempt_id=meeting_id,
        stage="probe-input",
        transport="grpc",
        direction="internal",
        media_type="audio/wav",
        payload=probe_input,
    )
    health = await asyncio.gather(
        *(
            provider.health(str(meeting_id))
            for provider in (providers.stt, providers.translation, providers.tts)
        )
    )
    if not all(item.healthy for item in health):
        raise RuntimeError(f"selected profile {profile_name} has an unhealthy stage")
    started = time.monotonic_ns()
    result = await execute_probe(providers, audio, source, target, effective_voice, meeting_id)
    elapsed_ms = (time.monotonic_ns() - started) / 1_000_000
    evidence = [ref for ref, _ in journal.committed() if ref.object_id not in before_ids]
    if not evidence:
        raise RuntimeError("probe produced no durable raw provider evidence")
    archived = (
        [] if profile_name == "fixture" else _archive_probe_evidence(meeting_id, journal, evidence)
    )
    return {
        "runId": str(result.run_id),
        "profile": profile_name,
        "source": source,
        "target": target,
        "transcript": result.transcript,
        "translation": result.translation,
        "ttsSha256": result.raw_sha256,
        "ttsBytes": len(result.synthesized_pcm),
        "attemptIds": [str(value) for value in result.provider_attempt_ids],
        "rawEvidenceCount": len(evidence),
        "archivedEvidence": archived,
        "elapsedMs": elapsed_ms,
        "stages": [item.provider for item in capabilities],
    }


def _capability_database_path() -> Path:
    default_directory = os.environ.get("SPOOL_DIR", tempfile.gettempdir())
    default_path = os.path.join(default_directory, "capabilities.sqlite3")
    return Path(os.environ.get("CAPABILITY_DATABASE", default_path))


def _capability_summary(capability: StageCapabilities) -> dict[str, object]:
    return {
        "provider": capability.provider,
        "stage": capability.stage,
        "endpoint": capability.endpoint,
        "languages": capability.languages,
        "models": capability.models,
        "voices": capability.voices,
        "limits": capability.limits,
    }


async def _run_refresh_or_preflight(args: argparse.Namespace) -> dict[str, object]:
    run_id = uuid4()
    profile = resolve_profile_config(args.profile, (args.source, args.target))
    journal = _spool()
    _configure_journal_context(
        journal,
        run_id,
        args.profile,
        args.source,
        args.target,
        None,
    )
    bootstrap_providers = ProviderRegistry.construct(profile, run_id, journal)
    bootstrap_stages = (
        bootstrap_providers.stt,
        bootstrap_providers.translation,
        bootstrap_providers.tts,
    )
    discovered = await asyncio.gather(*(provider.capabilities() for provider in bootstrap_stages))
    effective_voice = _effective_voice(
        profile,
        args.target,
        args.voice,
        _stage_capabilities(tuple(discovered))["tts"],
    )
    profile = _profile_voice(profile, effective_voice, args.target)
    _configure_journal_context(
        journal,
        run_id,
        args.profile,
        args.source,
        args.target,
        effective_voice,
    )
    providers = ProviderRegistry.construct(profile, run_id, journal)
    stage_providers = (providers.stt, providers.translation, providers.tts)
    health_results = await asyncio.gather(
        *(provider.health(str(run_id)) for provider in stage_providers)
    )
    if not all(result.healthy for result in health_results):
        raise RuntimeError("provider capability refresh health probe failed")

    capabilities: tuple[StageCapabilities, ...] = tuple(
        replace(capability, evidence=capability.evidence or health.evidence)
        for capability, health in zip(discovered, health_results, strict=True)
    )
    if any(capability.evidence is None for capability in capabilities):
        raise RuntimeError("provider capability refresh lacks durable probe evidence")

    result: dict[str, object] = {
        "runId": str(run_id),
        "profile": args.profile,
        "capabilities": [_capability_summary(capability) for capability in capabilities],
    }
    if args.command == "preflight":
        result["healthy"] = True
        return result

    stored_revision = CapabilityStore(_capability_database_path()).replace(
        args.profile,
        capabilities,
    )
    result.update(stored_revision)
    published_profile = _capability_refresh(
        args.profile,
        profile,
        args.source,
        args.target,
        capabilities,
    )
    await _publish_capabilities(published_profile)
    result["publishedProfileId"] = published_profile["profileId"]
    result["publishedCapabilityHash"] = published_profile["capabilityHash"]
    return result


def _load_benchmark_fixtures(manifest_path: Path) -> list[object]:
    manifest = json.loads(manifest_path.read_text("utf-8"))
    if not isinstance(manifest, dict) or manifest.get("schemaVersion") != 1:
        raise RuntimeError("benchmark manifest schemaVersion 1 is required")
    fixtures = manifest.get("fixtures")
    if not isinstance(fixtures, list) or not fixtures:
        raise RuntimeError("benchmark manifest must contain a non-empty fixtures array")
    return fixtures


def _validated_benchmark_fixture(
    fixture: object,
    manifest_path: Path,
) -> tuple[str, str, list[str], Path]:
    if not isinstance(fixture, dict):
        raise RuntimeError("benchmark fixture must be an object")
    fixture_id = fixture.get("id")
    source = fixture.get("sourceLanguage")
    targets = fixture.get("targetLanguages")
    audio_value = fixture.get("audio")
    validated_targets: list[str] = []
    valid_targets = False
    if isinstance(targets, list):
        for target in targets:
            if not isinstance(target, str) or not target:
                break
            validated_targets.append(target)
        valid_targets = bool(validated_targets) and len(validated_targets) == len(targets)
    if (
        not isinstance(fixture_id, str)
        or not fixture_id
        or not isinstance(source, str)
        or not source
        or not isinstance(audio_value, str)
        or not audio_value
        or not valid_targets
    ):
        raise RuntimeError(
            "benchmark fixture requires id, audio, sourceLanguage, and non-empty targetLanguages"
        )

    audio_path = Path(audio_value)
    if not audio_path.is_absolute():
        audio_path = manifest_path.parent / audio_path
    if not audio_path.is_file():
        raise RuntimeError(f"benchmark audio fixture is missing: {audio_path}")
    return fixture_id, source, validated_targets, audio_path


async def _benchmark_row(
    profile_name: str,
    fixture: object,
    manifest_path: Path,
    default_voice: str | None,
) -> list[dict[str, object]]:
    fixture_id, source, targets, audio_path = _validated_benchmark_fixture(
        fixture,
        manifest_path,
    )
    fixture_voice = fixture.get("voice") if isinstance(fixture, dict) else None
    requested_voice = str(fixture_voice) if fixture_voice else default_voice
    rows: list[dict[str, object]] = []
    for target in targets:
        row = await _one_probe(
            profile_name,
            source,
            target,
            requested_voice,
            audio_path,
        )
        row["fixtureId"] = fixture_id
        row["referenceText"] = fixture.get("referenceText") if isinstance(fixture, dict) else None
        row["usageUnits"] = {
            "sttAudioSeconds": round(_wav_seconds(audio_path), 6),
            "translationCharacters": len(str(row["transcript"])),
            "ttsCharacters": len(str(row["translation"])),
        }
        rows.append(row)
    return rows


def _benchmark_summary(
    profile_names: tuple[str, ...],
    rows: list[dict[str, object]],
) -> dict[str, object]:
    latency_values = [row["elapsedMs"] for row in rows]
    if not all(isinstance(value, int | float) for value in latency_values):
        raise RuntimeError("benchmark produced a non-numeric latency")
    latencies = sorted(float(value) for value in latency_values if isinstance(value, int | float))
    p95_index = min(
        len(latencies) - 1,
        max(0, (95 * len(latencies) + 99) // 100 - 1),
    )
    return {
        "schemaVersion": 1,
        "profiles": profile_names,
        "runs": rows,
        "p50Ms": statistics.median(latencies),
        "p95Ms": latencies[p95_index],
        "usageUnits": [{"runId": row["runId"], "units": row["usageUnits"]} for row in rows],
        "rawAttemptIds": [{"runId": row["runId"], "attemptIds": row["attemptIds"]} for row in rows],
        "humanScoring": [
            {
                "runId": row["runId"],
                "fixtureId": row["fixtureId"],
                "referenceText": row["referenceText"],
                "translation": row["translation"],
            }
            for row in rows
        ],
    }


async def _run_benchmark(args: argparse.Namespace) -> dict[str, object]:
    if args.fixtures is None or not args.fixtures.is_file():
        raise RuntimeError("--fixtures manifest is required")
    fixtures = _load_benchmark_fixtures(args.fixtures)
    profile_names = tuple(
        name.strip() for name in (args.profiles or args.profile).split(",") if name.strip()
    )
    rows: list[dict[str, object]] = []
    for profile_name in profile_names:
        for fixture in fixtures:
            rows.extend(
                await _benchmark_row(
                    profile_name,
                    fixture,
                    args.fixtures,
                    args.voice,
                )
            )
    return _benchmark_summary(profile_names, rows)


async def run(args: argparse.Namespace) -> dict[str, object]:
    if args.command in {"sync", "preflight"}:
        return await _run_refresh_or_preflight(args)
    if args.command == "probe":
        if args.audio is None or not args.audio.is_file():
            raise RuntimeError("--audio fixture is required")
        return await _one_probe(
            args.profile,
            args.source,
            args.target,
            args.voice,
            args.audio,
        )
    return await _run_benchmark(args)


def main() -> None:
    raw_interval = os.environ.get("OTEL_METRIC_EXPORT_INTERVAL", "").strip()
    try:
        metric_export_interval_millis = int(raw_interval) if raw_interval else None
    except ValueError:
        metric_export_interval_millis = None
    telemetry = configure_telemetry(
        service_name=os.environ.get("OTEL_SERVICE_NAME") or "transhooter-translation-worker",
        environment=os.environ.get("APP_ENV"),
        metric_export_interval_millis=metric_export_interval_millis,
    )
    try:
        if len(sys.argv) == 1:
            sys.argv.append("start")
        if sys.argv[1] != "providers":
            run_worker()
            return
        args = parser().parse_args()
        try:
            result = asyncio.run(run(args))
        except Exception as exc:
            raise SystemExit(f"provider command failed: {exc}") from exc
        print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    finally:
        telemetry.shutdown()


if __name__ == "__main__":
    main()
