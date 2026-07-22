from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from transhooter_worker.adapters.fixture.provider import (
    FIXTURE_REF,
    FixtureSttProvider,
    FixtureTranslationProvider,
    FixtureTtsProvider,
)
from transhooter_worker.adapters.spool import EncryptedSpool, deterministic_roomy_capacity
from transhooter_worker.application.session import DirectionSession, DirectionSpec
from transhooter_worker.domain.models import (
    AudioChunk,
    ErrorKind,
    OperationTerminal,
    Outcome,
    ProviderError,
    RetryAction,
    RetryAdvice,
    RetryDecision,
    SampleRange,
    SessionTerminal,
    Transport,
)
from transhooter_worker.runtime.control_client import ControlClient
from transhooter_worker.runtime.job import (
    DirectionMetadata,
    JobMetadata,
    RoomProviderSelectionMetadata,
    _build_provider_terminal_sink,
)


class CapturingControl:
    def __init__(self) -> None:
        self.reports: list[tuple[dict[str, Any], UUID | None]] = []

    async def provider_attempt(
        self, payload: dict[str, Any], *, event_id: UUID | None = None
    ) -> None:
        self.reports.append((payload, event_id))


def stage(provider: str, suffix: str) -> dict[str, object]:
    base: dict[str, object] = {
        "provider": provider,
        "endpoint": f"fixture://{suffix}",
        "region": "test",
        "model": "deterministic",
        "adapterBuild": "fixture-v1",
        "policy": "frozen-v1",
        "credential": {"reference": "fixture", "version": "fixture"},
        "limits": {},
    }
    if suffix == "stt":
        return base | {"locale": "en-US", "encoding": "LINEAR16"}
    if suffix == "translation":
        return base | {"sourceCode": "en", "targetCode": "de"}
    return base | {
        "locale": "de-DE",
        "voice": "fixture-voice",
        "encoding": "LINEAR16",
        "sampleRate": 48_000,
    }


def direction(source: UUID, destination: UUID) -> DirectionMetadata:
    return DirectionMetadata.model_validate(
        {
            "mode": "translated",
            "sourceParticipantId": str(source),
            "destinationParticipantId": str(destination),
            "capabilityRowId": str(uuid4()),
            "stt": stage("fixture", "stt"),
            "targetCode": "de",
            "translation": stage("fixture", "translation"),
            "tts": stage("fixture", "tts"),
        }
    )


def metadata(first: DirectionMetadata, second: DirectionMetadata) -> JobMetadata:
    selection = RoomProviderSelectionMetadata(
        profile_id="fixture",
        profile_revision=1,
        capability_hash="a" * 64,
        participant_ids=(first.source_participant_id, second.source_participant_id),
        directions=(first, second),
    )
    canonical = json.dumps(
        selection.model_dump(mode="json", by_alias=True, exclude_none=True),
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return JobMetadata.model_validate(
        {
            "schemaVersion": 1,
            "consultationId": str(uuid4()),
            "generation": 1,
            "roomName": str(uuid4()),
            "workerIdentity": str(uuid4()),
            "workerEpoch": 1,
            "writeEpoch": 0,
            "expectedParticipantIds": [str(value) for value in selection.participant_ids],
            "expectedLivekitIdentities": [str(uuid4()), str(uuid4())],
            "providerSelection": selection.model_dump(mode="json", by_alias=True),
            "snapshotHash": hashlib.sha256(canonical).hexdigest(),
        }
    )


def spool(tmp_path: Path) -> EncryptedSpool:
    return EncryptedSpool(
        tmp_path / "payloads",
        tmp_path / "spool.sqlite3",
        {"v1": b"k" * 32},
        "v1",
        capacity_probe=deterministic_roomy_capacity,
    )


def provider_error(
    attempt_id: UUID,
    *,
    kind: ErrorKind = ErrorKind.RATE_LIMIT,
    advice: RetryAdvice = RetryAdvice.RETRY_AFTER,
) -> ProviderError:
    return ProviderError(
        kind,
        "operation",
        advice,
        "fixture_error",
        None,
        250 if advice is RetryAdvice.RETRY_AFTER else None,
        attempt_id,
        (FIXTURE_REF,),
        "fixture error",
    )


def test_provider_terminal_outcomes_reject_crossed_error_and_retry_states() -> None:
    attempt_id = uuid4()
    stop = RetryDecision(RetryAction.STOP, None, "terminal", None)
    retry = RetryDecision(RetryAction.RETRY, 250, "retryable", attempt_id)

    with pytest.raises(ValueError, match="cannot carry an error"):
        OperationTerminal(
            uuid4(),
            uuid4(),
            attempt_id,
            Outcome.SUCCEEDED,
            provider_error(attempt_id),
            stop,
            0,
            0,
            0,
            Transport.HTTP,
            (FIXTURE_REF,),
            "fixture",
        )
    with pytest.raises(ValueError, match="require a non-cancellation error"):
        OperationTerminal(
            uuid4(),
            uuid4(),
            attempt_id,
            Outcome.FAILED,
            None,
            stop,
            0,
            0,
            0,
            Transport.HTTP,
            (FIXTURE_REF,),
            "fixture",
        )
    with pytest.raises(ValueError, match="cannot carry retry advice"):
        OperationTerminal(
            uuid4(),
            uuid4(),
            attempt_id,
            Outcome.SUCCEEDED,
            None,
            retry,
            0,
            0,
            0,
            Transport.HTTP,
            (FIXTURE_REF,),
            "fixture",
        )
    with pytest.raises(ValueError, match="retryable failed provider error"):
        OperationTerminal(
            uuid4(),
            uuid4(),
            attempt_id,
            Outcome.FAILED,
            provider_error(
                attempt_id,
                kind=ErrorKind.AUTHENTICATION,
                advice=RetryAdvice.UNSPECIFIED,
            ),
            retry,
            0,
            0,
            0,
            Transport.HTTP,
            (FIXTURE_REF,),
            "fixture",
        )

    with pytest.raises(ValueError, match="only a cancellation error"):
        OperationTerminal(
            uuid4(),
            uuid4(),
            attempt_id,
            Outcome.CANCELLED,
            provider_error(attempt_id),
            stop,
            0,
            0,
            0,
            Transport.HTTP,
            (FIXTURE_REF,),
            "fixture",
        )
    terminal = OperationTerminal(
        uuid4(),
        uuid4(),
        attempt_id,
        Outcome.FAILED,
        provider_error(attempt_id),
        retry,
        0,
        0,
        0,
        Transport.HTTP,
        (FIXTURE_REF,),
        "fixture",
    )
    assert terminal.retry is retry


def test_retry_decision_requires_delay_and_attempt_link_only_for_retries() -> None:
    with pytest.raises(ValueError, match="only retries have a delay"):
        RetryDecision(RetryAction.RETRY, None, "missing delay", uuid4())
    with pytest.raises(ValueError, match="terminal attempt link"):
        RetryDecision(RetryAction.RETRY, 1, "missing link", None)
    with pytest.raises(ValueError, match="only retries have a delay"):
        RetryDecision(RetryAction.STOP, 1, "unexpected delay", None)
    assert RetryAction.STOP.value == "do_not_retry"


@pytest.mark.asyncio
async def test_provider_reports_commit_stage_and_terminal_evidence_exactly_once(
    tmp_path: Path,
) -> None:
    source, destination = uuid4(), uuid4()
    selected = direction(source, destination)
    selected_other = direction(destination, source)
    job = metadata(selected, selected_other)
    encrypted_spool = spool(tmp_path)
    control = CapturingControl()
    report = _build_provider_terminal_sink(
        job,
        selected,
        encrypted_spool,
        cast(ControlClient, control),
    )
    stop = RetryDecision(RetryAction.STOP, None, "complete", None)
    stt_id, translation_id, tts_id = uuid4(), uuid4(), uuid4()
    terminals: tuple[tuple[str, SessionTerminal | OperationTerminal], ...] = (
        (
            "stt",
            SessionTerminal(
                uuid4(),
                stt_id,
                Outcome.SUCCEEDED,
                None,
                4000,
                1,
                0,
                Transport.GRPC,
                (FIXTURE_REF,),
            ),
        ),
        (
            "translation",
            OperationTerminal(
                uuid4(),
                uuid4(),
                translation_id,
                Outcome.SUCCEEDED,
                None,
                stop,
                1,
                1,
                1,
                Transport.HTTP,
                (FIXTURE_REF,),
                "fixture",
            ),
        ),
        (
            "tts",
            OperationTerminal(
                uuid4(),
                uuid4(),
                tts_id,
                Outcome.SUCCEEDED,
                None,
                stop,
                1,
                9600,
                9600,
                Transport.WEBSOCKET,
                (FIXTURE_REF,),
                "fixture",
            ),
        ),
    )

    for attempt_stage, terminal in terminals:
        await report(attempt_stage, terminal, 1, None, stop, 100, 120)
        await report(attempt_stage, terminal, 1, None, stop, 100, 120)

    assert len(control.reports) == 3
    assert {record[0]["stage"] for record in control.reports} == {"stt", "translation", "tts"}
    terminal_evidence = encrypted_spool.committed("terminal")
    assert len(terminal_evidence) == 3
    assert {reference.sha256 for reference, _ in terminal_evidence} == {
        record[0]["terminalHash"] for record in control.reports
    }
    assert all(
        encrypted_spool.context(reference.object_id)[2] == "terminal"
        for reference, _ in terminal_evidence
    )
    for attempt_stage, terminal in terminals:
        attempt_id = (
            terminal.attempt_id if isinstance(terminal, OperationTerminal) else terminal.session_id
        )
        stage_records = encrypted_spool.committed_scoped(
            job.consultation_id, attempt_stage, str(selected.capability_row_id)
        )
        matching = [reference for reference, _ in stage_records]
        assert len(matching) == 1
        payload = control.reports[[item[1] for item in terminals].index(terminal)][0]
        evidence = matching[0]
        assert evidence.sha256 == payload["terminalHash"]
        assert (
            hashlib.sha256(encrypted_spool.read(evidence.object_id)).hexdigest()
            == payload["terminalHash"]
        )
        _, evidence_attempt_id, evidence_stage, _, _ = encrypted_spool.context(evidence.object_id)
        assert evidence_attempt_id == attempt_id
        assert evidence_stage == attempt_stage
        assert payload["attemptId"] == str(attempt_id)
        assert payload["credentialVersion"] == "fixture"
        assert payload["credentialFingerprint"] == "fixture"
        assert payload["retryDecision"]["action"] == "do_not_retry"
        assert payload["rawReferences"][0]["objectId"] == str(FIXTURE_REF.object_id)
        assert payload["startedAtMs"] == 100
        assert payload["occurredAtMs"] == 120


@pytest.mark.asyncio
async def test_retry_decision_maps_delay_to_absolute_time_and_preserves_links(
    tmp_path: Path,
) -> None:
    source, destination = uuid4(), uuid4()
    selected = direction(source, destination)
    job = metadata(selected, direction(destination, source))
    control = CapturingControl()
    report = _build_provider_terminal_sink(
        job,
        selected,
        spool(tmp_path),
        cast(ControlClient, control),
    )
    predecessor = uuid4()
    current_attempt = uuid4()
    decision = RetryDecision(
        RetryAction.RETRY,
        250,
        "safe uncommitted replay",
        current_attempt,
    )
    terminal = OperationTerminal(
        uuid4(),
        uuid4(),
        current_attempt,
        Outcome.FAILED,
        provider_error(current_attempt),
        decision,
        0,
        0,
        0,
        Transport.HTTP,
        (FIXTURE_REF,),
        "fixture",
    )

    await report("translation", terminal, 2, predecessor, decision, 1_000, 1_200)

    payload = control.reports[0][0]
    assert payload["retryOfAttemptId"] == str(predecessor)
    assert payload["retryDecision"] == {
        "action": "retry",
        "reason": "safe uncommitted replay",
        "retryAtMs": 1_450,
        "previousAttemptId": str(current_attempt),
    }


@pytest.mark.asyncio
async def test_normal_two_direction_fixture_run_persists_all_stage_reports(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    first_id, second_id = uuid4(), uuid4()
    first = direction(first_id, second_id)
    second = direction(second_id, first_id)
    job = metadata(first, second)
    encrypted_spool = spool(tmp_path)
    control = CapturingControl()

    async def ignore(*_: object) -> None:
        return None

    for selected in (first, second):
        session = DirectionSession(
            DirectionSpec(
                selected.source_participant_id,
                selected.destination_participant_id,
                selected.source_language,
                selected.target_language,
                selected.voice,
                False,
            ),
            FixtureSttProvider(),
            FixtureTranslationProvider(),
            FixtureTtsProvider(),
            ignore,
            ignore,
            ignore,
            terminal_sink=_build_provider_terminal_sink(
                job,
                selected,
                encrypted_spool,
                cast(ControlClient, control),
            ),
        )
        await session.start()
        await session.send_audio(AudioChunk(uuid4(), 0, SampleRange(0, 4_000), b"\0\0" * 4_000))
        await session.boundary()
        await session.finish()

    assert len(control.reports) == 10
    assert len({payload["terminalId"] for payload, _ in control.reports}) == 10
    assert {(payload["directionId"], payload["stage"]) for payload, _ in control.reports} >= {
        (str(selected.capability_row_id), stage_name)
        for selected in (first, second)
        for stage_name in ("stt", "translation", "tts")
    }
    assert len(encrypted_spool.committed("terminal")) == 10
    assert (
        sum(
            len(
                encrypted_spool.committed_scoped(
                    job.consultation_id,
                    stage_name,
                    str(selected.capability_row_id),
                )
            )
            for selected in (first, second)
            for stage_name in ("stt", "translation", "tts")
        )
        == 10
    )
