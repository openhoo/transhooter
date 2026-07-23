from __future__ import annotations

import asyncio
import json
import signal
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event, Lock, get_ident
from typing import Any, Protocol
from uuid import UUID, uuid4

import httpx
import pytest

from transhooter_worker.adapters.archive_delivery import (
    ArchiveDeliveryExecutor,
    ArchiveObjectRegistrationClient,
    PermanentRegistrationError,
    RetryableRegistrationError,
    upload_committed_objects,
    upload_committed_objects_async,
)
from transhooter_worker.adapters.s3_archive import S3Archive
from transhooter_worker.adapters.spool import (
    CapacityProbe,
    EncryptedSpool,
    SpoolCapacity,
    SpoolUnavailable,
    deterministic_roomy_capacity,
)
from transhooter_worker.application.compactor import PcmCompactor
from transhooter_worker.domain.models import RawRef, SampleRange
from transhooter_worker.ports.archive import ObjectRecord
from transhooter_worker.runtime import spool_drainer
from transhooter_worker.runtime.control_client import PermanentControlRequestError

TEST_KEYRING = {"v1": b"k" * 32}


def capacity_at_boundary(_path: Path) -> SpoolCapacity:
    ten_mib = 10 << 20
    return SpoolCapacity(total_bytes=ten_mib, used_bytes=7 << 20)


def make_spool(
    tmp_path: Path,
    *,
    database_name: str = "journal.sqlite3",
    root_name: str = "payloads",
    capacity_probe: CapacityProbe = deterministic_roomy_capacity,
) -> EncryptedSpool:
    return EncryptedSpool(
        tmp_path / root_name,
        tmp_path / database_name,
        TEST_KEYRING,
        "v1",
        capacity_probe=capacity_probe,
    )


def frozen_provider_context(participant_id: UUID) -> dict[str, object]:
    return {
        "workerEpoch": 7,
        "writeEpoch": 9,
        "providerSelection": {
            "profileId": "google-eu",
            "directions": [
                {
                    "sourceParticipantId": str(participant_id),
                    "endpoint": "eu-speech.googleapis.com:443",
                    "api": "speech-v2",
                    "model": "long",
                    "voice": "en-US-Chirp3-HD-Achernar",
                    "sourceLanguage": "en-US",
                    "targetLanguage": "de-DE",
                    "credential": {"reference": "google-adc", "version": "v4"},
                }
            ],
        },
    }


class AlwaysVerifiedArchive:
    def put_create_once(
        self, key: str, body: bytes, content_type: str, sha256: str
    ) -> ObjectRecord:
        return ObjectRecord(
            str(uuid4()),
            key,
            "v1",
            len(body),
            sha256,
            "crc",
            content_type,
        )

    def verify(self, record: ObjectRecord) -> bool:
        return True


class SpoolOperation(Protocol):
    def __call__(self, *args: object, **kwargs: object) -> object: ...


class CommittedOperation(Protocol):
    def __call__(self) -> list[tuple[RawRef, SampleRange | None]]: ...


def test_terminal_drain_uploads_current_meeting_non_pcm_before_reconciliation(
    tmp_path: Path,
) -> None:
    spool = make_spool(tmp_path)
    meeting_id = uuid4()
    caption = spool.append(
        meeting_id=meeting_id,
        attempt_id=uuid4(),
        stage="caption",
        transport="websocket",
        direction="publish",
        media_type="application/json",
        payload=b'{"translatedText":"Guten Morgen"}',
    )
    pcm = spool.append(
        meeting_id=meeting_id,
        attempt_id=uuid4(),
        stage="stt-input",
        transport="livekit",
        direction=str(uuid4()),
        media_type="audio/L16",
        payload=b"\x00\x00",
        sample_range=SampleRange(0, 1),
    )
    other = spool.append(
        meeting_id=uuid4(),
        attempt_id=uuid4(),
        stage="caption",
        transport="websocket",
        direction="publish",
        media_type="application/json",
        payload=b"other meeting",
    )

    registrations: list[UUID] = []
    upload_committed_objects(
        spool,
        AlwaysVerifiedArchive(),
        lambda _meeting, object_id, _object_class, _record: registrations.append(object_id),
        meeting_id,
    )

    committed_ids = {ref.object_id for ref, _ in spool.committed()}
    assert caption.object_id not in committed_ids
    assert pcm.object_id in committed_ids
    assert other.object_id in committed_ids
    assert registrations == [caption.object_id]


def test_retryable_oldest_registration_does_not_starve_later_committed_object(
    tmp_path: Path,
) -> None:
    spool = make_spool(tmp_path)
    meeting_id = uuid4()
    oldest = spool.append(
        meeting_id=meeting_id,
        attempt_id=uuid4(),
        stage="translation",
        transport="http",
        direction="in",
        media_type="application/json",
        payload=b'{"translation":"retry"}',
    )
    later = spool.append(
        meeting_id=meeting_id,
        attempt_id=uuid4(),
        stage="caption",
        transport="http",
        direction="out",
        media_type="application/json",
        payload=b'{"translation":"accepted"}',
    )
    registrations: list[UUID] = []

    def register(
        _meeting: UUID,
        object_id: UUID,
        _object_class: str,
        _record: ObjectRecord,
    ) -> None:
        if object_id == oldest.object_id:
            raise RetryableRegistrationError("control unavailable")
        registrations.append(object_id)

    with pytest.raises(RetryableRegistrationError, match="control unavailable"):
        upload_committed_objects(spool, AlwaysVerifiedArchive(), register, meeting_id)

    assert registrations == [later.object_id]
    assert [ref.object_id for ref, _ in spool.committed()] == [oldest.object_id]
    assert spool.context(oldest.object_id)[3] == oldest.ordinal


def test_registration_failures_are_aggregated_after_independent_progress(
    tmp_path: Path,
) -> None:
    spool = make_spool(tmp_path)
    meeting_id = uuid4()
    refs = [
        spool.append(
            meeting_id=meeting_id,
            attempt_id=uuid4(),
            stage="translation",
            transport="http",
            direction="in",
            media_type="application/json",
            payload=f'{{"sequence":{sequence}}}'.encode(),
        )
        for sequence in range(3)
    ]

    def register(
        _meeting: UUID,
        object_id: UUID,
        _object_class: str,
        _record: ObjectRecord,
    ) -> None:
        if object_id != refs[1].object_id:
            raise RuntimeError(str(object_id))

    with pytest.raises(RetryableRegistrationError) as caught:
        upload_committed_objects(spool, AlwaysVerifiedArchive(), register, meeting_id)

    assert [str(error) for error in caught.value.failures] == [
        str(refs[0].object_id),
        str(refs[2].object_id),
    ]
    assert [ref.object_id for ref, _ in spool.committed()] == [
        refs[0].object_id,
        refs[2].object_id,
    ]


@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (503, RetryableRegistrationError),
        (401, RetryableRegistrationError),
        (403, RetryableRegistrationError),
        (429, RetryableRegistrationError),
        (409, PermanentRegistrationError),
    ],
)
def test_archive_registration_classifies_only_retryable_http_failures(
    tmp_path: Path,
    status: int,
    error_type: type[RuntimeError],
) -> None:
    bearer_file = tmp_path / "bearer"
    bearer_file.write_text("token", "utf-8")
    client = httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(status)))
    registration = ArchiveObjectRegistrationClient("http://web:3000", bearer_file, client)
    record = ObjectRecord("object", "key", "version", 1, "a" * 64, "checksum", "application/json")

    with pytest.raises(error_type) as caught:
        registration.register(uuid4(), uuid4(), "pipeline_exchange", record)
    assert type(caught.value) is error_type


def test_archive_registration_recovers_after_bearer_rotation(tmp_path: Path) -> None:
    bearer_file = tmp_path / "bearer"
    bearer_file.write_text("expired", "utf-8")
    observed_authorization: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        authorization = request.headers["Authorization"]
        observed_authorization.append(authorization)
        return httpx.Response(204 if authorization == "Bearer rotated" else 401)

    client = httpx.Client(transport=httpx.MockTransport(respond))
    registration = ArchiveObjectRegistrationClient("http://web:3000", bearer_file, client)
    record = ObjectRecord("object", "key", "version", 1, "a" * 64, "checksum", "application/json")

    with pytest.raises(RetryableRegistrationError):
        registration.register(uuid4(), uuid4(), "pipeline_exchange", record)
    bearer_file.write_text("rotated", "utf-8")
    registration.register(uuid4(), uuid4(), "pipeline_exchange", record)

    assert observed_authorization == ["Bearer expired", "Bearer rotated"]


def test_sigterm_unwinds_drainer_telemetry_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shutdown = Event()
    installed_handlers: list[object] = []

    class Telemetry:
        def shutdown(self) -> None:
            shutdown.set()

    monkeypatch.setattr(
        spool_drainer,
        "configure_telemetry",
        lambda *_args, **_kwargs: Telemetry(),
    )
    monkeypatch.setattr(
        spool_drainer.signal,
        "signal",
        lambda _signal, handler: installed_handlers.append(handler) or signal.SIG_DFL,
    )
    monkeypatch.setattr(spool_drainer, "_build_spool", lambda _root: object())
    monkeypatch.setattr(spool_drainer, "build_archive", lambda _root: object())
    monkeypatch.setattr(
        spool_drainer,
        "ArchiveObjectRegistrationClient",
        lambda *_args: type("Registration", (), {"register": lambda *_values: None})(),
    )
    monkeypatch.setattr(spool_drainer, "_drain_once", lambda *_args: None)

    def interrupt_sleep(_seconds: float) -> None:
        handler = installed_handlers[0]
        assert callable(handler)
        handler(signal.SIGTERM, None)

    monkeypatch.setattr(spool_drainer.time, "sleep", interrupt_sleep)
    monkeypatch.setenv("INTERNAL_TOKEN_FILE", "/unused/token")
    monkeypatch.setenv("CONTROL_INTERNAL_URL", "http://web:3000")
    monkeypatch.setenv("SPOOL_PATH", "/unused/spool")

    with pytest.raises(SystemExit) as caught:
        spool_drainer.main()

    assert caught.value.code == 0
    assert shutdown.is_set()
    assert installed_handlers == [spool_drainer._handle_sigterm, signal.SIG_DFL]


def test_non_pcm_upload_is_not_acknowledged_until_ledger_registration_succeeds(
    tmp_path: Path,
) -> None:
    spool = make_spool(tmp_path)
    meeting_id = uuid4()
    evidence = spool.append(
        meeting_id=meeting_id,
        attempt_id=uuid4(),
        stage="translation",
        transport="http",
        direction="in",
        media_type="application/json",
        payload=b'{"translation":"Guten Morgen"}',
    )
    attempts = 0

    def reject_registration(
        _meeting: UUID,
        _object_id: UUID,
        _object_class: str,
        _record: ObjectRecord,
    ) -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("ledger unavailable")

    with pytest.raises(RuntimeError, match="ledger unavailable"):
        upload_committed_objects(
            spool,
            AlwaysVerifiedArchive(),
            reject_registration,
            meeting_id,
        )
    assert evidence.object_id in {ref.object_id for ref, _ in spool.committed()}

    registered: list[tuple[UUID, str]] = []
    upload_committed_objects(
        spool,
        AlwaysVerifiedArchive(),
        lambda _meeting, object_id, object_class, _record: registered.append(
            (object_id, object_class)
        ),
        meeting_id,
    )
    assert attempts == 1
    assert registered == [(evidence.object_id, "pipeline_exchange")]
    assert evidence.object_id not in {ref.object_id for ref, _ in spool.committed()}


def test_permanently_rejected_evidence_is_quarantined_without_blocking_later_records(
    tmp_path: Path,
) -> None:
    spool = make_spool(tmp_path)
    meeting_id = uuid4()
    rejected = spool.append(
        meeting_id=meeting_id,
        attempt_id=uuid4(),
        stage="control-heartbeat",
        transport="http",
        direction="out",
        media_type="application/json",
        payload=b'{"health":"ready"}',
    )
    accepted = spool.append(
        meeting_id=meeting_id,
        attempt_id=uuid4(),
        stage="translation",
        transport="http",
        direction="in",
        media_type="application/json",
        payload=b'{"translation":"Guten Morgen"}',
    )
    registered: list[UUID] = []

    def register(
        _meeting: UUID,
        object_id: UUID,
        _object_class: str,
        _record: ObjectRecord,
    ) -> None:
        if object_id == rejected.object_id:
            raise PermanentRegistrationError("ARCHIVE_NOT_RECORDING")
        registered.append(object_id)

    upload_committed_objects(
        spool,
        AlwaysVerifiedArchive(),
        register,
        meeting_id,
    )

    assert registered == [accepted.object_id]
    assert rejected.object_id not in {ref.object_id for ref, _ in spool.committed()}
    assert spool.read(rejected.object_id) == b'{"health":"ready"}'


@pytest.mark.asyncio
async def test_inline_terminal_drain_quarantines_permanent_rejection_and_continues(
    tmp_path: Path,
) -> None:
    spool = make_spool(tmp_path)
    meeting_id = uuid4()
    rejected = spool.append(
        meeting_id=meeting_id,
        attempt_id=uuid4(),
        stage="terminal",
        transport="http",
        direction="in",
        media_type="application/json",
        payload=b'{"outcome":"failed"}',
    )
    accepted = spool.append(
        meeting_id=meeting_id,
        attempt_id=uuid4(),
        stage="translation",
        transport="http",
        direction="in",
        media_type="application/json",
        payload=b'{"translation":"Guten Morgen"}',
    )
    registered: list[UUID] = []

    async def register(
        _meeting: UUID,
        object_id: UUID,
        _object_class: str,
        _record: ObjectRecord,
    ) -> None:
        if object_id == rejected.object_id:
            raise PermanentControlRequestError("ARCHIVE_NOT_RECORDING")
        registered.append(object_id)

    await upload_committed_objects_async(
        spool,
        AlwaysVerifiedArchive(),
        register,
        meeting_id,
    )

    assert registered == [accepted.object_id]
    assert spool.committed() == []
    assert spool.read(rejected.object_id) == b'{"outcome":"failed"}'


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403])
async def test_inline_terminal_drain_keeps_auth_rejection_retryable(
    tmp_path: Path,
    status: int,
) -> None:
    spool = make_spool(tmp_path)
    meeting_id = uuid4()
    evidence = spool.append(
        meeting_id=meeting_id,
        attempt_id=uuid4(),
        stage="terminal",
        transport="http",
        direction="in",
        media_type="application/json",
        payload=b'{"outcome":"failed"}',
    )

    async def reject(*_arguments: object) -> None:
        raise PermanentControlRequestError(f"internal archive-object rejected with HTTP {status}")

    with pytest.raises(RetryableRegistrationError):
        await upload_committed_objects_async(
            spool,
            AlwaysVerifiedArchive(),
            reject,
            meeting_id,
        )

    assert [reference.object_id for reference, _ in spool.committed()] == [evidence.object_id]


@pytest.mark.asyncio
async def test_inline_terminal_drain_keeps_retryable_rejection_committed(
    tmp_path: Path,
) -> None:
    spool = make_spool(tmp_path)
    meeting_id = uuid4()
    evidence = spool.append(
        meeting_id=meeting_id,
        attempt_id=uuid4(),
        stage="terminal",
        transport="http",
        direction="in",
        media_type="application/json",
        payload=b'{"outcome":"failed"}',
    )
    later = spool.append(
        meeting_id=meeting_id,
        attempt_id=uuid4(),
        stage="translation",
        transport="http",
        direction="in",
        media_type="application/json",
        payload=b'{"translation":"accepted"}',
    )
    registered: list[UUID] = []

    async def reject(
        _meeting: UUID,
        object_id: UUID,
        _object_class: str,
        _record: ObjectRecord,
    ) -> None:
        if object_id == evidence.object_id:
            raise RuntimeError("control unavailable")
        registered.append(object_id)

    with pytest.raises(RuntimeError, match="control unavailable"):
        await upload_committed_objects_async(
            spool,
            AlwaysVerifiedArchive(),
            reject,
            meeting_id,
        )

    assert registered == [later.object_id]
    assert [reference.object_id for reference, _ in spool.committed()] == [evidence.object_id]


@pytest.mark.asyncio
async def test_inline_terminal_drain_uses_one_worker_thread_and_keeps_loop_responsive(
    tmp_path: Path,
) -> None:
    spool = make_spool(tmp_path)
    meeting_id = uuid4()
    evidence = spool.append(
        meeting_id=meeting_id,
        attempt_id=uuid4(),
        stage="terminal",
        transport="http",
        direction="in",
        media_type="application/json",
        payload=b'{"outcome":"succeeded"}',
    )
    loop_thread = get_ident()
    archive_entered = asyncio.Event()
    loop = asyncio.get_running_loop()
    release_archive = Event()
    observations_lock = Lock()
    worker_threads: list[int] = []
    registration_threads: list[int] = []

    def observe_worker_thread() -> None:
        with observations_lock:
            worker_threads.append(get_ident())

    original_committed: CommittedOperation = spool.committed
    for method_name in (
        "committed",
        "context",
        "read",
        "mark_uploaded",
        "compact_uploaded_envelopes",
    ):
        original: SpoolOperation = getattr(spool, method_name)

        def observed(
            *args: object,
            _operation: SpoolOperation = original,
            **kwargs: object,
        ) -> object:
            observe_worker_thread()
            return _operation(*args, **kwargs)

        setattr(spool, method_name, observed)

    class BlockingThreadBoundArchive(AlwaysVerifiedArchive):
        def put_create_once(
            self, key: str, body: bytes, content_type: str, sha256: str
        ) -> ObjectRecord:
            observe_worker_thread()
            loop.call_soon_threadsafe(archive_entered.set)
            release_archive.wait()
            return super().put_create_once(key, body, content_type, sha256)

    async def register(
        _meeting: UUID,
        object_id: UUID,
        _object_class: str,
        _record: ObjectRecord,
    ) -> None:
        with observations_lock:
            registration_threads.append(get_ident())
        assert object_id == evidence.object_id

    task = asyncio.create_task(
        upload_committed_objects_async(
            spool,
            BlockingThreadBoundArchive(),
            register,
            meeting_id,
        )
    )
    try:
        await archive_entered.wait()
        await asyncio.wait_for(asyncio.sleep(0), 0.1)
    finally:
        release_archive.set()
    await task

    assert worker_threads
    assert len(set(worker_threads)) == 1
    assert worker_threads[0] != loop_thread
    assert registration_threads == [loop_thread]
    assert original_committed() == []


@pytest.mark.asyncio
async def test_terminal_executor_reaches_multipart_journal_from_worker_thread(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from test_s3_archive import RecordingS3Client

    monkeypatch.setattr(S3Archive, "MULTIPART_THRESHOLD", 10)
    monkeypatch.setattr(S3Archive, "PART_SIZE", 6)
    spool = make_spool(tmp_path)
    meeting_id = uuid4()
    body = b"multipart terminal evidence"
    evidence = spool.append(
        meeting_id=meeting_id,
        attempt_id=uuid4(),
        stage="terminal",
        transport="http",
        direction="in",
        media_type="application/json",
        payload=body,
    )
    archive = S3Archive(
        RecordingS3Client(),
        "bucket",
        None,
        False,
        tmp_path / "multipart.sqlite3",
    )
    registered: list[UUID] = []

    async def register(
        _meeting: UUID,
        object_id: UUID,
        _object_class: str,
        _record: ObjectRecord,
    ) -> None:
        registered.append(object_id)

    async with ArchiveDeliveryExecutor() as executor:
        await upload_committed_objects_async(
            spool,
            archive,
            register,
            meeting_id,
            executor,
        )

    assert registered == [evidence.object_id]
    assert spool.committed() == []


@pytest.mark.asyncio
async def test_archive_delivery_executor_reuses_thread_across_sequence() -> None:
    threads: list[int] = []

    async with ArchiveDeliveryExecutor() as executor:
        for _ in range(3):
            threads.append(await executor.run(get_ident))

    assert len(set(threads)) == 1
    assert threads[0] != get_ident()


@pytest.mark.asyncio
async def test_archive_delivery_cancellation_waits_for_owned_operation() -> None:
    entered = asyncio.Event()
    release = Event()
    loop = asyncio.get_running_loop()

    def block() -> None:
        loop.call_soon_threadsafe(entered.set)
        release.wait()

    async with ArchiveDeliveryExecutor() as executor:
        task = asyncio.create_task(executor.run(block))
        await entered.wait()
        task.cancel()
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task


def test_uploaded_envelope_compaction_is_bounded_and_preserves_replay_evidence(
    tmp_path: Path,
) -> None:
    spool = make_spool(tmp_path)
    meeting_id = uuid4()
    ordinary = [
        spool.append(
            meeting_id=meeting_id,
            attempt_id=uuid4(),
            stage="translation",
            transport="http",
            direction="in",
            media_type="application/json",
            payload=f'{{"sequence":{sequence}}}'.encode(),
        )
        for sequence in range(2)
    ]
    attempt_id = uuid4()
    spool.append(
        meeting_id=meeting_id,
        attempt_id=attempt_id,
        stage="translation",
        transport="http",
        direction="in",
        media_type="application/json",
        payload=b'{"request":true}',
    )
    terminal = spool.terminal(attempt_id, b'{"outcome":"succeeded"}')
    provider_terminal = spool.append(
        meeting_id=meeting_id,
        attempt_id=uuid4(),
        stage="stt-terminal",
        transport="grpc",
        direction="in",
        media_type="application/json",
        payload=b'{"outcome":"failed"}',
    )
    checkpoint = spool.append(
        meeting_id=meeting_id,
        attempt_id=uuid4(),
        stage="checkpoint",
        transport="http",
        direction="internal",
        media_type="application/json",
        payload=b'{"acceptedInput":42,"terminal":true}',
    )
    pcm = spool.append(
        meeting_id=meeting_id,
        attempt_id=uuid4(),
        stage="stt-input",
        transport="grpc",
        direction="in",
        media_type="audio/L16",
        payload=b"\0\0",
        sample_range=SampleRange(41, 42),
    )
    for ref in (*ordinary, terminal, provider_terminal, checkpoint, pcm):
        spool.mark_uploaded(ref.object_id, "version", "checksum")
    paths = {
        UUID(object_id): Path(path)
        for object_id, path in spool._db.execute(
            "SELECT object_id, opaque_path FROM records"
        ).fetchall()
    }

    assert spool.compact_uploaded_envelopes(limit=1) == 1
    assert not paths[ordinary[0].object_id].exists()
    assert paths[ordinary[1].object_id].exists()
    assert spool.context(ordinary[0].object_id)[3] == ordinary[0].ordinal
    assert spool.read(terminal.object_id) == b'{"outcome":"succeeded"}'
    assert spool.read(provider_terminal.object_id) == b'{"outcome":"failed"}'
    assert spool.read(checkpoint.object_id) == b'{"acceptedInput":42,"terminal":true}'
    assert spool.read(pcm.object_id) == b"\0\0"
    assert spool.committed_scoped(meeting_id, "stt-input", "in", include_uploaded=True)[0][
        1
    ] == SampleRange(41, 42)


def test_compaction_recovery_accepts_crash_after_unlink(tmp_path: Path) -> None:
    spool = make_spool(tmp_path)
    ref = spool.append(
        meeting_id=uuid4(),
        attempt_id=uuid4(),
        stage="translation",
        transport="http",
        direction="in",
        media_type="application/json",
        payload=b"archived",
    )
    spool.mark_uploaded(ref.object_id, "version", "checksum")
    path = tmp_path / "payloads" / f"{ref.object_id}.wal"

    def crash_before_directory_fsync() -> None:
        raise OSError("simulated crash")

    spool._fsync_directory = crash_before_directory_fsync  # type: ignore[method-assign]
    with pytest.raises(SpoolUnavailable, match="encrypted spool operation failed"):
        spool.compact_uploaded_envelopes()
    assert not path.exists()
    spool._db.close()

    reopened = make_spool(tmp_path)
    assert reopened.context(ref.object_id)[3] == ref.ordinal
    assert reopened._db.execute(
        "SELECT state FROM records WHERE object_id = ?", (str(ref.object_id),)
    ).fetchone() == ("uploaded",)


def test_spool_encrypts_fsyncs_and_authenticates(tmp_path: Path) -> None:
    spool = make_spool(tmp_path)
    payload = b"\x01\x02" * 4000
    ref = spool.append(
        meeting_id=uuid4(),
        attempt_id=uuid4(),
        stage="stt-input",
        transport="grpc",
        direction="out",
        media_type="audio/L16",
        payload=payload,
        sample_range=SampleRange(0, 4000),
    )
    assert spool.read(ref.object_id) == payload
    encrypted = next((tmp_path / "payloads").glob("*.wal")).read_bytes()
    assert payload not in encrypted
    assert spool.committed("stt-input")[0][1] == SampleRange(0, 4000)


def test_spool_rejects_projected_usage_at_80_percent(tmp_path: Path) -> None:
    spool = make_spool(tmp_path, capacity_probe=capacity_at_boundary)

    with pytest.raises(
        SpoolUnavailable,
        match="encrypted spool cannot preserve payload below emergency 80% boundary",
    ):
        spool.append(
            meeting_id=uuid4(),
            attempt_id=uuid4(),
            stage="stt-input",
            transport="grpc",
            direction="out",
            media_type="audio/L16",
            payload=b"",
        )


def test_spool_envelope_binds_frozen_context(tmp_path: Path) -> None:
    spool = make_spool(tmp_path)
    participant, utterance = uuid4(), uuid4()
    context = frozen_provider_context(participant)
    spool.set_context(context)
    spool.append(
        meeting_id=uuid4(),
        attempt_id=utterance,
        stage="stt",
        transport="grpc",
        direction=str(participant),
        media_type="application/protobuf",
        payload=b"wire",
    )
    wal_bytes = next((tmp_path / "payloads").glob("*.wal")).read_bytes()
    header, _ = EncryptedSpool._unpack(wal_bytes)
    assert header["context"] == context
    assert header["attempt_id"] == str(utterance)
    assert header["direction"] == str(participant)


def test_spool_quarantines_tampering(tmp_path: Path) -> None:
    database = tmp_path / "journal.sqlite3"
    spool = make_spool(tmp_path)
    ref = spool.append(
        meeting_id=uuid4(),
        attempt_id=uuid4(),
        stage="stt",
        transport="grpc",
        direction="out",
        media_type="application/octet-stream",
        payload=b"preserved",
    )
    path = next((tmp_path / "payloads").glob("*.wal"))
    path.write_bytes(path.read_bytes()[:-1] + b"x")
    with pytest.raises(SpoolUnavailable):
        spool.read(ref.object_id)
    db = sqlite3.connect(database)
    assert db.execute(
        "SELECT state FROM records WHERE object_id=?", (str(ref.object_id),)
    ).fetchone() == ("quarantined",)


def test_terminal_is_create_once(tmp_path: Path) -> None:
    spool = make_spool(tmp_path)
    attempt = uuid4()
    spool.append(
        meeting_id=uuid4(),
        attempt_id=attempt,
        stage="translation",
        transport="grpc",
        direction="out",
        media_type="application/protobuf",
        payload=b"request",
    )
    first = spool.terminal(attempt, b'{"outcome":"failed"}')
    second = spool.terminal(attempt, b'{"outcome":"succeeded"}')
    assert first == second
    assert spool.read(first.object_id) == b'{"outcome":"failed"}'


def test_recovery_imports_authenticated_orphan_final(tmp_path: Path) -> None:
    first = make_spool(tmp_path, database_name="first.sqlite3")
    ref = first.append(
        meeting_id=uuid4(),
        attempt_id=uuid4(),
        stage="stt",
        transport="grpc",
        direction="in",
        media_type="application/protobuf",
        payload=b"orphan",
    )
    recovered = make_spool(tmp_path, database_name="recovered.sqlite3")
    assert recovered.read(ref.object_id) == b"orphan"
    assert recovered.committed()[0][0].object_id == ref.object_id


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("transport", "websocket"),
        ("direction", "out"),
        ("media_type", "text/plain"),
        ("sample_start", 99),
        ("context", {"workerEpoch": 999}),
    ],
)
def test_orphan_recovery_rejects_tampered_full_header(
    tmp_path: Path,
    field: str,
    replacement: object,
) -> None:
    first = make_spool(tmp_path, database_name="first.sqlite3")
    first.append(
        meeting_id=uuid4(),
        attempt_id=uuid4(),
        stage="stt",
        transport="grpc",
        direction="in",
        media_type="application/protobuf",
        payload=b"orphan",
        sample_range=SampleRange(0, 1),
    )
    path = next((tmp_path / "payloads").glob("*.wal"))
    header, encrypted = EncryptedSpool._unpack(path.read_bytes())
    header[field] = replacement
    path.write_bytes(EncryptedSpool._pack(header, encrypted))
    first._db.close()

    recovered = make_spool(tmp_path, database_name="recovered.sqlite3")

    assert recovered.committed() == []
    assert path.with_suffix(".quarantine").exists()


def test_checkpoint_coverage_is_stage_and_direction_scoped(tmp_path: Path) -> None:
    meeting = uuid4()
    source_a, destination_a = uuid4(), uuid4()
    source_b, destination_b = uuid4(), uuid4()
    spool = make_spool(tmp_path)
    checkpoint = spool.append(
        meeting_id=meeting,
        attempt_id=uuid4(),
        stage="checkpoint",
        transport="http",
        direction="internal",
        media_type="application/json",
        payload=json.dumps(
            {
                "sourceParticipantId": str(source_a),
                "destinationParticipantId": str(destination_a),
                "acceptedInput": 500,
                "emittedOutput": 100,
                "terminal": True,
            }
        ).encode(),
    )
    source_coverage = spool.covering_checkpoint(meeting, "stt-input", str(source_a), 500, True)
    destination_coverage = spool.covering_checkpoint(
        meeting, "tts-output", str(destination_a), 100, True
    )
    other_source_coverage = spool.covering_checkpoint(meeting, "stt-input", str(source_b), 1, True)
    other_destination_coverage = spool.covering_checkpoint(
        meeting, "tts-output", str(destination_b), 1, True
    )
    beyond_watermark_coverage = spool.covering_checkpoint(
        meeting, "tts-output", str(destination_a), 101, True
    )

    assert source_coverage == checkpoint.object_id
    assert destination_coverage == checkpoint.object_id
    assert other_source_coverage is None
    assert other_destination_coverage is None
    assert beyond_watermark_coverage is None


def test_drain_flushes_short_pcm_batches_on_both_sides_of_gap(tmp_path: Path) -> None:
    meeting = uuid4()
    spool = make_spool(tmp_path)
    spool.append(
        meeting_id=meeting,
        attempt_id=uuid4(),
        stage="tts-output",
        transport="websocket",
        direction="out",
        media_type="audio/L16",
        payload=b"\1\0" * 100,
        sample_range=SampleRange(0, 100),
    )
    spool.append(
        meeting_id=meeting,
        attempt_id=uuid4(),
        stage="tts-output",
        transport="websocket",
        direction="out",
        media_type="audio/L16",
        payload=b"\2\0" * 100,
        sample_range=SampleRange(200, 300),
    )

    compacted = PcmCompactor(
        spool,
        AlwaysVerifiedArchive(),
        meeting,
    ).compact("tts-output", "out", drain=True)
    assert [item.samples for item in compacted] == [
        SampleRange(0, 100),
        SampleRange(200, 300),
    ]


def checkpoint_delivery_arguments() -> dict[str, Any]:
    checkpoint_id = uuid4()
    source_id = uuid4()
    worker_epoch = 7
    checkpoint_hash = "checkpoint-hash"
    previous_hash = "previous-hash"
    body = json.dumps(
        {
            "checkpointId": str(checkpoint_id),
            "highWatermarkSha256": checkpoint_hash,
            "previousCheckpointSha256": previous_hash,
            "sourceParticipantId": str(source_id),
            "workerEpoch": worker_epoch,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return {
        "checkpoint_id": checkpoint_id,
        "meeting_id": uuid4(),
        "source_id": source_id,
        "worker_epoch": worker_epoch,
        "checkpoint_hash": checkpoint_hash,
        "previous_hash": previous_hash,
        "control_event_id": uuid4(),
        "body": body,
    }


def test_checkpoint_delivery_create_once_authenticates_identical_wal(tmp_path: Path) -> None:
    spool = make_spool(tmp_path)
    arguments = checkpoint_delivery_arguments()

    first = spool.register_checkpoint_delivery(**arguments)
    second = spool.register_checkpoint_delivery(**arguments)

    assert second == first
    assert second.body == arguments["body"]
    assert spool.read(second.raw_ref.object_id) == arguments["body"]


def test_checkpoint_delivery_rejects_identity_or_body_mismatch(tmp_path: Path) -> None:
    spool = make_spool(tmp_path)
    arguments = checkpoint_delivery_arguments()
    spool.register_checkpoint_delivery(**arguments)
    changed_body = json.loads(arguments["body"])
    changed_body["gaps"] = ["new"]
    different_canonical_body = json.dumps(
        changed_body, separators=(",", ":"), sort_keys=True
    ).encode()

    for changed in (
        {"body": different_canonical_body},
        {"checkpoint_hash": "different"},
        {"control_event_id": uuid4()},
        {"source_id": uuid4()},
    ):
        with pytest.raises(SpoolUnavailable, match="different evidence|differs"):
            spool.register_checkpoint_delivery(**(arguments | changed))


@pytest.mark.parametrize("damage", ["quarantined", "missing", "auth-corrupt"])
def test_checkpoint_delivery_recovery_rejects_invalid_live_wal(tmp_path: Path, damage: str) -> None:
    database = tmp_path / "journal.sqlite3"
    spool = make_spool(tmp_path)
    arguments = checkpoint_delivery_arguments()
    delivery = spool.register_checkpoint_delivery(**arguments)
    wal_path = tmp_path / "payloads" / f"{delivery.checkpoint_id}.wal"
    if damage == "quarantined":
        with sqlite3.connect(database) as database_connection:
            database_connection.execute(
                "UPDATE records SET state = 'quarantined' WHERE object_id = ?",
                (str(delivery.checkpoint_id),),
            )
    elif damage == "missing":
        wal_path.unlink()
    else:
        wal_path.write_bytes(wal_path.read_bytes()[:-1] + b"x")

    with pytest.raises(SpoolUnavailable):
        spool.register_checkpoint_delivery(**arguments)


def test_checkpoint_orphan_reconstructs_delivery_after_rename_commit_crash(
    tmp_path: Path,
) -> None:
    spool = make_spool(tmp_path)
    arguments = checkpoint_delivery_arguments()
    original = spool.register_checkpoint_delivery(**arguments)
    spool._db.execute(
        "DELETE FROM checkpoint_deliveries WHERE checkpoint_id = ?",
        (str(original.checkpoint_id),),
    )
    spool._db.execute(
        "DELETE FROM records WHERE object_id = ?",
        (str(original.checkpoint_id),),
    )
    spool._db.close()

    reopened = make_spool(tmp_path)
    recovered = reopened.list_checkpoint_deliveries(original.meeting_id, original.worker_epoch)

    assert len(recovered) == 1
    assert recovered[0].checkpoint_id == original.checkpoint_id
    assert recovered[0].meeting_id == original.meeting_id
    assert recovered[0].source_id == original.source_id
    assert recovered[0].worker_epoch == original.worker_epoch
    assert recovered[0].checkpoint_hash == original.checkpoint_hash
    assert recovered[0].previous_hash == original.previous_hash
    assert recovered[0].control_event_id == original.control_event_id
    assert recovered[0].acknowledged is False
    assert recovered[0].body == original.body
    assert recovered[0].raw_ref.object_id == original.raw_ref.object_id
    assert recovered[0].raw_ref.sha256 == original.raw_ref.sha256
    assert reopened.checkpoint_covers(
        original.checkpoint_id, "stt-input", str(original.source_id), 0
    )


def test_checkpoint_delivery_pending_and_acknowledged_survive_reopen(
    tmp_path: Path,
) -> None:
    spool = make_spool(tmp_path)
    first_arguments = checkpoint_delivery_arguments()
    second_arguments = checkpoint_delivery_arguments() | {
        "meeting_id": first_arguments["meeting_id"],
        "source_id": first_arguments["source_id"],
        "worker_epoch": first_arguments["worker_epoch"],
        "previous_hash": first_arguments["checkpoint_hash"],
    }
    second_body = json.loads(second_arguments["body"])
    second_body["sourceParticipantId"] = str(second_arguments["source_id"])
    second_body["previousCheckpointSha256"] = second_arguments["previous_hash"]
    second_arguments["body"] = json.dumps(
        second_body, separators=(",", ":"), sort_keys=True
    ).encode()
    first = spool.register_checkpoint_delivery(**first_arguments)
    second = spool.register_checkpoint_delivery(**second_arguments)
    spool.mark_checkpoint_delivery_acknowledged(first.control_event_id)
    spool._db.close()

    reopened = make_spool(tmp_path)
    recovered = reopened.list_checkpoint_deliveries(first.meeting_id, first.worker_epoch)

    assert [item.checkpoint_id for item in recovered] == [
        first.checkpoint_id,
        second.checkpoint_id,
    ]
    assert recovered[0].acknowledged is True
    assert recovered[1].acknowledged is False
    assert recovered[0].body == first.body
    assert recovered[1].body == second.body
    assert reopened.mark_checkpoint_delivery_acknowledged(second.control_event_id).acknowledged


def test_concurrent_append_and_terminal_share_flock_before_sqlite(
    tmp_path: Path,
) -> None:
    first = make_spool(tmp_path)
    second = make_spool(tmp_path)
    meeting_id = uuid4()
    attempt_id = uuid4()
    first.append(
        meeting_id=meeting_id,
        attempt_id=attempt_id,
        stage="translation",
        transport="http",
        direction="out",
        media_type="application/json",
        payload=b"request",
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        terminal = executor.submit(first.terminal, attempt_id, b'{"outcome":"failed"}')
        appended = executor.submit(
            second.append,
            meeting_id=meeting_id,
            attempt_id=uuid4(),
            stage="translation",
            transport="http",
            direction="out",
            media_type="application/json",
            payload=b"concurrent",
        )
        terminal_ref = terminal.result(timeout=5)
        append_ref = appended.result(timeout=5)

    assert first.read(terminal_ref.object_id) == b'{"outcome":"failed"}'
    assert second.read(append_ref.object_id) == b"concurrent"


def test_parallel_reads_serialize_access_to_the_shared_sqlite_connection(
    tmp_path: Path,
) -> None:
    spool = make_spool(tmp_path)
    meeting_id = uuid4()
    refs = [
        spool.append(
            meeting_id=meeting_id,
            attempt_id=uuid4(),
            stage="translation",
            transport="http",
            direction="out",
            media_type="application/json",
            payload=f"payload-{index}".encode(),
        )
        for index in range(2)
    ]
    connection = object.__getattribute__(spool, "_db")

    class ConcurrentAccessProbe:
        def __init__(self, delegate: sqlite3.Connection) -> None:
            self.delegate = delegate
            self.active = 0
            self.overlap = False
            self.guard = Lock()
            self.second_entry = Event()

        def execute(self, sql: str, parameters: tuple[object, ...] = ()) -> sqlite3.Cursor:
            with self.guard:
                self.active += 1
                first_entry = self.active == 1
                if not first_entry:
                    self.overlap = True
                    self.second_entry.set()
            try:
                if first_entry:
                    self.second_entry.wait(0.1)
                return self.delegate.execute(sql, parameters)
            finally:
                with self.guard:
                    self.active -= 1

    probe = ConcurrentAccessProbe(connection)
    object.__setattr__(spool, "_db", probe)
    with ThreadPoolExecutor(max_workers=2) as executor:
        reads = [executor.submit(spool.read, ref.object_id) for ref in refs]
        bodies = [read.result(timeout=5) for read in reads]

    assert probe.overlap is False
    assert bodies == [b"payload-0", b"payload-1"]
