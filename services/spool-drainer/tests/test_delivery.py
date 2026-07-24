from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4, uuid5

import pytest
from transhooter_spool import (
    ObjectRecord,
    RawRef,
    SampleRange,
    SpoolRecordContext,
    SpoolRecordDelivery,
)

from transhooter_spool_drainer.clients import (
    PermanentControlRequestError,
    WorkerTuple,
)
from transhooter_spool_drainer.delivery import (
    CompactedPcm,
    DeliveryRetryable,
    PcmCompactor,
    build_archive,
    drain_delivery_cycle,
)


def test_build_archive_reads_nonsecret_bucket_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    credentials = tmp_path / "credentials.json"
    credentials.write_text(
        json.dumps({"accessKeyId": "access", "secretAccessKey": "secret"}), encoding="utf-8"
    )
    captured: dict[str, object] = {}

    class Client:
        pass

    def client(service: str, **kwargs: object) -> Client:
        captured["service"] = service
        captured.update(kwargs)
        return Client()

    monkeypatch.setenv("S3_CREDENTIALS_FILE", str(credentials))
    monkeypatch.setenv("S3_BUCKET", "archive-bucket")
    monkeypatch.delenv("S3_BUCKET_FILE", raising=False)
    monkeypatch.setattr("transhooter_spool_drainer.delivery.boto3.client", client)

    archive = build_archive(tmp_path)

    assert archive.bucket == "archive-bucket"
    assert captured["service"] == "s3"


class FakeArchive:
    def put_create_once(
        self, key: str, body: bytes, content_type: str, sha256: str
    ) -> ObjectRecord:
        assert hashlib.sha256(body).hexdigest() == sha256
        return ObjectRecord("archive", key, "v1", len(body), sha256, "crc", content_type)

    def verify(self, _record: ObjectRecord) -> bool:
        return True


class FakeSpool:
    def __init__(self, deliveries: list[SpoolRecordDelivery]) -> None:
        self.deliveries = deliveries
        self.payloads = {delivery.raw_ref.object_id: b"payload" for delivery in deliveries}
        self.uploaded: list[UUID] = []
        self.permanent: list[tuple[UUID, str]] = []

    def list_record_deliveries(self, **kwargs: Any) -> tuple[SpoolRecordDelivery, ...]:
        states = kwargs.get("states")
        return tuple(item for item in self.deliveries if states is None or item.state in states)

    def read(self, object_id: UUID) -> bytes:
        return self.payloads[object_id]

    def mark_record_uploaded(self, object_id: UUID, _version: str, _checksum: str) -> None:
        self.uploaded.append(object_id)
        for index, item in enumerate(self.deliveries):
            if item.raw_ref.object_id == object_id:
                self.deliveries[index] = _delivery(object_id.int, "uploaded")

    def mark_record_delivery_permanent(
        self, object_id: UUID, error_kind: str, _failed_at: datetime
    ) -> None:
        self.permanent.append((object_id, error_kind))
        for index, item in enumerate(self.deliveries):
            if item.raw_ref.object_id == object_id:
                self.deliveries[index] = _delivery(object_id.int, "permanent")

    def compact_uploaded_envelopes(self) -> int:
        return 0

    def pcm_scopes(self, **_kwargs: object) -> tuple[object, ...]:
        return ()

    def list_checkpoint_deliveries(self, **_kwargs: object) -> tuple[object, ...]:
        return ()

    def list_consultation_seals(self, **_kwargs: object) -> tuple[object, ...]:
        return ()


def _delivery(number: int, state: str = "committed") -> SpoolRecordDelivery:
    object_id = UUID(int=number)
    context = SpoolRecordContext(
        meeting_id=UUID(int=100),
        attempt_id=UUID(int=200 + number),
        stage="translation",
        transport="http",
        direction="in",
        media_type="application/json",
        ordinal=number,
        generation=3,
        worker_id=UUID(int=300),
        worker_epoch=5,
        write_epoch=8,
        metadata=(),
    )
    return SpoolRecordDelivery(
        raw_ref=RawRef(
            object_id=object_id,
            ordinal=number,
            sha256=hashlib.sha256(b"payload").hexdigest(),
            size=7,
            media_type="application/json",
        ),
        sample_range=None,
        context=context,
        state=state,
        version_id=None,
        s3_checksum=None,
        error_kind=None,
        failed_at=None,
    )


class RecordingControl:
    def __init__(self, reject: UUID | None = None, permanent: bool = False) -> None:
        self.reject = reject
        self.permanent = permanent
        self.objects: list[UUID] = []

    def archive_object(self, _worker: WorkerTuple, **kwargs: Any) -> None:
        object_id = kwargs["object_id"]
        if object_id == self.reject:
            if self.permanent:
                raise PermanentControlRequestError("archive-object", 409, "WORKER_FENCED")
            raise RuntimeError("control unavailable")
        self.objects.append(object_id)

    def expired_worker_epochs(self) -> tuple[object, ...]:
        return ()


def test_retryable_oldest_record_does_not_starve_later_progress() -> None:
    first = _delivery(1)
    second = _delivery(2)
    spool = FakeSpool([first, second])
    control = RecordingControl(reject=first.raw_ref.object_id)

    with pytest.raises(DeliveryRetryable):
        drain_delivery_cycle(spool, FakeArchive(), control)  # type: ignore[arg-type]

    assert control.objects == [second.raw_ref.object_id]
    assert spool.uploaded == [second.raw_ref.object_id]
    assert spool.permanent == []


def test_permanent_tuple_fence_is_visible_and_not_retried() -> None:
    first = _delivery(1)
    second = _delivery(2)
    spool = FakeSpool([first, second])
    control = RecordingControl(reject=first.raw_ref.object_id, permanent=True)

    stats = drain_delivery_cycle(spool, FakeArchive(), control)  # type: ignore[arg-type]
    assert stats.permanent_records == 1
    assert stats.uploaded_records == 1
    assert spool.permanent == [(first.raw_ref.object_id, "WORKER_FENCED")]
    assert control.objects == [second.raw_ref.object_id]

    control.reject = None
    drain_delivery_cycle(spool, FakeArchive(), control)  # type: ignore[arg-type]
    assert control.objects == [second.raw_ref.object_id]


def test_permanent_pcm_tuple_fence_does_not_stop_independent_progress(monkeypatch) -> None:
    meeting_id = UUID(int=100)
    first_source = UUID(int=1)
    second_source = UUID(int=2)
    worker = WorkerTuple(meeting_id, 3, UUID(int=300), 5, 8)
    records = tuple(
        ObjectRecord(
            "archive",
            f"v1/meetings/{meeting_id}/audio/stt-input/in/{number}.pcm",
            "v1",
            7,
            "a" * 64,
            "crc",
            "audio/L16",
        )
        for number in (1, 2, 3, 4)
    )
    compacted = (
        CompactedPcm(
            records[0],
            records[1],
            SampleRange(0, 16_000),
            (first_source,),
            "stt-input",
            "in",
            worker,
            (first_source,),
        ),
        CompactedPcm(
            records[2],
            records[3],
            SampleRange(16_000, 32_000),
            (second_source,),
            "stt-input",
            "in",
            worker,
            (second_source,),
        ),
    )

    class PcmSpool(FakeSpool):
        def pcm_scopes(self, **_kwargs: object) -> tuple[tuple[UUID, str, str], ...]:
            return ((meeting_id, "stt-input", "in"),)

        def covering_checkpoint(self, *_args: object, **_kwargs: object) -> None:
            return None

    monkeypatch.setattr(PcmCompactor, "compact", lambda *_args, **_kwargs: compacted)
    rejected = uuid5(meeting_id, f"archive-object:{records[0].key}:{records[0].version_id}")
    control = RecordingControl(reject=rejected, permanent=True)
    spool = PcmSpool([])

    stats = drain_delivery_cycle(spool, FakeArchive(), control)  # type: ignore[arg-type]

    assert stats.permanent_records == 1
    assert stats.uploaded_records == 2
    assert spool.permanent == [(first_source, "WORKER_FENCED")]
    assert control.objects == [
        uuid5(meeting_id, f"archive-object:{record.key}:{record.version_id}")
        for record in records[2:]
    ]


def test_registration_precedes_local_uploaded_transition() -> None:
    delivery = _delivery(1)
    observations: list[str] = []

    class OrderedSpool(FakeSpool):
        def mark_record_uploaded(self, *args: object) -> None:
            observations.append("uploaded")
            super().mark_record_uploaded(*args)  # type: ignore[arg-type]

    class OrderedControl(RecordingControl):
        def archive_object(self, _worker: WorkerTuple, **kwargs: Any) -> None:
            observations.append("registered")
            super().archive_object(_worker, **kwargs)

    drain_delivery_cycle(OrderedSpool([delivery]), FakeArchive(), OrderedControl())  # type: ignore[arg-type]
    assert observations == ["registered", "uploaded"]


def test_checkpoint_validation_binds_durable_identity(monkeypatch) -> None:
    from types import SimpleNamespace

    from transhooter_spool_drainer import delivery as delivery_module

    checkpoint_id = uuid4()
    source_id = uuid4()
    destination_id = uuid4()
    checkpoint = {
        "checkpointId": str(checkpoint_id),
        "workerEpoch": 5,
        "sourceParticipantId": str(source_id),
        "destinationParticipantId": str(destination_id),
        "acceptedInputSequence": 0,
        "acceptedInput": 0,
        "receivedOutput": 0,
        "emittedOutput": 0,
        "previousCheckpointSha256": None,
        "expectedObjectIds": [],
        "observedObjectIds": [],
        "gaps": [],
        "terminal": False,
        "occurredAtMs": 1,
    }
    encoded_without_hash = json.dumps(checkpoint, separators=(",", ":"), sort_keys=True).encode()
    digest = hashlib.sha256(encoded_without_hash).hexdigest()
    checkpoint["highWatermarkSha256"] = digest
    body = json.dumps(checkpoint, separators=(",", ":"), sort_keys=True).encode()
    durable = SimpleNamespace(
        checkpoint_id=checkpoint_id,
        source_id=source_id,
        worker_epoch=5,
        previous_hash=None,
        checkpoint_hash=digest,
        body=body,
    )
    schema = Path(__file__).parents[3] / "packages/contracts/generated/contracts.schema.json"
    monkeypatch.setenv("CONTRACTS_SCHEMA_FILE", str(schema))
    delivery_module._checkpoint_validator.cache_clear()
    assert delivery_module._validated_checkpoint(durable) == checkpoint

    tampered = SimpleNamespace(
        **{**durable.__dict__, "body": body.replace(str(source_id).encode(), str(uuid4()).encode())}
    )
    with pytest.raises(ValueError, match="canonical|durable identity|hash"):
        delivery_module._validated_checkpoint(tampered)
