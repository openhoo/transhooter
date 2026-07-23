from __future__ import annotations

import hashlib
from pathlib import Path
from threading import Condition, get_ident
from typing import Any
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError

from transhooter_worker.adapters.s3_archive import ArchiveConflict, S3Archive


class RecordingS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], tuple[bytes, dict[str, str], str, str]] = {}
        self.parts: dict[str, dict[int, tuple[bytes, str, str]]] = {}
        self.aborted: list[str] = []
        self.multipart: dict[str, Any] = {}
        self.completion_error: ClientError | None = None
        self.active_uploads: dict[str, str] = {}
        self.next_upload = 1
        self.completion_calls = 0
        self.upload_calls: list[tuple[str, int]] = []
        self.upload_finish_order: list[int] = []
        self.completion_part_numbers: list[int] = []

    def put_object(self, **kwargs: Any) -> dict[str, str]:
        self.objects[(kwargs["Key"], "v1")] = (
            bytes(kwargs["Body"]),
            kwargs["Metadata"],
            "crc",
            kwargs["ContentType"],
        )
        return {"VersionId": "v1", "ChecksumCRC64NVME": "crc"}

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        if "VersionId" in kwargs:
            version = kwargs["VersionId"]
        else:
            version = "v2" if (kwargs["Key"], "v2") in self.objects else "v1"
        body, metadata, checksum, content_type = self.objects[(kwargs["Key"], version)]
        return {
            "ContentLength": len(body),
            "Metadata": metadata,
            "ChecksumCRC64NVME": checksum,
            "ContentType": content_type,
            "VersionId": version,
        }

    def create_multipart_upload(self, **kwargs: Any) -> dict[str, str]:
        self.multipart = kwargs
        self.parts[kwargs["Key"]] = {}
        upload_id = f"upload-{self.next_upload}"
        self.next_upload += 1
        self.active_uploads[upload_id] = kwargs["Key"]
        return {"UploadId": upload_id}

    def list_multipart_uploads(self, **_: Any) -> dict[str, Any]:
        return {
            "Uploads": [
                {"Key": key, "UploadId": upload_id}
                for upload_id, key in self.active_uploads.items()
            ]
        }

    def list_parts(self, **kwargs: Any) -> dict[str, list[dict[str, Any]]]:
        return {
            "Parts": [
                {
                    "PartNumber": number,
                    "ETag": part[1],
                    "ChecksumCRC64NVME": part[2],
                    "Size": len(part[0]),
                }
                for number, part in self.parts.get(kwargs["Key"], {}).items()
            ]
        }

    def upload_part(self, **kwargs: Any) -> dict[str, str]:
        part_number = kwargs["PartNumber"]
        self.upload_calls.append((str(kwargs["Key"]), int(part_number)))
        etag = f"etag-{part_number}"
        self.upload_finish_order.append(int(part_number))
        checksum = str(kwargs["ChecksumCRC64NVME"])
        self.parts[kwargs["Key"]][part_number] = (
            bytes(kwargs["Body"]),
            etag,
            checksum,
        )
        return {"ETag": etag, "ChecksumCRC64NVME": checksum}

    def complete_multipart_upload(self, **kwargs: Any) -> dict[str, str]:
        self.completion_calls += 1
        self.completion_part_numbers = [
            int(part["PartNumber"]) for part in kwargs["MultipartUpload"]["Parts"]
        ]
        key = kwargs["Key"]
        uploaded_parts = self.parts[key]
        body = b"".join(uploaded_parts[number][0] for number in sorted(uploaded_parts))
        self.objects[(key, "v2")] = (
            body,
            self.multipart["Metadata"],
            "crc-multi",
            self.multipart["ContentType"],
        )
        self.active_uploads.pop(str(kwargs["UploadId"]), None)
        if self.completion_error is not None:
            raise self.completion_error
        return {"VersionId": "v2", "ChecksumCRC64NVME": "crc-multi"}

    def get_object_attributes(self, **kwargs: Any) -> dict[str, Any]:
        stored_body = self.objects[(kwargs["Key"], kwargs["VersionId"])][0]
        return {
            "ObjectSize": len(stored_body),
            "Checksum": {"ChecksumCRC64NVME": "crc-multi"},
        }

    def abort_multipart_upload(self, **kwargs: Any) -> None:
        upload_id = str(kwargs["UploadId"])
        self.aborted.append(upload_id)
        self.active_uploads.pop(upload_id, None)


class BoundedConcurrencyS3Client(RecordingS3Client):
    def __init__(self, initial_wave_size: int) -> None:
        super().__init__()
        self._condition = Condition()
        self._initial_wave_size = initial_wave_size
        self._initial_wave_started = 0
        self.active_uploads_count = 0
        self.peak_uploads = 0

    def upload_part(self, **kwargs: Any) -> dict[str, str]:
        with self._condition:
            self.active_uploads_count += 1
            self.peak_uploads = max(self.peak_uploads, self.active_uploads_count)
            if self._initial_wave_started < self._initial_wave_size:
                self._initial_wave_started += 1
                if self._initial_wave_started == self._initial_wave_size:
                    self._condition.notify_all()
                elif not self._condition.wait_for(
                    lambda: self._initial_wave_started == self._initial_wave_size,
                    timeout=1,
                ):
                    raise AssertionError("multipart uploads did not execute concurrently")
        try:
            return super().upload_part(**kwargs)
        finally:
            with self._condition:
                self.active_uploads_count -= 1
                self._condition.notify_all()


class ReverseCompletionS3Client(RecordingS3Client):
    def __init__(self, part_count: int) -> None:
        super().__init__()
        self._condition = Condition()
        self._part_count = part_count
        self._started = 0
        self._next_to_finish = part_count

    def upload_part(self, **kwargs: Any) -> dict[str, str]:
        part_number = int(kwargs["PartNumber"])
        with self._condition:
            self._started += 1
            if self._started == self._part_count:
                self._condition.notify_all()
            elif not self._condition.wait_for(
                lambda: self._started == self._part_count,
                timeout=1,
            ):
                raise AssertionError("multipart uploads did not start concurrently")
            if not self._condition.wait_for(
                lambda: part_number == self._next_to_finish,
                timeout=1,
            ):
                raise AssertionError("multipart upload did not finish in forced order")
        response = super().upload_part(**kwargs)
        with self._condition:
            self._next_to_finish -= 1
            self._condition.notify_all()
        return response


class FailOnePartOnceS3Client(RecordingS3Client):
    def __init__(self, part_count: int, failing_part: int) -> None:
        super().__init__()
        self._condition = Condition()
        self._part_count = part_count
        self._failing_part = failing_part
        self._initial_started = 0
        self._initial_wave_released = False
        self._failed = False

    def upload_part(self, **kwargs: Any) -> dict[str, str]:
        part_number = int(kwargs["PartNumber"])
        with self._condition:
            if not self._initial_wave_released:
                self._initial_started += 1
                if self._initial_started == self._part_count:
                    self._initial_wave_released = True
                    self._condition.notify_all()
                elif not self._condition.wait_for(
                    lambda: self._initial_wave_released,
                    timeout=1,
                ):
                    raise AssertionError("initial multipart wave did not start concurrently")
            if part_number == self._failing_part and not self._failed:
                self._failed = True
                self.upload_calls.append((str(kwargs["Key"]), part_number))
                raise RuntimeError("injected multipart upload failure")
        return super().upload_part(**kwargs)


class OwnerThreadRecordingArchive(S3Archive):
    def __init__(self, *args: Any) -> None:
        super().__init__(*args)
        self.persisted_threads: list[int] = []

    def _persist_multipart_part(self, **kwargs: Any) -> dict[str, Any]:
        self.persisted_threads.append(get_ident())
        return super()._persist_multipart_part(**kwargs)


def archive_client(
    tmp_path: Path,
) -> tuple[S3Archive, RecordingS3Client]:
    client = RecordingS3Client()
    archive = S3Archive(client, "bucket", None, False, tmp_path / "multipart.sqlite3")
    return archive, client


def sha256_hex(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def journal_upload(
    archive: S3Archive,
    key: str,
    sha256: str,
    upload_id: str,
    updated_ms: int,
    content_type: str = "audio/L16",
) -> None:
    assert archive._db is not None
    archive._db.execute(
        """
        INSERT INTO multipart_uploads(
            key, sha256, upload_id, state, updated_ms, content_type, owner_id
        ) VALUES (?, ?, ?, 'uploading', ?, ?, ?)
        """,
        (
            key,
            sha256,
            upload_id,
            updated_ms,
            content_type,
            archive._multipart_owner(),
        ),
    )


def no_such_upload(operation: str) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": "NoSuchUpload", "Message": "upload is gone"},
            "ResponseMetadata": {"HTTPStatusCode": 404},
        },
        operation,
    )


def test_only_nosuchupload_is_classified_as_missing() -> None:
    generic_not_found = ClientError(
        {
            "Error": {"Code": "NotFound", "Message": "unrelated resource is absent"},
            "ResponseMetadata": {"HTTPStatusCode": 404},
        },
        "ListParts",
    )

    assert S3Archive._is_missing_upload(no_such_upload("ListParts"))
    assert not S3Archive._is_missing_upload(generic_not_found)


def test_archive_rejects_noncanonical_or_nonopaque_keys(tmp_path: Path) -> None:
    archive, _ = archive_client(tmp_path)
    with pytest.raises(ValueError):
        archive.put_create_once(
            "audio/customer@example.com/x", b"x", "text/plain", sha256_hex(b"x")
        )


def test_archive_multipart_persists_parts_and_verifies(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(S3Archive, "MULTIPART_THRESHOLD", 10)
    monkeypatch.setattr(S3Archive, "PART_SIZE", 6)
    archive, client = archive_client(tmp_path)
    body = b"a" * 17
    meeting = uuid4()
    key = f"v1/meetings/{meeting}/audio/stt-input/source/000.pcm"
    record = archive.put_create_once(key, body, "audio/L16", sha256_hex(body))
    assert record.version_id == "v2"
    assert archive.verify(record)
    assert client.multipart["Metadata"]["transhooter-multipart-owner"] == (
        archive._multipart_owner()
    )
    assert len(client.parts[key]) == 3
    assert archive._db is not None
    intended = archive._db.execute(
        """
        SELECT part_number, byte_start, byte_end, size, checksum
        FROM multipart_parts
        WHERE key = ?
        ORDER BY part_number
        """,
        (key,),
    ).fetchall()
    assert [(row[0], row[1], row[2], row[3]) for row in intended] == [
        (1, 0, 6, 6),
        (2, 6, 12, 6),
        (3, 12, 17, 5),
    ]
    assert [row[4] for row in intended] == [
        S3Archive._crc64nvme(body[0:6]),
        S3Archive._crc64nvme(body[6:12]),
        S3Archive._crc64nvme(body[12:17]),
    ]


def test_archive_bounds_parallel_multipart_uploads(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(S3Archive, "MULTIPART_THRESHOLD", 10)
    monkeypatch.setattr(S3Archive, "PART_SIZE", 6)
    monkeypatch.setattr(S3Archive, "MULTIPART_UPLOAD_CONCURRENCY", 3)
    client = BoundedConcurrencyS3Client(initial_wave_size=3)
    archive = S3Archive(client, "bucket", None, False, tmp_path / "multipart.sqlite3")
    body = b"a" * 47
    key = f"v1/meetings/{uuid4()}/audio/stt-input/source/concurrent.pcm"

    archive.put_create_once(key, body, "audio/L16", sha256_hex(body))

    assert client.peak_uploads == 3
    assert client.active_uploads_count == 0
    assert len(client.upload_calls) == 8


def test_archive_completes_parallel_parts_in_part_number_order(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(S3Archive, "MULTIPART_THRESHOLD", 10)
    monkeypatch.setattr(S3Archive, "PART_SIZE", 6)
    client = ReverseCompletionS3Client(part_count=4)
    archive = S3Archive(client, "bucket", None, False, tmp_path / "multipart.sqlite3")
    body = b"b" * 23
    key = f"v1/meetings/{uuid4()}/audio/stt-input/source/ordered.pcm"

    archive.put_create_once(key, body, "audio/L16", sha256_hex(body))

    assert client.upload_finish_order == [4, 3, 2, 1]
    assert client.completion_part_numbers == [1, 2, 3, 4]


def test_archive_parallel_failure_persists_successes_and_retries_missing_part(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(S3Archive, "MULTIPART_THRESHOLD", 10)
    monkeypatch.setattr(S3Archive, "PART_SIZE", 6)
    client = FailOnePartOnceS3Client(part_count=3, failing_part=2)
    archive = OwnerThreadRecordingArchive(
        client, "bucket", None, False, tmp_path / "multipart.sqlite3"
    )
    owner_thread = get_ident()
    assert archive._db is not None
    database = archive._db
    body = b"c" * 17
    key = f"v1/meetings/{uuid4()}/audio/stt-input/source/retry.pcm"

    with pytest.raises(RuntimeError, match="injected multipart upload failure"):
        archive.put_create_once(key, body, "audio/L16", sha256_hex(body))

    assert database is not None
    assert database.execute(
        """
        SELECT part_number, etag
        FROM multipart_parts
        WHERE key = ?
        ORDER BY part_number
        """,
        (key,),
    ).fetchall() == [(1, "etag-1"), (2, ""), (3, "etag-3")]
    assert archive.persisted_threads == [owner_thread, owner_thread]
    assert client.active_uploads == {"upload-1": key}
    assert client.aborted == []

    record = archive.put_create_once(key, body, "audio/L16", sha256_hex(body))

    assert record.version_id == "v2"
    assert client.next_upload == 2
    assert client.upload_calls.count((key, 1)) == 1
    assert client.upload_calls.count((key, 2)) == 2
    assert client.upload_calls.count((key, 3)) == 1
    assert client.completion_part_numbers == [1, 2, 3]


def test_crc64nvme_matches_standard_check_vector() -> None:
    assert S3Archive._crc64nvme(b"123456789") == "rosUhgp5mIg="


def test_archive_reuses_only_checksum_and_size_matched_remote_part(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(S3Archive, "MULTIPART_THRESHOLD", 10)
    monkeypatch.setattr(S3Archive, "PART_SIZE", 6)
    archive, client = archive_client(tmp_path)
    assert archive._db is not None
    body = b"abcdefghijklmnopq"
    key = f"v1/meetings/{uuid4()}/audio/stt-input/source/resume.pcm"
    journal_upload(
        archive,
        key,
        sha256_hex(body),
        "resumed-upload",
        archive._now_ms(),
    )
    client.multipart = {
        "Metadata": {"sha256": sha256_hex(body)},
        "ContentType": "audio/L16",
    }
    client.parts[key] = {
        1: (
            body[:6],
            "durable-etag",
            S3Archive._crc64nvme(body[:6]),
        )
    }

    record = archive.put_create_once(key, body, "audio/L16", sha256_hex(body))

    assert record.version_id == "v2"
    assert sorted(client.upload_calls) == [(key, 2), (key, 3)]
    assert client.parts[key][1][1] == "durable-etag"
    assert client.completion_part_numbers == [1, 2, 3]


def test_archive_same_sha_multipart_replay_returns_durable_identity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(S3Archive, "MULTIPART_THRESHOLD", 10)
    monkeypatch.setattr(S3Archive, "PART_SIZE", 6)
    archive, client = archive_client(tmp_path)
    body = b"one durable multipart identity"
    key = f"v1/meetings/{uuid4()}/audio/stt-input/source/replay.pcm"

    first = archive.put_create_once(key, body, "audio/L16", sha256_hex(body))
    assert archive._db is not None
    archive._db.close()
    reopened = S3Archive(client, "bucket", None, False, tmp_path / "multipart.sqlite3")
    replayed = reopened.put_create_once(key, body, "audio/L16", sha256_hex(body))

    assert replayed == first
    assert client.completion_calls == 1
    assert reopened._db is not None
    assert reopened._db.execute(
        """
        SELECT object_id, version_id, object_size, object_checksum, content_type
        FROM multipart_uploads
        WHERE key = ?
        """,
        (key,),
    ).fetchone() == (
        first.object_id,
        first.version_id,
        first.size,
        first.s3_checksum,
        first.content_type,
    )


@pytest.mark.parametrize(
    ("remote_body", "reported_size"),
    [
        pytest.param(b"xxxxxx", 6, id="wrong-body"),
        pytest.param(b"aaaaa", 5, id="wrong-size"),
    ],
)
def test_archive_aborts_incompatible_remote_part_before_reuse(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    remote_body: bytes,
    reported_size: int,
) -> None:
    monkeypatch.setattr(S3Archive, "MULTIPART_THRESHOLD", 10)
    monkeypatch.setattr(S3Archive, "PART_SIZE", 6)
    archive, client = archive_client(tmp_path)
    assert archive._db is not None
    body = b"a" * 17
    key = f"v1/meetings/{uuid4()}/audio/stt-input/source/mismatch.pcm"
    journal_upload(
        archive,
        key,
        sha256_hex(body),
        "incompatible-upload",
        archive._now_ms(),
    )
    client.parts[key] = {
        1: (
            remote_body[:reported_size],
            "incompatible-etag",
            S3Archive._crc64nvme(remote_body),
        )
    }

    record = archive.put_create_once(key, body, "audio/L16", sha256_hex(body))

    assert client.aborted == ["incompatible-upload"]
    assert record.version_id == "v2"
    assert client.parts[key][1][0] == body[:6]


def test_archive_does_not_abort_upload_owned_by_another_journal(
    tmp_path: Path,
) -> None:
    client = RecordingS3Client()
    key = f"v1/meetings/{uuid4()}/audio/stt-input/source/orphan.pcm"
    first = S3Archive(client, "bucket", None, False, tmp_path / "first.sqlite3")
    created = client.create_multipart_upload(
        Bucket="bucket",
        Key=key,
        ContentType="audio/L16",
        ChecksumAlgorithm="CRC64NVME",
        Metadata={
            "sha256": "a" * 64,
            "transhooter-multipart-owner": first._multipart_owner(),
        },
    )
    journal_upload(first, key, "a" * 64, created["UploadId"], 0)

    second = S3Archive(client, "bucket", None, False, tmp_path / "second.sqlite3")

    assert second.abort_abandoned(second._now_ms()) == 0
    assert client.aborted == []
    assert client.active_uploads == {created["UploadId"]: key}


def test_archive_rejects_completed_multipart_content_type_replay(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(S3Archive, "MULTIPART_THRESHOLD", 10)
    archive, client = archive_client(tmp_path)
    body = b"durable multipart replay"
    key = f"v1/meetings/{uuid4()}/audio/stt-input/source/content-type.pcm"
    archive.put_create_once(key, body, "audio/L16", sha256_hex(body))

    with pytest.raises(ArchiveConflict):
        archive.put_create_once(key, body, "application/octet-stream", sha256_hex(body))

    assert client.completion_calls == 1


def test_archive_rejects_in_progress_multipart_content_type_replay(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(S3Archive, "MULTIPART_THRESHOLD", 10)
    archive, client = archive_client(tmp_path)
    body = b"durable multipart replay"
    key = f"v1/meetings/{uuid4()}/audio/stt-input/source/in-progress.pcm"
    journal_upload(
        archive,
        key,
        sha256_hex(body),
        "owned-in-progress",
        archive._now_ms(),
    )
    client.parts[key] = {}

    with pytest.raises(ArchiveConflict):
        archive.put_create_once(key, body, "application/octet-stream", sha256_hex(body))

    assert client.aborted == []


def test_archive_recovers_completed_multipart_after_nosuchupload(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(S3Archive, "MULTIPART_THRESHOLD", 10)
    monkeypatch.setattr(S3Archive, "PART_SIZE", 6)
    archive, client = archive_client(tmp_path)
    client.completion_error = ClientError(
        {
            "Error": {"Code": "NoSuchUpload", "Message": "already completed"},
            "ResponseMetadata": {"HTTPStatusCode": 404},
        },
        "CompleteMultipartUpload",
    )
    body = b"completed-before-crash"
    key = f"v1/meetings/{uuid4()}/audio/stt-input/source/001.pcm"

    record = archive.put_create_once(key, body, "audio/L16", sha256_hex(body))

    assert record.version_id == "v2"
    assert archive.verify(record)
    assert archive._db is not None
    assert archive._db.execute(
        "SELECT state FROM multipart_uploads WHERE key = ?", (key,)
    ).fetchone() == ("complete",)
    replayed = archive.put_create_once(key, body, "audio/L16", sha256_hex(body))
    assert replayed == record
    assert client.completion_calls == 1


def test_archive_restarts_upload_with_unexpected_remote_part_superset(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(S3Archive, "MULTIPART_THRESHOLD", 10)
    monkeypatch.setattr(S3Archive, "PART_SIZE", 6)
    archive, client = archive_client(tmp_path)
    assert archive._db is not None
    body = b"a" * 17
    key = f"v1/meetings/{uuid4()}/audio/stt-input/source/extra.pcm"
    journal_upload(
        archive,
        key,
        sha256_hex(body),
        "old-upload",
        archive._now_ms(),
    )
    client.parts[key] = {4: (b"unexpected", "etag-4", "part-crc")}

    record = archive.put_create_once(key, body, "audio/L16", sha256_hex(body))

    assert client.aborted == ["old-upload"]
    assert record.version_id == "v2"
    assert sorted(client.parts[key]) == [1, 2, 3]


def test_archive_startup_aborts_stale_upload_and_reuses_same_identity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(S3Archive, "MULTIPART_THRESHOLD", 10)
    monkeypatch.setattr(S3Archive, "PART_SIZE", 6)
    archive, client = archive_client(tmp_path)
    assert archive._db is not None
    body = b"a" * 17
    key = f"v1/meetings/{uuid4()}/audio/stt-input/source/002.pcm"
    journal_upload(archive, key, sha256_hex(body), "stale-upload", 0)
    archive._db.execute(
        """
        INSERT INTO multipart_parts(key, part_number, etag, checksum, size)
        VALUES (?, 1, 'etag-1', 'part-crc', 6)
        """,
        (key,),
    )
    client.parts[key] = {
        1: (b"a" * 6, "etag-1", "part-crc"),
        2: (b"b" * 4, "etag-2", "part-crc"),
    }
    archive._db.close()

    reopened = S3Archive(client, "bucket", None, False, tmp_path / "multipart.sqlite3")

    assert client.aborted == ["stale-upload"]
    assert reopened._db is not None
    assert reopened._db.execute(
        "SELECT COUNT(*) FROM multipart_uploads WHERE key = ?", (key,)
    ).fetchone() == (0,)
    assert reopened._db.execute(
        "SELECT COUNT(*) FROM multipart_parts WHERE key = ?", (key,)
    ).fetchone() == (0,)

    record = reopened.put_create_once(key, body, "audio/L16", sha256_hex(body))

    assert record.version_id == "v2"
    assert client.next_upload == 2
    assert client.completion_part_numbers == [1, 2, 3]
    assert reopened._db is not None
    reopened._db.close()
    replay_archive = S3Archive(client, "bucket", None, False, tmp_path / "multipart.sqlite3")
    replayed = replay_archive.put_create_once(key, body, "audio/L16", sha256_hex(body))

    assert replayed == record
    assert client.completion_calls == 1


def test_archive_startup_forgets_missing_upload_and_reuses_same_identity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(S3Archive, "MULTIPART_THRESHOLD", 10)
    monkeypatch.setattr(S3Archive, "PART_SIZE", 6)
    archive, client = archive_client(tmp_path)
    assert archive._db is not None
    body = b"b" * 17
    key = f"v1/meetings/{uuid4()}/audio/stt-input/source/003.pcm"
    journal_upload(archive, key, sha256_hex(body), "missing-upload", 0)
    archive._db.execute(
        """
        INSERT INTO multipart_parts(key, part_number, etag, checksum, size)
        VALUES (?, 1, 'etag-1', 'part-crc', 6)
        """,
        (key,),
    )
    archive._db.close()
    list_parts = client.list_parts

    def missing_parts(**kwargs: Any) -> dict[str, list[dict[str, Any]]]:
        if kwargs["UploadId"] == "missing-upload":
            raise no_such_upload("ListParts")
        return list_parts(**kwargs)

    monkeypatch.setattr(client, "list_parts", missing_parts)
    reopened = S3Archive(client, "bucket", None, False, tmp_path / "multipart.sqlite3")

    assert reopened._db is not None
    assert reopened._db.execute(
        "SELECT COUNT(*) FROM multipart_uploads WHERE key = ?", (key,)
    ).fetchone() == (0,)
    assert reopened._db.execute(
        "SELECT COUNT(*) FROM multipart_parts WHERE key = ?", (key,)
    ).fetchone() == (0,)

    record = reopened.put_create_once(key, body, "audio/L16", sha256_hex(body))

    assert record.version_id == "v2"
    assert client.next_upload == 2
    assert client.completion_part_numbers == [1, 2, 3]


def test_archive_startup_accepts_missing_upload_after_ambiguous_abort(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    archive, client = archive_client(tmp_path)
    assert archive._db is not None
    key = f"v1/meetings/{uuid4()}/audio/stt-input/source/004.pcm"
    journal_upload(archive, key, "c" * 64, "ambiguous-abort", 0)
    client.parts[key] = {1: (b"a" * 6, "etag-1", "part-crc")}
    archive._db.close()

    def missing_after_abort(**kwargs: Any) -> None:
        client.aborted.append(kwargs["UploadId"])
        raise no_such_upload("AbortMultipartUpload")

    monkeypatch.setattr(client, "abort_multipart_upload", missing_after_abort)
    reopened = S3Archive(client, "bucket", None, False, tmp_path / "multipart.sqlite3")

    assert client.aborted == ["ambiguous-abort"]
    assert reopened._db is not None
    assert reopened._db.execute(
        "SELECT COUNT(*) FROM multipart_uploads WHERE key = ?", (key,)
    ).fetchone() == (0,)
    assert reopened._db.execute(
        "SELECT COUNT(*) FROM multipart_parts WHERE key = ?", (key,)
    ).fetchone() == (0,)
