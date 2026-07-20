from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError

from transhooter_worker.adapters.s3_archive import S3Archive


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
        checksum = str(kwargs["ChecksumCRC64NVME"])
        self.parts[kwargs["Key"]][part_number] = (
            bytes(kwargs["Body"]),
            etag,
            checksum,
        )
        return {"ETag": etag, "ChecksumCRC64NVME": checksum}

    def complete_multipart_upload(self, **kwargs: Any) -> dict[str, str]:
        self.completion_calls += 1
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


def archive_client(
    tmp_path: Path,
) -> tuple[S3Archive, RecordingS3Client]:
    client = RecordingS3Client()
    archive = S3Archive(client, "bucket", None, False, tmp_path / "multipart.sqlite3")
    return archive, client


def sha256_hex(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


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
    archive._db.execute(
        """
        INSERT INTO multipart_uploads(key, sha256, upload_id, state, updated_ms)
        VALUES (?, ?, 'resumed-upload', 'uploading', ?)
        """,
        (key, sha256_hex(body), archive._now_ms()),
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
    assert client.upload_calls == [(key, 2), (key, 3)]
    assert client.parts[key][1][1] == "durable-etag"


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
    archive._db.execute(
        """
        INSERT INTO multipart_uploads(key, sha256, upload_id, state, updated_ms)
        VALUES (?, ?, 'incompatible-upload', 'uploading', ?)
        """,
        (key, sha256_hex(body), archive._now_ms()),
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


def test_archive_startup_aborts_only_unjournaled_owned_prefix_uploads(
    tmp_path: Path,
) -> None:
    client = RecordingS3Client()
    owned_key = f"v1/meetings/{uuid4()}/audio/stt-input/source/orphan.pcm"
    foreign_key = "foreign/uploads/do-not-touch"
    client.active_uploads = {
        "owned-orphan": owned_key,
        "foreign-orphan": foreign_key,
    }

    S3Archive(client, "bucket", None, False, tmp_path / "multipart.sqlite3")

    assert client.aborted == ["owned-orphan"]
    assert client.active_uploads == {"foreign-orphan": foreign_key}


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
    archive._db.execute(
        """
        INSERT INTO multipart_uploads(key, sha256, upload_id, state, updated_ms)
        VALUES (?, ?, 'old-upload', 'uploading', ?)
        """,
        (key, sha256_hex(body), archive._now_ms()),
    )
    client.parts[key] = {4: (b"unexpected", "etag-4", "part-crc")}

    record = archive.put_create_once(key, body, "audio/L16", sha256_hex(body))

    assert client.aborted == ["old-upload"]
    assert record.version_id == "v2"
    assert sorted(client.parts[key]) == [1, 2, 3]


def test_archive_startup_reconciles_remote_part_superset_and_aborts_stale_upload(
    tmp_path: Path,
) -> None:
    archive, client = archive_client(tmp_path)
    assert archive._db is not None
    key = f"v1/meetings/{uuid4()}/audio/stt-input/source/002.pcm"
    archive._db.execute(
        """
        INSERT INTO multipart_uploads(key, sha256, upload_id, state, updated_ms)
        VALUES (?, ?, ?, 'uploading', 0)
        """,
        (key, "a" * 64, "stale-upload"),
    )
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
        "SELECT state FROM multipart_uploads WHERE key = ?", (key,)
    ).fetchone() == ("aborted",)
    assert reopened._db.execute(
        "SELECT COUNT(*) FROM multipart_parts WHERE key = ?", (key,)
    ).fetchone() == (1,)


def test_archive_startup_forgets_stale_upload_missing_during_reconcile(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    archive, client = archive_client(tmp_path)
    assert archive._db is not None
    key = f"v1/meetings/{uuid4()}/audio/stt-input/source/003.pcm"
    archive._db.execute(
        """
        INSERT INTO multipart_uploads(key, sha256, upload_id, state, updated_ms)
        VALUES (?, ?, 'missing-upload', 'uploading', 0)
        """,
        (key, "b" * 64),
    )
    archive._db.execute(
        """
        INSERT INTO multipart_parts(key, part_number, etag, checksum, size)
        VALUES (?, 1, 'etag-1', 'part-crc', 6)
        """,
        (key,),
    )
    archive._db.close()

    def missing_parts(**_: Any) -> dict[str, list[dict[str, Any]]]:
        raise no_such_upload("ListParts")

    monkeypatch.setattr(client, "list_parts", missing_parts)
    reopened = S3Archive(client, "bucket", None, False, tmp_path / "multipart.sqlite3")

    assert reopened._db is not None
    assert reopened._db.execute(
        "SELECT COUNT(*) FROM multipart_uploads WHERE key = ?", (key,)
    ).fetchone() == (0,)
    assert reopened._db.execute(
        "SELECT COUNT(*) FROM multipart_parts WHERE key = ?", (key,)
    ).fetchone() == (0,)


def test_archive_startup_accepts_missing_upload_after_ambiguous_abort(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    archive, client = archive_client(tmp_path)
    assert archive._db is not None
    key = f"v1/meetings/{uuid4()}/audio/stt-input/source/004.pcm"
    archive._db.execute(
        """
        INSERT INTO multipart_uploads(key, sha256, upload_id, state, updated_ms)
        VALUES (?, ?, 'ambiguous-abort', 'uploading', 0)
        """,
        (key, "c" * 64),
    )
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
