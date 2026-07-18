from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from transhooter_worker.adapters.s3_archive import S3Archive


class RecordingS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], tuple[bytes, dict[str, str], str, str]] = {}
        self.parts: dict[str, dict[int, tuple[bytes, str, str]]] = {}
        self.aborted: list[str] = []
        self.multipart: dict[str, Any] = {}

    def put_object(self, **kwargs: Any) -> dict[str, str]:
        self.objects[(kwargs["Key"], "v1")] = (
            bytes(kwargs["Body"]),
            kwargs["Metadata"],
            "crc",
            kwargs["ContentType"],
        )
        return {"VersionId": "v1", "ChecksumCRC64NVME": "crc"}

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        version = kwargs.get("VersionId") or "v1"
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
        return {"UploadId": "upload-1"}

    def list_parts(self, **kwargs: Any) -> dict[str, list[dict[str, Any]]]:
        return {
            "Parts": [
                {"PartNumber": number, "ETag": part[1], "ChecksumCRC64NVME": part[2]}
                for number, part in self.parts.get(kwargs["Key"], {}).items()
            ]
        }

    def upload_part(self, **kwargs: Any) -> dict[str, str]:
        part_number = kwargs["PartNumber"]
        etag = f"etag-{part_number}"
        self.parts[kwargs["Key"]][part_number] = (
            bytes(kwargs["Body"]),
            etag,
            "part-crc",
        )
        return {"ETag": etag, "ChecksumCRC64NVME": "part-crc"}

    def complete_multipart_upload(self, **kwargs: Any) -> dict[str, str]:
        key = kwargs["Key"]
        uploaded_parts = self.parts[key]
        body = b"".join(uploaded_parts[number][0] for number in sorted(uploaded_parts))
        self.objects[(key, "v2")] = (
            body,
            self.multipart["Metadata"],
            "crc-multi",
            self.multipart["ContentType"],
        )
        return {"VersionId": "v2", "ChecksumCRC64NVME": "crc-multi"}

    def get_object_attributes(self, **kwargs: Any) -> dict[str, Any]:
        stored_body = self.objects[(kwargs["Key"], kwargs["VersionId"])][0]
        return {
            "ObjectSize": len(stored_body),
            "Checksum": {"ChecksumCRC64NVME": "crc-multi"},
        }

    def abort_multipart_upload(self, **kwargs: Any) -> None:
        self.aborted.append(kwargs["UploadId"])


def archive_client(
    tmp_path: Path,
) -> tuple[S3Archive, RecordingS3Client]:
    client = RecordingS3Client()
    archive = S3Archive(client, "bucket", None, False, tmp_path / "multipart.sqlite3")
    return archive, client


def sha256_hex(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


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
