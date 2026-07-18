from __future__ import annotations

import hashlib
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from botocore.exceptions import ClientError  # type: ignore[import-untyped]

from transhooter_worker.ports.archive import ObjectRecord

_MULTIPART_SCHEMA = """
CREATE TABLE IF NOT EXISTS multipart_uploads (
    key TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL,
    upload_id TEXT NOT NULL,
    state TEXT NOT NULL,
    updated_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS multipart_parts (
    key TEXT NOT NULL,
    part_number INTEGER NOT NULL,
    etag TEXT NOT NULL,
    checksum TEXT NOT NULL,
    size INTEGER NOT NULL,
    PRIMARY KEY (key, part_number),
    FOREIGN KEY (key) REFERENCES multipart_uploads(key)
);
"""


class ArchiveConflict(RuntimeError):
    pass


@dataclass(slots=True)
class S3Archive:
    client: Any
    bucket: str
    kms_key_id: str | None
    require_kms: bool = True
    multipart_database: Path | None = None
    _db: sqlite3.Connection | None = field(init=False, default=None, repr=False)

    MULTIPART_THRESHOLD = 100 * 1024 * 1024
    PART_SIZE = 16 * 1024 * 1024
    _CLASSES = frozenset({"pipeline", "audio", "captions", "media", "inventory"})

    def __post_init__(self) -> None:
        self._db = None
        if self.multipart_database:
            self._db = sqlite3.connect(self.multipart_database, isolation_level=None)
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=FULL")
            self._db.executescript(_MULTIPART_SCHEMA)

    def put_create_once(
        self, key: str, body: bytes, content_type: str, sha256: str
    ) -> ObjectRecord:
        self._validate_key(key)
        if hashlib.sha256(body).hexdigest() != sha256:
            raise ValueError("provided SHA-256 does not match body")
        if len(body) >= self.MULTIPART_THRESHOLD:
            return self._put_multipart(key, body, content_type, sha256)

        args = self._object_arguments(
            key=key,
            content_type=content_type,
            sha256=sha256,
            body=body,
            conditional=True,
        )
        try:
            response = self.client.put_object(**args)
        except ClientError as exc:
            recovered = self._recover_ambiguous(key, len(body), sha256, content_type)
            if recovered is not None:
                return recovered
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status in (409, 412):
                raise ArchiveConflict(key) from exc
            raise
        except Exception:
            recovered = self._recover_ambiguous(key, len(body), sha256, content_type)
            if recovered is not None:
                return recovered
            raise
        return self._verified_record(key, len(body), sha256, content_type, response)

    def _put_multipart(self, key: str, body: bytes, content_type: str, sha256: str) -> ObjectRecord:
        database = self._multipart_db()
        upload_id = self._load_or_create_upload(database, key, content_type, sha256)
        remote_parts = self._list_remote_parts(key, upload_id)
        completion_parts = [
            self._persist_multipart_part(
                database=database,
                key=key,
                upload_id=upload_id,
                part_number=offset // self.PART_SIZE + 1,
                part=body[offset : offset + self.PART_SIZE],
                remote=remote_parts.get(offset // self.PART_SIZE + 1),
            )
            for offset in range(0, len(body), self.PART_SIZE)
        ]
        try:
            response = self.client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": completion_parts},
                IfNoneMatch="*",
            )
        except ClientError as exc:
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status not in (409, 412):
                raise
            recovered = self._recover_ambiguous(key, len(body), sha256, content_type)
            if recovered is None:
                raise ArchiveConflict(key) from exc
            self._mark_upload_complete(database, key)
            return recovered

        self._mark_upload_complete(database, key)
        record = self._verified_record(key, len(body), sha256, content_type, response)
        self._verify_multipart_size(record, len(body))
        return record

    def _multipart_db(self) -> sqlite3.Connection:
        if self._db is None:
            raise RuntimeError("durable multipart database is required for objects >=100 MiB")
        return self._db

    def _load_or_create_upload(
        self,
        database: sqlite3.Connection,
        key: str,
        content_type: str,
        sha256: str,
    ) -> str:
        existing = database.execute(
            """
            SELECT sha256, upload_id, state
            FROM multipart_uploads
            WHERE key = ?
            """,
            (key,),
        ).fetchone()
        if existing is not None:
            if existing[0] != sha256 or existing[2] == "complete":
                raise ArchiveConflict(key)
            return str(existing[1])

        args = self._object_arguments(
            key=key,
            content_type=content_type,
            sha256=sha256,
        )
        created = self.client.create_multipart_upload(**args)
        upload_id = str(created["UploadId"])
        database.execute(
            """
            INSERT INTO multipart_uploads(
                key, sha256, upload_id, state, updated_ms
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (key, sha256, upload_id, "uploading", self._now_ms()),
        )
        return upload_id

    def _list_remote_parts(self, key: str, upload_id: str) -> dict[int, dict[str, Any]]:
        response = self.client.list_parts(
            Bucket=self.bucket,
            Key=key,
            UploadId=upload_id,
        )
        return {int(item["PartNumber"]): item for item in response.get("Parts", [])}

    def _persist_multipart_part(
        self,
        *,
        database: sqlite3.Connection,
        key: str,
        upload_id: str,
        part_number: int,
        part: bytes,
        remote: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if remote is None:
            remote = self.client.upload_part(
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=part,
                ChecksumAlgorithm="CRC64NVME",
            )
        etag = str(remote["ETag"])
        checksum = str(remote.get("ChecksumCRC64NVME", ""))
        if not checksum:
            raise RuntimeError("multipart part has no CRC64NVME checksum")
        database.execute(
            """
            INSERT OR REPLACE INTO multipart_parts(
                key, part_number, etag, checksum, size
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (key, part_number, etag, checksum, len(part)),
        )
        database.execute(
            "UPDATE multipart_uploads SET updated_ms = ? WHERE key = ?",
            (self._now_ms(), key),
        )
        return {
            "PartNumber": part_number,
            "ETag": etag,
            "ChecksumCRC64NVME": checksum,
        }

    def _mark_upload_complete(self, database: sqlite3.Connection, key: str) -> None:
        database.execute(
            """
            UPDATE multipart_uploads
            SET state = 'complete', updated_ms = ?
            WHERE key = ?
            """,
            (self._now_ms(), key),
        )

    def _verify_multipart_size(self, record: ObjectRecord, expected_size: int) -> None:
        attributes = self.client.get_object_attributes(
            Bucket=self.bucket,
            Key=record.key,
            VersionId=record.version_id,
            ObjectAttributes=["ObjectSize", "Checksum"],
        )
        if int(attributes.get("ObjectSize", -1)) != expected_size:
            raise RuntimeError("multipart object attribute size mismatch")

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    def abort_abandoned(self, older_than_ms: int) -> int:
        if self._db is None:
            return 0
        rows = self._db.execute(
            """
            SELECT key, upload_id
            FROM multipart_uploads
            WHERE state = 'uploading' AND updated_ms < ?
            """,
            (older_than_ms,),
        ).fetchall()
        aborted = 0
        for key, upload_id in rows:
            if not self._remote_parts_match_local_count(key, upload_id):
                continue
            self.client.abort_multipart_upload(
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
            )
            self._db.execute(
                """
                UPDATE multipart_uploads
                SET state = 'aborted', updated_ms = ?
                WHERE key = ?
                """,
                (self._now_ms(), key),
            )
            aborted += 1
        return aborted

    def _remote_parts_match_local_count(self, key: str, upload_id: str) -> bool:
        assert self._db is not None
        remote_count = len(self._list_remote_parts(key, upload_id))
        row = self._db.execute(
            "SELECT COUNT(*) FROM multipart_parts WHERE key = ?",
            (key,),
        ).fetchone()
        local_count = int(row[0])
        return remote_count == local_count

    def _recover_ambiguous(
        self, key: str, size: int, sha256: str, content_type: str
    ) -> ObjectRecord | None:
        try:
            head = self.client.head_object(Bucket=self.bucket, Key=key, ChecksumMode="ENABLED")
        except Exception:
            return None
        if (
            int(head.get("ContentLength", -1)) != size
            or head.get("Metadata", {}).get("sha256") != sha256
        ):
            return None
        response = {
            "VersionId": head.get("VersionId"),
            "ChecksumCRC64NVME": head.get("ChecksumCRC64NVME"),
        }
        return self._verified_record(key, size, sha256, content_type, response)

    def verify(self, record: ObjectRecord) -> bool:
        head = self.client.head_object(
            Bucket=self.bucket, Key=record.key, VersionId=record.version_id, ChecksumMode="ENABLED"
        )
        encryption_ok = (not self.require_kms) or (
            head.get("ServerSideEncryption") == "aws:kms"
            and head.get("SSEKMSKeyId") == self.kms_key_id
            and head.get("BucketKeyEnabled") is True
        )
        return (
            int(head["ContentLength"]) == record.size
            and head.get("ContentType") == record.content_type
            and head.get("Metadata", {}).get("sha256") == record.sha256
            and head.get("ChecksumCRC64NVME") == record.s3_checksum
            and encryption_ok
        )

    def _verified_record(
        self,
        key: str,
        size: int,
        sha256: str,
        content_type: str,
        response: dict[str, Any],
    ) -> ObjectRecord:
        checksum = response.get("ChecksumCRC64NVME")
        version = response.get("VersionId")
        if not checksum or not version:
            raise RuntimeError("archive did not return CRC64NVME checksum and version ID")
        record = ObjectRecord(
            object_id=str(uuid4()),
            key=key,
            version_id=str(version),
            size=size,
            sha256=sha256,
            s3_checksum=str(checksum),
            content_type=content_type,
        )
        if not self.verify(record):
            raise RuntimeError("archive verification failed after create")
        return record

    def _object_arguments(
        self,
        *,
        key: str,
        content_type: str,
        sha256: str,
        body: bytes | None = None,
        conditional: bool = False,
    ) -> dict[str, Any]:
        args: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": key,
            "ContentType": content_type,
            "ChecksumAlgorithm": "CRC64NVME",
            "Metadata": {"sha256": sha256},
        }
        if body is not None:
            args["Body"] = body
        if conditional:
            args["IfNoneMatch"] = "*"
        self._encryption(args)
        return args

    def _encryption(self, args: dict[str, Any]) -> None:
        if self.kms_key_id:
            args.update(
                {
                    "ServerSideEncryption": "aws:kms",
                    "SSEKMSKeyId": self.kms_key_id,
                    "BucketKeyEnabled": True,
                }
            )
        elif self.require_kms:
            raise RuntimeError("SSE-KMS is required but no KMS key was configured")

    @classmethod
    def _validate_key(cls, key: str) -> None:
        parts = key.split("/")
        if (
            len(parts) < 5
            or parts[0] != "v1"
            or parts[1] != "meetings"
            or parts[3] not in cls._CLASSES
        ):
            raise ValueError("archive key must use canonical opaque meeting key class")
        try:
            UUID(parts[2])
        except ValueError as exc:
            raise ValueError("archive key meeting identity must be a UUID") from exc
        if any(value in {".", "..", ""} for value in parts):
            raise ValueError("archive key contains an unsafe segment")
