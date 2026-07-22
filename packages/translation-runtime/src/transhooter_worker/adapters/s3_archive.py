from __future__ import annotations

import base64
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
CREATE TABLE IF NOT EXISTS multipart_journal (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    owner_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS multipart_uploads (
    key TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL,
    upload_id TEXT NOT NULL,
    state TEXT NOT NULL,
    updated_ms INTEGER NOT NULL,
    object_id TEXT,
    version_id TEXT,
    object_size INTEGER,
    object_checksum TEXT,
    content_type TEXT,
    owner_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS multipart_parts (
    key TEXT NOT NULL,
    part_number INTEGER NOT NULL,
    etag TEXT NOT NULL,
    checksum TEXT NOT NULL,
    size INTEGER NOT NULL,
    byte_start INTEGER,
    byte_end INTEGER,
    PRIMARY KEY (key, part_number),
    FOREIGN KEY (key) REFERENCES multipart_uploads(key)
);
"""


class ArchiveConflict(RuntimeError):
    pass


class _MultipartPartMismatch(RuntimeError):
    pass


@dataclass(slots=True)
class S3Archive:
    client: Any
    bucket: str
    kms_key_id: str | None
    require_kms: bool = True
    multipart_database: Path | None = None
    _db: sqlite3.Connection | None = field(init=False, default=None, repr=False)
    _multipart_owner_id: str | None = field(init=False, default=None, repr=False)

    MULTIPART_THRESHOLD = 100 * 1024 * 1024
    PART_SIZE = 16 * 1024 * 1024
    _CLASSES = frozenset({"pipeline", "audio", "captions", "media", "inventory"})

    def __post_init__(self) -> None:
        self._db = None
        self._multipart_owner_id = None
        if self.multipart_database:
            self._db = sqlite3.connect(self.multipart_database, isolation_level=None)
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=FULL")
            self._db.executescript(_MULTIPART_SCHEMA)
            self._multipart_owner_id = self._load_multipart_owner(self._db)
            self.abort_abandoned(self._now_ms() - 24 * 60 * 60 * 1000)

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

    def _put_multipart(
        self,
        key: str,
        body: bytes,
        content_type: str,
        sha256: str,
        *,
        allow_restart: bool = True,
    ) -> ObjectRecord:
        database = self._multipart_db()
        completed = self._load_completed_record(database, key, sha256, content_type)
        if completed is not None:
            if not self.verify(completed):
                raise RuntimeError("durable completed multipart object no longer verifies")
            return completed

        upload_id = self._load_or_create_upload(database, key, content_type, sha256)
        intended = self._journal_intended_parts(database, key, body)
        try:
            remote_parts = self._list_remote_parts(key, upload_id)
        except ClientError as exc:
            recovered = self._recover_ambiguous(key, len(body), sha256, content_type)
            if recovered is not None:
                self._verify_multipart_size(recovered, len(body))
                self._persist_completed_record(database, recovered)
                return recovered
            if allow_restart and self._is_missing_upload(exc):
                self._forget_upload(database, key)
                return self._put_multipart(key, body, content_type, sha256, allow_restart=False)
            raise

        if not self._remote_parts_match(remote_parts, intended):
            if not allow_restart:
                raise ArchiveConflict(key)
            self._abort_and_forget(database, key, upload_id)
            return self._put_multipart(key, body, content_type, sha256, allow_restart=False)

        try:
            completion_parts = [
                self._persist_multipart_part(
                    database=database,
                    key=key,
                    upload_id=upload_id,
                    part_number=part_number,
                    part=part,
                    checksum=checksum,
                    remote=remote_parts.get(part_number),
                )
                for part_number, (_, _, part, checksum) in intended.items()
            ]
        except _MultipartPartMismatch as exc:
            if not allow_restart:
                raise ArchiveConflict(key) from exc
            self._abort_and_forget(database, key, upload_id)
            return self._put_multipart(key, body, content_type, sha256, allow_restart=False)
        try:
            response = self.client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": completion_parts},
                IfNoneMatch="*",
            )
        except ClientError as exc:
            recovered = self._recover_ambiguous(key, len(body), sha256, content_type)
            if recovered is not None:
                self._verify_multipart_size(recovered, len(body))
                self._persist_completed_record(database, recovered)
                return recovered
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if allow_restart and self._is_missing_upload(exc):
                self._forget_upload(database, key)
                return self._put_multipart(key, body, content_type, sha256, allow_restart=False)
            if status in (409, 412):
                raise ArchiveConflict(key) from exc
            raise
        except Exception:
            recovered = self._recover_ambiguous(key, len(body), sha256, content_type)
            if recovered is not None:
                self._verify_multipart_size(recovered, len(body))
                self._persist_completed_record(database, recovered)
                return recovered
            raise

        record = self._verified_record(key, len(body), sha256, content_type, response)
        self._verify_multipart_size(record, len(body))
        self._persist_completed_record(database, record)
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
            SELECT sha256, upload_id, state, content_type, owner_id
            FROM multipart_uploads
            WHERE key = ?
            """,
            (key,),
        ).fetchone()
        if existing is not None:
            if (
                existing[0] != sha256
                or existing[2] != "uploading"
                or existing[4] != self._multipart_owner()
            ):
                raise ArchiveConflict(key)
            if existing[3] is None:
                self._abort_and_forget(database, key, str(existing[1]))
            elif existing[3] != content_type:
                raise ArchiveConflict(key)
            else:
                return str(existing[1])

        args = self._object_arguments(
            key=key,
            content_type=content_type,
            sha256=sha256,
            multipart_owner_id=self._multipart_owner(),
        )
        created = self.client.create_multipart_upload(**args)
        upload_id = str(created["UploadId"])
        try:
            database.execute(
                """
                INSERT INTO multipart_uploads(
                    key, sha256, upload_id, state, updated_ms, content_type, owner_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    key,
                    sha256,
                    upload_id,
                    "uploading",
                    self._now_ms(),
                    content_type,
                    self._multipart_owner(),
                ),
            )
        except BaseException:
            self.client.abort_multipart_upload(Bucket=self.bucket, Key=key, UploadId=upload_id)
            raise
        return upload_id

    def _list_remote_parts(self, key: str, upload_id: str) -> dict[int, dict[str, Any]]:
        parts: dict[int, dict[str, Any]] = {}
        marker: int | None = None
        while True:
            arguments: dict[str, Any] = {
                "Bucket": self.bucket,
                "Key": key,
                "UploadId": upload_id,
            }
            if marker is not None:
                arguments["PartNumberMarker"] = marker
            response = self.client.list_parts(**arguments)
            for item in response.get("Parts", []):
                parts[int(item["PartNumber"])] = item
            if not response.get("IsTruncated"):
                return parts
            next_marker = response.get("NextPartNumberMarker")
            if next_marker is None:
                raise RuntimeError("truncated multipart listing omitted its continuation marker")
            marker = int(next_marker)

    def _persist_multipart_part(
        self,
        *,
        database: sqlite3.Connection,
        key: str,
        upload_id: str,
        part_number: int,
        part: bytes,
        checksum: str,
        remote: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if remote is None:
            remote = self.client.upload_part(
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=part,
                ChecksumCRC64NVME=checksum,
            )
        etag = str(remote["ETag"])
        remote_checksum = str(remote.get("ChecksumCRC64NVME", ""))
        if remote_checksum != checksum:
            raise _MultipartPartMismatch("multipart part checksum differs from journaled body")
        database.execute(
            """
            UPDATE multipart_parts
            SET etag = ?
            WHERE key = ? AND part_number = ?
            """,
            (etag, key, part_number),
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

    @staticmethod
    def _load_multipart_owner(database: sqlite3.Connection) -> str:
        database.execute(
            """
            INSERT OR IGNORE INTO multipart_journal(singleton, owner_id)
            VALUES (1, ?)
            """,
            (str(uuid4()),),
        )
        row = database.execute(
            "SELECT owner_id FROM multipart_journal WHERE singleton = 1"
        ).fetchone()
        if row is None or not isinstance(row[0], str) or not row[0]:
            raise RuntimeError("multipart journal ownership identity is unavailable")
        return row[0]

    def _multipart_owner(self) -> str:
        if self._multipart_owner_id is None:
            raise RuntimeError("durable multipart ownership identity is unavailable")
        return self._multipart_owner_id

    def _journal_intended_parts(
        self, database: sqlite3.Connection, key: str, body: bytes
    ) -> dict[int, tuple[int, int, bytes, str]]:
        intended: dict[int, tuple[int, int, bytes, str]] = {}
        database.execute("BEGIN IMMEDIATE")
        try:
            for start in range(0, len(body), self.PART_SIZE):
                part_number = start // self.PART_SIZE + 1
                end = min(start + self.PART_SIZE, len(body))
                part = body[start:end]
                checksum = self._crc64nvme(part)
                intended[part_number] = (start, end, part, checksum)
                database.execute(
                    """
                    INSERT INTO multipart_parts(
                        key, part_number, etag, checksum, size, byte_start, byte_end
                    ) VALUES (?, ?, '', ?, ?, ?, ?)
                    ON CONFLICT(key, part_number) DO UPDATE SET
                        checksum = excluded.checksum,
                        size = excluded.size,
                        byte_start = excluded.byte_start,
                        byte_end = excluded.byte_end
                    """,
                    (key, part_number, checksum, len(part), start, end),
                )
            placeholders = ",".join("?" for _ in intended)
            database.execute(
                f"""
                DELETE FROM multipart_parts
                WHERE key = ? AND part_number NOT IN ({placeholders})
                """,
                (key, *intended),
            )
        except BaseException:
            database.execute("ROLLBACK")
            raise
        database.execute("COMMIT")
        return intended

    @staticmethod
    def _remote_parts_match(
        remote_parts: dict[int, dict[str, Any]],
        intended: dict[int, tuple[int, int, bytes, str]],
    ) -> bool:
        if set(remote_parts) - set(intended):
            return False
        for number, remote in remote_parts.items():
            _, _, part, checksum = intended[number]
            if (
                int(remote.get("Size", -1)) != len(part)
                or remote.get("ChecksumCRC64NVME") != checksum
            ):
                return False
        return True

    @staticmethod
    def _crc64nvme(body: bytes) -> str:
        crc = 0xFFFFFFFFFFFFFFFF
        polynomial = 0x9A6C9329AC4BC9B5
        for byte in body:
            crc ^= byte
            for _ in range(8):
                crc = (crc >> 1) ^ (polynomial if crc & 1 else 0)
        value = (crc ^ 0xFFFFFFFFFFFFFFFF).to_bytes(8, "big")
        return base64.b64encode(value).decode("ascii")

    def _load_completed_record(
        self,
        database: sqlite3.Connection,
        key: str,
        sha256: str,
        content_type: str,
    ) -> ObjectRecord | None:
        row = database.execute(
            """
            SELECT sha256, state, object_id, version_id, object_size,
                   object_checksum, content_type, owner_id
            FROM multipart_uploads
            WHERE key = ?
            """,
            (key,),
        ).fetchone()
        if row is None or row[1] != "complete":
            return None
        if row[0] != sha256 or row[6] != content_type or row[7] != self._multipart_owner():
            raise ArchiveConflict(key)
        if any(value is None for value in row[2:7]):
            raise RuntimeError("completed multipart journal identity is incomplete")
        return ObjectRecord(
            object_id=str(row[2]),
            key=key,
            version_id=str(row[3]),
            size=int(row[4]),
            sha256=str(row[0]),
            s3_checksum=str(row[5]),
            content_type=str(row[6]),
        )

    @staticmethod
    def _persist_completed_record(database: sqlite3.Connection, record: ObjectRecord) -> None:
        database.execute("BEGIN IMMEDIATE")
        try:
            cursor = database.execute(
                """
                UPDATE multipart_uploads
                SET state = 'complete',
                    updated_ms = ?,
                    object_id = ?,
                    version_id = ?,
                    object_size = ?,
                    object_checksum = ?,
                    content_type = ?
                WHERE key = ? AND sha256 = ? AND content_type = ?
                """,
                (
                    S3Archive._now_ms(),
                    record.object_id,
                    record.version_id,
                    record.size,
                    record.s3_checksum,
                    record.content_type,
                    record.key,
                    record.sha256,
                    record.content_type,
                ),
            )
            if cursor.rowcount != 1:
                raise ArchiveConflict(record.key)
        except BaseException:
            database.execute("ROLLBACK")
            raise
        database.execute("COMMIT")

    def _abort_and_forget(self, database: sqlite3.Connection, key: str, upload_id: str) -> None:
        owner = database.execute(
            "SELECT upload_id, owner_id FROM multipart_uploads WHERE key = ?",
            (key,),
        ).fetchone()
        if owner != (upload_id, self._multipart_owner()):
            raise ArchiveConflict(key)
        try:
            self.client.abort_multipart_upload(
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
            )
        except ClientError as exc:
            if not self._is_missing_upload(exc):
                raise
        self._forget_upload(database, key)

    @staticmethod
    def _is_missing_upload(exc: ClientError) -> bool:
        error = exc.response.get("Error", {})
        code: object = error.get("Code")
        return isinstance(code, str) and code == "NoSuchUpload"

    @staticmethod
    def _forget_upload(database: sqlite3.Connection, key: str) -> None:
        database.execute("BEGIN IMMEDIATE")
        try:
            database.execute("DELETE FROM multipart_parts WHERE key = ?", (key,))
            database.execute("DELETE FROM multipart_uploads WHERE key = ?", (key,))
        except BaseException:
            database.execute("ROLLBACK")
            raise
        database.execute("COMMIT")

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
            WHERE state = 'uploading' AND updated_ms < ? AND owner_id = ?
            """,
            (older_than_ms, self._multipart_owner()),
        ).fetchall()
        aborted = 0
        for key, upload_id in rows:
            try:
                self._reconcile_remote_parts(key, upload_id)
            except ClientError as exc:
                if not self._is_missing_upload(exc):
                    raise
                self._forget_upload(self._db, key)
                aborted += 1
                continue
            try:
                self.client.abort_multipart_upload(
                    Bucket=self.bucket,
                    Key=key,
                    UploadId=upload_id,
                )
            except ClientError as exc:
                if not self._is_missing_upload(exc):
                    raise
                self._forget_upload(self._db, key)
                aborted += 1
                continue
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

    def _reconcile_remote_parts(self, key: str, upload_id: str) -> None:
        self._list_remote_parts(key, upload_id)

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
        multipart_owner_id: str | None = None,
    ) -> dict[str, Any]:
        metadata = {"sha256": sha256}
        if multipart_owner_id is not None:
            metadata["transhooter-multipart-owner"] = multipart_owner_id
        args: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": key,
            "ContentType": content_type,
            "ChecksumAlgorithm": "CRC64NVME",
            "Metadata": metadata,
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
