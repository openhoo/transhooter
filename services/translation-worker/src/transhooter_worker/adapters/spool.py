from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sqlite3
import threading
import time
from base64 import b64decode, b64encode
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Any, Concatenate, Protocol, cast
from uuid import UUID, uuid4

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from transhooter_worker.domain.models import RawRef, SampleRange

_SPOOL_SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
    object_id TEXT NOT NULL UNIQUE,
    attempt_id TEXT NOT NULL,
    meeting_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    transport TEXT NOT NULL,
    direction TEXT NOT NULL,
    media_type TEXT NOT NULL,
    opaque_path TEXT NOT NULL UNIQUE,
    key_id TEXT NOT NULL,
    nonce BLOB NOT NULL,
    plaintext_sha256 TEXT NOT NULL,
    ciphertext_sha256 TEXT NOT NULL,
    size INTEGER NOT NULL CHECK(size >= 0),
    sample_start INTEGER,
    sample_end INTEGER,
    metadata_json BLOB NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('committed', 'uploaded', 'quarantined')),
    version_id TEXT,
    s3_checksum TEXT
);
CREATE TABLE IF NOT EXISTS terminals (
    attempt_id TEXT PRIMARY KEY,
    raw_object_id TEXT NOT NULL REFERENCES records(object_id)
);
CREATE TABLE IF NOT EXISTS checkpoint_deliveries (
    checkpoint_id TEXT PRIMARY KEY REFERENCES records(object_id),
    meeting_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    worker_epoch INTEGER NOT NULL CHECK(worker_epoch >= 1),
    checkpoint_hash TEXT NOT NULL,
    previous_hash TEXT,
    control_event_id TEXT NOT NULL UNIQUE,
    acknowledged INTEGER NOT NULL DEFAULT 0 CHECK(acknowledged IN (0, 1))
);
CREATE INDEX IF NOT EXISTS checkpoint_deliveries_scope_order
ON checkpoint_deliveries(meeting_id, worker_epoch, source_id, checkpoint_id);
"""


@dataclass(frozen=True, slots=True)
class SpoolCheckpointDelivery:
    checkpoint_id: UUID
    meeting_id: UUID
    source_id: UUID
    worker_epoch: int
    checkpoint_hash: str
    previous_hash: str | None
    control_event_id: UUID
    acknowledged: bool
    body: bytes
    raw_ref: RawRef


@dataclass(frozen=True, slots=True)
class SpoolCapacity:
    total_bytes: int
    used_bytes: int


class CapacityProbe(Protocol):
    def __call__(self, path: Path, /) -> SpoolCapacity: ...


def statvfs_capacity(path: Path) -> SpoolCapacity:
    stats = os.statvfs(path)
    total = stats.f_blocks * stats.f_frsize
    used = total - stats.f_bavail * stats.f_frsize
    return SpoolCapacity(total_bytes=total, used_bytes=used)


def deterministic_roomy_capacity(_path: Path) -> SpoolCapacity:
    return SpoolCapacity(total_bytes=1 << 40, used_bytes=0)


class SpoolUnavailable(RuntimeError):
    pass


def _locked[**P, R](
    method: Callable[Concatenate[EncryptedSpool, P], R],
) -> Callable[Concatenate[EncryptedSpool, P], R]:
    @wraps(method)
    def wrapper(self: EncryptedSpool, *args: P.args, **kwargs: P.kwargs) -> R:
        with self._exclusive():
            return method(self, *args, **kwargs)

    return cast(Callable[Concatenate["EncryptedSpool", P], R], wrapper)


class EncryptedSpool:
    """Write-before-effect encrypted journal with FULL-sync SQLite indexing."""

    def __init__(
        self,
        root: Path,
        database: Path,
        keys: dict[str, bytes],
        active_key_id: str,
        *,
        capacity_probe: CapacityProbe = statvfs_capacity,
    ) -> None:
        if active_key_id not in keys or len(keys[active_key_id]) != 32:
            raise ValueError("active AES-256 key is missing or invalid")
        self._root = root
        self._root.mkdir(parents=True, exist_ok=True)
        self._keys = keys
        self._active_key_id = active_key_id
        self._capacity_probe = capacity_probe
        self._lock = threading.RLock()
        self._lock_file = (self._root / ".spool.lock").open("a+b", buffering=0)
        self._append_counts: dict[UUID, int] = {}
        self._context: dict[str, Any] = {}
        self._db = sqlite3.connect(database, isolation_level=None, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute("PRAGMA foreign_keys=ON")
        with self._exclusive():
            self._db.executescript(_SPOOL_SCHEMA)
            self._recover_unlocked()

    @contextmanager
    def _exclusive(self) -> Any:
        with self._lock:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)

    @classmethod
    def from_keyring(
        cls,
        root: Path,
        database: Path,
        keyring_path: Path,
        *,
        capacity_probe: CapacityProbe = statvfs_capacity,
    ) -> EncryptedSpool:
        raw = json.loads(keyring_path.read_text("utf-8"))
        active = str(raw["active"])
        keys = {str(k): b64decode(v, validate=True) for k, v in dict(raw["keys"]).items()}
        return cls(root, database, keys, active, capacity_probe=capacity_probe)

    def set_context(self, context: dict[str, Any]) -> None:
        with self._lock:
            self._context = json.loads(json.dumps(context, separators=(",", ":")))

    @_locked
    def append(
        self,
        *,
        meeting_id: UUID,
        attempt_id: UUID,
        stage: str,
        transport: str,
        direction: str,
        media_type: str,
        payload: bytes,
        sample_range: SampleRange | None = None,
        metadata: tuple[tuple[str, str], ...] = (),
        object_id: UUID | None = None,
    ) -> RawRef:
        if object_id is not None:
            existing = self._existing_append(
                object_id,
                meeting_id,
                attempt_id,
                stage,
                transport,
                direction,
                media_type,
                payload,
                sample_range,
                metadata,
            )
            if existing is not None:
                return existing
        self._check_append_admission(meeting_id, len(payload))
        object_id = object_id or uuid4()
        key_id = self._active_key_id
        nonce = os.urandom(12)
        plaintext_hash = hashlib.sha256(payload).hexdigest()
        aad = self._envelope_aad(meeting_id, attempt_id, object_id, stage, metadata, version=2)
        encrypted = AESGCM(self._keys[key_id]).encrypt(nonce, payload, aad)
        ciphertext_hash = hashlib.sha256(encrypted).hexdigest()
        header = self._build_header(
            object_id=object_id,
            attempt_id=attempt_id,
            meeting_id=meeting_id,
            stage=stage,
            transport=transport,
            direction=direction,
            media_type=media_type,
            key_id=key_id,
            nonce=nonce,
            plaintext_hash=plaintext_hash,
            ciphertext_hash=ciphertext_hash,
            payload_size=len(payload),
            sample_range=sample_range,
            metadata=metadata,
        )
        final_path = self._root / f"{object_id}.wal"
        temp_path = self._root / f".{object_id}.tmp"
        ordinal = self._commit_envelope(
            header=header,
            encrypted=encrypted,
            final_path=final_path,
            temp_path=temp_path,
            nonce=nonce,
            metadata=metadata,
        )
        return RawRef(
            object_id=object_id,
            ordinal=ordinal,
            sha256=plaintext_hash,
            size=len(payload),
            media_type=media_type,
        )

    def _existing_append(
        self,
        object_id: UUID,
        meeting_id: UUID,
        attempt_id: UUID,
        stage: str,
        transport: str,
        direction: str,
        media_type: str,
        payload: bytes,
        sample_range: SampleRange | None,
        metadata: tuple[tuple[str, str], ...],
    ) -> RawRef | None:
        row = self._db.execute(
            """
            SELECT
                ordinal, meeting_id, attempt_id, stage, transport, direction,
                media_type, plaintext_sha256, size, sample_start, sample_end,
                metadata_json, opaque_path, key_id, nonce, ciphertext_sha256,
                state
            FROM records
            WHERE object_id = ?
            """,
            (str(object_id),),
        ).fetchone()
        if row is None:
            return None
        payload_hash = hashlib.sha256(payload).hexdigest()
        if row[16] not in {"committed", "uploaded"}:
            raise SpoolUnavailable("deterministic spool evidence is not reusable")
        if (
            row[1] != str(meeting_id)
            or row[2] != str(attempt_id)
            or row[3] != stage
            or row[4] != transport
            or row[5] != direction
            or row[6] != media_type
            or row[7] != payload_hash
            or row[8] != len(payload)
            or row[9] != (sample_range.start if sample_range else None)
            or row[10] != (sample_range.end if sample_range else None)
            or bytes(row[11]) != json.dumps(metadata, separators=(",", ":")).encode()
        ):
            raise SpoolUnavailable(
                "deterministic spool identity was reused with different evidence"
            )
        indexed = (row[1], row[2], row[3], row[12], row[13], row[14], row[7], row[15])
        authenticated = self._decrypt_indexed_record(object_id, indexed)
        if authenticated != payload:
            raise SpoolUnavailable("deterministic spool payload mismatch")
        header, _ = self._unpack(Path(row[12]).read_bytes())
        expected_header = {
            "object_id": str(object_id),
            "attempt_id": str(attempt_id),
            "meeting_id": str(meeting_id),
            "stage": stage,
            "transport": transport,
            "direction": direction,
            "media_type": media_type,
            "plaintext_sha256": payload_hash,
            "size": len(payload),
            "sample_start": sample_range.start if sample_range else None,
            "sample_end": sample_range.end if sample_range else None,
        }
        header_metadata = tuple(tuple(item) for item in header.get("metadata", ()))
        if (
            any(header.get(key) != value for key, value in expected_header.items())
            or header_metadata != metadata
        ):
            raise SpoolUnavailable("deterministic spool header mismatch")
        return RawRef(
            object_id=object_id,
            ordinal=int(row[0]),
            sha256=str(row[7]),
            size=int(row[8]),
            media_type=str(row[6]),
        )

    def _check_append_admission(self, meeting_id: UUID, payload_size: int) -> None:
        faults = self._fixture_faults(meeting_id)
        append_number = self._append_counts.get(meeting_id, 0) + 1
        self._append_counts[meeting_id] = append_number
        if bool(faults.get("unwritable", False)):
            raise SpoolUnavailable("injected unwritable spool")
        if float(faults.get("pressureRatio", 0)) >= 0.8:
            raise SpoolUnavailable("injected spool pressure at or above 80%")
        wal_limit = int(faults.get("walFailAfterAppends", 0))
        if wal_limit and append_number >= wal_limit:
            raise SpoolUnavailable("injected WAL fsync failure")
        sqlite_limit = int(faults.get("sqliteFailAfterAppends", 0))
        if sqlite_limit and append_number >= sqlite_limit:
            raise sqlite3.OperationalError("injected SQLite commit failure")

        capacity = self._capacity_probe(self._root)
        projected = capacity.used_bytes + payload_size + (1 << 20)
        if capacity.total_bytes <= 0 or projected / capacity.total_bytes >= 0.8:
            raise SpoolUnavailable(
                "encrypted spool cannot preserve payload below emergency 80% boundary"
            )

    def _build_header(
        self,
        *,
        object_id: UUID,
        attempt_id: UUID,
        meeting_id: UUID,
        stage: str,
        transport: str,
        direction: str,
        media_type: str,
        key_id: str,
        nonce: bytes,
        plaintext_hash: str,
        ciphertext_hash: str,
        payload_size: int,
        sample_range: SampleRange | None,
        metadata: tuple[tuple[str, str], ...],
    ) -> dict[str, Any]:
        return {
            "aad_version": 2,
            "object_id": str(object_id),
            "attempt_id": str(attempt_id),
            "meeting_id": str(meeting_id),
            "stage": stage,
            "transport": transport,
            "direction": direction,
            "media_type": media_type,
            "key_id": key_id,
            "nonce": b64encode(nonce).decode(),
            "plaintext_sha256": plaintext_hash,
            "ciphertext_sha256": ciphertext_hash,
            "size": payload_size,
            "sample_start": sample_range.start if sample_range else None,
            "sample_end": sample_range.end if sample_range else None,
            "metadata": metadata,
            "context": self._context,
        }

    @staticmethod
    def _envelope_aad(
        meeting_id: UUID,
        attempt_id: UUID,
        object_id: UUID,
        stage: str,
        metadata: tuple[tuple[str, ...], ...],
        *,
        version: int,
    ) -> bytes:
        base = f"{meeting_id}:{attempt_id}:{object_id}:{stage}".encode()
        if version < 2:
            return base
        return base + b":" + json.dumps(metadata, separators=(",", ":")).encode()

    def _commit_envelope(
        self,
        *,
        header: dict[str, Any],
        encrypted: bytes,
        final_path: Path,
        temp_path: Path,
        nonce: bytes,
        metadata: tuple[tuple[str, str], ...],
    ) -> int:
        envelope = self._pack(header, encrypted)
        try:
            self._write_fsync(temp_path, envelope)
            os.replace(temp_path, final_path)
            self._fsync_directory()
            cursor = self._db.execute(
                """
                INSERT INTO records(
                    object_id, attempt_id, meeting_id, stage, transport,
                    direction, media_type, opaque_path, key_id, nonce,
                    plaintext_sha256, ciphertext_sha256, size, sample_start,
                    sample_end, metadata_json, state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    header["object_id"],
                    header["attempt_id"],
                    header["meeting_id"],
                    header["stage"],
                    header["transport"],
                    header["direction"],
                    header["media_type"],
                    str(final_path),
                    header["key_id"],
                    nonce,
                    header["plaintext_sha256"],
                    header["ciphertext_sha256"],
                    header["size"],
                    header["sample_start"],
                    header["sample_end"],
                    json.dumps(metadata, separators=(",", ":")).encode(),
                    "committed",
                ),
            )
        except BaseException:
            temp_path.unlink(missing_ok=True)
            final_path.unlink(missing_ok=True)
            raise
        if cursor.lastrowid is None:
            raise SpoolUnavailable("SQLite did not assign a journal ordinal")
        return int(cursor.lastrowid)

    def terminal(self, attempt_id: UUID, payload: bytes) -> RawRef:
        with self._exclusive():
            existing = self._existing_terminal(attempt_id)
            if existing is not None:
                return existing
            meeting_id, transport, metadata = self._attempt_context(attempt_id)
            self._db.execute("BEGIN IMMEDIATE")
            try:
                ref = self.append(
                    meeting_id=meeting_id,
                    attempt_id=attempt_id,
                    stage="terminal",
                    transport=transport,
                    direction="internal",
                    media_type="application/json",
                    payload=payload,
                    metadata=metadata,
                )
                self._db.execute(
                    """
                    INSERT INTO terminals(attempt_id, raw_object_id)
                    VALUES (?, ?)
                    """,
                    (str(attempt_id), str(ref.object_id)),
                )
                self._db.execute("COMMIT")
                return ref
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def _existing_terminal(self, attempt_id: UUID) -> RawRef | None:
        row = self._db.execute(
            """
            SELECT
                records.object_id,
                records.ordinal,
                records.plaintext_sha256,
                records.size,
                records.media_type,
                records.state
            FROM terminals
            JOIN records ON records.object_id = terminals.raw_object_id
            WHERE terminals.attempt_id = ?
            """,
            (str(attempt_id),),
        ).fetchone()
        if row is None:
            return None
        if row[5] not in {"committed", "uploaded"}:
            raise SpoolUnavailable("terminal evidence is not reusable")
        self.read(UUID(row[0]))
        return RawRef(
            object_id=UUID(row[0]),
            ordinal=row[1],
            sha256=row[2],
            size=row[3],
            media_type=row[4],
        )

    @staticmethod
    def _checkpoint_body_identity(
        body: bytes,
    ) -> tuple[UUID, UUID, int, str, str | None]:
        payload = json.loads(body.decode("utf-8"))
        if (
            not isinstance(payload, dict)
            or json.dumps(payload, separators=(",", ":"), sort_keys=True).encode() != body
        ):
            raise ValueError("checkpoint body is not canonical")
        checkpoint_id = UUID(str(payload["checkpointId"]))
        source_id = UUID(str(payload["sourceParticipantId"]))
        worker_epoch = payload["workerEpoch"]
        checkpoint_hash = payload["highWatermarkSha256"]
        previous_hash = payload.get("previousCheckpointSha256")
        if (
            not isinstance(worker_epoch, int)
            or isinstance(worker_epoch, bool)
            or worker_epoch < 1
            or not isinstance(checkpoint_hash, str)
            or not checkpoint_hash
            or (previous_hash is not None and not isinstance(previous_hash, str))
        ):
            raise ValueError("checkpoint body identity is malformed")
        return checkpoint_id, source_id, worker_epoch, checkpoint_hash, previous_hash

    def _attempt_context(self, attempt_id: UUID) -> tuple[UUID, str, tuple[tuple[str, str], ...]]:
        row = self._db.execute(
            """
            SELECT meeting_id, transport, metadata_json
            FROM records
            WHERE attempt_id = ?
            ORDER BY ordinal
            LIMIT 1
            """,
            (str(attempt_id),),
        ).fetchone()
        if row is None:
            raise SpoolUnavailable("terminal cannot precede its attempt evidence")
        metadata = tuple(
            (str(key), str(value)) for key, value in json.loads(bytes(row[2]).decode())
        )
        return UUID(row[0]), str(row[1]), metadata

    def register_checkpoint_delivery(
        self,
        *,
        checkpoint_id: UUID,
        meeting_id: UUID,
        source_id: UUID,
        worker_epoch: int,
        checkpoint_hash: str,
        previous_hash: str | None,
        control_event_id: UUID,
        body: bytes,
    ) -> SpoolCheckpointDelivery:
        if worker_epoch < 1:
            raise ValueError("worker_epoch must be positive")
        try:
            body_identity = self._checkpoint_body_identity(body)
        except (KeyError, TypeError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
            raise SpoolUnavailable("checkpoint body identity is invalid") from exc
        if body_identity != (
            checkpoint_id,
            source_id,
            worker_epoch,
            checkpoint_hash,
            previous_hash,
        ):
            raise SpoolUnavailable("checkpoint body identity differs from delivery metadata")
        metadata = (("controlEventId", str(control_event_id)),)
        with self._exclusive():
            existing = self._checkpoint_delivery(checkpoint_id)
            if existing is not None:
                self._require_checkpoint_delivery_identity(
                    existing,
                    meeting_id=meeting_id,
                    source_id=source_id,
                    worker_epoch=worker_epoch,
                    checkpoint_hash=checkpoint_hash,
                    previous_hash=previous_hash,
                    control_event_id=control_event_id,
                    body=body,
                )
                return existing
            self._db.execute("BEGIN IMMEDIATE")
            try:
                raw_ref = self.append(
                    meeting_id=meeting_id,
                    attempt_id=checkpoint_id,
                    stage="checkpoint",
                    transport="http",
                    direction=str(source_id),
                    media_type="application/json",
                    payload=body,
                    metadata=metadata,
                    object_id=checkpoint_id,
                )
                self._db.execute(
                    """
                    INSERT INTO checkpoint_deliveries(
                        checkpoint_id, meeting_id, source_id, worker_epoch,
                        checkpoint_hash, previous_hash, control_event_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(checkpoint_id),
                        str(meeting_id),
                        str(source_id),
                        worker_epoch,
                        checkpoint_hash,
                        previous_hash,
                        str(control_event_id),
                    ),
                )
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            return SpoolCheckpointDelivery(
                checkpoint_id=checkpoint_id,
                meeting_id=meeting_id,
                source_id=source_id,
                worker_epoch=worker_epoch,
                checkpoint_hash=checkpoint_hash,
                previous_hash=previous_hash,
                control_event_id=control_event_id,
                acknowledged=False,
                body=body,
                raw_ref=raw_ref,
            )

    def list_checkpoint_deliveries(
        self, meeting_id: UUID, worker_epoch: int
    ) -> tuple[SpoolCheckpointDelivery, ...]:
        with self._exclusive():
            rows = self._db.execute(
                """
                SELECT checkpoint_deliveries.checkpoint_id
                FROM checkpoint_deliveries
                JOIN records ON records.object_id = checkpoint_deliveries.checkpoint_id
                WHERE checkpoint_deliveries.meeting_id = ?
                  AND checkpoint_deliveries.worker_epoch = ?
                ORDER BY checkpoint_deliveries.source_id, records.ordinal
                """,
                (str(meeting_id), worker_epoch),
            ).fetchall()
            deliveries: list[SpoolCheckpointDelivery] = []
            for (checkpoint_id,) in rows:
                delivery = self._checkpoint_delivery(UUID(checkpoint_id))
                if delivery is None:
                    raise SpoolUnavailable("checkpoint delivery disappeared during recovery")
                deliveries.append(delivery)
            return tuple(deliveries)

    def mark_checkpoint_delivery_acknowledged(
        self, control_event_id: UUID
    ) -> SpoolCheckpointDelivery:
        with self._exclusive():
            self._db.execute("BEGIN IMMEDIATE")
            try:
                cursor = self._db.execute(
                    """
                    UPDATE checkpoint_deliveries
                    SET acknowledged = 1
                    WHERE control_event_id = ?
                    """,
                    (str(control_event_id),),
                )
                if cursor.rowcount != 1:
                    raise KeyError(control_event_id)
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            row = self._db.execute(
                "SELECT checkpoint_id FROM checkpoint_deliveries WHERE control_event_id = ?",
                (str(control_event_id),),
            ).fetchone()
            if row is None:
                raise SpoolUnavailable("acknowledged checkpoint delivery disappeared")
            delivery = self._checkpoint_delivery(UUID(row[0]))
            if delivery is None:
                raise SpoolUnavailable("acknowledged checkpoint delivery is unreadable")
            return delivery

    def _checkpoint_delivery(self, checkpoint_id: UUID) -> SpoolCheckpointDelivery | None:
        row = self._db.execute(
            """
            SELECT
                checkpoint_deliveries.meeting_id,
                checkpoint_deliveries.source_id,
                checkpoint_deliveries.worker_epoch,
                checkpoint_deliveries.checkpoint_hash,
                checkpoint_deliveries.previous_hash,
                checkpoint_deliveries.control_event_id,
                checkpoint_deliveries.acknowledged,
                records.object_id,
                records.ordinal,
                records.plaintext_sha256,
                records.size,
                records.media_type,
                records.state,
                records.opaque_path,
                records.attempt_id,
                records.stage,
                records.transport,
                records.direction,
                records.metadata_json
            FROM checkpoint_deliveries
            JOIN records ON records.object_id = checkpoint_deliveries.checkpoint_id
            WHERE checkpoint_deliveries.checkpoint_id = ?
            """,
            (str(checkpoint_id),),
        ).fetchone()
        if row is None:
            return None
        if row[12] not in {"committed", "uploaded"}:
            raise SpoolUnavailable("checkpoint delivery evidence is not reusable")
        if not Path(row[13]).is_file():
            raise SpoolUnavailable("checkpoint delivery WAL evidence is missing")
        expected_metadata = (("controlEventId", str(row[5])),)
        if (
            row[14] != str(checkpoint_id)
            or row[15] != "checkpoint"
            or row[16] != "http"
            or row[17] != row[1]
            or row[11] != "application/json"
            or bytes(row[18]) != json.dumps(expected_metadata, separators=(",", ":")).encode()
        ):
            raise SpoolUnavailable("checkpoint delivery WAL identity mismatch")
        body = self.read(checkpoint_id)
        header, _ = self._unpack(Path(row[13]).read_bytes())
        expected_header = {
            "object_id": str(checkpoint_id),
            "attempt_id": str(checkpoint_id),
            "meeting_id": row[0],
            "stage": "checkpoint",
            "transport": "http",
            "direction": row[1],
            "media_type": "application/json",
            "plaintext_sha256": row[9],
            "size": row[10],
            "sample_start": None,
            "sample_end": None,
        }
        if (
            any(header.get(key) != value for key, value in expected_header.items())
            or tuple(tuple(item) for item in header.get("metadata", ())) != expected_metadata
        ):
            raise SpoolUnavailable("checkpoint delivery live WAL header mismatch")
        return SpoolCheckpointDelivery(
            checkpoint_id=checkpoint_id,
            meeting_id=UUID(row[0]),
            source_id=UUID(row[1]),
            worker_epoch=int(row[2]),
            checkpoint_hash=str(row[3]),
            previous_hash=str(row[4]) if row[4] is not None else None,
            control_event_id=UUID(row[5]),
            acknowledged=bool(row[6]),
            body=body,
            raw_ref=RawRef(
                object_id=UUID(row[7]),
                ordinal=int(row[8]),
                sha256=str(row[9]),
                size=int(row[10]),
                media_type=str(row[11]),
            ),
        )

    @staticmethod
    def _require_checkpoint_delivery_identity(
        delivery: SpoolCheckpointDelivery,
        *,
        meeting_id: UUID,
        source_id: UUID,
        worker_epoch: int,
        checkpoint_hash: str,
        previous_hash: str | None,
        control_event_id: UUID,
        body: bytes,
    ) -> None:
        if (
            delivery.meeting_id != meeting_id
            or delivery.source_id != source_id
            or delivery.worker_epoch != worker_epoch
            or delivery.checkpoint_hash != checkpoint_hash
            or delivery.previous_hash != previous_hash
            or delivery.control_event_id != control_event_id
            or delivery.body != body
        ):
            raise SpoolUnavailable(
                "deterministic checkpoint delivery was reused with different evidence"
            )

    @_locked
    def read(self, object_id: UUID) -> bytes:
        row = self._db.execute(
            """
            SELECT
                meeting_id,
                attempt_id,
                stage,
                opaque_path,
                key_id,
                nonce,
                plaintext_sha256,
                ciphertext_sha256
            FROM records
            WHERE object_id = ?
            """,
            (str(object_id),),
        ).fetchone()
        if row is None:
            raise KeyError(object_id)
        try:
            return self._decrypt_indexed_record(object_id, row)
        except SpoolUnavailable:
            self._quarantine(object_id)
            raise

    def _decrypt_indexed_record(self, object_id: UUID, row: tuple[Any, ...]) -> bytes:
        try:
            header, encrypted = self._unpack(Path(row[3]).read_bytes())
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise SpoolUnavailable("invalid spool envelope") from exc
        if (
            header.get("object_id") != str(object_id)
            or hashlib.sha256(encrypted).hexdigest() != row[7]
        ):
            raise SpoolUnavailable("ciphertext authentication hash mismatch")
        try:
            metadata = tuple(tuple(str(value) for value in item) for item in header["metadata"])
            aad = self._envelope_aad(
                UUID(row[0]),
                UUID(row[1]),
                object_id,
                str(row[2]),
                metadata,
                version=int(header.get("aad_version", 1)),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise SpoolUnavailable("invalid authenticated spool header") from exc
        try:
            plain = AESGCM(self._keys[row[4]]).decrypt(row[5], encrypted, aad)
        except (InvalidTag, KeyError, ValueError) as exc:
            raise SpoolUnavailable("spool key missing or authentication failed") from exc
        if hashlib.sha256(plain).hexdigest() != row[6]:
            raise SpoolUnavailable("plaintext hash mismatch")
        return plain

    @_locked
    def committed(self, stage: str | None = None) -> list[tuple[RawRef, SampleRange | None]]:
        sql = """
            SELECT
                object_id, ordinal, plaintext_sha256, size, media_type,
                sample_start, sample_end
            FROM records
            WHERE state = 'committed'
        """
        args: tuple[Any, ...] = ()
        if stage:
            sql += " AND stage = ?"
            args = (stage,)
        rows = self._db.execute(sql + " ORDER BY ordinal", args).fetchall()
        return [self._record_with_range(row) for row in rows]

    @staticmethod
    def _record_with_range(
        row: tuple[Any, ...],
    ) -> tuple[RawRef, SampleRange | None]:
        ref = RawRef(
            object_id=UUID(row[0]),
            ordinal=row[1],
            sha256=row[2],
            size=row[3],
            media_type=row[4],
        )
        sample_range = SampleRange(start=row[5], end=row[6]) if row[5] is not None else None
        return ref, sample_range

    @_locked
    def context(self, object_id: UUID) -> tuple[UUID, UUID, str, int, str]:
        row = self._db.execute(
            """
            SELECT meeting_id, attempt_id, stage, ordinal, media_type
            FROM records
            WHERE object_id = ?
            """,
            (str(object_id),),
        ).fetchone()
        if row is None:
            raise KeyError(object_id)
        return UUID(row[0]), UUID(row[1]), str(row[2]), int(row[3]), str(row[4])

    @_locked
    def committed_scoped(
        self,
        meeting_id: UUID,
        stage: str,
        direction: str,
        include_uploaded: bool = False,
    ) -> list[tuple[RawRef, SampleRange | None]]:
        rows = self._db.execute(
            """
            SELECT
                object_id, ordinal, plaintext_sha256, size, media_type,
                sample_start, sample_end
            FROM records
            WHERE (state = 'committed' OR (? = 1 AND state = 'uploaded'))
              AND meeting_id = ?
              AND stage = ?
              AND direction = ?
            ORDER BY ordinal
            """,
            (int(include_uploaded), str(meeting_id), stage, direction),
        ).fetchall()
        return [self._record_with_range(row) for row in rows]

    @_locked
    def pcm_scopes(self, include_uploaded: bool = False) -> list[tuple[UUID, str, str]]:
        rows = self._db.execute(
            """
            SELECT DISTINCT meeting_id, stage, direction
            FROM records
            WHERE (state = 'committed' OR (? = 1 AND state = 'uploaded'))
              AND stage IN ('stt-input', 'tts-output', 'livekit-output')
            ORDER BY meeting_id, stage, direction
            """,
            (int(include_uploaded),),
        ).fetchall()
        return [(UUID(row[0]), str(row[1]), str(row[2])) for row in rows]

    @_locked
    def covering_checkpoint(
        self,
        meeting_id: UUID,
        stage: str,
        direction: str,
        sample_end: int,
        terminal_only: bool = False,
    ) -> UUID | None:
        rows = self._db.execute(
            """
            SELECT object_id
            FROM records
            WHERE meeting_id = ?
              AND stage = 'checkpoint'
              AND state IN ('committed', 'uploaded')
            ORDER BY ordinal DESC
            """,
            (str(meeting_id),),
        ).fetchall()
        for (object_id,) in rows:
            payload = json.loads(self.read(UUID(object_id)))
            if (
                not terminal_only or payload.get("terminal") is True
            ) and self._checkpoint_watermark(payload, stage, direction) >= sample_end:
                return UUID(object_id)
        return None

    @_locked
    def checkpoint_covers(
        self, checkpoint_id: UUID, stage: str, direction: str, sample_end: int
    ) -> bool:
        row = self._db.execute(
            "SELECT stage, state FROM records WHERE object_id = ?",
            (str(checkpoint_id),),
        ).fetchone()
        if row is None or row[0] != "checkpoint" or row[1] not in {"committed", "uploaded"}:
            return False
        return (
            self._checkpoint_watermark(json.loads(self.read(checkpoint_id)), stage, direction)
            >= sample_end
        )

    @staticmethod
    def _checkpoint_watermark(payload: dict[str, Any], stage: str, direction: str) -> int:
        if stage == "stt-input":
            return (
                int(payload.get("acceptedInput", 0))
                if payload.get("sourceParticipantId") == direction
                else -1
            )
        if stage in {"tts-output", "livekit-output"}:
            return (
                int(payload.get("emittedOutput", 0))
                if payload.get("destinationParticipantId") == direction
                else -1
            )
        return -1

    def mark_uploaded(self, object_id: UUID, version_id: str, checksum: str) -> None:
        with self._exclusive():
            self._db.execute(
                """
                UPDATE records
                SET state = 'uploaded', version_id = ?, s3_checksum = ?
                WHERE object_id = ? AND state = 'committed'
                """,
                (version_id, checksum, str(object_id)),
            )

    def recover(self) -> None:
        with self._exclusive():
            self._recover_unlocked()

    def _recover_unlocked(self) -> None:
        referenced = {
            Path(row[0]) for row in self._db.execute("SELECT opaque_path FROM records").fetchall()
        }
        self._remove_stale_temps()
        self._import_orphan_finals(referenced)
        self._recover_checkpoint_deliveries()
        missing = [path for path in referenced if not path.exists()]
        if missing:
            raise SpoolUnavailable(f"{len(missing)} indexed spool payloads are missing")

    def _recover_checkpoint_deliveries(self) -> None:
        rows = self._db.execute(
            """
            SELECT records.object_id, records.meeting_id, records.opaque_path
            FROM records
            LEFT JOIN checkpoint_deliveries
              ON checkpoint_deliveries.checkpoint_id = records.object_id
            WHERE records.stage = 'checkpoint'
              AND records.state IN ('committed', 'uploaded')
              AND checkpoint_deliveries.checkpoint_id IS NULL
            ORDER BY records.ordinal
            """
        ).fetchall()
        for object_id_value, meeting_id_value, opaque_path in rows:
            object_id = UUID(object_id_value)
            try:
                body = self.read(object_id)
                header, _ = self._unpack(Path(opaque_path).read_bytes())
                self._db.execute("BEGIN IMMEDIATE")
                try:
                    self._insert_orphan_checkpoint_delivery(
                        header=header,
                        body=body,
                        checkpoint_id=object_id,
                        meeting_id=UUID(meeting_id_value),
                    )
                    self._db.execute("COMMIT")
                except BaseException:
                    self._db.execute("ROLLBACK")
                    raise
            except (
                KeyError,
                OSError,
                TypeError,
                UnicodeError,
                ValueError,
                json.JSONDecodeError,
                SpoolUnavailable,
            ):
                self._quarantine(object_id)

    def _remove_stale_temps(self) -> None:
        now = time.time()
        for temp in self._root.glob(".*.tmp"):
            if now - temp.stat().st_mtime > 3600:
                temp.unlink(missing_ok=True)

    def _import_orphan_finals(self, referenced: set[Path]) -> None:
        for final in self._root.glob("*.wal"):
            if final in referenced:
                continue
            try:
                header, encrypted = self._unpack(final.read_bytes())
                self._import_orphan(final, header, encrypted)
            except (
                InvalidTag,
                KeyError,
                TypeError,
                UnicodeError,
                ValueError,
                json.JSONDecodeError,
            ):
                final.rename(final.with_suffix(".quarantine"))

    def _import_orphan(self, final: Path, header: dict[str, Any], encrypted: bytes) -> None:
        object_id = UUID(str(header["object_id"]))
        attempt_id = UUID(str(header["attempt_id"]))
        meeting_id = UUID(str(header["meeting_id"]))
        key_id = str(header["key_id"])
        nonce = b64decode(str(header["nonce"]), validate=True)
        ciphertext_hash = str(header["ciphertext_sha256"])
        if final.stem != str(object_id) or hashlib.sha256(encrypted).hexdigest() != ciphertext_hash:
            raise ValueError("orphan ciphertext hash mismatch")
        try:
            metadata = tuple(tuple(str(value) for value in item) for item in header["metadata"])
            aad = self._envelope_aad(
                meeting_id,
                attempt_id,
                object_id,
                str(header["stage"]),
                metadata,
                version=int(header.get("aad_version", 1)),
            )
            plain = AESGCM(self._keys[key_id]).decrypt(nonce, encrypted, aad)
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("orphan authenticated header is invalid") from exc
        plaintext_hash = str(header["plaintext_sha256"])
        if hashlib.sha256(plain).hexdigest() != plaintext_hash or len(plain) != int(header["size"]):
            raise ValueError("orphan plaintext mismatch")
        self._db.execute("BEGIN IMMEDIATE")
        try:
            self._insert_orphan_record(
                final=final,
                header=header,
                object_id=object_id,
                attempt_id=attempt_id,
                meeting_id=meeting_id,
                key_id=key_id,
                nonce=nonce,
                plaintext_hash=plaintext_hash,
                ciphertext_hash=ciphertext_hash,
            )
            if header["stage"] == "checkpoint":
                self._insert_orphan_checkpoint_delivery(
                    header=header,
                    body=plain,
                    checkpoint_id=object_id,
                    meeting_id=meeting_id,
                )
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise

    def _insert_orphan_record(
        self,
        *,
        final: Path,
        header: dict[str, Any],
        object_id: UUID,
        attempt_id: UUID,
        meeting_id: UUID,
        key_id: str,
        nonce: bytes,
        plaintext_hash: str,
        ciphertext_hash: str,
    ) -> None:
        self._db.execute(
            """
            INSERT INTO records(
                object_id, attempt_id, meeting_id, stage, transport, direction,
                media_type, opaque_path, key_id, nonce, plaintext_sha256,
                ciphertext_sha256, size, sample_start, sample_end,
                metadata_json, state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(object_id),
                str(attempt_id),
                str(meeting_id),
                str(header["stage"]),
                str(header["transport"]),
                str(header["direction"]),
                str(header["media_type"]),
                str(final),
                key_id,
                nonce,
                plaintext_hash,
                ciphertext_hash,
                int(header["size"]),
                header.get("sample_start"),
                header.get("sample_end"),
                json.dumps(header.get("metadata", ()), separators=(",", ":")).encode(),
                "committed",
            ),
        )
        if header["stage"] == "terminal":
            self._db.execute(
                """
                INSERT OR IGNORE INTO terminals(attempt_id, raw_object_id)
                VALUES (?, ?)
                """,
                (str(attempt_id), str(object_id)),
            )

    def _insert_orphan_checkpoint_delivery(
        self,
        *,
        header: dict[str, Any],
        body: bytes,
        checkpoint_id: UUID,
        meeting_id: UUID,
    ) -> None:
        if int(header.get("aad_version", 1)) < 2:
            raise ValueError("legacy orphan checkpoint lacks authenticated replay identity")
        payload = json.loads(body.decode("utf-8"))
        if (
            not isinstance(payload, dict)
            or json.dumps(payload, separators=(",", ":"), sort_keys=True).encode() != body
        ):
            raise ValueError("orphan checkpoint body is not canonical")
        source_id = UUID(str(payload["sourceParticipantId"]))
        body_checkpoint_id = UUID(str(payload["checkpointId"]))
        worker_epoch = payload["workerEpoch"]
        checkpoint_hash = payload["highWatermarkSha256"]
        previous_hash = payload.get("previousCheckpointSha256")
        metadata = tuple(tuple(item) for item in header["metadata"])
        if (
            body_checkpoint_id != checkpoint_id
            or not isinstance(worker_epoch, int)
            or isinstance(worker_epoch, bool)
            or worker_epoch < 1
            or not isinstance(checkpoint_hash, str)
            or not checkpoint_hash
            or (previous_hash is not None and not isinstance(previous_hash, str))
            or header["attempt_id"] != str(checkpoint_id)
            or header["meeting_id"] != str(meeting_id)
            or header["transport"] != "http"
            or header["direction"] != str(source_id)
            or header["media_type"] != "application/json"
            or len(metadata) != 1
            or len(metadata[0]) != 2
            or metadata[0][0] != "controlEventId"
        ):
            raise ValueError("orphan checkpoint replay identity is malformed")
        control_event_id = UUID(str(metadata[0][1]))
        self._db.execute(
            """
            INSERT INTO checkpoint_deliveries(
                checkpoint_id, meeting_id, source_id, worker_epoch,
                checkpoint_hash, previous_hash, control_event_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(checkpoint_id),
                str(meeting_id),
                str(source_id),
                worker_epoch,
                checkpoint_hash,
                previous_hash,
                str(control_event_id),
            ),
        )

    def frame(
        self,
        meeting_id: UUID,
        publication_id: UUID,
        destination_id: UUID,
        sequence: int,
        pcm: bytes,
        samples: SampleRange,
    ) -> RawRef:
        return self.append(
            meeting_id=meeting_id,
            attempt_id=publication_id,
            stage="livekit-output",
            transport="websocket",
            direction=str(destination_id),
            media_type="audio/L16",
            payload=pcm,
            sample_range=samples,
            metadata=(("sequence", str(sequence)), ("publicationId", str(publication_id))),
        )

    def usage_ratio(self) -> float:
        capacity = self._capacity_probe(self._root)
        return capacity.used_bytes / capacity.total_bytes if capacity.total_bytes > 0 else 1.0

    def _quarantine(self, object_id: UUID) -> None:
        self._db.execute(
            "UPDATE records SET state='quarantined' WHERE object_id=?", (str(object_id),)
        )

    @staticmethod
    def _pack(header: dict[str, Any], encrypted: bytes) -> bytes:
        encoded = json.dumps(header, separators=(",", ":"), sort_keys=True).encode()
        return b"TSW1" + f"{len(encoded):08x}".encode() + encoded + encrypted

    @staticmethod
    def _unpack(envelope: bytes) -> tuple[dict[str, Any], bytes]:
        if len(envelope) < 12 or envelope[:4] != b"TSW1":
            raise ValueError("invalid spool magic")
        length = int(envelope[4:12], 16)
        if length <= 0 or 12 + length >= len(envelope):
            raise ValueError("invalid spool header length")
        header = json.loads(envelope[12 : 12 + length])
        if not isinstance(header, dict):
            raise ValueError("invalid spool header")
        return header, envelope[12 + length :]

    @staticmethod
    def _fixture_faults(meeting_id: UUID) -> dict[str, Any]:
        if os.environ.get("APP_ENV") != "test":
            return {}
        path = os.environ.get("FIXTURE_SCENARIO_FILE")
        if not path:
            return {}
        try:
            data = json.loads(Path(path).read_text("utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SpoolUnavailable("fixture scenario file is invalid") from exc
        meetings = data.get("consultations", {}) if isinstance(data, dict) else {}
        selected = (
            meetings.get(str(meeting_id), meetings.get("*", {}))
            if isinstance(meetings, dict)
            else {}
        )
        section = selected.get("spool", {}) if isinstance(selected, dict) else {}
        if not isinstance(section, dict):
            raise SpoolUnavailable("fixture spool scenario must be an object")
        return section

    @staticmethod
    def _write_fsync(path: Path, body: bytes) -> None:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            view = memoryview(body)
            while view:
                written = os.write(fd, view)
                view = view[written:]
            os.fsync(fd)
        finally:
            os.close(fd)

    def _fsync_directory(self) -> None:
        fd = os.open(self._root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
