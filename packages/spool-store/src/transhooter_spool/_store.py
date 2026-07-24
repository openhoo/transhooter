from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import threading
from base64 import b64decode, b64encode
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import Any, Literal, Self, cast
from uuid import UUID, uuid4, uuid5

from cryptography.exceptions import InvalidTag

from transhooter_spool._crypto import (
    decrypt_aesgcm,
    encrypt_aesgcm,
    fsync_directory,
    header_aad,
    pack_envelope,
    sha256_hex,
    unpack_envelope,
    write_fsync,
)
from transhooter_spool._models import (
    CapacityProbe,
    DeliveryState,
    HandoffState,
    Metadata,
    RawRef,
    RecordState,
    SampleRange,
    SpoolCheckpointDelivery,
    SpoolConsultationSeal,
    SpoolRecordContext,
    SpoolRecordDelivery,
    SpoolUnavailable,
    TerminalCheckpointIntent,
    statvfs_capacity,
)
from transhooter_spool._schema import SCHEMA, SCHEMA_VERSION, UNSUPPORTED_SCHEMA

_TupleKey = tuple[UUID, int, UUID, int, int]
_AUTHORITY_TOKEN = object()
_TABLES = {
    "checkpoint_deliveries",
    "compacted_envelopes",
    "consultation_handoffs",
    "consultation_seals",
    "records",
    "spool_meta",
}
_CHECKPOINT_COLUMNS = (
    "checkpoint_id",
    "record_id",
    "meeting_id",
    "generation",
    "worker_id",
    "worker_epoch",
    "write_epoch",
    "source_id",
    "checkpoint_hash",
    "previous_hash",
    "control_event_id",
    "object_key",
    "evidence_ordinal",
    "delivery_state",
    "error_kind",
    "failed_at",
)


def _now() -> datetime:
    return datetime.now(UTC)


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be timezone-aware UTC")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise SpoolUnavailable("stored timestamp is not timezone-aware UTC")
    return parsed.astimezone(UTC)


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def _canonical_metadata(metadata: Metadata) -> Metadata:
    if not isinstance(metadata, tuple):
        raise TypeError("metadata must be an immutable tuple")
    normalized: list[tuple[str, str]] = []
    for item in metadata:
        if not isinstance(item, tuple) or len(item) != 2:
            raise TypeError("metadata entries must be string pairs")
        key, value = item
        if not isinstance(key, str) or not isinstance(value, str) or not key:
            raise ValueError("metadata entries must contain a non-empty string key")
        normalized.append((key, value))
    return tuple(normalized)


def _tuple_values(key: _TupleKey) -> tuple[str, int, str, int, int]:
    meeting_id, generation, worker_id, worker_epoch, write_epoch = key
    return str(meeting_id), generation, str(worker_id), worker_epoch, write_epoch


def _validate_tuple(key: _TupleKey) -> None:
    _, generation, _, worker_epoch, write_epoch = key
    if generation < 0:
        raise ValueError("generation must be non-negative")
    if worker_epoch < 1:
        raise ValueError("worker_epoch must be positive")
    if write_epoch < 0:
        raise ValueError("write_epoch must be non-negative")


class _Authority:
    __slots__ = ("_closed", "_fd", "_key", "_store")

    def __init__(
        self,
        token: object,
        store: EncryptedSpool,
        key: _TupleKey,
        fd: int,
    ) -> None:
        if token is not _AUTHORITY_TOKEN:
            raise TypeError("consultation authorities can only be issued by EncryptedSpool")
        self._store = store
        self._key = key
        self._fd = fd
        self._closed = False

    @property
    def meeting_id(self) -> UUID:
        return self._key[0]

    @property
    def generation(self) -> int:
        return self._key[1]

    @property
    def worker_id(self) -> UUID:
        return self._key[2]

    @property
    def worker_epoch(self) -> int:
        return self._key[3]

    @property
    def write_epoch(self) -> int:
        return self._key[4]

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)

    def __enter__(self) -> Self:
        if self._closed:
            raise SpoolUnavailable("consultation authority is closed")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __copy__(self) -> Self:
        raise TypeError("consultation authorities are noncopyable")

    def __deepcopy__(self, memo: dict[int, object]) -> Self:
        del memo
        raise TypeError("consultation authorities are noncopyable")


class ConsultationProducerAuthority(_Authority):
    pass


class ConsultationRecoveryAuthority(_Authority):
    pass


class EncryptedSpool:
    """Schema-v2 encrypted local evidence store with tuple-scoped authority."""

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
        if any(len(key) != 32 for key in keys.values()):
            raise ValueError("every spool key must be an AES-256 key")
        self._root = root
        self._database_path = database
        self._keys = dict(keys)
        self._active_key_id = active_key_id
        self._capacity_probe = capacity_probe
        self._lock = threading.RLock()
        self._closed = False
        self._append_counts: dict[UUID, int] = {}
        proof = self._validate_cutover_proof()
        self._root.mkdir(parents=True, exist_ok=True)
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_file = (self._root / ".spool.lock").open("a+b", buffering=0)
        self._authority_root = self._root / ".authority"
        self._authority_root.mkdir(exist_ok=True)
        self._db = sqlite3.connect(database, isolation_level=None, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute("PRAGMA foreign_keys=ON")
        try:
            with self._exclusive():
                self._initialize_schema(proof)
                self._recover_compacted_envelopes_unlocked()
                self._reject_legacy_orphans_unlocked()
                self._require_indexed_payloads_unlocked()
            if proof is not None:
                proof.unlink()
                fsync_directory(proof.parent)
        except BaseException:
            self._db.close()
            self._lock_file.close()
            raise

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
        if not isinstance(raw, dict) or not isinstance(raw.get("keys"), dict):
            raise ValueError("spool keyring is malformed")
        active = str(raw["active"])
        keys = {
            str(key): b64decode(str(value), validate=True)
            for key, value in cast(dict[object, object], raw["keys"]).items()
        }
        return cls(root, database, keys, active, capacity_probe=capacity_probe)

    def producer(self) -> SpoolProducer:
        return SpoolProducer(self)

    def drainer(self) -> SpoolDrainer:
        return SpoolDrainer(self)

    def checkpoints(self) -> SpoolCheckpointStore:
        return SpoolCheckpointStore(self)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._db.close()
            self._lock_file.close()

    def __enter__(self) -> Self:
        self._require_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _require_open(self) -> None:
        if self._closed:
            raise SpoolUnavailable("encrypted spool is closed")

    @contextmanager
    def _exclusive(self) -> Any:
        self._require_open()
        with self._lock:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)

    def _validate_cutover_proof(self) -> Path | None:
        value = os.environ.get("SPOOL_CUTOVER_PROOF_FILE")
        if not value:
            return None
        proof = Path(value)
        if (
            self._root.resolve() == proof.resolve()
            or self._root.resolve() in proof.resolve().parents
        ):
            raise SpoolUnavailable("spool cutover proof must be outside the spool root")
        try:
            payload = json.loads(proof.read_text("utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SpoolUnavailable("spool cutover proof is invalid") from exc
        expected = {"createdAt", "device", "inode", "listingSha256", "nonce", "path", "schema"}
        if not isinstance(payload, dict) or set(payload) != expected:
            raise SpoolUnavailable("spool cutover proof is invalid")
        if (
            payload["schema"] != SCHEMA_VERSION
            or not isinstance(payload["createdAt"], str)
            or not isinstance(payload["device"], int)
            or isinstance(payload["device"], bool)
            or not isinstance(payload["inode"], int)
            or isinstance(payload["inode"], bool)
            or not isinstance(payload["nonce"], str)
            or len(payload["nonce"]) != 43
            or payload["listingSha256"] != sha256_hex(b"")
            or not self._root.is_dir()
            or payload["path"] != str(self._root.resolve())
        ):
            raise SpoolUnavailable("spool cutover proof is invalid")
        stat = self._root.stat()
        if stat.st_dev != payload["device"] or stat.st_ino != payload["inode"]:
            raise SpoolUnavailable("spool cutover proof does not identify this volume")
        if any(self._root.iterdir()):
            raise SpoolUnavailable("spool cutover proof requires an empty spool root")
        return proof

    def _initialize_schema(self, proof: Path | None) -> None:
        tables = {
            str(row[0])
            for row in self._db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        version = int(self._db.execute("PRAGMA user_version").fetchone()[0])
        if not tables and version == 0:
            self._db.executescript("BEGIN IMMEDIATE;\n" + SCHEMA)
            try:
                self._db.execute(
                    "INSERT INTO spool_meta(key,value) VALUES ('schema_version',?)",
                    (str(SCHEMA_VERSION),),
                )
                if proof is not None:
                    payload = json.loads(proof.read_text("utf-8"))
                    self._db.execute(
                        "INSERT INTO spool_meta(key,value) VALUES ('cutover_nonce',?)",
                        (str(payload["nonce"]),),
                    )
                self._db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                self._db.execute("COMMIT")
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute("ROLLBACK")
                raise
            fsync_directory(self._database_path.parent)
            return
        columns = tuple(
            str(row[1]) for row in self._db.execute("PRAGMA table_info(checkpoint_deliveries)")
        )
        meta = (
            self._db.execute("SELECT value FROM spool_meta WHERE key='schema_version'").fetchone()
            if "spool_meta" in tables
            else None
        )
        if (
            version != SCHEMA_VERSION
            or tables != _TABLES
            or columns != _CHECKPOINT_COLUMNS
            or meta != (str(SCHEMA_VERSION),)
        ):
            raise SpoolUnavailable(UNSUPPORTED_SCHEMA)

    def _authority_path(self, key: _TupleKey) -> Path:
        meeting_id, generation, worker_id, worker_epoch, write_epoch = key
        return self._authority_root / (
            f"{meeting_id}.{generation}.{worker_id}.{worker_epoch}.{write_epoch}.lock"
        )

    def _acquire_authority_fd(self, key: _TupleKey, *, blocking: bool) -> int | None:
        fd = os.open(self._authority_path(key), os.O_RDWR | os.O_CREAT, 0o600)
        operation = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
        try:
            fcntl.flock(fd, operation)
        except BlockingIOError:
            os.close(fd)
            return None
        return fd

    def open_consultation_producer(
        self,
        *,
        meeting_id: UUID,
        generation: int,
        worker_id: UUID,
        worker_epoch: int,
        write_epoch: int,
    ) -> ConsultationProducerAuthority:
        key = (meeting_id, generation, worker_id, worker_epoch, write_epoch)
        _validate_tuple(key)
        fd = self._acquire_authority_fd(key, blocking=False)
        if fd is None:
            raise SpoolUnavailable("consultation tuple is already owned")
        authority = ConsultationProducerAuthority(_AUTHORITY_TOKEN, self, key, fd)
        try:
            with self._exclusive():
                row = self._handoff_row_unlocked(key)
                if row is None:
                    self._db.execute(
                        """
                        INSERT INTO consultation_handoffs(
                          meeting_id,generation,worker_id,worker_epoch,write_epoch,state,started_at
                        ) VALUES (?,?,?,?,?,'active',?)
                        """,
                        (*_tuple_values(key), _timestamp(_now())),
                    )
                elif row[0] != "active":
                    raise SpoolUnavailable(f"consultation handoff is {row[0]}")
                self._recover_orphans_for_authority_unlocked(authority)
            return authority
        except BaseException:
            authority.close()
            raise

    def acquire_consultation_recovery(
        self,
        *,
        meeting_id: UUID,
        generation: int,
        worker_id: UUID,
        worker_epoch: int,
        write_epoch: int,
        blocking: bool = False,
    ) -> ConsultationRecoveryAuthority | None:
        key = (meeting_id, generation, worker_id, worker_epoch, write_epoch)
        _validate_tuple(key)
        fd = self._acquire_authority_fd(key, blocking=blocking)
        if fd is None:
            return None
        authority = ConsultationRecoveryAuthority(_AUTHORITY_TOKEN, self, key, fd)
        try:
            with self._exclusive():
                if self._handoff_row_unlocked(key) is None:
                    raise SpoolUnavailable("consultation handoff does not exist")
                self._recover_orphans_for_authority_unlocked(authority)
            return authority
        except BaseException:
            authority.close()
            raise

    def _require_authority(
        self,
        authority: ConsultationProducerAuthority | ConsultationRecoveryAuthority,
        expected: type[ConsultationProducerAuthority] | type[ConsultationRecoveryAuthority],
    ) -> _TupleKey:
        if type(authority) is not expected or authority._store is not self or authority.closed:
            raise SpoolUnavailable("consultation authority is closed, foreign, or wrong-kind")
        return authority._key

    def _handoff_row_unlocked(self, key: _TupleKey) -> tuple[Any, ...] | None:
        return cast(
            tuple[Any, ...] | None,
            self._db.execute(
                """
                SELECT state,reason,started_at,settling_at,sealed_at,relinquished_at
                FROM consultation_handoffs
                WHERE meeting_id=? AND generation=? AND worker_id=? AND worker_epoch=? AND write_epoch=?
                """,
                _tuple_values(key),
            ).fetchone(),
        )

    def begin_consultation_settlement(self, authority: ConsultationProducerAuthority) -> None:
        key = self._require_authority(authority, ConsultationProducerAuthority)
        with self._exclusive():
            row = self._handoff_row_unlocked(key)
            if row is None:
                raise SpoolUnavailable("consultation handoff does not exist")
            if row[0] == "settling":
                return
            if row[0] != "active":
                raise SpoolUnavailable(f"consultation handoff is {row[0]}")
            self._db.execute(
                """
                UPDATE consultation_handoffs SET state='settling',settling_at=?
                WHERE meeting_id=? AND generation=? AND worker_id=? AND worker_epoch=? AND write_epoch=?
                """,
                (_timestamp(_now()), *_tuple_values(key)),
            )

    def relinquish_consultation(
        self, authority: ConsultationProducerAuthority, reason: str
    ) -> None:
        key = self._require_authority(authority, ConsultationProducerAuthority)
        self._relinquish_unlocked_with_lock(key, reason, allowed={"active", "settling"})

    def relinquish_expired_consultation(
        self, authority: ConsultationRecoveryAuthority, reason: str
    ) -> None:
        key = self._require_authority(authority, ConsultationRecoveryAuthority)
        self._relinquish_unlocked_with_lock(key, reason, allowed={"active", "settling", "sealed"})

    def _relinquish_unlocked_with_lock(
        self, key: _TupleKey, reason: str, *, allowed: set[str]
    ) -> None:
        if not 1 <= len(reason) <= 512:
            raise ValueError("relinquishment reason must contain 1 to 512 characters")
        with self._exclusive():
            row = self._handoff_row_unlocked(key)
            if row is None:
                raise SpoolUnavailable("consultation handoff does not exist")
            if row[0] == "relinquished":
                if row[1] != reason:
                    raise SpoolUnavailable("consultation was relinquished for a different reason")
                return
            if row[0] not in allowed:
                raise SpoolUnavailable(f"consultation handoff is {row[0]}")
            self._db.execute(
                """
                UPDATE consultation_handoffs
                SET state='relinquished',relinquished_at=?,reason=?
                WHERE meeting_id=? AND generation=? AND worker_id=? AND worker_epoch=? AND write_epoch=?
                """,
                (_timestamp(_now()), reason, *_tuple_values(key)),
            )

    def append(
        self,
        authority: ConsultationProducerAuthority,
        *,
        meeting_id: UUID,
        attempt_id: UUID,
        stage: str,
        transport: str,
        direction: str,
        media_type: str,
        payload: bytes,
        sample_range: SampleRange | None = None,
        metadata: Metadata = (),
        object_id: UUID | None = None,
    ) -> RawRef:
        key = self._require_authority(authority, ConsultationProducerAuthority)
        if meeting_id != key[0]:
            raise SpoolUnavailable("record meeting does not match producer authority")
        with self._exclusive():
            self._require_writable_handoff_unlocked(key)
            return self._append_unlocked(
                key=key,
                attempt_id=attempt_id,
                stage=stage,
                transport=transport,
                direction=direction,
                media_type=media_type,
                payload=payload,
                sample_range=sample_range,
                metadata=metadata,
                object_id=object_id,
            )

    def _require_writable_handoff_unlocked(self, key: _TupleKey) -> None:
        row = self._handoff_row_unlocked(key)
        if row is None:
            raise SpoolUnavailable("consultation handoff does not exist")
        if row[0] == "sealed":
            raise SpoolUnavailable("consultation evidence is sealed")
        if row[0] == "relinquished":
            raise SpoolUnavailable("consultation evidence is relinquished")
        if row[0] not in {"active", "settling"}:
            raise SpoolUnavailable("consultation handoff is not writable")

    def _append_unlocked(
        self,
        *,
        key: _TupleKey,
        attempt_id: UUID,
        stage: str,
        transport: str,
        direction: str,
        media_type: str,
        payload: bytes,
        sample_range: SampleRange | None,
        metadata: Metadata,
        object_id: UUID | None,
    ) -> RawRef:
        metadata = _canonical_metadata(metadata)
        if object_id is not None:
            existing = self._existing_append_unlocked(
                object_id=object_id,
                key=key,
                attempt_id=attempt_id,
                stage=stage,
                transport=transport,
                direction=direction,
                media_type=media_type,
                payload=payload,
                sample_range=sample_range,
                metadata=metadata,
            )
            if existing is not None:
                return existing
        faults = self._fixture_faults(key[0])
        append_number = self._append_counts.get(key[0], 0) + 1
        self._append_counts[key[0]] = append_number
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
        projected = capacity.used_bytes + len(payload) + (1 << 20)
        if capacity.total_bytes <= 0 or projected / capacity.total_bytes >= 0.8:
            raise SpoolUnavailable(
                "encrypted spool cannot preserve payload below emergency 80% boundary"
            )
        object_id = object_id or uuid4()
        nonce = os.urandom(12)
        plaintext_hash = sha256_hex(payload)
        meeting_id, generation, worker_id, worker_epoch, write_epoch = key
        header: dict[str, Any] = {
            "aad_version": 3,
            "schema_version": SCHEMA_VERSION,
            "object_id": str(object_id),
            "attempt_id": str(attempt_id),
            "meeting_id": str(meeting_id),
            "generation": generation,
            "worker_id": str(worker_id),
            "worker_epoch": worker_epoch,
            "write_epoch": write_epoch,
            "stage": stage,
            "transport": transport,
            "direction": direction,
            "media_type": media_type,
            "key_id": self._active_key_id,
            "nonce": b64encode(nonce).decode(),
            "plaintext_sha256": plaintext_hash,
            "ciphertext_sha256": "",
            "size": len(payload),
            "sample_start": sample_range.start if sample_range else None,
            "sample_end": sample_range.end if sample_range else None,
            "metadata": metadata,
        }
        encrypted = encrypt_aesgcm(
            self._keys[self._active_key_id], nonce, payload, header_aad(header)
        )
        header["ciphertext_sha256"] = sha256_hex(encrypted)
        final_path = self._root / f"{object_id}.wal"
        temp_path = self._root / f".{object_id}.tmp"
        try:
            write_fsync(temp_path, pack_envelope(header, encrypted))
            os.replace(temp_path, final_path)
            fsync_directory(self._root)
            cursor = self._db.execute(
                """
                INSERT INTO records(
                  object_id,attempt_id,meeting_id,generation,worker_id,worker_epoch,write_epoch,
                  stage,transport,direction,media_type,opaque_path,key_id,nonce,plaintext_sha256,
                  ciphertext_sha256,size,sample_start,sample_end,metadata_json,state
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'committed')
                """,
                (
                    str(object_id),
                    str(attempt_id),
                    str(meeting_id),
                    generation,
                    str(worker_id),
                    worker_epoch,
                    write_epoch,
                    stage,
                    transport,
                    direction,
                    media_type,
                    str(final_path),
                    self._active_key_id,
                    nonce,
                    plaintext_hash,
                    header["ciphertext_sha256"],
                    len(payload),
                    sample_range.start if sample_range else None,
                    sample_range.end if sample_range else None,
                    _canonical_json(metadata),
                ),
            )
        except BaseException:
            temp_path.unlink(missing_ok=True)
            if not self._db.in_transaction:
                final_path.unlink(missing_ok=True)
            raise
        if cursor.lastrowid is None:
            raise SpoolUnavailable("SQLite did not assign a journal ordinal")
        return RawRef(object_id, int(cursor.lastrowid), plaintext_hash, len(payload), media_type)

    def _existing_append_unlocked(
        self,
        *,
        object_id: UUID,
        key: _TupleKey,
        attempt_id: UUID,
        stage: str,
        transport: str,
        direction: str,
        media_type: str,
        payload: bytes,
        sample_range: SampleRange | None,
        metadata: Metadata,
    ) -> RawRef | None:
        row = self._db.execute(
            """
            SELECT ordinal,attempt_id,meeting_id,generation,worker_id,worker_epoch,write_epoch,
                   stage,transport,direction,media_type,plaintext_sha256,size,sample_start,sample_end,
                   metadata_json,state
            FROM records WHERE object_id=?
            """,
            (str(object_id),),
        ).fetchone()
        if row is None:
            return None
        expected = (
            str(attempt_id),
            *_tuple_values(key),
            stage,
            transport,
            direction,
            media_type,
            sha256_hex(payload),
            len(payload),
            sample_range.start if sample_range else None,
            sample_range.end if sample_range else None,
            _canonical_json(metadata),
        )
        actual = (
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
            row[9],
            row[10],
            row[11],
            row[12],
            row[13],
            row[14],
            bytes(row[15]),
        )
        if row[16] not in {"committed", "uploaded"} or actual != expected:
            raise SpoolUnavailable(
                "deterministic spool identity was reused with different evidence"
            )
        header, body = self._read_authenticated_record_unlocked(object_id)
        if body != payload or tuple(tuple(item) for item in header["metadata"]) != metadata:
            raise SpoolUnavailable("deterministic spool payload or header mismatch")
        return RawRef(object_id, int(row[0]), str(row[11]), int(row[12]), str(row[10]))

    def terminal(
        self,
        authority: ConsultationProducerAuthority,
        attempt_id: UUID,
        payload: bytes,
    ) -> RawRef:
        key = self._require_authority(authority, ConsultationProducerAuthority)
        with self._exclusive():
            self._require_writable_handoff_unlocked(key)
            existing = self._db.execute(
                "SELECT object_id FROM records WHERE attempt_id=? AND stage='terminal' ORDER BY ordinal LIMIT 1",
                (str(attempt_id),),
            ).fetchone()
            if existing is not None:
                return self._raw_ref_unlocked(UUID(existing[0]))
            context = self._db.execute(
                """
                SELECT transport,metadata_json FROM records
                WHERE attempt_id=? AND meeting_id=? AND generation=? AND worker_id=?
                  AND worker_epoch=? AND write_epoch=? ORDER BY ordinal LIMIT 1
                """,
                (str(attempt_id), *_tuple_values(key)),
            ).fetchone()
            if context is None:
                raise SpoolUnavailable("terminal cannot precede its attempt evidence")
            metadata = tuple(tuple(item) for item in json.loads(bytes(context[1])))
            return self._append_unlocked(
                key=key,
                attempt_id=attempt_id,
                stage="terminal",
                transport=str(context[0]),
                direction="internal",
                media_type="application/json",
                payload=payload,
                sample_range=None,
                metadata=cast(Metadata, metadata),
                object_id=None,
            )

    def frame(
        self,
        authority: ConsultationProducerAuthority,
        meeting_id: UUID,
        publication_id: UUID,
        destination_id: UUID,
        sequence: int,
        pcm: bytes,
        samples: SampleRange,
    ) -> RawRef:
        return self.append(
            authority,
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

    def register_checkpoint_delivery(
        self,
        authority: ConsultationProducerAuthority,
        *,
        checkpoint_id: UUID,
        meeting_id: UUID,
        generation: int,
        worker_id: UUID,
        worker_epoch: int,
        write_epoch: int,
        source_id: UUID,
        checkpoint_hash: str,
        previous_hash: str | None,
        control_event_id: UUID,
        object_key: str,
        body: bytes,
    ) -> SpoolCheckpointDelivery:
        key = self._require_authority(authority, ConsultationProducerAuthority)
        supplied = (meeting_id, generation, worker_id, worker_epoch, write_epoch)
        if key != supplied:
            raise SpoolUnavailable("checkpoint tuple does not match producer authority")
        self._validate_checkpoint_body(
            body, checkpoint_id, source_id, worker_epoch, checkpoint_hash, previous_hash
        )
        if not object_key:
            raise ValueError("checkpoint object_key must not be empty")
        metadata: Metadata = (
            ("controlEventId", str(control_event_id)),
            ("generation", str(generation)),
            ("workerId", str(worker_id)),
            ("workerEpoch", str(worker_epoch)),
            ("writeEpoch", str(write_epoch)),
            ("objectKey", object_key),
            ("evidenceOrdinal", ""),
        )
        with self._exclusive():
            self._require_writable_handoff_unlocked(key)
            existing = self._checkpoint_delivery_unlocked(checkpoint_id)
            if existing is not None:
                expected = (
                    checkpoint_id,
                    checkpoint_id,
                    meeting_id,
                    generation,
                    worker_id,
                    worker_epoch,
                    write_epoch,
                    source_id,
                    checkpoint_hash,
                    previous_hash,
                    control_event_id,
                    object_key,
                    None,
                    body,
                )
                actual = (
                    existing.checkpoint_id,
                    existing.record_id,
                    existing.meeting_id,
                    existing.generation,
                    existing.worker_id,
                    existing.worker_epoch,
                    existing.write_epoch,
                    existing.source_id,
                    existing.checkpoint_hash,
                    existing.previous_hash,
                    existing.control_event_id,
                    existing.object_key,
                    existing.evidence_ordinal,
                    existing.body,
                )
                if actual != expected:
                    raise SpoolUnavailable(
                        "deterministic checkpoint delivery was reused with different evidence"
                    )
                return existing
            self._db.execute("BEGIN IMMEDIATE")
            try:
                ref = self._append_unlocked(
                    key=key,
                    attempt_id=checkpoint_id,
                    stage="checkpoint",
                    transport="internal",
                    direction=str(source_id),
                    media_type="application/json",
                    payload=body,
                    sample_range=None,
                    metadata=metadata,
                    object_id=checkpoint_id,
                )
                self._db.execute(
                    """
                    INSERT INTO checkpoint_deliveries(
                      checkpoint_id,record_id,meeting_id,generation,worker_id,worker_epoch,write_epoch,
                      source_id,checkpoint_hash,previous_hash,control_event_id,object_key,evidence_ordinal
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NULL)
                    """,
                    (
                        str(checkpoint_id),
                        str(checkpoint_id),
                        *_tuple_values(key),
                        str(source_id),
                        checkpoint_hash,
                        previous_hash,
                        str(control_event_id),
                        object_key,
                    ),
                )
                self._db.execute("COMMIT")
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute("ROLLBACK")
                raise
            delivery = self._checkpoint_delivery_unlocked(checkpoint_id)
            if delivery is None or delivery.raw_ref != ref:
                raise SpoolUnavailable("checkpoint delivery commit disappeared")
            return delivery

    @staticmethod
    def _validate_checkpoint_body(
        body: bytes,
        checkpoint_id: UUID,
        source_id: UUID,
        worker_epoch: int,
        checkpoint_hash: str,
        previous_hash: str | None,
    ) -> dict[str, Any]:
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise SpoolUnavailable("checkpoint body identity is invalid") from exc
        if not isinstance(payload, dict) or _canonical_json(payload) != body:
            raise SpoolUnavailable("checkpoint body is not canonical")
        expected = {
            "checkpointId": str(checkpoint_id),
            "sourceParticipantId": str(source_id),
            "workerEpoch": worker_epoch,
            "highWatermarkSha256": checkpoint_hash,
            "previousCheckpointSha256": previous_hash,
        }
        if any(payload.get(name) != value for name, value in expected.items()):
            raise SpoolUnavailable("checkpoint body identity differs from delivery metadata")
        return cast(dict[str, Any], payload)

    def seal_terminal_checkpoints(
        self,
        authority: ConsultationProducerAuthority,
        *,
        terminal_outcome: str,
        completion_event_id: UUID,
        intents: tuple[TerminalCheckpointIntent, TerminalCheckpointIntent],
        failure_payload: bytes | None = None,
    ) -> SpoolConsultationSeal:
        key = self._require_authority(authority, ConsultationProducerAuthority)
        if not 1 <= len(terminal_outcome) <= 128:
            raise ValueError("terminal_outcome must contain 1 to 128 characters")
        if len(intents) != 2:
            raise ValueError("terminal seal requires exactly two checkpoint intents")
        if failure_payload is not None:
            if len(failure_payload) > 65536:
                raise ValueError("terminal failure payload exceeds 65536 bytes")
            try:
                failure = json.loads(failure_payload.decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise ValueError("terminal failure payload must be canonical JSON") from exc
            if _canonical_json(failure) != failure_payload:
                raise ValueError("terminal failure payload must be canonical JSON")
        else:
            failure = None
        ordered = tuple(sorted(intents, key=lambda item: str(item.source_id)))
        if (
            ordered[0].checkpoint_id == ordered[1].checkpoint_id
            or ordered[0].source_id == ordered[1].source_id
            or ordered[0].control_event_id == ordered[1].control_event_id
            or ordered[0].object_key == ordered[1].object_key
        ):
            raise ValueError("terminal checkpoint identities must be distinct")
        for intent in ordered:
            self._validate_checkpoint_body(
                intent.body,
                intent.checkpoint_id,
                intent.source_id,
                key[3],
                intent.checkpoint_hash,
                intent.previous_hash,
            )
        seal_id = uuid5(
            key[0],
            f"checkpoint-seal:{key[1]}:{key[2]}:{key[3]}:{key[4]}:{completion_event_id}",
        )
        with self._exclusive():
            existing = self._seal_by_tuple_unlocked(key)
            if existing is not None:
                if (
                    existing.seal_id != seal_id
                    or existing.terminal_outcome != terminal_outcome
                    or existing.completion_event_id != completion_event_id
                    or existing.failure != failure
                    or existing.first_checkpoint_id != ordered[0].checkpoint_id
                    or existing.second_checkpoint_id != ordered[1].checkpoint_id
                ):
                    raise SpoolUnavailable("terminal seal was reused with different evidence")
                return existing
            row = self._handoff_row_unlocked(key)
            if row is None or row[0] != "settling":
                raise SpoolUnavailable("terminal seal requires a settling consultation handoff")
            evidence_ordinal = int(
                self._db.execute(
                    """
                    SELECT COALESCE(MAX(ordinal),0) FROM records
                    WHERE meeting_id=? AND generation=? AND worker_id=? AND worker_epoch=?
                      AND write_epoch=? AND stage NOT IN ('checkpoint','checkpoint-seal')
                    """,
                    _tuple_values(key),
                ).fetchone()[0]
            )
            entries: list[dict[str, Any]] = []
            for intent in ordered:
                entries.append(
                    {
                        "checkpoint": json.loads(intent.body),
                        "checkpointHash": intent.checkpoint_hash,
                        "checkpointId": str(intent.checkpoint_id),
                        "controlEventId": str(intent.control_event_id),
                        "objectKey": intent.object_key,
                        "previousHash": intent.previous_hash,
                        "sourceId": str(intent.source_id),
                    }
                )
            meeting_id, generation, worker_id, worker_epoch, write_epoch = key
            payload_object: dict[str, Any] = {
                "checkpoints": entries,
                "completionEventId": str(completion_event_id),
                "evidenceOrdinal": evidence_ordinal,
                "failurePayload": failure,
                "generation": generation,
                "meetingId": str(meeting_id),
                "sealId": str(seal_id),
                "terminalOutcome": terminal_outcome,
                "workerEpoch": worker_epoch,
                "workerId": str(worker_id),
                "writeEpoch": write_epoch,
            }
            body = _canonical_json(payload_object)
            metadata: Metadata = (
                ("completionEventId", str(completion_event_id)),
                ("generation", str(generation)),
                ("workerId", str(worker_id)),
                ("workerEpoch", str(worker_epoch)),
                ("writeEpoch", str(write_epoch)),
                ("evidenceOrdinal", str(evidence_ordinal)),
            )
            self._db.execute("BEGIN IMMEDIATE")
            try:
                ref = self._append_unlocked(
                    key=key,
                    attempt_id=seal_id,
                    stage="checkpoint-seal",
                    transport="internal",
                    direction="internal",
                    media_type="application/json",
                    payload=body,
                    sample_range=None,
                    metadata=metadata,
                    object_id=seal_id,
                )
                for intent in ordered:
                    self._db.execute(
                        """
                        INSERT INTO checkpoint_deliveries(
                          checkpoint_id,record_id,meeting_id,generation,worker_id,worker_epoch,write_epoch,
                          source_id,checkpoint_hash,previous_hash,control_event_id,object_key,evidence_ordinal
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            str(intent.checkpoint_id),
                            str(seal_id),
                            *_tuple_values(key),
                            str(intent.source_id),
                            intent.checkpoint_hash,
                            intent.previous_hash,
                            str(intent.control_event_id),
                            intent.object_key,
                            evidence_ordinal,
                        ),
                    )
                self._db.execute(
                    """
                    INSERT INTO consultation_seals(
                      seal_id,meeting_id,generation,worker_id,worker_epoch,write_epoch,evidence_ordinal,
                      terminal_outcome,completion_event_id,failure_payload,checkpoint_id_a,checkpoint_id_b
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        str(seal_id),
                        *_tuple_values(key),
                        evidence_ordinal,
                        terminal_outcome,
                        str(completion_event_id),
                        failure_payload,
                        str(ordered[0].checkpoint_id),
                        str(ordered[1].checkpoint_id),
                    ),
                )
                timestamp = _timestamp(_now())
                self._db.execute(
                    """
                    UPDATE consultation_handoffs SET state='sealed',sealed_at=?
                    WHERE meeting_id=? AND generation=? AND worker_id=? AND worker_epoch=? AND write_epoch=?
                      AND state='settling'
                    """,
                    (timestamp, *_tuple_values(key)),
                )
                self._db.execute("COMMIT")
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute("ROLLBACK")
                raise
            seal = self._seal_by_tuple_unlocked(key)
            if seal is None or seal.raw_ref != ref:
                raise SpoolUnavailable("terminal seal commit disappeared")
            return seal

    def read(self, object_id: UUID) -> bytes:
        with self._exclusive():
            return self._read_authenticated_record_unlocked(object_id)[1]

    def _read_authenticated_record_unlocked(self, object_id: UUID) -> tuple[dict[str, Any], bytes]:
        row = self._db.execute(
            """
            SELECT attempt_id,meeting_id,generation,worker_id,worker_epoch,write_epoch,stage,transport,
                   direction,media_type,opaque_path,key_id,nonce,plaintext_sha256,ciphertext_sha256,size,
                   sample_start,sample_end,metadata_json,state
            FROM records WHERE object_id=?
            """,
            (str(object_id),),
        ).fetchone()
        if row is None:
            raise KeyError(object_id)
        if row[19] == "quarantined":
            raise SpoolUnavailable("spool record is quarantined")
        try:
            header, encrypted = unpack_envelope(Path(row[10]).read_bytes())
            expected = {
                "aad_version": 3,
                "schema_version": SCHEMA_VERSION,
                "object_id": str(object_id),
                "attempt_id": row[0],
                "meeting_id": row[1],
                "generation": row[2],
                "worker_id": row[3],
                "worker_epoch": row[4],
                "write_epoch": row[5],
                "stage": row[6],
                "transport": row[7],
                "direction": row[8],
                "media_type": row[9],
                "key_id": row[11],
                "nonce": b64encode(bytes(row[12])).decode(),
                "plaintext_sha256": row[13],
                "ciphertext_sha256": row[14],
                "size": row[15],
                "sample_start": row[16],
                "sample_end": row[17],
                "metadata": json.loads(bytes(row[18])),
            }
            if any(header.get(name) != value for name, value in expected.items()):
                raise SpoolUnavailable("authenticated spool header differs from its index")
            if sha256_hex(encrypted) != row[14]:
                raise SpoolUnavailable("ciphertext authentication hash mismatch")
            plain = decrypt_aesgcm(
                self._keys[str(row[11])], bytes(row[12]), encrypted, header_aad(header)
            )
            if sha256_hex(plain) != row[13] or len(plain) != row[15]:
                raise SpoolUnavailable("plaintext authentication mismatch")
            return header, plain
        except (InvalidTag, KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            self._quarantine_unlocked(object_id)
            if isinstance(exc, SpoolUnavailable):
                raise
            raise SpoolUnavailable("spool key missing or authentication failed") from exc
        except SpoolUnavailable:
            self._quarantine_unlocked(object_id)
            raise

    def _raw_ref_unlocked(self, object_id: UUID) -> RawRef:
        row = self._db.execute(
            "SELECT ordinal,plaintext_sha256,size,media_type FROM records WHERE object_id=?",
            (str(object_id),),
        ).fetchone()
        if row is None:
            raise KeyError(object_id)
        return RawRef(object_id, int(row[0]), str(row[1]), int(row[2]), str(row[3]))

    def _record_delivery_unlocked(self, object_id: UUID) -> SpoolRecordDelivery:
        row = self._db.execute(
            """
            SELECT ordinal,plaintext_sha256,size,media_type,sample_start,sample_end,meeting_id,
                   attempt_id,stage,transport,direction,generation,worker_id,worker_epoch,write_epoch,
                   metadata_json,state,version_id,s3_checksum,error_kind,failed_at
            FROM records WHERE object_id=?
            """,
            (str(object_id),),
        ).fetchone()
        if row is None:
            raise KeyError(object_id)
        context = SpoolRecordContext(
            meeting_id=UUID(row[6]),
            attempt_id=UUID(row[7]),
            stage=str(row[8]),
            transport=str(row[9]),
            direction=str(row[10]),
            media_type=str(row[3]),
            ordinal=int(row[0]),
            generation=int(row[11]),
            worker_id=UUID(row[12]),
            worker_epoch=int(row[13]),
            write_epoch=int(row[14]),
            metadata=cast(Metadata, tuple(tuple(item) for item in json.loads(bytes(row[15])))),
        )
        return SpoolRecordDelivery(
            raw_ref=RawRef(object_id, int(row[0]), str(row[1]), int(row[2]), str(row[3])),
            sample_range=SampleRange(int(row[4]), int(row[5])) if row[4] is not None else None,
            context=context,
            state=cast(RecordState, str(row[16])),
            version_id=str(row[17]) if row[17] is not None else None,
            s3_checksum=str(row[18]) if row[18] is not None else None,
            error_kind=str(row[19]) if row[19] is not None else None,
            failed_at=_parse_timestamp(str(row[20])) if row[20] is not None else None,
        )

    def list_record_deliveries(
        self,
        *,
        meeting_id: UUID | None = None,
        states: frozenset[RecordState] | set[RecordState] | None = None,
    ) -> tuple[SpoolRecordDelivery, ...]:
        selected = frozenset(states or {"committed"})
        if not selected or not selected <= {"committed", "uploaded", "permanent", "quarantined"}:
            raise ValueError("record states are invalid")
        placeholders = ",".join("?" for _ in selected)
        sql = f"SELECT object_id FROM records WHERE state IN ({placeholders})"
        arguments: list[object] = list(sorted(selected))
        if meeting_id is not None:
            sql += " AND meeting_id=?"
            arguments.append(str(meeting_id))
        sql += " ORDER BY ordinal"
        with self._exclusive():
            return tuple(
                self._record_delivery_unlocked(UUID(row[0]))
                for row in self._db.execute(sql, tuple(arguments)).fetchall()
            )

    def context(self, object_id: UUID) -> SpoolRecordContext:
        with self._exclusive():
            return self._record_delivery_unlocked(object_id).context

    def committed(self, stage: str | None = None) -> list[tuple[RawRef, SampleRange | None]]:
        with self._exclusive():
            sql = "SELECT object_id FROM records WHERE state='committed'"
            args: tuple[object, ...] = ()
            if stage is not None:
                sql += " AND stage=?"
                args = (stage,)
            sql += " ORDER BY ordinal"
            return [
                (delivery.raw_ref, delivery.sample_range)
                for (object_id,) in self._db.execute(sql, args).fetchall()
                if (delivery := self._record_delivery_unlocked(UUID(object_id)))
            ]

    def committed_scoped(
        self,
        meeting_id: UUID,
        stage: str,
        direction: str,
        include_uploaded: bool = False,
    ) -> list[tuple[RawRef, SampleRange | None]]:
        with self._exclusive():
            rows = self._db.execute(
                """
                SELECT object_id FROM records
                WHERE (state='committed' OR (?=1 AND state='uploaded'))
                  AND meeting_id=? AND stage=? AND direction=? ORDER BY ordinal
                """,
                (int(include_uploaded), str(meeting_id), stage, direction),
            ).fetchall()
            return [
                (delivery.raw_ref, delivery.sample_range)
                for (object_id,) in rows
                if (delivery := self._record_delivery_unlocked(UUID(object_id)))
            ]

    def mark_record_uploaded(self, object_id: UUID, version_id: str, checksum: str) -> None:
        if not version_id or not checksum:
            raise ValueError("upload identity must not be empty")
        with self._exclusive():
            row = self._db.execute(
                "SELECT state,version_id,s3_checksum FROM records WHERE object_id=?",
                (str(object_id),),
            ).fetchone()
            if row is None:
                raise KeyError(object_id)
            if row[0] == "uploaded":
                if row[1:] != (version_id, checksum):
                    raise SpoolUnavailable("record was uploaded with a different identity")
                return
            if row[0] != "committed":
                raise SpoolUnavailable(f"record delivery is {row[0]}")
            self._db.execute(
                "UPDATE records SET state='uploaded',version_id=?,s3_checksum=? WHERE object_id=?",
                (version_id, checksum, str(object_id)),
            )

    def mark_uploaded(self, object_id: UUID, version_id: str, checksum: str) -> None:
        self.mark_record_uploaded(object_id, version_id, checksum)

    def mark_record_delivery_permanent(
        self, object_id: UUID, error_kind: str, failed_at: datetime
    ) -> SpoolRecordDelivery:
        timestamp = _timestamp(failed_at)
        if not 1 <= len(error_kind) <= 128:
            raise ValueError("error_kind must contain 1 to 128 characters")
        with self._exclusive():
            row = self._db.execute(
                "SELECT state,error_kind,failed_at,stage FROM records WHERE object_id=?",
                (str(object_id),),
            ).fetchone()
            if row is None:
                raise KeyError(object_id)
            if row[3] in {"checkpoint", "checkpoint-seal"}:
                raise SpoolUnavailable("checkpoint outcomes belong to checkpoint deliveries")
            if row[0] == "permanent":
                if row[1:3] != (error_kind, timestamp):
                    raise SpoolUnavailable("record has a different permanent outcome")
            elif row[0] == "committed":
                self._db.execute(
                    "UPDATE records SET state='permanent',error_kind=?,failed_at=? WHERE object_id=?",
                    (error_kind, timestamp, str(object_id)),
                )
            else:
                raise SpoolUnavailable(f"record delivery is {row[0]}")
            return self._record_delivery_unlocked(object_id)

    def quarantine(self, object_id: UUID) -> None:
        with self._exclusive():
            self._quarantine_unlocked(object_id)

    def _quarantine_unlocked(self, object_id: UUID) -> None:
        self._db.execute(
            """
            UPDATE records SET state='quarantined',version_id=NULL,s3_checksum=NULL,error_kind=NULL,failed_at=NULL
            WHERE object_id=?
            """,
            (str(object_id),),
        )

    def compact_uploaded_envelopes(self, limit: int = 128) -> int:
        if limit < 1:
            return 0
        with self._exclusive():
            self._db.execute("BEGIN IMMEDIATE")
            try:
                rows = self._db.execute(
                    """
                    SELECT records.object_id,records.opaque_path FROM records
                    LEFT JOIN compacted_envelopes USING(object_id)
                    WHERE records.state='uploaded' AND records.stage NOT IN ('checkpoint','checkpoint-seal','terminal')
                      AND records.stage NOT LIKE '%-terminal' AND records.sample_start IS NULL
                      AND compacted_envelopes.object_id IS NULL
                    ORDER BY records.ordinal LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
                timestamp = _timestamp(_now())
                for object_id, _ in rows:
                    self._db.execute(
                        "INSERT INTO compacted_envelopes(object_id,compacted_at) VALUES (?,?)",
                        (object_id, timestamp),
                    )
                self._db.execute("COMMIT")
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute("ROLLBACK")
                raise
            for _, opaque_path in rows:
                Path(opaque_path).unlink(missing_ok=True)
            if rows:
                fsync_directory(self._root)
            return len(rows)

    def pcm_scopes(self, include_uploaded: bool = False) -> list[tuple[UUID, str, str]]:
        with self._exclusive():
            rows = self._db.execute(
                """
                SELECT DISTINCT meeting_id,stage,direction FROM records
                WHERE (state='committed' OR (?=1 AND state='uploaded'))
                  AND stage IN ('stt-input','tts-output','livekit-output')
                ORDER BY meeting_id,stage,direction
                """,
                (int(include_uploaded),),
            ).fetchall()
            return [(UUID(row[0]), str(row[1]), str(row[2])) for row in rows]

    def covering_checkpoint(
        self,
        meeting_id: UUID,
        stage: str,
        direction: str,
        sample_end: int,
        terminal_only: bool = False,
    ) -> UUID | None:
        with self._exclusive():
            sql = """
                SELECT checkpoint_deliveries.checkpoint_id
                FROM checkpoint_deliveries JOIN records
                  ON records.object_id=checkpoint_deliveries.record_id
                WHERE checkpoint_deliveries.meeting_id=?
                  AND checkpoint_deliveries.delivery_state!='permanent'
            """
            args: list[object] = [str(meeting_id)]
            if terminal_only:
                sql += " AND checkpoint_deliveries.evidence_ordinal IS NOT NULL"
            sql += " ORDER BY records.ordinal DESC,checkpoint_deliveries.source_id DESC"
            for (checkpoint_id,) in self._db.execute(sql, tuple(args)).fetchall():
                delivery = self._checkpoint_delivery_unlocked(UUID(checkpoint_id))
                if (
                    delivery is not None
                    and self._checkpoint_watermark(json.loads(delivery.body), stage, direction)
                    >= sample_end
                ):
                    return delivery.checkpoint_id
            return None

    def checkpoint_covers(
        self, checkpoint_id: UUID, stage: str, direction: str, sample_end: int
    ) -> bool:
        with self._exclusive():
            delivery = self._checkpoint_delivery_unlocked(checkpoint_id)
            return (
                delivery is not None
                and self._checkpoint_watermark(json.loads(delivery.body), stage, direction)
                >= sample_end
            )

    @staticmethod
    def _checkpoint_watermark(payload: dict[str, Any], stage: str, direction: str) -> int:
        if stage == "stt-input" and payload.get("sourceParticipantId") == direction:
            return int(payload.get("acceptedInput", 0))
        if (
            stage in {"tts-output", "livekit-output"}
            and payload.get("destinationParticipantId") == direction
        ):
            return int(payload.get("emittedOutput", 0))
        return -1

    def list_checkpoint_deliveries(
        self,
        *,
        meeting_id: UUID | None = None,
        generation: int | None = None,
        worker_id: UUID | None = None,
        worker_epoch: int | None = None,
        write_epoch: int | None = None,
        source_id: UUID | None = None,
        states: frozenset[DeliveryState] | set[DeliveryState] | None = None,
    ) -> tuple[SpoolCheckpointDelivery, ...]:
        clauses: list[str] = []
        arguments: list[object] = []
        filters = (
            ("meeting_id", str(meeting_id) if meeting_id is not None else None),
            ("generation", generation),
            ("worker_id", str(worker_id) if worker_id is not None else None),
            ("worker_epoch", worker_epoch),
            ("write_epoch", write_epoch),
            ("source_id", str(source_id) if source_id is not None else None),
        )
        for column, value in filters:
            if value is not None:
                clauses.append(f"checkpoint_deliveries.{column}=?")
                arguments.append(value)
        if states is not None:
            selected = frozenset(states)
            if not selected or not selected <= {"pending", "acknowledged", "permanent"}:
                raise ValueError("checkpoint delivery states are invalid")
            clauses.append(
                "checkpoint_deliveries.delivery_state IN (" + ",".join("?" for _ in selected) + ")"
            )
            arguments.extend(sorted(selected))
        sql = """
            SELECT checkpoint_deliveries.checkpoint_id FROM checkpoint_deliveries
            JOIN records ON records.object_id=checkpoint_deliveries.record_id
        """
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY checkpoint_deliveries.source_id,records.ordinal"
        with self._exclusive():
            deliveries: list[SpoolCheckpointDelivery] = []
            for (checkpoint_id,) in self._db.execute(sql, tuple(arguments)).fetchall():
                delivery = self._checkpoint_delivery_unlocked(UUID(checkpoint_id))
                if delivery is None:
                    raise SpoolUnavailable("checkpoint delivery disappeared")
                deliveries.append(delivery)
            return tuple(deliveries)

    def _checkpoint_delivery_unlocked(self, checkpoint_id: UUID) -> SpoolCheckpointDelivery | None:
        row = self._db.execute(
            """
            SELECT d.record_id,d.meeting_id,d.generation,d.worker_id,d.worker_epoch,d.write_epoch,
                   d.source_id,d.checkpoint_hash,d.previous_hash,d.control_event_id,d.object_key,
                   d.evidence_ordinal,d.delivery_state,d.error_kind,d.failed_at,
                   r.ordinal,r.plaintext_sha256,r.size,r.media_type,r.state
            FROM checkpoint_deliveries d JOIN records r ON r.object_id=d.record_id
            WHERE d.checkpoint_id=?
            """,
            (str(checkpoint_id),),
        ).fetchone()
        if row is None:
            return None
        if row[19] == "quarantined":
            raise SpoolUnavailable("checkpoint carrier is quarantined")
        record_id = UUID(row[0])
        _, carrier = self._read_authenticated_record_unlocked(record_id)
        if row[11] is None:
            body = carrier
        else:
            payload = json.loads(carrier)
            entries = payload.get("checkpoints") if isinstance(payload, dict) else None
            if not isinstance(entries, list) or len(entries) != 2:
                raise SpoolUnavailable("terminal checkpoint carrier is malformed")
            matches = [
                entry for entry in entries if entry.get("checkpointId") == str(checkpoint_id)
            ]
            if len(matches) != 1 or not isinstance(matches[0].get("checkpoint"), dict):
                raise SpoolUnavailable("terminal checkpoint carrier entry is missing")
            body = _canonical_json(matches[0]["checkpoint"])
        return SpoolCheckpointDelivery(
            checkpoint_id=checkpoint_id,
            record_id=record_id,
            meeting_id=UUID(row[1]),
            generation=int(row[2]),
            worker_id=UUID(row[3]),
            worker_epoch=int(row[4]),
            write_epoch=int(row[5]),
            source_id=UUID(row[6]),
            checkpoint_hash=str(row[7]),
            previous_hash=str(row[8]) if row[8] is not None else None,
            control_event_id=UUID(row[9]),
            object_key=str(row[10]),
            evidence_ordinal=int(row[11]) if row[11] is not None else None,
            body=body,
            raw_ref=RawRef(record_id, int(row[15]), str(row[16]), int(row[17]), str(row[18])),
            delivery_state=cast(DeliveryState, str(row[12])),
            error_kind=str(row[13]) if row[13] is not None else None,
            failed_at=_parse_timestamp(str(row[14])) if row[14] is not None else None,
        )

    def mark_checkpoint_delivery_acknowledged(
        self, control_event_id: UUID
    ) -> SpoolCheckpointDelivery:
        return self._mark_checkpoint(control_event_id, state="acknowledged")

    def mark_checkpoint_delivery_permanent(
        self, control_event_id: UUID, error_kind: str, failed_at: datetime
    ) -> SpoolCheckpointDelivery:
        if not 1 <= len(error_kind) <= 128:
            raise ValueError("error_kind must contain 1 to 128 characters")
        return self._mark_checkpoint(
            control_event_id,
            state="permanent",
            error_kind=error_kind,
            failed_at=_timestamp(failed_at),
        )

    def _mark_checkpoint(
        self,
        control_event_id: UUID,
        *,
        state: Literal["acknowledged", "permanent"],
        error_kind: str | None = None,
        failed_at: str | None = None,
    ) -> SpoolCheckpointDelivery:
        with self._exclusive():
            row = self._db.execute(
                """
                SELECT checkpoint_id,delivery_state,error_kind,failed_at
                FROM checkpoint_deliveries WHERE control_event_id=?
                """,
                (str(control_event_id),),
            ).fetchone()
            if row is None:
                raise KeyError(control_event_id)
            if row[1] == state:
                if (row[2], row[3]) != (error_kind, failed_at):
                    raise SpoolUnavailable("checkpoint has a different terminal delivery outcome")
            elif row[1] == "pending":
                self._db.execute(
                    """
                    UPDATE checkpoint_deliveries
                    SET delivery_state=?,error_kind=?,failed_at=? WHERE control_event_id=?
                    """,
                    (state, error_kind, failed_at, str(control_event_id)),
                )
            else:
                raise SpoolUnavailable(f"checkpoint delivery is {row[1]}")
            delivery = self._checkpoint_delivery_unlocked(UUID(row[0]))
            if delivery is None:
                raise SpoolUnavailable("checkpoint delivery disappeared")
            return delivery

    def list_consultation_seals(
        self,
        *,
        meeting_id: UUID | None = None,
        generation: int | None = None,
        worker_id: UUID | None = None,
        worker_epoch: int | None = None,
        write_epoch: int | None = None,
        completion_states: frozenset[str] | set[str] | None = None,
    ) -> tuple[SpoolConsultationSeal, ...]:
        clauses: list[str] = []
        arguments: list[object] = []
        filters = (
            ("meeting_id", str(meeting_id) if meeting_id is not None else None),
            ("generation", generation),
            ("worker_id", str(worker_id) if worker_id is not None else None),
            ("worker_epoch", worker_epoch),
            ("write_epoch", write_epoch),
        )
        for column, value in filters:
            if value is not None:
                clauses.append(f"{column}=?")
                arguments.append(value)
        if completion_states is not None:
            selected = frozenset(completion_states)
            if not selected or not selected <= {"pending", "acknowledged"}:
                raise ValueError("completion states are invalid")
            clauses.append("completion_state IN (" + ",".join("?" for _ in selected) + ")")
            arguments.extend(sorted(selected))
        sql = "SELECT seal_id FROM consultation_seals"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY rowid"
        with self._exclusive():
            return tuple(
                self._seal_unlocked(UUID(row[0]))
                for row in self._db.execute(sql, tuple(arguments)).fetchall()
            )

    def _seal_by_tuple_unlocked(self, key: _TupleKey) -> SpoolConsultationSeal | None:
        row = self._db.execute(
            """
            SELECT seal_id FROM consultation_seals
            WHERE meeting_id=? AND generation=? AND worker_id=? AND worker_epoch=? AND write_epoch=?
            """,
            _tuple_values(key),
        ).fetchone()
        return self._seal_unlocked(UUID(row[0])) if row is not None else None

    def _seal_unlocked(self, seal_id: UUID) -> SpoolConsultationSeal:
        row = self._db.execute(
            """
            SELECT meeting_id,generation,worker_id,worker_epoch,write_epoch,evidence_ordinal,
                   terminal_outcome,completion_event_id,failure_payload,checkpoint_id_a,checkpoint_id_b,
                   completion_state
            FROM consultation_seals WHERE seal_id=?
            """,
            (str(seal_id),),
        ).fetchone()
        if row is None:
            raise KeyError(seal_id)
        self._read_authenticated_record_unlocked(seal_id)
        return SpoolConsultationSeal(
            seal_id=seal_id,
            meeting_id=UUID(row[0]),
            generation=int(row[1]),
            worker_id=UUID(row[2]),
            worker_epoch=int(row[3]),
            write_epoch=int(row[4]),
            evidence_ordinal=int(row[5]),
            terminal_outcome=str(row[6]),
            completion_event_id=UUID(row[7]),
            failure=cast(dict[str, object], json.loads(bytes(row[8])))
            if row[8] is not None
            else None,
            first_checkpoint_id=UUID(row[9]),
            second_checkpoint_id=UUID(row[10]),
            completion_state=cast(Literal["pending", "acknowledged"], str(row[11])),
            raw_ref=self._raw_ref_unlocked(seal_id),
        )

    def mark_consultation_completion_acknowledged(self, seal_id: UUID) -> SpoolConsultationSeal:
        with self._exclusive():
            cursor = self._db.execute(
                """
                UPDATE consultation_seals SET completion_state='acknowledged'
                WHERE seal_id=? AND completion_state='pending'
                """,
                (str(seal_id),),
            )
            if (
                cursor.rowcount == 0
                and self._db.execute(
                    "SELECT 1 FROM consultation_seals WHERE seal_id=?", (str(seal_id),)
                ).fetchone()
                is None
            ):
                raise KeyError(seal_id)
            return self._seal_unlocked(seal_id)

    def consultation_handoff(
        self,
        *,
        meeting_id: UUID,
        generation: int,
        worker_id: UUID,
        worker_epoch: int,
        write_epoch: int,
    ) -> HandoffState | None:
        key = (meeting_id, generation, worker_id, worker_epoch, write_epoch)
        _validate_tuple(key)
        with self._exclusive():
            row = self._handoff_row_unlocked(key)
            return cast(HandoffState, str(row[0])) if row is not None else None

    def consultation_relinquishment_reason(
        self,
        *,
        meeting_id: UUID,
        generation: int,
        worker_id: UUID,
        worker_epoch: int,
        write_epoch: int,
    ) -> str | None:
        key = (meeting_id, generation, worker_id, worker_epoch, write_epoch)
        _validate_tuple(key)
        with self._exclusive():
            row = self._handoff_row_unlocked(key)
            return str(row[1]) if row is not None and row[1] is not None else None

    @staticmethod
    def _fixture_faults(meeting_id: UUID) -> dict[str, Any]:
        if os.environ.get("APP_ENV") != "test":
            return {}
        path = os.environ.get("FIXTURE_SCENARIO_FILE")
        if not path:
            return {}
        try:
            data = json.loads(Path(path).read_text("utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise SpoolUnavailable("fixture scenario file is invalid") from error
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


    def usage_ratio(self) -> float:
        capacity = self._capacity_probe(self._root)
        return capacity.used_bytes / capacity.total_bytes if capacity.total_bytes > 0 else 1.0

    def recover(
        self, authority: ConsultationProducerAuthority | ConsultationRecoveryAuthority
    ) -> None:
        if isinstance(authority, ConsultationProducerAuthority):
            self._require_authority(authority, ConsultationProducerAuthority)
        else:
            self._require_authority(authority, ConsultationRecoveryAuthority)
        with self._exclusive():
            self._recover_orphans_for_authority_unlocked(authority)
            self._require_indexed_payloads_unlocked()

    def _recover_compacted_envelopes_unlocked(self) -> None:
        rows = self._db.execute(
            """
            SELECT records.opaque_path FROM compacted_envelopes
            JOIN records USING(object_id)
            """
        ).fetchall()
        for (path,) in rows:
            Path(path).unlink(missing_ok=True)
        if rows:
            fsync_directory(self._root)

    def _reject_legacy_orphans_unlocked(self) -> None:
        referenced = {
            Path(row[0]).resolve()
            for row in self._db.execute("SELECT opaque_path FROM records").fetchall()
        }
        for path in self._root.glob("*.wal"):
            if path.resolve() in referenced:
                continue
            try:
                header, _ = unpack_envelope(path.read_bytes())
            except (OSError, ValueError, json.JSONDecodeError):
                path.rename(path.with_suffix(".quarantine"))
                continue
            if header.get("schema_version") != SCHEMA_VERSION:
                path.rename(path.with_suffix(".quarantine"))

    def _recover_orphans_for_authority_unlocked(self, authority: _Authority) -> None:
        key = authority._key
        referenced = {
            Path(row[0]).resolve()
            for row in self._db.execute("SELECT opaque_path FROM records").fetchall()
        }
        for path in self._root.glob("*.wal"):
            if path.resolve() in referenced:
                continue
            try:
                header, encrypted = unpack_envelope(path.read_bytes())
                header_key = (
                    UUID(str(header["meeting_id"])),
                    int(header["generation"]),
                    UUID(str(header["worker_id"])),
                    int(header["worker_epoch"]),
                    int(header["write_epoch"]),
                )
                if header_key != key:
                    continue
                self._import_orphan_unlocked(path, header, encrypted, key)
            except (
                InvalidTag,
                KeyError,
                OSError,
                TypeError,
                ValueError,
                json.JSONDecodeError,
                SpoolUnavailable,
            ):
                path.rename(path.with_suffix(".quarantine"))

    def _import_orphan_unlocked(
        self, path: Path, header: dict[str, Any], encrypted: bytes, key: _TupleKey
    ) -> None:
        if header.get("aad_version") != 3 or header.get("schema_version") != SCHEMA_VERSION:
            raise ValueError("orphan schema is unsupported")
        object_id = UUID(str(header["object_id"]))
        attempt_id = UUID(str(header["attempt_id"]))
        key_id = str(header["key_id"])
        nonce = b64decode(str(header["nonce"]), validate=True)
        if path.stem != str(object_id) or sha256_hex(encrypted) != header["ciphertext_sha256"]:
            raise ValueError("orphan ciphertext identity mismatch")
        plain = decrypt_aesgcm(self._keys[key_id], nonce, encrypted, header_aad(header))
        if sha256_hex(plain) != header["plaintext_sha256"] or len(plain) != header["size"]:
            raise ValueError("orphan plaintext identity mismatch")
        metadata = _canonical_metadata(
            cast(Metadata, tuple(tuple(item) for item in header.get("metadata", ())))
        )
        self._db.execute("BEGIN IMMEDIATE")
        try:
            cursor = self._db.execute(
                """
                INSERT INTO records(
                  object_id,attempt_id,meeting_id,generation,worker_id,worker_epoch,write_epoch,
                  stage,transport,direction,media_type,opaque_path,key_id,nonce,plaintext_sha256,
                  ciphertext_sha256,size,sample_start,sample_end,metadata_json,state
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'committed')
                """,
                (
                    str(object_id),
                    str(attempt_id),
                    *_tuple_values(key),
                    str(header["stage"]),
                    str(header["transport"]),
                    str(header["direction"]),
                    str(header["media_type"]),
                    str(path),
                    key_id,
                    nonce,
                    str(header["plaintext_sha256"]),
                    str(header["ciphertext_sha256"]),
                    int(header["size"]),
                    header.get("sample_start"),
                    header.get("sample_end"),
                    _canonical_json(metadata),
                ),
            )
            ordinal = int(cursor.lastrowid or 0)
            if header["stage"] == "checkpoint":
                self._import_checkpoint_orphan_unlocked(header, plain, object_id, key)
            elif header["stage"] == "checkpoint-seal":
                self._import_seal_orphan_unlocked(header, plain, object_id, key, ordinal)
            self._db.execute("COMMIT")
        except BaseException:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            raise

    def _import_checkpoint_orphan_unlocked(
        self, header: dict[str, Any], body: bytes, checkpoint_id: UUID, key: _TupleKey
    ) -> None:
        metadata = dict(cast(Metadata, tuple(tuple(item) for item in header["metadata"])))
        source_id = UUID(str(header["direction"]))
        checkpoint_hash = str(json.loads(body)["highWatermarkSha256"])
        previous_hash_value = json.loads(body).get("previousCheckpointSha256")
        previous_hash = str(previous_hash_value) if previous_hash_value is not None else None
        self._validate_checkpoint_body(
            body, checkpoint_id, source_id, key[3], checkpoint_hash, previous_hash
        )
        expected = {
            "generation": str(key[1]),
            "workerId": str(key[2]),
            "workerEpoch": str(key[3]),
            "writeEpoch": str(key[4]),
            "evidenceOrdinal": "",
        }
        if any(metadata.get(name) != value for name, value in expected.items()):
            raise ValueError("orphan checkpoint tuple metadata is invalid")
        self._db.execute(
            """
            INSERT INTO checkpoint_deliveries(
              checkpoint_id,record_id,meeting_id,generation,worker_id,worker_epoch,write_epoch,
              source_id,checkpoint_hash,previous_hash,control_event_id,object_key,evidence_ordinal
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NULL)
            """,
            (
                str(checkpoint_id),
                str(checkpoint_id),
                *_tuple_values(key),
                str(source_id),
                checkpoint_hash,
                previous_hash,
                metadata["controlEventId"],
                metadata["objectKey"],
            ),
        )

    def _import_seal_orphan_unlocked(
        self,
        header: dict[str, Any],
        body: bytes,
        seal_id: UUID,
        key: _TupleKey,
        ordinal: int,
    ) -> None:
        payload = json.loads(body)
        if not isinstance(payload, dict) or _canonical_json(payload) != body:
            raise ValueError("orphan terminal seal is not canonical")
        entries = payload.get("checkpoints")
        if (
            payload.get("sealId") != str(seal_id)
            or payload.get("meetingId") != str(key[0])
            or payload.get("generation") != key[1]
            or payload.get("workerId") != str(key[2])
            or payload.get("workerEpoch") != key[3]
            or payload.get("writeEpoch") != key[4]
            or not isinstance(entries, list)
            or len(entries) != 2
        ):
            raise ValueError("orphan terminal seal identity is malformed")
        checkpoint_ids: list[UUID] = []
        for entry in entries:
            if not isinstance(entry, dict) or not isinstance(entry.get("checkpoint"), dict):
                raise ValueError("orphan terminal checkpoint entry is malformed")
            checkpoint_id = UUID(str(entry["checkpointId"]))
            source_id = UUID(str(entry["sourceId"]))
            checkpoint_body = _canonical_json(entry["checkpoint"])
            previous = entry.get("previousHash")
            previous_hash = str(previous) if previous is not None else None
            self._validate_checkpoint_body(
                checkpoint_body,
                checkpoint_id,
                source_id,
                key[3],
                str(entry["checkpointHash"]),
                previous_hash,
            )
            checkpoint_ids.append(checkpoint_id)
            self._db.execute(
                """
                INSERT INTO checkpoint_deliveries(
                  checkpoint_id,record_id,meeting_id,generation,worker_id,worker_epoch,write_epoch,
                  source_id,checkpoint_hash,previous_hash,control_event_id,object_key,evidence_ordinal
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(checkpoint_id),
                    str(seal_id),
                    *_tuple_values(key),
                    str(source_id),
                    str(entry["checkpointHash"]),
                    previous_hash,
                    str(entry["controlEventId"]),
                    str(entry["objectKey"]),
                    int(payload["evidenceOrdinal"]),
                ),
            )
        if len(set(checkpoint_ids)) != 2:
            raise ValueError("orphan terminal checkpoint pair is not distinct")
        failure = payload.get("failurePayload")
        failure_payload = _canonical_json(failure) if failure is not None else None
        self._db.execute(
            """
            INSERT INTO consultation_seals(
              seal_id,meeting_id,generation,worker_id,worker_epoch,write_epoch,evidence_ordinal,
              terminal_outcome,completion_event_id,failure_payload,checkpoint_id_a,checkpoint_id_b
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                str(seal_id),
                *_tuple_values(key),
                int(payload["evidenceOrdinal"]),
                str(payload["terminalOutcome"]),
                str(payload["completionEventId"]),
                failure_payload,
                str(checkpoint_ids[0]),
                str(checkpoint_ids[1]),
            ),
        )
        timestamp = _timestamp(_now())
        cursor = self._db.execute(
            """
            UPDATE consultation_handoffs SET state='sealed',settling_at=COALESCE(settling_at,?),sealed_at=?
            WHERE meeting_id=? AND generation=? AND worker_id=? AND worker_epoch=? AND write_epoch=?
              AND state IN ('active','settling')
            """,
            (timestamp, timestamp, *_tuple_values(key)),
        )
        if cursor.rowcount != 1 or ordinal < int(payload["evidenceOrdinal"]):
            raise ValueError("orphan terminal seal handoff or boundary is invalid")

    def _require_indexed_payloads_unlocked(self) -> None:
        missing = [
            row[0]
            for row in self._db.execute(
                """
                SELECT records.opaque_path FROM records
                LEFT JOIN compacted_envelopes USING(object_id)
                WHERE compacted_envelopes.object_id IS NULL
                """
            ).fetchall()
            if not Path(row[0]).is_file()
        ]
        if missing:
            raise SpoolUnavailable(f"{len(missing)} indexed spool payloads are missing")


class SpoolProducer:
    def __init__(self, store: EncryptedSpool) -> None:
        self._store = store

    def open_consultation_producer(self, **kwargs: Any) -> ConsultationProducerAuthority:
        return self._store.open_consultation_producer(**kwargs)

    def begin_consultation_settlement(self, authority: ConsultationProducerAuthority) -> None:
        self._store.begin_consultation_settlement(authority)

    def relinquish_consultation(
        self, authority: ConsultationProducerAuthority, reason: str
    ) -> None:
        self._store.relinquish_consultation(authority, reason)

    def append(self, authority: ConsultationProducerAuthority, **kwargs: Any) -> RawRef:
        return self._store.append(authority, **kwargs)

    def terminal(
        self, authority: ConsultationProducerAuthority, attempt_id: UUID, payload: bytes
    ) -> RawRef:
        return self._store.terminal(authority, attempt_id, payload)

    def frame(self, authority: ConsultationProducerAuthority, *args: Any) -> RawRef:
        return self._store.frame(authority, *args)

    def register_checkpoint_delivery(
        self, authority: ConsultationProducerAuthority, **kwargs: Any
    ) -> SpoolCheckpointDelivery:
        return self._store.register_checkpoint_delivery(authority, **kwargs)

    def seal_terminal_checkpoints(
        self, authority: ConsultationProducerAuthority, **kwargs: Any
    ) -> SpoolConsultationSeal:
        return self._store.seal_terminal_checkpoints(authority, **kwargs)

    def recover(self, authority: ConsultationProducerAuthority) -> None:
        self._store.recover(authority)


class SpoolCheckpointStore:
    def __init__(self, store: EncryptedSpool) -> None:
        self._store = store

    def list_checkpoint_deliveries(self, **kwargs: Any) -> tuple[SpoolCheckpointDelivery, ...]:
        return self._store.list_checkpoint_deliveries(**kwargs)

    def mark_checkpoint_delivery_acknowledged(
        self, control_event_id: UUID
    ) -> SpoolCheckpointDelivery:
        return self._store.mark_checkpoint_delivery_acknowledged(control_event_id)

    def mark_checkpoint_delivery_permanent(
        self, control_event_id: UUID, error_kind: str, failed_at: datetime
    ) -> SpoolCheckpointDelivery:
        return self._store.mark_checkpoint_delivery_permanent(
            control_event_id, error_kind, failed_at
        )

    def list_consultation_seals(self, **kwargs: Any) -> tuple[SpoolConsultationSeal, ...]:
        return self._store.list_consultation_seals(**kwargs)

    def mark_consultation_completion_acknowledged(self, seal_id: UUID) -> SpoolConsultationSeal:
        return self._store.mark_consultation_completion_acknowledged(seal_id)

    def consultation_handoff(self, **kwargs: Any) -> HandoffState | None:
        return self._store.consultation_handoff(**kwargs)

    def consultation_relinquishment_reason(self, **kwargs: Any) -> str | None:
        return self._store.consultation_relinquishment_reason(**kwargs)


class SpoolDrainer(SpoolCheckpointStore):
    def acquire_consultation_recovery(self, **kwargs: Any) -> ConsultationRecoveryAuthority | None:
        return self._store.acquire_consultation_recovery(**kwargs)

    def relinquish_expired_consultation(
        self, authority: ConsultationRecoveryAuthority, reason: str
    ) -> None:
        self._store.relinquish_expired_consultation(authority, reason)

    def recover(self, authority: ConsultationRecoveryAuthority) -> None:
        self._store.recover(authority)

    def read(self, object_id: UUID) -> bytes:
        return self._store.read(object_id)

    def list_record_deliveries(self, **kwargs: Any) -> tuple[SpoolRecordDelivery, ...]:
        return self._store.list_record_deliveries(**kwargs)

    def mark_record_uploaded(self, object_id: UUID, version_id: str, checksum: str) -> None:
        self._store.mark_record_uploaded(object_id, version_id, checksum)

    def mark_record_delivery_permanent(
        self, object_id: UUID, error_kind: str, failed_at: datetime
    ) -> SpoolRecordDelivery:
        return self._store.mark_record_delivery_permanent(object_id, error_kind, failed_at)

    def compact_uploaded_envelopes(self, limit: int = 128) -> int:
        return self._store.compact_uploaded_envelopes(limit)

    def pcm_scopes(self, include_uploaded: bool = False) -> list[tuple[UUID, str, str]]:
        return self._store.pcm_scopes(include_uploaded)

    def committed_scoped(
        self, meeting_id: UUID, stage: str, direction: str, include_uploaded: bool = False
    ) -> list[tuple[RawRef, SampleRange | None]]:
        return self._store.committed_scoped(meeting_id, stage, direction, include_uploaded)

    def covering_checkpoint(
        self,
        meeting_id: UUID,
        stage: str,
        direction: str,
        sample_end: int,
        terminal_only: bool = False,
    ) -> UUID | None:
        return self._store.covering_checkpoint(
            meeting_id, stage, direction, sample_end, terminal_only
        )

    def checkpoint_covers(
        self, checkpoint_id: UUID, stage: str, direction: str, sample_end: int
    ) -> bool:
        return self._store.checkpoint_covers(checkpoint_id, stage, direction, sample_end)

    def usage_ratio(self) -> float:
        return self._store.usage_ratio()

    def close(self) -> None:
        self._store.close()
