#!/bin/sh
set -eu

usage() {
  echo "usage: $0 pre-stop|pre-start|post-start" >&2
  exit 2
}

mode=${1:-}
case "$mode" in
  pre-stop | pre-start | post-start) ;;
  *) usage ;;
esac

python_bin=${PYTHON_BIN:-python3}
spool_path=${SPOOL_PATH:-/var/lib/transhooter/spool}
spool_database=${SPOOL_DATABASE:-$spool_path/journal.sqlite3}

check_no_active_reservations() {
  database_url_file=${DATABASE_URL_FILE:?DATABASE_URL_FILE is required for pre-stop}
  [ -r "$database_url_file" ] || {
    echo "database URL file is not readable: $database_url_file" >&2
    exit 1
  }
  psql_bin=${PSQL_BIN:-psql}
  command -v "$psql_bin" >/dev/null 2>&1 || {
    echo "psql is required for the pre-stop reservation fence" >&2
    exit 1
  }
  database_url=$(cat "$database_url_file")
  [ -n "$database_url" ] || {
    echo "database URL file is empty: $database_url_file" >&2
    exit 1
  }
  active_count=$(
    PGCONNECT_TIMEOUT=${PGCONNECT_TIMEOUT:-10} "$psql_bin" --no-psqlrc \
      --set ON_ERROR_STOP=1 --tuples-only --no-align "$database_url" \
      --command "SELECT count(*) FROM worker_reservations reservation JOIN worker_job_epochs job ON job.consultation_id=reservation.consultation_id AND job.generation=reservation.generation AND job.worker_id=reservation.worker_id AND job.epoch=reservation.epoch WHERE reservation.released_at IS NULL AND reservation.fenced_at IS NULL AND job.fenced_at IS NULL AND job.terminal_at IS NULL;"
  )
  active_count=$(printf '%s' "$active_count" | tr -d '[:space:]')
  case "$active_count" in
    '' | *[!0-9]*)
      echo "invalid active reservation count returned by PostgreSQL" >&2
      exit 1
      ;;
  esac
  [ "$active_count" -eq 0 ] || {
    echo "pre-stop rejected: $active_count active translation reservation(s) remain" >&2
    exit 1
  }
}

if [ "$mode" = pre-stop ]; then
  check_no_active_reservations
fi

SPOOL_CUTOVER_MODE=$mode \
SPOOL_PATH=$spool_path \
SPOOL_DATABASE=$spool_database \
"$python_bin" - <<'PY'
from __future__ import annotations

import base64
import datetime as dt
import fcntl
import hashlib
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

MODE = os.environ["SPOOL_CUTOVER_MODE"]
ROOT = Path(os.environ["SPOOL_PATH"]).resolve()
DATABASE = Path(os.environ["SPOOL_DATABASE"]).resolve()
SCHEMA_VERSION = 2
EMPTY_LISTING_DIGEST = hashlib.sha256(b"").hexdigest()


def fail(message: str) -> None:
    raise SystemExit(message)


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    body = (json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n").encode()
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(body)
        while view:
            view = view[os.write(descriptor, view) :]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    fsync_directory(path.parent)


def required_external_path(name: str) -> Path:
    value = os.environ.get(name, "").strip()
    if not value:
        fail(f"{name} is required")
    path = Path(value).resolve()
    try:
        path.relative_to(ROOT)
    except ValueError:
        return path
    fail(f"{name} must be outside SPOOL_PATH")


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text("utf-8"))
    except (OSError, ValueError) as error:
        fail(f"invalid cutover proof {path}: {error}")
    if not isinstance(value, dict):
        fail(f"invalid cutover proof {path}: expected object")
    return value


def table_exists(database: sqlite3.Connection, table: str) -> bool:
    return (
        database.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        is not None
    )


def columns(database: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(database, table):
        return set()
    return {str(row[1]) for row in database.execute(f'PRAGMA table_info("{table}")')}


def scalar(database: sqlite3.Connection, statement: str) -> int:
    row = database.execute(statement).fetchone()
    return int(row[0]) if row is not None else 0


def read_keyring(path: Path) -> dict[str, bytes]:
    try:
        raw = json.loads(path.read_text("utf-8"))
        active = raw["active"]
        encoded = raw["keys"]
        if not isinstance(active, str) or not isinstance(encoded, dict) or active not in encoded:
            raise ValueError("invalid active key")
        keys = {
            str(key_id): base64.b64decode(str(value), validate=True)
            for key_id, value in encoded.items()
        }
    except (OSError, KeyError, TypeError, ValueError) as error:
        fail(f"invalid spool keyring: {error}")
    if any(len(key) != 32 for key in keys.values()):
        fail("invalid spool keyring: every key must be 32 bytes")
    return keys


def unpack_envelope(body: bytes) -> tuple[dict[str, Any], bytes]:
    if len(body) < 12 or body[:4] != b"TSW1":
        raise ValueError("invalid spool magic")
    try:
        header_length = int(body[4:12], 16)
    except ValueError as error:
        raise ValueError("invalid spool header length") from error
    if header_length <= 0 or 12 + header_length >= len(body):
        raise ValueError("invalid spool header length")
    header = json.loads(body[12 : 12 + header_length])
    if not isinstance(header, dict):
        raise ValueError("invalid spool header")
    return header, body[12 + header_length :]


def header_aad(header: dict[str, Any]) -> bytes:
    if header.get("aad_version") != 3:
        raise ValueError("unsupported full-header AAD version")
    authenticated = {key: value for key, value in header.items() if key != "ciphertext_sha256"}
    return json.dumps(authenticated, separators=(",", ":"), sort_keys=True).encode()


def authenticate_legacy_spool() -> None:
    keyring_value = os.environ.get("SPOOL_KEYRING_FILE", "").strip()
    if not keyring_value:
        fail("SPOOL_KEYRING_FILE is required for pre-stop")
    keyring = read_keyring(Path(keyring_value))
    if not ROOT.is_dir():
        fail(f"legacy spool path is not a directory: {ROOT}")
    if not DATABASE.is_file():
        fail(f"legacy spool database is missing: {DATABASE}")

    lock_path = ROOT / ".spool.lock"
    if not lock_path.is_file():
        fail(f"legacy spool lock is missing: {lock_path}")
    with lock_path.open("a+b", buffering=0) as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            fail("legacy spool is still owned by a running producer or drainer")

        database = sqlite3.connect(f"file:{DATABASE}?mode=ro", uri=True, isolation_level=None)
        try:
            database.execute("PRAGMA query_only=ON")
            record_columns = columns(database, "records")
            required = {
                "object_id", "attempt_id", "meeting_id", "stage", "transport",
                "direction", "media_type", "opaque_path", "key_id", "nonce",
                "plaintext_sha256", "ciphertext_sha256", "size", "sample_start",
                "sample_end", "metadata_json", "state",
            }
            if not required.issubset(record_columns):
                fail("legacy spool records schema is unknown or incomplete")
            query = "SELECT object_id,attempt_id,meeting_id,stage,transport,direction,media_type,opaque_path,key_id,nonce,plaintext_sha256,ciphertext_sha256,size,sample_start,sample_end,metadata_json,state FROM records"
            rows = {str(row[0]): row for row in database.execute(query)}
            wal_files = sorted(ROOT.glob("*.wal"), key=lambda path: path.name)
            for wal_path in wal_files:
                row = rows.get(wal_path.stem)
                try:
                    header, encrypted = unpack_envelope(wal_path.read_bytes())
                    key_id = str(header["key_id"])
                    nonce = base64.b64decode(str(header["nonce"]), validate=True)
                    plaintext = AESGCM(keyring[key_id]).decrypt(
                        nonce, encrypted, header_aad(header)
                    )
                except (InvalidTag, OSError, KeyError, TypeError, ValueError) as error:
                    fail(f"malformed or unauthenticated final WAL {wal_path.name}: {error}")
                if row is None:
                    fail(f"authenticated final WAL lacks its SQLite index row: {wal_path.name}")
                if hashlib.sha256(encrypted).hexdigest() != str(header.get("ciphertext_sha256")):
                    fail(f"ciphertext digest mismatch in {wal_path.name}")
                if hashlib.sha256(plaintext).hexdigest() != str(header.get("plaintext_sha256")):
                    fail(f"plaintext digest mismatch in {wal_path.name}")
                expected = {
                    "object_id": str(row[0]),
                    "attempt_id": str(row[1]),
                    "meeting_id": str(row[2]),
                    "stage": str(row[3]),
                    "transport": str(row[4]),
                    "direction": str(row[5]),
                    "media_type": str(row[6]),
                    "key_id": str(row[8]),
                    "plaintext_sha256": str(row[10]),
                    "ciphertext_sha256": str(row[11]),
                    "size": int(row[12]),
                    "sample_start": row[13],
                    "sample_end": row[14],
                }
                if any(header.get(key) != value for key, value in expected.items()):
                    fail(f"authenticated WAL/index identity mismatch: {wal_path.name}")
                if nonce != bytes(row[9]) or len(plaintext) != int(row[12]):
                    fail(f"authenticated WAL/index payload mismatch: {wal_path.name}")
                try:
                    indexed_metadata = json.loads(bytes(row[15]))
                except (TypeError, ValueError) as error:
                    fail(f"invalid indexed metadata for {wal_path.name}: {error}")
                if header.get("metadata") != indexed_metadata:
                    fail(f"authenticated WAL/index metadata mismatch: {wal_path.name}")
                if Path(str(row[7])).resolve() != wal_path.resolve():
                    fail(f"authenticated WAL/index path mismatch: {wal_path.name}")

            quarantined = scalar(database, "SELECT count(*) FROM records WHERE state='quarantined'")
            if quarantined:
                fail(f"pre-stop rejected: {quarantined} quarantined record(s) remain")
            actionable = scalar(
                database,
                "SELECT count(*) FROM records WHERE state IN ('committed','permanent')",
            )
            checkpoint_columns = columns(database, "checkpoint_deliveries")
            if "delivery_state" in checkpoint_columns:
                actionable += scalar(
                    database,
                    "SELECT count(*) FROM checkpoint_deliveries WHERE delivery_state IN ('pending','permanent')",
                )
            elif "acknowledged" in checkpoint_columns:
                actionable += scalar(
                    database,
                    "SELECT count(*) FROM checkpoint_deliveries WHERE acknowledged=0",
                )
            if actionable:
                fail(f"pre-stop rejected: {actionable} actionable spool item(s) remain")
        finally:
            database.close()


def pre_start() -> None:
    proof_path = required_external_path("SPOOL_CUTOVER_PROOF_FILE")
    receipt_path = required_external_path("SPOOL_CUTOVER_RECEIPT_FILE")
    if proof_path.exists() or receipt_path.exists():
        fail("cutover proof or receipt already exists; refusing nonce reuse")
    if not ROOT.is_dir():
        fail(f"replacement spool path is not a directory: {ROOT}")
    listing = sorted(path.name for path in ROOT.iterdir())
    if listing:
        fail("replacement spool volume is not empty: " + ", ".join(listing))
    stat = ROOT.stat()
    proof = {
        "createdAt": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
        "device": stat.st_dev,
        "inode": stat.st_ino,
        "listingSha256": EMPTY_LISTING_DIGEST,
        "nonce": base64.urlsafe_b64encode(os.urandom(32)).rstrip(b"=").decode(),
        "path": str(ROOT),
        "schema": SCHEMA_VERSION,
    }
    atomic_json(proof_path, proof)
    try:
        atomic_json(receipt_path, proof)
    except BaseException:
        proof_path.unlink(missing_ok=True)
        fsync_directory(proof_path.parent)
        raise


def spool_meta(database: sqlite3.Connection) -> dict[str, str]:
    meta_columns = columns(database, "spool_meta")
    if {"key", "value"}.issubset(meta_columns):
        return {str(key): str(value) for key, value in database.execute("SELECT key,value FROM spool_meta")}
    if {"name", "value"}.issubset(meta_columns):
        return {str(key): str(value) for key, value in database.execute("SELECT name,value FROM spool_meta")}
    fail("schema-v2 spool_meta does not expose key/value metadata")


def post_start() -> None:
    proof_path = required_external_path("SPOOL_CUTOVER_PROOF_FILE")
    receipt_path = required_external_path("SPOOL_CUTOVER_RECEIPT_FILE")
    if proof_path.exists():
        fail("schema-v2 initialization did not consume the external cutover proof")
    receipt = read_json(receipt_path)
    if receipt.get("schema") != SCHEMA_VERSION:
        fail("cutover receipt schema mismatch")
    stat = ROOT.stat()
    expected_identity = {
        "path": str(ROOT),
        "device": stat.st_dev,
        "inode": stat.st_ino,
    }
    if any(receipt.get(key) != value for key, value in expected_identity.items()):
        fail("replacement spool volume identity does not match the cutover receipt")
    if not DATABASE.is_file():
        fail(f"schema-v2 spool database is missing: {DATABASE}")
    database = sqlite3.connect(f"file:{DATABASE}?mode=ro", uri=True, isolation_level=None)
    try:
        database.execute("PRAGMA query_only=ON")
        meta = spool_meta(database)
    finally:
        database.close()
    schema = meta.get("schema_version", meta.get("schema"))
    nonce = meta.get("cutover_nonce", meta.get("cutoverNonce"))
    if schema != str(SCHEMA_VERSION):
        fail("schema-v2 spool metadata version mismatch")
    if nonce != receipt.get("nonce"):
        fail("schema-v2 spool metadata nonce mismatch")


if MODE == "pre-stop":
    authenticate_legacy_spool()
elif MODE == "pre-start":
    pre_start()
elif MODE == "post-start":
    post_start()
else:
    fail(f"unsupported cutover mode: {MODE}")
PY

if [ "$mode" = post-start ]; then
  healthcheck=${SPOOL_CUTOVER_HEALTHCHECK:-transhooter-spool-drainer-healthcheck}
  command -v "$healthcheck" >/dev/null 2>&1 || {
    echo "cutover healthcheck command is unavailable: $healthcheck" >&2
    exit 1
  }
  "$healthcheck"
fi
