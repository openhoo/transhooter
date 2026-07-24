from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def sha256_hex(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def encrypt_aesgcm(key: bytes, nonce: bytes, payload: bytes, aad: bytes) -> bytes:
    return AESGCM(key).encrypt(nonce, payload, aad)


def decrypt_aesgcm(key: bytes, nonce: bytes, payload: bytes, aad: bytes) -> bytes:
    return AESGCM(key).decrypt(nonce, payload, aad)


def header_aad(header: dict[str, Any]) -> bytes:
    if int(header.get("aad_version", 0)) != 3:
        raise ValueError("unsupported full-header AAD version")
    authenticated = {key: value for key, value in header.items() if key != "ciphertext_sha256"}
    return json.dumps(authenticated, separators=(",", ":"), sort_keys=True).encode()


def pack_envelope(header: dict[str, Any], encrypted: bytes) -> bytes:
    encoded = json.dumps(header, separators=(",", ":"), sort_keys=True).encode()
    return b"TSW1" + f"{len(encoded):08x}".encode() + encoded + encrypted


def unpack_envelope(envelope: bytes) -> tuple[dict[str, Any], bytes]:
    if len(envelope) < 12 or envelope[:4] != b"TSW1":
        raise ValueError("invalid spool magic")
    try:
        length = int(envelope[4:12], 16)
    except ValueError as exc:
        raise ValueError("invalid spool header length") from exc
    if length <= 0 or 12 + length >= len(envelope):
        raise ValueError("invalid spool header length")
    header = json.loads(envelope[12 : 12 + length])
    if not isinstance(header, dict):
        raise ValueError("invalid spool header")
    return header, envelope[12 + length :]


def write_fsync(path: Path, body: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(body)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)


def fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
