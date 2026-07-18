from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from uuid import UUID

import boto3  # type: ignore[import-untyped]

from transhooter_worker.adapters.s3_archive import S3Archive
from transhooter_worker.adapters.spool import EncryptedSpool
from transhooter_worker.application.compactor import PcmCompactor


def _secret(name: str) -> str:
    path = os.environ.get(name + "_FILE")
    if path:
        value = Path(path).read_text("utf-8").strip()
    else:
        value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name}_FILE is required")
    return value


def _build_spool(root: Path) -> EncryptedSpool:
    database = Path(os.environ.get("SPOOL_DATABASE", str(root / "journal.sqlite3")))
    return EncryptedSpool.from_keyring(
        root,
        database,
        Path(os.environ["SPOOL_KEYRING_FILE"]),
    )


def build_archive(root: Path) -> S3Archive:
    credentials_file = os.environ.get("S3_CREDENTIALS_FILE")
    if not credentials_file:
        raise RuntimeError("S3_CREDENTIALS_FILE is required")
    credentials = json.loads(Path(credentials_file).read_text("utf-8"))
    access_key = credentials.get("accessKeyId")
    secret_key = credentials.get("secretAccessKey")
    if (
        not isinstance(access_key, str)
        or not access_key
        or not isinstance(secret_key, str)
        or not secret_key
    ):
        raise RuntimeError("S3 credential file is invalid")
    client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT"),
        region_name=os.environ.get("S3_REGION", "eu-central-1"),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    return S3Archive(
        client,
        _secret("S3_BUCKET"),
        os.environ.get("S3_KMS_KEY_ID"),
        os.environ.get("ARCHIVE_REQUIRE_KMS", "true").lower() == "true",
        root / "multipart.sqlite3",
    )


def _compact_pcm_scopes(spool: EncryptedSpool, archive: S3Archive) -> None:
    for meeting, stage, direction in spool.pcm_scopes():
        sample_rate = 16_000 if stage == "stt-input" else 48_000
        compactor = PcmCompactor(spool, archive, meeting, sample_rate)
        terminal_checkpoint = spool.covering_checkpoint(meeting, stage, direction, 0, True)
        closed_objects = compactor.compact(
            stage,
            direction,
            drain=terminal_checkpoint is not None,
        )
        for closed_pcm_object in closed_objects:
            checkpoint = spool.covering_checkpoint(
                meeting,
                stage,
                direction,
                closed_pcm_object.samples.end,
            )
            if checkpoint is not None:
                compactor.acknowledge_covering_checkpoint(
                    closed_pcm_object,
                    str(checkpoint),
                )


def upload_committed_objects(
    spool: EncryptedSpool,
    archive: S3Archive,
    meeting_id: UUID | None = None,
) -> None:
    pcm_stages = {"stt-input", "tts-output", "livekit-output"}
    for spool_ref, _ in spool.committed():
        meeting, attempt, stage, ordinal, media_type = spool.context(spool_ref.object_id)
        if (meeting_id is not None and meeting != meeting_id) or stage in pcm_stages:
            continue
        suffix = "json" if "json" in media_type else "bin"
        key = f"v1/meetings/{meeting}/pipeline/{stage}/raw/{attempt}/{ordinal:020d}.{suffix}"
        body = spool.read(spool_ref.object_id)
        archive_record = archive.put_create_once(
            key,
            body,
            media_type,
            hashlib.sha256(body).hexdigest(),
        )
        spool.mark_uploaded(
            spool_ref.object_id,
            archive_record.version_id,
            archive_record.s3_checksum,
        )


def _drain_once(spool: EncryptedSpool, archive: S3Archive) -> None:
    _compact_pcm_scopes(spool, archive)
    upload_committed_objects(spool, archive)


def main() -> None:
    root = Path(os.environ.get("SPOOL_PATH", os.environ.get("SPOOL_DIR", "")))
    spool = _build_spool(root)
    archive = build_archive(root)
    drain_once = os.environ.get("DRAIN_ONCE", "false").lower() == "true"
    while True:
        _drain_once(spool, archive)
        if drain_once:
            return
        time.sleep(1)


if __name__ == "__main__":
    main()
