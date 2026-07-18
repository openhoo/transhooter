from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from pathlib import Path
from uuid import uuid4

from transhooter_worker.domain.models import StageCapabilities

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS profile_revisions (
    revision_id TEXT PRIMARY KEY,
    profile TEXT NOT NULL,
    capability_hash TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL,
    created_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS active_profiles (
    profile TEXT PRIMARY KEY,
    revision_id TEXT NOT NULL REFERENCES profile_revisions(revision_id)
);
"""


def _validate_complete_snapshot(capabilities: tuple[StageCapabilities, ...]) -> None:
    has_incomplete_stage = any(
        not capability.languages or not capability.models or not capability.endpoint
        for capability in capabilities
    )
    has_tts_without_voices = any(
        capability.stage == "tts" and not capability.voices for capability in capabilities
    )
    if len(capabilities) != 3 or has_incomplete_stage or has_tts_without_voices:
        raise RuntimeError("incomplete provider capability snapshot")


def _serialize_capabilities(capabilities: tuple[StageCapabilities, ...]) -> str:
    serialized_capabilities = [
        {
            "provider": capability.provider,
            "stage": capability.stage,
            "endpoint": capability.endpoint,
            "regions": capability.regions,
            "languages": capability.languages,
            "models": capability.models,
            "voices": capability.voices,
            "limits": capability.limits,
        }
        for capability in capabilities
    ]
    return json.dumps(serialized_capabilities, separators=(",", ":"), sort_keys=True)


class CapabilityStore:
    def __init__(self, path: Path) -> None:
        self._db = sqlite3.connect(path, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.executescript(_SCHEMA_SQL)

    def replace(
        self,
        profile: str,
        capabilities: tuple[StageCapabilities, ...],
    ) -> dict[str, str]:
        _validate_complete_snapshot(capabilities)
        payload = _serialize_capabilities(capabilities)
        digest = hashlib.sha256(payload.encode()).hexdigest()
        revision_id = str(uuid4())
        self._db.execute("BEGIN IMMEDIATE")
        try:
            existing = self._db.execute(
                """
                SELECT revision_id
                FROM profile_revisions
                WHERE capability_hash = ?
                """,
                (digest,),
            ).fetchone()
            if existing:
                revision_id = str(existing[0])
            else:
                self._db.execute(
                    """
                    INSERT INTO profile_revisions(
                        revision_id,
                        profile,
                        capability_hash,
                        payload_json,
                        created_ms
                    )
                    VALUES(?, ?, ?, ?, ?)
                    """,
                    (
                        revision_id,
                        profile,
                        digest,
                        payload,
                        int(time.time() * 1000),
                    ),
                )
            self._db.execute(
                """
                INSERT INTO active_profiles(profile, revision_id)
                VALUES(?, ?)
                ON CONFLICT(profile) DO UPDATE
                SET revision_id = excluded.revision_id
                """,
                (profile, revision_id),
            )
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise
        return {"revisionId": revision_id, "capabilityHash": digest}
