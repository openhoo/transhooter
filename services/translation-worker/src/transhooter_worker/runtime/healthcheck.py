from __future__ import annotations

import os
import sys
from pathlib import Path

from transhooter_spool import EncryptedSpool


def main() -> None:
    if len(sys.argv) != 1:
        raise SystemExit("worker healthcheck accepts no arguments")
    root = Path(os.environ.get("SPOOL_PATH", ""))
    keyring_value = os.environ.get("SPOOL_KEYRING_FILE", "")
    if not root.is_dir() or not keyring_value:
        raise SystemExit("spool path/keyring unavailable")
    spool = EncryptedSpool.from_keyring(
        root,
        Path(os.environ.get("SPOOL_DATABASE", str(root / "journal.sqlite3"))),
        Path(keyring_value),
    )
    threshold = float(os.environ.get("SPOOL_WORKER_HEALTH_RATIO", "0.7"))
    if not 0 < threshold < 1:
        raise SystemExit("SPOOL_WORKER_HEALTH_RATIO must be between zero and one")
    if spool.usage_ratio() >= threshold:
        raise SystemExit(f"spool usage is at or above {int(threshold * 100)}%")
    for ref, _ in spool.committed():
        spool.read(ref.object_id)
    print("healthy")


if __name__ == "__main__":
    main()
