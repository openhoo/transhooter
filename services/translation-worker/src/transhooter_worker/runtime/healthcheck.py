from __future__ import annotations

import argparse
import os
from pathlib import Path

from transhooter_worker.adapters.spool import EncryptedSpool


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--drainer", action="store_true")
    args = parser.parse_args()
    root = Path(os.environ.get("SPOOL_PATH", ""))
    keyring_value = os.environ.get("SPOOL_KEYRING_FILE", "")
    if not root.is_dir() or not keyring_value:
        raise SystemExit("spool path/keyring unavailable")
    spool = EncryptedSpool.from_keyring(
        root,
        Path(os.environ.get("SPOOL_DATABASE", str(root / "journal.sqlite3"))),
        Path(keyring_value),
    )
    threshold_name = "SPOOL_DRAINER_HEALTH_RATIO" if args.drainer else "SPOOL_WORKER_HEALTH_RATIO"
    threshold = float(os.environ.get(threshold_name, "0.8" if args.drainer else "0.7"))
    if not 0 < threshold < 1:
        raise SystemExit(f"{threshold_name} must be between zero and one")
    if spool.usage_ratio() >= threshold:
        raise SystemExit(f"spool usage is at or above {int(threshold * 100)}%")
    for ref, _ in spool.committed():
        spool.read(ref.object_id)
    print("healthy")


if __name__ == "__main__":
    main()
