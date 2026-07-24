from __future__ import annotations

import os
from pathlib import Path

from transhooter_spool import EncryptedSpool


def main() -> None:
    root = Path(os.environ.get("SPOOL_PATH", ""))
    keyring_value = os.environ.get("SPOOL_KEYRING_FILE", "")
    if not root.is_dir() or not keyring_value:
        raise SystemExit("spool path/keyring unavailable")
    shared = EncryptedSpool.from_keyring(
        root,
        Path(os.environ.get("SPOOL_DATABASE", str(root / "journal.sqlite3"))),
        Path(keyring_value),
    )
    drainer_factory = getattr(shared, "drainer", None)
    spool = drainer_factory() if callable(drainer_factory) else shared
    threshold = float(os.environ.get("SPOOL_DRAINER_HEALTH_RATIO", "0.8"))
    if not 0 < threshold < 1:
        raise SystemExit("SPOOL_DRAINER_HEALTH_RATIO must be between zero and one")
    if spool.usage_ratio() >= threshold:
        raise SystemExit(f"spool usage is at or above {int(threshold * 100)}%")
    for delivery in spool.list_record_deliveries(states={"committed", "permanent"}):
        spool.read(delivery.raw_ref.object_id)
    print("healthy")


if __name__ == "__main__":
    main()
