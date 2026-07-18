from __future__ import annotations

import sys


def main() -> None:
    from transhooter_worker.__main__ import main as root_main

    sys.argv.insert(1, "providers")
    root_main()


if __name__ == "__main__":
    main()
