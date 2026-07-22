from __future__ import annotations

import asyncio
import json
import os
import sys

from transhooter_worker.provider_cli import (
    _approved_voice as _approved_voice,
)
from transhooter_worker.provider_cli import (
    _capability_refresh as _capability_refresh,
)
from transhooter_worker.provider_cli import (
    _configure_journal_context as _configure_journal_context,
)
from transhooter_worker.provider_cli import (
    _effective_voice as _effective_voice,
)
from transhooter_worker.provider_cli import (
    _profile_id as _profile_id,
)
from transhooter_worker.provider_cli import (
    _profile_voice as _profile_voice,
)
from transhooter_worker.provider_cli import (
    _publish_capabilities as _publish_capabilities,
)
from transhooter_worker.provider_cli import (
    parser,
    run,
)
from transhooter_worker.runtime.job import run_worker
from transhooter_worker.telemetry import configure_telemetry


def main() -> None:
    raw_interval = os.environ.get("OTEL_METRIC_EXPORT_INTERVAL", "").strip()
    try:
        metric_export_interval_millis = int(raw_interval) if raw_interval else None
    except ValueError:
        metric_export_interval_millis = None
    telemetry = configure_telemetry(
        service_name=os.environ.get("OTEL_SERVICE_NAME") or "transhooter-translation-worker",
        environment=os.environ.get("APP_ENV"),
        metric_export_interval_millis=metric_export_interval_millis,
    )
    try:
        if len(sys.argv) == 1:
            sys.argv.append("start")
        if sys.argv[1] != "providers":
            run_worker()
            return
        args = parser().parse_args()
        try:
            result = asyncio.run(run(args))
        except Exception as exc:
            raise SystemExit(f"provider command failed: {exc}") from exc
        print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    finally:
        telemetry.shutdown()


if __name__ == "__main__":
    main()
