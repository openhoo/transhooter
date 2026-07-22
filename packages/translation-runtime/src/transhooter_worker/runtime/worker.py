from __future__ import annotations

import os

from transhooter_worker.runtime.job import run_worker
from transhooter_worker.telemetry import configure_telemetry


def _metric_export_interval() -> int | None:
    raw_interval = os.environ.get("OTEL_METRIC_EXPORT_INTERVAL", "").strip()
    try:
        return int(raw_interval) if raw_interval else None
    except ValueError:
        return None


def main() -> None:
    telemetry = configure_telemetry(
        service_name=os.environ.get("OTEL_SERVICE_NAME") or "transhooter-translation-worker",
        environment=os.environ.get("APP_ENV"),
        metric_export_interval_millis=_metric_export_interval(),
    )
    try:
        run_worker()
    finally:
        telemetry.shutdown()


if __name__ == "__main__":
    main()
