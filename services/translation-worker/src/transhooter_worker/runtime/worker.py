from __future__ import annotations

import multiprocessing as mp
import os
import signal

from transhooter_worker.runtime.job import run_worker
from transhooter_worker.telemetry import configure_telemetry

_FATAL_PRESERVATION_EXIT = 74


def _metric_export_interval() -> int | None:
    raw_interval = os.environ.get("OTEL_METRIC_EXPORT_INTERVAL", "").strip()
    try:
        return int(raw_interval) if raw_interval else None
    except ValueError:
        return None


def _run_worker_process() -> None:
    run_worker()


def main() -> None:
    telemetry = configure_telemetry(
        service_name=os.environ.get("OTEL_SERVICE_NAME") or "transhooter-translation-worker",
        environment=os.environ.get("APP_ENV"),
        metric_export_interval_millis=_metric_export_interval(),
    )
    worker = mp.get_context("spawn").Process(
        target=_run_worker_process,
        name="translation-worker-runtime",
    )
    worker.start()

    def forward_signal(signum: int, _frame: object) -> None:
        if worker.is_alive() and worker.pid is not None:
            os.kill(worker.pid, signum)

    signal.signal(signal.SIGTERM, forward_signal)
    signal.signal(signal.SIGINT, forward_signal)
    try:
        worker.join()
        if worker.exitcode == _FATAL_PRESERVATION_EXIT:
            os._exit(_FATAL_PRESERVATION_EXIT)
            return
        if worker.exitcode not in (0, None):
            raise SystemExit(worker.exitcode)
    finally:
        if worker.is_alive():
            worker.terminate()
            worker.join(timeout=30)
        telemetry.shutdown()


if __name__ == "__main__":
    main()
