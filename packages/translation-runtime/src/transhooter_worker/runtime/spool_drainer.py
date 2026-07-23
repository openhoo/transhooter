from __future__ import annotations

import logging
import os
import signal
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from types import FrameType
from uuid import UUID

import httpx
from opentelemetry import metrics, trace
from opentelemetry.trace import Span, Status, StatusCode

from transhooter_worker.adapters.archive_delivery import (
    ArchiveObjectRegistrationClient,
    RetryableRegistrationError,
    build_archive,
    upload_committed_objects,
)
from transhooter_worker.adapters.s3_archive import S3Archive
from transhooter_worker.adapters.spool import EncryptedSpool
from transhooter_worker.application.compactor import PcmCompactor
from transhooter_worker.ports.archive import ObjectRecord
from transhooter_worker.telemetry import bounded_error_kind, configure_telemetry

logger = logging.getLogger(__name__)

_METER = metrics.get_meter(__name__)
_TRACER = trace.get_tracer(__name__)
_DRAIN_DURATION = _METER.create_histogram(
    "transhooter.worker.spool.drain.duration",
    unit="s",
)
_SPOOL_UTILIZATION = _METER.create_histogram(
    "transhooter.worker.spool.utilization",
    description="Encrypted spool utilization ratio observed by worker heartbeats.",
    unit="1",
)


@contextmanager
def _span(
    name: str,
    attributes: dict[str, str] | None = None,
) -> Iterator[Span]:
    try:
        context = _TRACER.start_as_current_span(
            name,
            attributes=attributes,
            record_exception=False,
            set_status_on_exception=False,
        )
        span = context.__enter__()
    except Exception:
        yield trace.NonRecordingSpan(trace.INVALID_SPAN_CONTEXT)
        return

    try:
        yield span
    except BaseException as error:
        try:
            context.__exit__(type(error), error, error.__traceback__)
        except Exception:
            pass
        raise
    else:
        try:
            context.__exit__(None, None, None)
        except Exception:
            pass


def _finish_span(
    span: Span,
    result: str,
    error: BaseException | None = None,
) -> None:
    try:
        span.set_attribute("result", result)
        if error is None:
            span.set_status(Status(StatusCode.OK))
            return
        span.set_attribute("error.kind", bounded_error_kind(error))
        span.set_status(Status(StatusCode.ERROR))
    except Exception:
        pass


def _record_drain(
    result: str,
    duration_seconds: float,
    error: BaseException | None = None,
) -> None:
    attributes = {"result": result}
    if error is not None:
        attributes["error.kind"] = bounded_error_kind(error)
    try:
        _DRAIN_DURATION.record(duration_seconds, attributes)
    except Exception:
        pass


def _record_utilization(spool: EncryptedSpool) -> None:
    try:
        ratio = spool.usage_ratio()
        _SPOOL_UTILIZATION.record(
            max(0.0, min(1.0, ratio)),
            {"role": "drainer"},
        )
    except Exception:
        pass


def _metric_export_interval() -> int | None:
    raw = os.environ.get("OTEL_METRIC_EXPORT_INTERVAL", "").strip()
    try:
        interval = int(raw)
    except ValueError:
        return None
    return interval if interval > 0 else None


def _build_spool(root: Path) -> EncryptedSpool:
    database = Path(os.environ.get("SPOOL_DATABASE", str(root / "journal.sqlite3")))
    return EncryptedSpool.from_keyring(
        root,
        database,
        Path(os.environ["SPOOL_KEYRING_FILE"]),
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
                    checkpoint,
                )


def _drain_once(
    spool: EncryptedSpool,
    archive: S3Archive,
    register: Callable[[UUID, UUID, str, ObjectRecord], None],
) -> None:
    started = time.monotonic()
    result = "failed"
    drain_error: BaseException | None = None
    with _span("transhooter.worker.spool.drain") as span:
        try:
            _compact_pcm_scopes(spool, archive)
            upload_committed_objects(spool, archive, register)
        except (httpx.TransportError, RetryableRegistrationError) as error:
            result = "deferred"
            drain_error = error
            raise
        except BaseException as error:
            drain_error = error
            raise
        else:
            result = "ok"
        finally:
            _record_drain(result, time.monotonic() - started, drain_error)
            _record_utilization(spool)
            _finish_span(span, result, drain_error)


def _handle_sigterm(_signum: int, _frame: FrameType | None) -> None:
    raise SystemExit(0)


def main() -> None:
    telemetry = configure_telemetry(
        os.environ.get("OTEL_SERVICE_NAME", "").strip() or "transhooter-spool-drainer",
        endpoint=None,
        environment=os.environ.get("APP_ENV"),
        metric_export_interval_millis=_metric_export_interval(),
    )
    previous_sigterm_handler = signal.signal(signal.SIGTERM, _handle_sigterm)
    try:
        token_file = os.environ.get("INTERNAL_TOKEN_FILE")
        if not token_file:
            raise RuntimeError("INTERNAL_TOKEN_FILE is required for spool draining")
        root = Path(os.environ["SPOOL_PATH"])
        spool = _build_spool(root)
        archive = build_archive(root)
        registration = ArchiveObjectRegistrationClient(
            os.environ["CONTROL_INTERNAL_URL"],
            Path(token_file),
        )
        drain_once = os.environ.get("DRAIN_ONCE", "false").lower() == "true"
        while True:
            try:
                _drain_once(spool, archive, registration.register)
            except (httpx.TransportError, RetryableRegistrationError) as error:
                if drain_once:
                    raise
                logger.warning(
                    "spool drain result=deferred error.kind=%s",
                    bounded_error_kind(error),
                )
            else:
                if drain_once:
                    return
            time.sleep(1)
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm_handler)
        telemetry.shutdown()


if __name__ == "__main__":
    main()
