from __future__ import annotations

import hashlib
import json
import logging
import os
import signal
import time
from datetime import UTC
from pathlib import Path
from threading import Event
from types import FrameType
from uuid import uuid5

from opentelemetry import metrics
from transhooter_spool import EncryptedSpool, SpoolDrainer, SpoolRecordDelivery

from .clients import (
    AbandonWorkerEpoch,
    ControlRequestError,
    DrainerControlClient,
    ExpiredWorkerEpoch,
    RetryableControlRequestError,
    WorkerTuple,
)
from .delivery import DeliveryRetryable, build_archive, drain_delivery_cycle
from .telemetry import bounded_error_kind, configure_telemetry

logger = logging.getLogger(__name__)
_METER = metrics.get_meter(__name__)
_DRAIN_DURATION = _METER.create_histogram("transhooter.spool.drainer.cycle.duration", unit="s")
_SPOOL_UTILIZATION = _METER.create_histogram(
    "transhooter.spool.drainer.utilization",
    description="Encrypted spool utilization observed by the drainer.",
    unit="1",
)
_PERMANENT_OUTCOMES = _METER.create_gauge(
    "transhooter.spool.drainer.permanent.outcomes", unit="{outcome}"
)
_STOP = Event()


def request_stop(_signum: int, _frame: FrameType | None) -> None:
    _STOP.set()


def main() -> None:
    telemetry = configure_telemetry(
        os.environ.get("OTEL_SERVICE_NAME", "").strip() or "transhooter-spool-drainer",
        endpoint=None,
        environment=os.environ.get("APP_ENV"),
        metric_export_interval_millis=_metric_export_interval(),
    )
    previous = signal.signal(signal.SIGTERM, request_stop)
    _STOP.clear()
    archive = None
    control = None
    spool = None
    try:
        token_file = os.environ.get("INTERNAL_TOKEN_FILE")
        if not token_file:
            raise RuntimeError("INTERNAL_TOKEN_FILE is required for spool draining")
        root = Path(os.environ["SPOOL_PATH"])
        spool = _build_spool(root)
        archive = build_archive(root)
        control = DrainerControlClient(os.environ["CONTROL_INTERNAL_URL"], Path(token_file))
        once = os.environ.get("DRAIN_ONCE", "false").lower() == "true"
        while not _STOP.is_set():
            started = time.monotonic()
            result = "failed"
            error: BaseException | None = None
            try:
                delivery_error: BaseException | None = None
                try:
                    drain_delivery_cycle(
                        spool,
                        archive,
                        control,
                        before_operation=lambda: not _STOP.is_set(),
                    )
                except (DeliveryRetryable, RetryableControlRequestError) as caught:
                    delivery_error = caught
                if not _STOP.is_set():
                    _recover_expired(spool, control)
                if delivery_error is not None:
                    raise delivery_error
                result = "ok"
            except (DeliveryRetryable, RetryableControlRequestError) as caught:
                control_error = _control_error(caught)
                if control_error is None:
                    logger.warning(
                        "spool drain result=deferred error.kind=%s error.type=%s",
                        bounded_error_kind(caught),
                        type(caught.__cause__ or caught).__name__,
                    )
                else:
                    logger.warning(
                        "spool drain result=deferred control.operation=%s control.status=%s control.code=%s",
                        control_error.operation,
                        control_error.status,
                        control_error.code,
                    )
            except BaseException as caught:
                error = caught
                raise
            finally:
                _record_cycle(spool, result, time.monotonic() - started, error)
            if once:
                return
            _STOP.wait(1)
    finally:
        if control is not None:
            control.close()
        if archive is not None:
            close = getattr(archive, "close", None)
            if callable(close):
                close()
        if spool is not None:
            close = getattr(spool, "close", None)
            if callable(close):
                close()
        telemetry.force_flush()
        telemetry.shutdown()
        signal.signal(signal.SIGTERM, previous)
        _STOP.clear()


def _control_error(error: BaseException) -> ControlRequestError | None:
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, ControlRequestError):
            return current
        current = current.__cause__
    return None


def _build_spool(root: Path) -> SpoolDrainer:
    shared = EncryptedSpool.from_keyring(
        root,
        Path(os.environ.get("SPOOL_DATABASE", str(root / "journal.sqlite3"))),
        Path(os.environ["SPOOL_KEYRING_FILE"]),
    )
    return shared.drainer()


def _recover_expired(spool: SpoolDrainer, control: DrainerControlClient) -> None:
    for expired in control.expired_worker_epochs():
        if _STOP.is_set():
            return
        _recover_one(spool, control, expired)


def _recover_one(
    spool: SpoolDrainer,
    control: DrainerControlClient,
    expired: ExpiredWorkerEpoch,
) -> None:
    worker = expired.worker
    handoff = spool.consultation_handoff(
        meeting_id=worker.consultation_id,
        generation=worker.generation,
        worker_id=worker.worker_id,
        worker_epoch=worker.worker_epoch,
        write_epoch=worker.write_epoch,
    )
    if handoff not in {"active", "settling", "sealed", "relinquished"}:
        return
    authority = spool.acquire_consultation_recovery(
        meeting_id=worker.consultation_id,
        generation=worker.generation,
        worker_id=worker.worker_id,
        worker_epoch=worker.worker_epoch,
        write_epoch=worker.write_epoch,
        blocking=False,
    )
    if authority is None:
        return
    with authority:
        permanent = tuple(
            delivery
            for delivery in spool.list_record_deliveries(
                meeting_id=worker.consultation_id,
                states={"permanent", "quarantined"},
            )
            if (
                delivery.context.generation,
                delivery.context.worker_id,
                delivery.context.worker_epoch,
                delivery.context.write_epoch,
            )
            == (
                worker.generation,
                worker.worker_id,
                worker.worker_epoch,
                worker.write_epoch,
            )
        )
        seals = tuple(
            spool.list_consultation_seals(
                meeting_id=worker.consultation_id,
                generation=worker.generation,
                worker_id=worker.worker_id,
                worker_epoch=worker.worker_epoch,
                write_epoch=worker.write_epoch,
            )
        )
        seal = seals[0] if len(seals) == 1 else None
        if handoff == "sealed" and not permanent:
            return
        if handoff != "relinquished":
            reason = (
                "expired sealed worker epoch has undeliverable terminal evidence"
                if handoff == "sealed"
                else "expired worker epoch has no terminal seal"
            )
            spool.relinquish_expired_consultation(authority, reason)
        else:
            reason = spool.consultation_relinquishment_reason(
                meeting_id=worker.consultation_id,
                generation=worker.generation,
                worker_id=worker.worker_id,
                worker_epoch=worker.worker_epoch,
                write_epoch=worker.write_epoch,
            ) or "expired worker epoch was relinquished"
        permanent_digest = _permanent_digest(permanent)
        handoff_digest = _handoff_digest(worker, reason)
        event_id = uuid5(
            worker.consultation_id,
            f"worker-abandon:{worker.generation}:{worker.worker_id}:"
            f"{worker.worker_epoch}:{worker.write_epoch}:{handoff_digest}:{permanent_digest}",
        )
        control.abandon_worker_epoch(
            AbandonWorkerEpoch(
                worker=worker,
                abandonment_event_id=event_id,
                reason=reason,
                handoff_digest=handoff_digest,
                permanent_outcome_digest=permanent_digest,
                seal_id=None if seal is None else seal.seal_id,
                completion_event_id=None if seal is None else seal.completion_event_id,
            )
        )
        if seal is not None:
            spool.mark_consultation_completion_acknowledged(seal.seal_id)


def _handoff_digest(worker: WorkerTuple, reason: str) -> str:
    value = {
        "consultationId": str(worker.consultation_id),
        "generation": worker.generation,
        "workerId": str(worker.worker_id),
        "epoch": worker.worker_epoch,
        "writeEpoch": worker.write_epoch,
        "state": "relinquished",
        "reason": reason,
    }
    return hashlib.sha256(
        json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()


def _permanent_digest(deliveries: tuple[SpoolRecordDelivery, ...]) -> str:
    value = [
        {
            "objectId": str(delivery.raw_ref.object_id),
            "state": delivery.state,
            "errorKind": delivery.error_kind,
            "failedAt": (
                None
                if delivery.failed_at is None
                else delivery.failed_at.astimezone(UTC).isoformat()
            ),
        }
        for delivery in sorted(deliveries, key=lambda item: item.raw_ref.ordinal)
    ]
    return hashlib.sha256(
        json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()


def _record_cycle(
    spool: SpoolDrainer,
    result: str,
    duration: float,
    error: BaseException | None,
) -> None:
    attributes = {"result": result}
    if error is not None:
        attributes["error.kind"] = bounded_error_kind(error)
    try:
        _DRAIN_DURATION.record(duration, attributes)
        ratio = spool.usage_ratio()
        _SPOOL_UTILIZATION.record(max(0.0, min(1.0, ratio)), {"role": "drainer"})
        permanent = len(spool.list_record_deliveries(states={"permanent"}))
        _PERMANENT_OUTCOMES.set(permanent, {"role": "drainer"})
    except Exception:
        pass


def _metric_export_interval() -> int | None:
    raw = os.environ.get("OTEL_METRIC_EXPORT_INTERVAL", "").strip()
    try:
        interval = int(raw)
    except ValueError:
        return None
    return interval if interval > 0 else None


if __name__ == "__main__":
    main()
