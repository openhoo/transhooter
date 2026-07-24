from __future__ import annotations

import signal
from threading import Event

from transhooter_spool_drainer import runtime


def test_sigterm_only_requests_quiescence() -> None:
    runtime._STOP.clear()
    runtime.request_stop(signal.SIGTERM, None)
    assert runtime._STOP.is_set()
    runtime._STOP.clear()


def test_stop_before_next_operation_blocks_new_delivery() -> None:
    stop = Event()
    starts: list[str] = []

    def allowed() -> bool:
        starts.append("checked")
        return not stop.is_set()

    stop.set()
    assert not allowed()
    assert starts == ["checked"]


def test_main_quiesces_after_active_cycle_and_cleans_up(monkeypatch) -> None:
    events: list[str] = []
    handlers: list[object] = []

    class Telemetry:
        def force_flush(self) -> None:
            events.append("flush")

        def shutdown(self) -> None:
            events.append("shutdown")

    class Closable:
        def close(self) -> None:
            events.append("close")

    class Control(Closable):
        pass

    monkeypatch.setattr(runtime, "configure_telemetry", lambda *_a, **_k: Telemetry())
    monkeypatch.setattr(
        runtime.signal,
        "signal",
        lambda _sig, handler: handlers.append(handler) or signal.SIG_DFL,
    )
    monkeypatch.setattr(runtime, "_build_spool", lambda _root: Closable())
    monkeypatch.setattr(runtime, "build_archive", lambda _root: Closable())
    monkeypatch.setattr(runtime, "DrainerControlClient", lambda *_a: Control())

    def cycle(_spool, _archive, _control, *, before_operation):
        events.append("remote-start")
        runtime.request_stop(signal.SIGTERM, None)
        events.append("remote-settled")
        assert not before_operation()

    monkeypatch.setattr(runtime, "drain_delivery_cycle", cycle)
    monkeypatch.setenv("INTERNAL_TOKEN_FILE", "/unused/token")
    monkeypatch.setenv("CONTROL_INTERNAL_URL", "http://web")
    monkeypatch.setenv("SPOOL_PATH", "/unused/spool")

    runtime.main()

    assert events[:2] == ["remote-start", "remote-settled"]
    assert events[2:5] == ["close", "close", "close"]
    assert events[5:] == ["flush", "shutdown"]
    assert handlers == [runtime.request_stop, signal.SIG_DFL]


def test_main_recovers_expired_epochs_when_delivery_is_retryable(monkeypatch) -> None:
    events: list[str] = []

    class Telemetry:
        def force_flush(self) -> None:
            pass

        def shutdown(self) -> None:
            pass

    class Closable:
        def close(self) -> None:
            pass

    monkeypatch.setattr(runtime, "configure_telemetry", lambda *_a, **_k: Telemetry())
    monkeypatch.setattr(runtime.signal, "signal", lambda *_a: signal.SIG_DFL)
    monkeypatch.setattr(runtime, "_build_spool", lambda _root: Closable())
    monkeypatch.setattr(runtime, "build_archive", lambda _root: Closable())
    monkeypatch.setattr(runtime, "DrainerControlClient", lambda *_a: Closable())

    def cycle(*_args, **_kwargs):
        events.append("delivery")
        raise runtime.DeliveryRetryable("pending delivery")

    def recover(*_args):
        events.append("recovery")

    monkeypatch.setattr(runtime, "drain_delivery_cycle", cycle)
    monkeypatch.setattr(runtime, "_recover_expired", recover)
    monkeypatch.setenv("INTERNAL_TOKEN_FILE", "/unused/token")
    monkeypatch.setenv("CONTROL_INTERNAL_URL", "http://web")
    monkeypatch.setenv("SPOOL_PATH", "/unused/spool")
    monkeypatch.setenv("DRAIN_ONCE", "true")

    try:
        runtime.main()
    except runtime.DeliveryRetryable:
        pass

    assert events == ["delivery", "recovery"]


def test_expired_active_tuple_relinquishes_then_abandons() -> None:
    from uuid import UUID

    from transhooter_spool_drainer.clients import ExpiredWorkerEpoch, WorkerTuple

    events: list[str] = []

    class Authority:
        def __enter__(self):
            events.append("locked")
            return self

        def __exit__(self, *_args):
            events.append("released")

    class Spool:
        def consultation_handoff(self, **_kwargs):
            return "active"

        def acquire_consultation_recovery(self, **_kwargs):
            return Authority()

        def relinquish_expired_consultation(self, _authority, reason):
            events.append(f"relinquished:{reason}")

        def list_record_deliveries(self, **_kwargs):
            return ()

        def list_consultation_seals(self, **_kwargs):
            return ()
        def mark_consultation_completion_acknowledged(self, _seal_id):
            events.append("ack")


    class Control:
        def abandon_worker_epoch(self, request):
            events.append(f"abandoned:{request.reason}")

    expired = ExpiredWorkerEpoch(WorkerTuple(UUID(int=1), 2, UUID(int=3), 4, 5))
    runtime._recover_one(Spool(), Control(), expired)  # type: ignore[arg-type]
    assert events == [
        "locked",
        "relinquished:expired worker epoch has no terminal seal",
        "abandoned:expired worker epoch has no terminal seal",
        "released",
    ]


def test_expired_sealed_tuple_abandons_only_with_permanent_evidence() -> None:
    from dataclasses import dataclass
    from datetime import UTC, datetime
    from uuid import UUID

    from transhooter_spool import RawRef, SpoolRecordContext, SpoolRecordDelivery

    from transhooter_spool_drainer.clients import ExpiredWorkerEpoch, WorkerTuple

    events: list[str] = []
    worker = WorkerTuple(UUID(int=1), 2, UUID(int=3), 4, 5)

    class Authority:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    @dataclass(frozen=True)
    class Seal:
        seal_id: UUID
        completion_event_id: UUID

    context = SpoolRecordContext(
        meeting_id=worker.consultation_id,
        attempt_id=UUID(int=12),
        stage="translation",
        transport="http",
        direction="in",
        media_type="application/json",
        ordinal=1,
        generation=worker.generation,
        worker_id=worker.worker_id,
        worker_epoch=worker.worker_epoch,
        write_epoch=worker.write_epoch,
        metadata=(),
    )
    permanent = SpoolRecordDelivery(
        raw_ref=RawRef(UUID(int=13), 1, "a" * 64, 1, "application/json"),
        sample_range=None,
        context=context,
        state="permanent",
        version_id=None,
        s3_checksum=None,
        error_kind="WORKER_FENCED",
        failed_at=datetime.now(UTC),
    )

    class Spool:
        def consultation_handoff(self, **_kwargs): return "sealed"
        def acquire_consultation_recovery(self, **_kwargs): return Authority()
        def relinquish_expired_consultation(self, _authority, reason): events.append(reason)
        def list_record_deliveries(self, **_kwargs): return (permanent,)
        def list_consultation_seals(self, **_kwargs): return (Seal(UUID(int=10), UUID(int=11)),)
        def mark_consultation_completion_acknowledged(self, _seal_id): events.append("ack")

    class Control:
        def abandon_worker_epoch(self, request):
            assert request.seal_id == UUID(int=10)
            assert request.completion_event_id == UUID(int=11)
            assert request.permanent_outcome_digest != hashlib.sha256(b"[]").hexdigest()
            events.append("abandon")

    import hashlib
    runtime._recover_one(Spool(), Control(), ExpiredWorkerEpoch(worker))  # type: ignore[arg-type]
    assert events == [
        "expired sealed worker epoch has undeliverable terminal evidence",
        "abandon",
        "ack",
    ]


def test_expired_sealed_tuple_without_permanent_evidence_stays_sealed() -> None:
    from dataclasses import dataclass
    from uuid import UUID

    from transhooter_spool_drainer.clients import ExpiredWorkerEpoch, WorkerTuple

    events: list[str] = []
    worker = WorkerTuple(UUID(int=1), 2, UUID(int=3), 4, 5)

    class Authority:
        def __enter__(self):
            events.append("locked")
            return self

        def __exit__(self, *_args):
            events.append("released")

    @dataclass(frozen=True)
    class Seal:
        seal_id: UUID
        completion_event_id: UUID

    class Spool:
        def consultation_handoff(self, **_kwargs): return "sealed"
        def acquire_consultation_recovery(self, **_kwargs): return Authority()
        def list_record_deliveries(self, **_kwargs): return ()
        def list_consultation_seals(self, **_kwargs): return (Seal(UUID(int=10), UUID(int=11)),)
        def relinquish_expired_consultation(self, _authority, _reason): events.append("relinquish")
        def mark_consultation_completion_acknowledged(self, _seal_id): events.append("ack")

    class Control:
        def abandon_worker_epoch(self, _request): events.append("abandon")

    runtime._recover_one(Spool(), Control(), ExpiredWorkerEpoch(worker))  # type: ignore[arg-type]

    assert events == ["locked", "released"]
