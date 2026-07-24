import pytest

from transhooter_worker.runtime import worker


class FakeTelemetry:
    def __init__(self) -> None:
        self.shutdown_calls = 0

    def shutdown(self) -> None:
        self.shutdown_calls += 1


class FakeProcess:
    pid = 1234
    exitcode = 74

    def __init__(self) -> None:
        self.started = False
        self.joined: list[float | None] = []

    def start(self) -> None:
        self.started = True

    def join(self, timeout: float | None = None) -> None:
        self.joined.append(timeout)

    def is_alive(self) -> bool:
        return False

    def terminate(self) -> None:
        raise AssertionError("terminated completed process")


def test_fatal_preservation_exit_terminates_container_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    telemetry = FakeTelemetry()
    process = FakeProcess()
    exits: list[int] = []

    monkeypatch.setattr(worker, "configure_telemetry", lambda **_kwargs: telemetry)
    monkeypatch.setattr(worker.mp, "get_context", lambda _method: FakeContext(process))
    monkeypatch.setattr(worker.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(worker.os, "_exit", lambda code: exits.append(code))

    worker.main()

    assert process.started is True
    assert process.joined == [None]
    assert exits == [74]
    assert telemetry.shutdown_calls == 1


class FakeContext:
    def __init__(self, process: FakeProcess) -> None:
        self.process = process

    def Process(self, **_kwargs: object) -> FakeProcess:
        return self.process
