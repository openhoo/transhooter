import asyncio
from uuid import uuid4

import pytest

from transhooter_worker.application.session import DirectionResult
from transhooter_worker.runtime.job import _drain_runtime, _MemoizedShutdown


@pytest.mark.asyncio
async def test_memoized_shutdown_shields_one_task_from_cancelled_caller() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def shutdown() -> None:
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()

    owner = _MemoizedShutdown(shutdown)
    first = asyncio.create_task(owner.run())
    await entered.wait()
    first.cancel()
    await asyncio.sleep(0)
    assert not first.done()

    second = asyncio.create_task(owner.run())
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await first
    await second

    assert calls == 1
    assert owner.started


@pytest.mark.asyncio
async def test_failed_session_drain_preserves_final_watermarks_for_terminal_seal() -> None:
    class Session:
        _last_input = 8_000
        _last_output = 1_920

        async def finish(self) -> DirectionResult:
            raise RuntimeError("provider shutdown failed")

    class Providers:
        async def aclose(self) -> None:
            return None

    source_id = uuid4()
    with pytest.raises(RuntimeError, match="provider shutdown failed") as raised:
        await _drain_runtime(set(), {source_id: Session()}, {}, [Providers()])  # type: ignore[arg-type]

    assert raised.value.direction_results == {  # type: ignore[attr-defined]
        source_id: DirectionResult(8_000, 1_920)
    }


@pytest.mark.asyncio
async def test_direction_result_is_immutable_final_watermark() -> None:
    result = DirectionResult(4_000, 960)
    with pytest.raises(AttributeError):
        result.input_sample = 8_000  # type: ignore[misc]
