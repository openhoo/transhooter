import asyncio
from uuid import uuid4

import pytest

from transhooter_worker.application.pipeline import (
    OrderedStageQueue,
    split_text,
    upsert_final_span,
)
from transhooter_worker.application.retry import FrozenRetryPolicy
from transhooter_worker.domain.models import (
    ErrorKind,
    Finality,
    ProviderError,
    RawRef,
    RetryAction,
    RetryAdvice,
    SampleRange,
    TranscriptEvent,
)
from transhooter_worker.domain.sample_clock import SampleClock


def test_sample_ranges_are_inclusive_exclusive() -> None:
    clock = SampleClock(16000)
    assert clock.advance_pcm16(8000) == SampleRange(0, 4000)
    assert clock.advance_pcm16(8000) == SampleRange(4000, 8000)
    with pytest.raises(ValueError):
        SampleRange(3, 3)


def test_retry_never_replays_emitted_output() -> None:
    attempt = uuid4()
    error = ProviderError(
        ErrorKind.TRANSPORT,
        "operation",
        RetryAdvice.UNSPECIFIED,
        None,
        None,
        None,
        attempt,
        (),
        "lost",
    )
    policy = FrozenRetryPolicy(3, 100, 1000)
    assert policy.decide(error, 1, 1, 0, 0, 0.5).action is RetryAction.RETRY
    assert policy.decide(error, 1, 1, 1, 0, 0.5).action is RetryAction.DEGRADE
    assert policy.decide(error, 1, 1, 0, 1, 0.5).action is RetryAction.DEGRADE


def test_pathological_text_splits_on_sentence_boundaries() -> None:
    text = ("a" * 2000) + ". " + ("b" * 2500) + ". " + ("c" * 100)
    pieces = split_text(text, 4500)
    assert "".join(pieces).replace(" ", "") == text.replace(" ", "")
    assert all(len(piece) <= 4500 for piece in pieces)


def test_final_span_upsert_preserves_sample_order_and_highest_revision() -> None:
    ref = RawRef(uuid4(), 0, "0" * 64, 1, "application/json")
    earlier = SampleRange(28_000, 32_000)
    replayed = SampleRange(32_000, 36_000)
    spans: list[TranscriptEvent] = []
    for samples, revision, text in (
        (replayed, 4, "old"),
        (earlier, 1, "first"),
        (replayed, 3, "ignored"),
        (replayed, 4, "equal replacement"),
        (replayed, 5, "corrected"),
    ):
        upsert_final_span(
            spans,
            TranscriptEvent(samples, revision, Finality.SPAN_FINAL, text, (), None, ref),
        )

    assert [(item.samples, item.revision, item.text) for item in spans] == [
        (earlier, 1, "first"),
        (replayed, 5, "corrected"),
    ]


def test_equal_revision_replay_cannot_replace_final_span() -> None:
    ref = RawRef(uuid4(), 0, "0" * 64, 1, "application/json")
    samples = SampleRange(0, 4_000)
    spans = [TranscriptEvent(samples, 7, Finality.SPAN_FINAL, "authoritative", (), None, ref)]

    upsert_final_span(
        spans,
        TranscriptEvent(samples, 7, Finality.SPAN_FINAL, "conflicting replay", (), None, ref),
    )

    assert spans[0].text == "authoritative"
    assert spans[0].revision == 7


@pytest.mark.asyncio
async def test_ordered_stage_queue_drops_provisionals_but_blocks_finals() -> None:
    queue = OrderedStageQueue(1)
    executed: list[str] = []

    async def first() -> None:
        executed.append("first")

    async def dropped() -> None:
        executed.append("dropped")

    async def final() -> None:
        executed.append("final")

    await queue.submit(False, first)
    await queue.submit(False, dropped)
    final_submission = asyncio.create_task(queue.submit(True, final))
    await asyncio.sleep(0)
    assert not final_submission.done()

    runner = asyncio.create_task(queue.run())
    await final_submission
    await queue.join()
    runner.cancel()
    await asyncio.gather(runner, return_exceptions=True)

    assert executed == ["first", "final"]


@pytest.mark.asyncio
async def test_ordered_stage_queue_failure_rejects_blocked_final_and_unblocks_join() -> None:
    queue = OrderedStageQueue(1)
    entered = asyncio.Event()
    fail = asyncio.Event()

    async def failing() -> None:
        entered.set()
        await fail.wait()
        raise RuntimeError("stage failed")

    async def pending() -> None:
        raise AssertionError("pending work must be discarded")

    runner = asyncio.create_task(queue.run())
    await queue.submit(True, failing)
    await entered.wait()
    await queue.submit(True, pending)
    blocked = asyncio.create_task(queue.submit(True, pending))
    await asyncio.sleep(0)
    assert not blocked.done()

    fail.set()

    with pytest.raises(RuntimeError, match="stage failed"):
        await blocked
    with pytest.raises(RuntimeError, match="stage failed"):
        await queue.join()
    with pytest.raises(RuntimeError, match="stage failed"):
        await runner
