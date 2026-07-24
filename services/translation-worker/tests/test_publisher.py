from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest

from transhooter_worker.runtime import publisher as module


class RecordingParticipant:
    def __init__(self, fail_final_permission: bool = False) -> None:
        self.log: list[tuple[str, Any]] = []
        self.fail_final_permission = fail_final_permission
        self.published = 0

    def set_track_subscription_permissions(self, **kwargs: Any) -> None:
        self.log.append(("permission", kwargs))
        if self.fail_final_permission and len(self.log) > 1:
            raise RuntimeError("permission failure")

    async def publish_track(self, track: object, options: object) -> SimpleNamespace:
        self.published += 1
        self.log.append(("publish", track))
        return SimpleNamespace(sid=f"sid-{self.published}")

    async def unpublish_track(self, sid: str) -> None:
        self.log.append(("unpublish", sid))


class RecordingFrameJournal:
    def __init__(self, order: list[str]) -> None:
        self.order = order

    def frame(self, *args: object) -> None:
        self.order.append("journal")


class RecordingAudioSource:
    def __init__(self, order: list[str]) -> None:
        self.order = order

    async def capture_frame(self, frame: object) -> None:
        self.order.append("capture")

    async def wait_for_playout(self) -> None:
        return None


def patch_rtc_audio(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module.rtc, "AudioSource", lambda *args: object())
    monkeypatch.setattr(
        module.rtc.LocalAudioTrack,
        "create_audio_track",
        lambda *args: object(),
    )


@pytest.mark.asyncio
async def test_empty_publish_uses_permission_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_rtc_audio(monkeypatch)
    participant = RecordingParticipant()
    room = SimpleNamespace(local_participant=participant)

    target = str(uuid4())
    _, publication = (await module.publish_private_interpretation_tracks(room, (target,)))[target]

    assert publication.sid == "sid-1"
    assert [row[0] for row in participant.log] == [
        "permission",
        "publish",
        "permission",
    ]


@pytest.mark.asyncio
async def test_final_permission_failure_unpublishes_track(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_rtc_audio(monkeypatch)
    participant = RecordingParticipant(fail_final_permission=True)
    room = SimpleNamespace(local_participant=participant)

    target = str(uuid4())
    with pytest.raises(RuntimeError):
        await module.publish_private_interpretation_tracks(room, (target,))

    assert [row[0] for row in participant.log] == [
        "permission",
        "publish",
        "permission",
        "unpublish",
    ]


@pytest.mark.asyncio
async def test_two_tracks_apply_one_complete_permission_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_rtc_audio(monkeypatch)
    participant = RecordingParticipant()
    targets = (str(uuid4()), str(uuid4()))
    published = await module.publish_private_interpretation_tracks(
        SimpleNamespace(local_participant=participant), targets
    )
    assert set(published) == set(targets)
    assert [row[0] for row in participant.log] == [
        "permission",
        "publish",
        "publish",
        "permission",
    ]
    permissions = participant.log[-1][1]["participant_permissions"]
    assert {
        (item.participant_identity, tuple(item.allowed_track_sids)) for item in permissions
    } == {(targets[0], ("sid-1",)), (targets[1], ("sid-2",))}


@pytest.mark.asyncio
async def test_frame_is_journaled_before_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order: list[str] = []
    monkeypatch.setattr(module.rtc, "AudioFrame", lambda **kwargs: object())
    worker = module.PreservedAudioPublisher(
        uuid4(),
        object(),
        uuid4(),
        uuid4(),
        RecordingAudioSource(order),
        RecordingFrameJournal(order),
    )
    await worker.publish(b"\0" * 1920)
    assert order == ["journal", "capture"]
