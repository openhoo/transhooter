from __future__ import annotations

import asyncio
from uuid import UUID

from livekit import rtc

from transhooter_worker.domain.models import SampleRange
from transhooter_worker.ports.exchange_journal import PublicationJournal


class PreservedAudioPublisher:
    """Normalizes 48 kHz mono PCM into preserved 20 ms frames before capture."""

    FRAME_SAMPLES = 960
    FRAME_BYTES = 1920

    def __init__(
        self,
        meeting_id: UUID,
        publication_id: UUID,
        destination_id: UUID,
        source: rtc.AudioSource,
        journal: PublicationJournal,
    ) -> None:
        self._meeting = meeting_id
        self._publication = publication_id
        self._destination = destination_id
        self._source = source
        self._journal = journal
        self._sequence = 0
        self._sample = 0
        self._lock = asyncio.Lock()
        self._first_accepted = asyncio.Event()

    async def publish(self, pcm: bytes) -> None:
        if len(pcm) % self.FRAME_BYTES:
            raise ValueError("published PCM must be complete 20 ms 48 kHz mono frames")
        async with self._lock:
            for offset in range(0, len(pcm), self.FRAME_BYTES):
                body = pcm[offset : offset + self.FRAME_BYTES]
                span = SampleRange(self._sample, self._sample + self.FRAME_SAMPLES)
                self._journal.frame(
                    self._meeting, self._publication, self._destination, self._sequence, body, span
                )
                frame = rtc.AudioFrame(
                    data=body,
                    sample_rate=48000,
                    num_channels=1,
                    samples_per_channel=self.FRAME_SAMPLES,
                )
                await self._source.capture_frame(frame)
                self._first_accepted.set()
                self._sequence += 1
                self._sample = span.end

    async def wait_first_accepted(self) -> None:
        await self._first_accepted.wait()

    async def drain(self) -> None:
        await self._source.wait_for_playout()


async def publish_private_interpretation_tracks(
    room: rtc.Room, target_identities: tuple[str, ...]
) -> dict[str, tuple[rtc.AudioSource, rtc.LocalTrackPublication]]:
    """Publish empty tracks under deny-all, then apply the complete identity/SID table once."""
    participant = room.local_participant
    participant.set_track_subscription_permissions(
        allow_all_participants=False, participant_permissions=[]
    )
    published: dict[str, tuple[rtc.AudioSource, rtc.LocalTrackPublication]] = {}
    try:
        for target_identity in target_identities:
            source = rtc.AudioSource(48000, 1)
            track = rtc.LocalAudioTrack.create_audio_track(
                f"interpretation:{target_identity}", source
            )
            publication = await participant.publish_track(
                track, rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_MICROPHONE)
            )
            published[target_identity] = (source, publication)
        permissions = [
            rtc.ParticipantTrackPermission(
                participant_identity=identity, allow_all=False, allowed_track_sids=[publication.sid]
            )
            for identity, (_, publication) in published.items()
        ]
        participant.set_track_subscription_permissions(
            allow_all_participants=False, participant_permissions=permissions
        )
        return published
    except BaseException:
        await asyncio.gather(
            *(
                participant.unpublish_track(publication.sid)
                for _, publication in published.values()
            ),
            return_exceptions=True,
        )
        raise
