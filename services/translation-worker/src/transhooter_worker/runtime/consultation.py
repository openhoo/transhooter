from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import ClassVar
from uuid import UUID

from livekit import agents, rtc

from transhooter_worker.adapters.spool import EncryptedSpool
from transhooter_worker.adapters.terminal import terminal_bytes
from transhooter_worker.application.pipeline import CaptionRevision
from transhooter_worker.application.session import DirectionSession
from transhooter_worker.domain.models import AudioChunk, AudioEvent, SampleRange
from transhooter_worker.runtime.job_metadata import CaptionPacket, DirectionMetadata, JobMetadata
from transhooter_worker.runtime.publisher import PreservedAudioPublisher


@dataclass(slots=True)
class SourceTrackTimeline:
    FRAME_SAMPLES: ClassVar[int] = 4_000
    cursor: int = 0
    sequence: int = 0
    generation: int = 0

    def replace(self) -> int:
        self.generation += 1
        return self.generation

    def claim(self, generation: int, samples: int) -> tuple[int, SampleRange] | None:
        if generation != self.generation:
            return None
        if samples != self.FRAME_SAMPLES:
            raise RuntimeError("source audio frame is not 250 ms at 16 kHz")
        sequence = self.sequence
        span = SampleRange(self.cursor, self.cursor + samples)
        self.cursor = span.end
        self.sequence += 1
        return sequence, span


@dataclass(frozen=True, slots=True)
class DirectionSinks:
    caption: Callable[[CaptionRevision], Awaitable[None]]
    audio: Callable[[bytes], Awaitable[None]]
    checkpoint: Callable[[int, int, bool], Awaitable[None]]
    normalized: Callable[[object], Awaitable[None]]


def _build_direction_sinks(
    ctx: agents.JobContext,
    metadata: JobMetadata,
    direction: DirectionMetadata,
    livekit_by_participant: dict[UUID, UUID],
    spool: EncryptedSpool,
    publisher: PreservedAudioPublisher | None,
    persist_checkpoint: Callable[[UUID, UUID, int, int, bool, int, int], Awaitable[None]],
    initial_output_sample: int = 0,
) -> DirectionSinks:
    checkpoint_lock = asyncio.Lock()

    async def caption_sink(revision: CaptionRevision) -> None:
        packet = CaptionPacket(
            schemaVersion=1,
            consultationId=metadata.consultation_id,
            destinationParticipantId=direction.destination_participant_id,
            sourceParticipantId=direction.source_participant_id,
            utteranceId=revision.utterance_id,
            revision=revision.revision,
            finality="final" if revision.final else "provisional",
            sourceLanguage=direction.source_language,
            targetLanguage=direction.target_language,
            sourceText=revision.source_text,
            translatedText=revision.translated_text,
            sourceSampleStart=revision.samples.start,
            sourceSampleEnd=revision.samples.end,
            occurredAtMs=int(time.time() * 1000),
        )
        payload = packet.model_dump_json().encode()
        spool.append(
            meeting_id=metadata.consultation_id,
            attempt_id=revision.utterance_id,
            stage="caption",
            transport="websocket",
            direction="publish",
            media_type="application/json",
            payload=payload,
            sample_range=revision.samples,
        )
        await ctx.room.local_participant.publish_data(
            payload,
            reliable=True,
            destination_identities=[
                str(livekit_by_participant[direction.destination_participant_id])
            ],
            topic="consultation.translation.v1",
        )

    async def audio_sink(pcm: bytes) -> None:
        if publisher is None:
            raise RuntimeError("same-language direction attempted interpretation publication")
        await publisher.publish(pcm)

    tts_raw_cursor = initial_output_sample

    async def normalized_sink(event: object) -> None:
        nonlocal tts_raw_cursor
        samples = getattr(event, "samples", None)
        sample_range = samples if isinstance(samples, SampleRange) else None
        if isinstance(event, AudioEvent):
            raw_range = SampleRange(tts_raw_cursor, tts_raw_cursor + len(event.pcm) // 2)
            spool.append(
                meeting_id=metadata.consultation_id,
                attempt_id=event.operation_id,
                stage="tts-output",
                transport="grpc"
                if direction.tts and direction.tts.provider == "google"
                else "websocket",
                direction=str(direction.destination_participant_id),
                media_type="audio/L16",
                payload=event.pcm,
                sample_range=raw_range,
                metadata=(
                    ("providerSampleStart", str(event.samples.start)),
                    ("providerSampleEnd", str(event.samples.end)),
                ),
            )
            tts_raw_cursor = raw_range.end
        spool.append(
            meeting_id=metadata.consultation_id,
            attempt_id=direction.source_participant_id,
            stage="normalized",
            transport="internal",
            direction=str(direction.source_participant_id),
            media_type="application/json",
            payload=terminal_bytes(event),
            sample_range=sample_range,
        )

    async def checkpoint_sink(input_sample: int, output_sample: int, terminal: bool) -> None:
        async with checkpoint_lock:
            await persist_checkpoint(
                direction.source_participant_id,
                direction.destination_participant_id,
                input_sample,
                output_sample,
                terminal,
                input_sample // SourceTrackTimeline.FRAME_SAMPLES,
                tts_raw_cursor,
            )

    return DirectionSinks(caption_sink, audio_sink, checkpoint_sink, normalized_sink)


def _install_room_handlers(
    ctx: agents.JobContext,
    metadata: JobMetadata,
    spool: EncryptedSpool,
    sessions_by_identity: dict[UUID, DirectionSession],
    timelines: dict[UUID, SourceTrackTimeline],
    sessions_ready: asyncio.Event,
    stream_tasks: set[asyncio.Task[None]],
    stream_failure: asyncio.Future[BaseException],
    disconnected: asyncio.Event,
    drain_requested: asyncio.Event,
) -> Callable[[rtc.RemoteTrackPublication, rtc.RemoteParticipant], None]:
    active_stream_tasks: dict[UUID, asyncio.Task[None]] = {}

    async def consume_track(
        track: rtc.Track, participant: rtc.RemoteParticipant, generation: int
    ) -> None:
        await sessions_ready.wait()
        try:
            source_identity = UUID(participant.identity)
        except ValueError:
            return
        session = sessions_by_identity.get(source_identity)
        if session is None or track.kind != rtc.TrackKind.KIND_AUDIO:
            return
        stream = rtc.AudioStream(track, sample_rate=16000, num_channels=1, frame_size_ms=250)
        timeline = timelines[source_identity]
        try:
            async for event in stream:
                claimed_frame = timeline.claim(generation, event.frame.samples_per_channel)
                if claimed_frame is None:
                    return
                sequence, sample_range = claimed_frame
                pcm = bytes(event.frame.data)
                spool.append(
                    meeting_id=metadata.consultation_id,
                    attempt_id=source_identity,
                    stage="stt-input",
                    transport="grpc",
                    direction=str(source_identity),
                    media_type="audio/L16",
                    payload=pcm,
                    sample_range=sample_range,
                    metadata=(("sequence", str(sequence)),),
                )
                await session.send_audio(
                    AudioChunk(source_identity, sequence, sample_range, pcm, 16000, 1, "LINEAR16")
                )
            if timeline.generation == generation:
                await session.boundary()
        finally:
            await stream.aclose()

    def track_done(source_identity: UUID, task: asyncio.Task[None]) -> None:
        stream_tasks.discard(task)
        if active_stream_tasks.get(source_identity) is task:
            del active_stream_tasks[source_identity]
        if (
            not task.cancelled()
            and (error := task.exception()) is not None
            and not stream_failure.done()
        ):
            stream_failure.set_result(error)

    def subscribe_if_allowed(
        publication: rtc.RemoteTrackPublication,
        participant: rtc.RemoteParticipant,
    ) -> None:
        try:
            identity = UUID(participant.identity)
        except ValueError:
            return
        if identity in timelines and publication.source == rtc.TrackSource.SOURCE_MICROPHONE:
            publication.set_subscribed(True)

    @ctx.room.on("track_published")
    def on_track_published(
        publication: rtc.RemoteTrackPublication,
        participant: rtc.RemoteParticipant,
    ) -> None:
        subscribe_if_allowed(publication, participant)

    @ctx.room.on("track_subscribed")
    def on_track_subscribed(
        track: rtc.Track,
        publication: rtc.RemoteTrackPublication,
        participant: rtc.RemoteParticipant,
    ) -> None:
        if publication.source != rtc.TrackSource.SOURCE_MICROPHONE:
            return
        try:
            source_identity = UUID(participant.identity)
        except ValueError:
            return
        if source_identity not in timelines:
            return
        generation = timelines[source_identity].replace()
        previous_task = active_stream_tasks.get(source_identity)
        if previous_task is not None:
            previous_task.cancel()
        stream_task = asyncio.create_task(consume_track(track, participant, generation))
        active_stream_tasks[source_identity] = stream_task
        stream_tasks.add(stream_task)
        stream_task.add_done_callback(lambda task: track_done(source_identity, task))

    @ctx.room.on("data_received")
    def on_data_received(packet: rtc.DataPacket) -> None:
        if packet.participant is not None or packet.topic != "consultation.status.v1":
            return
        try:
            message = json.loads(packet.data)
        except (ValueError, TypeError):
            return
        if (
            message.get("consultationId") == str(metadata.consultation_id)
            and message.get("generation") == metadata.generation
            and message.get("reasonCode") == "SHUTDOWN"
        ):
            drain_requested.set()

    @ctx.room.on("disconnected")
    def on_disconnected(*_: object) -> None:
        disconnected.set()

    return subscribe_if_allowed
