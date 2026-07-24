from __future__ import annotations

import asyncio
import hashlib
import io
import struct
import wave
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

from transhooter_spool import SampleRange

from transhooter_worker.domain.models import (
    AudioChunk,
    AudioEvent,
    BoundaryEvent,
    Finality,
    OperationTerminalEvent,
    SessionTerminalEvent,
    SynthesisUtterance,
    TranscriptEvent,
    TranslationRequest,
)
from transhooter_worker.runtime.provider_registry import Providers


@dataclass(frozen=True, slots=True)
class ProbeResult:
    run_id: UUID
    transcript: str
    translation: str
    synthesized_pcm: bytes
    provider_attempt_ids: tuple[UUID, UUID, UUID]
    raw_sha256: str


def read_pcm16_mono_16k(path: Path) -> bytes:
    with wave.open(str(path), "rb") as source:
        rate = source.getframerate()
        if (
            source.getnchannels() != 1
            or source.getsampwidth() != 2
            or rate not in {16_000, 48_000}
            or source.getcomptype() != "NONE"
        ):
            raise ValueError("probe audio must be uncompressed mono LINEAR16 WAV at 16 or 48 kHz")
        pcm = source.readframes(source.getnframes())
    if not pcm:
        raise ValueError("probe audio contains no samples")
    if rate == 16_000:
        return pcm
    samples = [value[0] for value in struct.iter_unpack("<h", pcm)]
    usable = len(samples) - len(samples) % 3
    return b"".join(
        struct.pack("<h", round((samples[index] + samples[index + 1] + samples[index + 2]) / 3))
        for index in range(0, usable, 3)
    )


def _stt_chunks(
    operation_id: UUID,
    pcm: bytes,
) -> Iterator[AudioChunk]:
    sample_cursor = 0
    for sequence, offset in enumerate(range(0, len(pcm), 8_000)):
        pcm_chunk = pcm[offset : offset + 8_000]
        if len(pcm_chunk) % 2:
            raise ValueError("probe PCM is not sample aligned")
        sample_range = SampleRange(sample_cursor, sample_cursor + len(pcm_chunk) // 2)
        yield AudioChunk(operation_id, sequence, sample_range, pcm_chunk)
        sample_cursor = sample_range.end


async def _probe_stt(
    providers: Providers,
    pcm: bytes,
    source_language: str,
) -> tuple[UUID, str, SampleRange]:
    stt_attempt_id = uuid4()
    stt_session = await providers.stt.open(stt_attempt_id, source_language)
    transcripts: list[TranscriptEvent] = []
    boundaries: list[BoundaryEvent] = []

    async def consume_events() -> None:
        async for event in stt_session.events():
            if isinstance(event, TranscriptEvent):
                transcripts.append(event)
            elif isinstance(event, BoundaryEvent):
                boundaries.append(event)
            elif isinstance(event, SessionTerminalEvent):
                return

    stt_event_consumer = asyncio.create_task(consume_events())
    for audio_chunk in _stt_chunks(stt_attempt_id, pcm):
        await stt_session.send_audio(audio_chunk)
    boundary_id = uuid4()
    boundary_receipt = await stt_session.request_boundary(boundary_id)
    if not boundary_receipt.accepted:
        raise RuntimeError("STT provider rejected required final boundary")
    stt_terminal = await stt_session.finish()
    await stt_event_consumer
    if stt_terminal.outcome.value != "succeeded":
        raise RuntimeError("STT probe terminated unsuccessfully")
    final_events = [event for event in transcripts if event.finality is Finality.SPAN_FINAL]
    if not final_events or not boundaries or boundaries[-1].boundary_id != boundary_id:
        raise RuntimeError("STT probe produced no correlated final transcript boundary")
    final_events.sort(key=lambda event: event.samples.start)
    transcript = " ".join(event.text.strip() for event in final_events).strip()
    if not transcript:
        raise RuntimeError("STT probe final transcript is empty")
    source_range = SampleRange(final_events[0].samples.start, final_events[-1].samples.end)
    return stt_attempt_id, transcript, source_range


async def _probe_translation(
    providers: Providers,
    source_language: str,
    target_language: str,
    transcript: str,
    source_range: SampleRange,
) -> tuple[UUID, str]:
    translation_attempt_id = uuid4()
    request = TranslationRequest(
        uuid4(),
        translation_attempt_id,
        "final",
        source_language,
        target_language,
        transcript,
        source_range,
    )
    outcome = await (await providers.translation.start(request)).result()
    if (
        outcome.result is None
        or not outcome.result.text.strip()
        or outcome.terminal.outcome.value != "succeeded"
    ):
        raise RuntimeError("Translation probe produced no successful non-empty result")
    return translation_attempt_id, outcome.result.text


def _validate_playable_pcm(synthesized_pcm: bytes) -> None:
    playable = io.BytesIO()
    with wave.open(playable, "wb") as target:
        target.setnchannels(1)
        target.setsampwidth(2)
        target.setframerate(48_000)
        target.writeframes(synthesized_pcm)
    with wave.open(io.BytesIO(playable.getvalue()), "rb") as verification:
        if verification.getnframes() == 0:
            raise RuntimeError("TTS probe WAV validation failed")


async def _probe_tts(
    providers: Providers,
    target_language: str,
    voice: str,
    translated_text: str,
    source_range: SampleRange,
) -> tuple[UUID, bytes]:
    tts_attempt_id = uuid4()
    tts_session = await providers.tts.open(uuid4(), target_language, voice)
    synthesis = await tts_session.start(
        SynthesisUtterance(
            uuid4(),
            tts_attempt_id,
            translated_text,
            target_language,
            voice,
            source_range,
        )
    )
    chunks: list[bytes] = []
    synthesis_terminal = None
    async for event in synthesis.events():
        if isinstance(event, AudioEvent):
            if event.sample_rate != 48_000 or event.channels != 1 or len(event.pcm) % 2:
                raise RuntimeError("TTS probe returned an unapproved audio format")
            chunks.append(event.pcm)
        elif isinstance(event, OperationTerminalEvent):
            synthesis_terminal = event.terminal
    await tts_session.finish()
    synthesized_pcm = b"".join(chunks)
    if (
        synthesis_terminal is None
        or synthesis_terminal.outcome.value != "succeeded"
        or not synthesized_pcm
    ):
        raise RuntimeError("TTS probe produced no successful playable audio")
    _validate_playable_pcm(synthesized_pcm)
    return tts_attempt_id, synthesized_pcm


async def execute_probe(
    providers: Providers,
    audio: Path,
    source_language: str,
    target_language: str,
    voice: str,
    run_id: UUID | None = None,
) -> ProbeResult:
    probe_run_id = run_id or uuid4()
    pcm = read_pcm16_mono_16k(audio)
    stt_attempt_id, transcript, source_range = await _probe_stt(
        providers,
        pcm,
        source_language,
    )
    translation_attempt_id, translated_text = await _probe_translation(
        providers,
        source_language,
        target_language,
        transcript,
        source_range,
    )
    tts_attempt_id, synthesized_pcm = await _probe_tts(
        providers,
        target_language,
        voice,
        translated_text,
        source_range,
    )
    return ProbeResult(
        probe_run_id,
        transcript,
        translated_text,
        synthesized_pcm,
        (stt_attempt_id, translation_attempt_id, tts_attempt_id),
        hashlib.sha256(synthesized_pcm).hexdigest(),
    )
