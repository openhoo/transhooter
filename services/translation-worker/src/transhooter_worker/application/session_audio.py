from __future__ import annotations

from transhooter_worker.domain.models import AudioChunk, SampleRange


def _audio_after_watermark(chunk: AudioChunk, watermark: int) -> AudioChunk | None:
    if chunk.samples.end <= watermark:
        return None
    if chunk.samples.start >= watermark:
        return chunk
    if len(chunk.pcm) % chunk.samples.length:
        raise ValueError("audio chunk PCM length does not match its sample range")
    bytes_per_sample = len(chunk.pcm) // chunk.samples.length
    if bytes_per_sample <= 0:
        raise ValueError("audio chunk has no PCM sample data")
    offset = (watermark - chunk.samples.start) * bytes_per_sample
    return AudioChunk(
        operation_id=chunk.operation_id,
        sequence=chunk.sequence,
        samples=SampleRange(watermark, chunk.samples.end),
        pcm=chunk.pcm[offset:],
        sample_rate=chunk.sample_rate,
        channels=chunk.channels,
        encoding=chunk.encoding,
    )
