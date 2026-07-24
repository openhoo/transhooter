from dataclasses import dataclass

from transhooter_spool import SampleRange


@dataclass(slots=True)
class SampleClock:
    sample_rate: int
    cursor: int = 0

    def advance_pcm16(self, byte_count: int, channels: int = 1) -> SampleRange:
        frame_bytes = 2 * channels
        if byte_count <= 0 or byte_count % frame_bytes:
            raise ValueError("PCM16 length must contain complete non-empty frames")
        start = self.cursor
        self.cursor += byte_count // frame_bytes
        return SampleRange(start, self.cursor)

    def milliseconds(self, sample: int) -> int:
        if sample < 0:
            raise ValueError("sample must be non-negative")
        return sample * 1000 // self.sample_rate
