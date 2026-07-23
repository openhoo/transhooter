from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from uuid import UUID

from transhooter_worker.domain.models import RawRef, SampleRange
from transhooter_worker.ports.archive import ArchiveStore, ObjectRecord, SpoolRecords


@dataclass(frozen=True, slots=True)
class CompactedPcm:
    pcm: ObjectRecord
    sidecar: ObjectRecord
    samples: SampleRange
    source_refs: tuple[RawRef, ...]
    stage: str
    direction: str


class PcmCompactor:
    def __init__(
        self,
        spool: SpoolRecords,
        archive: ArchiveStore,
        meeting_id: UUID,
        sample_rate: int = 48_000,
    ) -> None:
        self._spool = spool
        self._archive = archive
        self._meeting_id = meeting_id
        self._rate = sample_rate

    def compact(
        self,
        stage: str,
        direction: str,
        drain: bool = False,
        include_uploaded: bool = False,
    ) -> list[CompactedPcm]:
        records = [
            (ref, span)
            for ref, span in self._spool.committed_scoped(
                self._meeting_id,
                stage,
                direction,
                include_uploaded,
            )
            if span is not None
        ]
        records.sort(key=lambda item: item[1].start if item[1] else -1)
        output: list[CompactedPcm] = []
        batch: list[tuple[RawRef, SampleRange]] = []
        samples = 0
        expected: int | None = None
        for ref, span in records:
            assert span is not None
            if expected is not None and span.start != expected:
                if batch and (drain or samples >= self._rate * 10):
                    output.append(self._flush(stage, direction, batch))
                batch, samples = [], 0
            batch.append((ref, span))
            samples += span.length
            expected = span.end
            if samples >= self._rate * 10:
                output.append(self._flush(stage, direction, batch))
                batch, samples, expected = [], 0, None
        if drain and batch:
            output.append(self._flush(stage, direction, batch))
        return output

    def _flush(
        self, stage: str, direction: str, batch: list[tuple[RawRef, SampleRange]]
    ) -> CompactedPcm:
        start, end = batch[0][1].start, batch[-1][1].end
        pcm = b"".join(self._spool.read(ref.object_id) for ref, _ in batch)
        prefix = f"v1/meetings/{self._meeting_id}/audio/{stage}/{direction}/{start:020d}-{end:020d}"
        digest = hashlib.sha256(pcm).hexdigest()
        pcm_record = self._archive.put_create_once(prefix + ".pcm", pcm, "audio/L16", digest)
        sidecar_body = json.dumps(
            {
                "encoding": "LINEAR16",
                "rate": self._rate,
                "channels": 1,
                "format": "raw",
                "sampleStart": start,
                "sampleEnd": end,
                "sha256": digest,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        sidecar = self._archive.put_create_once(
            prefix + ".json",
            sidecar_body,
            "application/json",
            hashlib.sha256(sidecar_body).hexdigest(),
        )
        return CompactedPcm(
            pcm_record,
            sidecar,
            SampleRange(start, end),
            tuple(ref for ref, _ in batch),
            stage,
            direction,
        )

    def acknowledge_covering_checkpoint(self, compacted: CompactedPcm, checkpoint_id: UUID) -> None:
        if not self._spool.checkpoint_covers(
            checkpoint_id, compacted.stage, compacted.direction, compacted.samples.end
        ):
            raise ValueError("checkpoint is absent or does not durably cover compacted samples")
        for ref in compacted.source_refs:
            self._spool.mark_uploaded(
                ref.object_id, compacted.pcm.version_id, compacted.pcm.s3_checksum
            )
