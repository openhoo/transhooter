import type { CaptionPacket, StatusPacket } from "@transhooter/contracts";

export type AudioMode = "interpreted" | "overlay" | "original";

type AudioGains = {
  original: number;
  interpretation: number;
};

type ExpectedCaptionRoute = {
  consultationId: string;
  destinationParticipantId: string;
  sourceParticipantId: string;
};

type ExpectedStatusRoute = {
  consultationId: string;
  generation: number;
};

type CaptionRevision = Pick<CaptionPacket, "finality" | "revision" | "sourceSampleStart">;

export const CAPTION_REVISION_RECENT_LIMIT = 128;
export const CAPTION_REVISION_TOMBSTONE_LIMIT = 128;

export class CaptionRevisionPolicy {
  readonly #recent = new Map<string, CaptionRevision>();
  readonly #tombstones = new Map<string, CaptionRevision>();
  #evictionWatermarkSample = Number.NEGATIVE_INFINITY;

  accepts(candidate: CaptionPacket): boolean {
    const current =
      this.#recent.get(candidate.utteranceId) ?? this.#tombstones.get(candidate.utteranceId);

    if (current) {
      if (candidate.revision <= current.revision) {
        return false;
      }
      if (current.finality === "final" && candidate.finality !== "final") {
        return false;
      }
    } else if (candidate.sourceSampleStart <= this.#evictionWatermarkSample) {
      return false;
    }

    this.#tombstones.delete(candidate.utteranceId);
    this.#recent.delete(candidate.utteranceId);
    this.#recent.set(candidate.utteranceId, {
      finality: candidate.finality,
      sourceSampleStart: candidate.sourceSampleStart,
      revision: candidate.revision,
    });
    this.#trim();
    return true;
  }

  get trackedUtteranceCount(): number {
    return this.#recent.size + this.#tombstones.size;
  }

  #trim() {
    while (this.#recent.size > CAPTION_REVISION_RECENT_LIMIT) {
      const oldest = this.#recent.entries().next().value;
      if (!oldest) {
        break;
      }
      const [utteranceId, revision] = oldest;
      this.#recent.delete(utteranceId);
      this.#tombstones.set(utteranceId, revision);
    }

    while (this.#tombstones.size > CAPTION_REVISION_TOMBSTONE_LIMIT) {
      const oldest = this.#tombstones.entries().next().value;
      if (!oldest) {
        break;
      }
      const [utteranceId, revision] = oldest;
      this.#tombstones.delete(utteranceId);
      this.#evictionWatermarkSample = Math.max(
        this.#evictionWatermarkSample,
        revision.sourceSampleStart,
      );
    }
  }
}

type ReleasableLocalTrack = {
  detach(): unknown;
  stop(): unknown;
};

export function audioGains(
  mode: AudioMode,
  interpretationAvailable: boolean,
  sameLanguage: boolean,
): AudioGains {
  if (sameLanguage || !interpretationAvailable) {
    return { original: 1, interpretation: 0 };
  }

  switch (mode) {
    case "interpreted":
      return { original: 0, interpretation: 1 };
    case "overlay":
      return { original: 0.18, interpretation: 1 };
    case "original":
      return { original: 1, interpretation: 0 };
  }
}

export function acceptsCaption(
  candidate: CaptionPacket,
  current: CaptionRevision | undefined,
  expected: ExpectedCaptionRoute,
  senderIsAgent: boolean,
): boolean {
  const consultationMatches = candidate.consultationId === expected.consultationId;
  const destinationMatches =
    candidate.destinationParticipantId === expected.destinationParticipantId;
  const sourceMatches = candidate.sourceParticipantId === expected.sourceParticipantId;

  if (!senderIsAgent || !consultationMatches || !destinationMatches || !sourceMatches) {
    return false;
  }

  if (!current) {
    return true;
  }

  if (candidate.revision <= current.revision) {
    return false;
  }

  if (current.finality === "final" && candidate.finality !== "final") {
    return false;
  }

  return true;
}

export function acceptsStatus(
  candidate: StatusPacket,
  participantWasPresent: boolean,
  expected: ExpectedStatusRoute,
): boolean {
  if (participantWasPresent) {
    return false;
  }

  const consultationMatches = candidate.consultationId === expected.consultationId;
  const generationMatches = candidate.generation === expected.generation;
  return consultationMatches && generationMatches;
}

export async function releaseLocalTrack(
  track: ReleasableLocalTrack,
  unpublish: () => Promise<unknown>,
): Promise<void> {
  try {
    await unpublish();
  } finally {
    track.detach();
    track.stop();
  }
}
