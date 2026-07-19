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
  current: CaptionPacket | undefined,
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
