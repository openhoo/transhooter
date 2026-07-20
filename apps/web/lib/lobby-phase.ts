export type LobbyPhase =
  | "preferences"
  | "waiting"
  | "consent"
  | "consent-waiting"
  | "ready"
  | "terminal";

type LobbyContentKind =
  | "preferences"
  | "waiting"
  | "provider-consent"
  | "consent-waiting"
  | "terminal";

export type LobbyPhaseDescriptor = {
  stage: 1 | 2 | null;
  polls: boolean;
  announcement: string;
  contentKind: LobbyContentKind;
};

const LOBBY_PHASE_DESCRIPTORS = {
  preferences: {
    stage: 1,
    polls: false,
    announcement: "Step 1: Choose devices and language.",
    contentKind: "preferences",
  },
  waiting: {
    stage: 1,
    polls: true,
    announcement: "Step 1: Preferences saved. Waiting for the other participant’s preferences.",
    contentKind: "waiting",
  },
  consent: {
    stage: 2,
    polls: true,
    announcement: "Step 2: Provider details ready. Review and consent.",
    contentKind: "provider-consent",
  },
  "consent-waiting": {
    stage: 2,
    polls: true,
    announcement: "Step 2: Your consent is recorded. Waiting for the other participant’s consent.",
    contentKind: "consent-waiting",
  },
  ready: {
    stage: 2,
    polls: true,
    announcement: "Consent recorded. Preparing the consultation.",
    contentKind: "provider-consent",
  },
  terminal: {
    stage: null,
    polls: false,
    announcement: "This consultation is closed.",
    contentKind: "terminal",
  },
} as const satisfies Record<LobbyPhase, LobbyPhaseDescriptor>;

export function describeLobbyPhase(phase: LobbyPhase): LobbyPhaseDescriptor {
  return LOBBY_PHASE_DESCRIPTORS[phase];
}

type PreviewTrack = {
  stop: () => void;
};

export type PreviewStream = {
  getTracks: () => PreviewTrack[];
};

export type LobbyPreviewRequest = {
  readonly generation: number;
  readonly signal: AbortSignal;
};

function stopStream(stream: PreviewStream): void {
  for (const track of stream.getTracks()) {
    try {
      track.stop();
    } catch {
      // A broken track must not prevent the remaining devices from being released.
    }
  }
}

export class LobbyPreviewFence<TStream extends PreviewStream = PreviewStream> {
  private controller: AbortController | null = null;
  private generation = 0;
  private ownedStream: TStream | null = null;

  begin(): LobbyPreviewRequest {
    this.cancel();
    const controller = new AbortController();
    this.controller = controller;
    return { generation: this.generation, signal: controller.signal };
  }

  adopt(request: LobbyPreviewRequest, stream: TStream): boolean {
    if (!this.isCurrent(request)) {
      stopStream(stream);
      return false;
    }

    this.releaseOwnedStream();
    this.ownedStream = stream;
    return true;
  }

  owns(request: LobbyPreviewRequest, stream: TStream): boolean {
    return this.isCurrent(request) && this.ownedStream === stream;
  }

  settle(request: LobbyPreviewRequest): boolean {
    if (!this.isCurrent(request)) {
      return false;
    }
    this.cancel();
    return true;
  }

  cancel(): void {
    this.generation += 1;
    this.controller?.abort();
    this.controller = null;
    this.releaseOwnedStream();
  }

  private isCurrent(request: LobbyPreviewRequest): boolean {
    return (
      !request.signal.aborted &&
      request.generation === this.generation &&
      this.controller?.signal === request.signal
    );
  }

  private releaseOwnedStream(): void {
    const ownedStream = this.ownedStream;
    this.ownedStream = null;
    if (ownedStream) {
      stopStream(ownedStream);
    }
  }
}
