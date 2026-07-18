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
