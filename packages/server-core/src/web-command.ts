import type {
  ArchiveObjectRecord,
  FinalInventory,
  ProviderAttemptReport,
  WorkerCheckpoint,
} from "@transhooter/contracts";
import type { ApplicationOperations } from "./application-operations";
import type { ArchiveService } from "./archives/service";
import type { AuthService } from "./auth/service";
import type { ConsultationService } from "./consultations/service";
import type { UUID } from "./domain/model";
import type { CapabilityRefresh, LanguageService } from "./languages/service";
import type { InternalPrincipalVerifier, SessionRecord, UserRecord } from "./ports/index";

export type AuthCommand =
  | {
      kind: "auth.requestMagicLink";
      email: string;
    }
  | {
      kind: "auth.beginExchange";
      token: string;
    }
  | {
      kind: "auth.verifyExchange";
      nonce: string;
    }
  | {
      kind: "auth.authenticate";
    }
  | {
      kind: "auth.logout";
    }
  | {
      kind: "auth.reauthenticate";
      consultationId: UUID;
    }
  | {
      kind: "auth.provisionCustomer";
      email: string;
      displayName: string;
    };

export type ConsultationCommand =
  | {
      kind: "consultation.create";
      customerUserId: UUID;
      providerProfileId: string;
      creationIdempotencyKey: UUID;
    }
  | {
      kind: "consultation.createInvitation";
      customerEmail: string;
      customerName: string;
      providerProfileId: string;
      creationIdempotencyKey: UUID;
    }
  | {
      kind: "consultation.get";
      consultationId: UUID;
    }
  | {
      kind: "consultation.lobby";
      consultationId: UUID;
    }
  | {
      kind: "consultation.list";
    }
  | {
      kind: "consultation.preferences";
      consultationId: UUID;
      displayName: string;
      language: string;
    }
  | {
      kind: "consultation.consent";
      consultationId: UUID;
      snapshotHash: string;
    }
  | {
      kind: "consultation.join";
      consultationId: UUID;
    }
  | {
      kind: "consultation.token";
      consultationId: UUID;
    }
  | {
      kind: "consultation.resend";
      consultationId: UUID;
    }
  | {
      kind: "consultation.options";
      providerProfileId: string;
    }
  | {
      kind: "consultation.profileMetadata";
    }
  | {
      kind: "consultation.room";
      consultationId: UUID;
    }
  | {
      kind: "consultation.end";
      consultationId: UUID;
    }
  | {
      kind: "consultation.cancel";
      consultationId: UUID;
    };

export type ArchiveCommand =
  | {
      kind: "archive.list";
    }
  | {
      kind: "archive.get";
      archiveId: UUID;
    }
  | {
      kind: "archive.objects";
      archiveId: UUID;
      cursor: string | null;
      limit: number;
    }
  | {
      kind: "archive.download";
      archiveId: UUID;
      objectId: UUID;
    }
  | {
      kind: "archive.hold";
      consultationId: UUID;
      reason: string;
    }
  | {
      kind: "archive.releaseHold";
      consultationId: UUID;
      holdId: UUID;
    }
  | {
      kind: "archive.delete";
      consultationId: UUID;
      reason: string;
    };

export type AdminCommand =
  | {
      kind: "admin.failures";
    }
  | {
      kind: "admin.languages";
      providerProfileId: string;
    }
  | {
      kind: "language.enable";
      capabilityId: UUID;
      profileId: UUID;
      profileRevision: number;
      enabled: boolean;
    };

export type InternalCommand =
  | {
      kind: "internal.capability";
      refresh: CapabilityRefresh;
    }
  | {
      kind: "internal.heartbeat";
      consultationId: UUID;
      generation: number;
      workerId: UUID;
      epoch: number;
    }
  | {
      kind: "internal.checkpoint";
      workerId: UUID;
      consultationId: UUID;
      generation: number;
      writeEpoch: number;
      objectKey: string;
      checkpoint: WorkerCheckpoint;
    }
  | {
      kind: "internal.providerAttempt";
      consultationId: UUID;
      generation: number;
      workerId: UUID;
      epoch: number;
      eventId: UUID;
      report: ProviderAttemptReport;
    }
  | {
      kind: "internal.archiveObject";
      consultationId: UUID;
      generation: number;
      workerId: UUID;
      workerEpoch: number;
      writerEpoch: number;
      causalKey: string;
      object: ArchiveObjectRecord;
    }
  | {
      kind: "internal.expiredWorkerEpochs";
    }
  | {
      kind: "internal.completeWorkerEpoch";
      consultationId: UUID;
      generation: number;
      workerId: UUID;
      epoch: number;
      writeEpoch: number;
      completionEventId: UUID;
      outcome: "clean" | "failed";
      terminalCheckpoints: readonly [
        { checkpointId: UUID; checkpointHash: string },
        { checkpointId: UUID; checkpointHash: string },
      ];
      failure: {
        kind: string;
        message: string;
        phase?: string | undefined;
        snapshotHash?: string | undefined;
        lastCheckpointHashes: Readonly<Record<UUID, string>>;
      } | null;
    }
  | {
      kind: "internal.abandonWorkerEpoch";
      consultationId: UUID;
      generation: number;
      workerId: UUID;
      epoch: number;
      writeEpoch: number;
      abandonmentEventId: UUID;
      sealId?: UUID | undefined;
      completionEventId?: UUID | undefined;
      reason: string;
      handoffDigest: string;
      permanentOutcomeDigest: string;
    }
  | {
      kind: "internal.finalize";
      consultationId: UUID;
      inventory: FinalInventory;
    }
  | {
      kind: "internal.egressLayout";
      consultationId: UUID;
      generation: number;
    }
  | {
      kind: "internal.archiveRecording";
      consultationId: UUID;
    }
  | {
      kind: "internal.deleteDrain";
      consultationId: UUID;
      reason: string;
      writeEpoch: number;
    };

export type WebCommand =
  | AuthCommand
  | ConsultationCommand
  | ArchiveCommand
  | AdminCommand
  | InternalCommand;

export interface WebExecutionContext {
  sessionToken?: string;
  csrfToken?: string;
  request?: Request;
  internalHeaders?: Readonly<Record<string, string | undefined>>;
}

export interface WebApplicationConfig {
  auth: AuthService;
  consultations: ConsultationService;
  archives: ArchiveService;
  languages: LanguageService;
  operations: ApplicationOperations;
  internalPrincipalVerifier: InternalPrincipalVerifier;
  publicBaseUrl: string;
  clientIp: (request: Request) => string;
  ready: () => Promise<boolean>;
}

export interface WebApplication {
  ready(): Promise<boolean>;
  execute(command: WebCommand, context: WebExecutionContext): Promise<unknown>;
}

export interface Authenticated {
  session: SessionRecord;
  user: UserRecord;
}

export type UnauthenticatedAuthCommand = Extract<
  AuthCommand,
  {
    kind: "auth.requestMagicLink" | "auth.beginExchange" | "auth.verifyExchange";
  }
>;

export type AuthenticatedCommand = Exclude<
  WebCommand,
  | InternalCommand
  | { kind: "auth.requestMagicLink" }
  | { kind: "auth.beginExchange" }
  | { kind: "auth.verifyExchange" }
>;

export type AuthenticatedAuthCommand = Extract<AuthenticatedCommand, { kind: `auth.${string}` }>;
