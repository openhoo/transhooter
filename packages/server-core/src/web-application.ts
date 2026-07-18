import type {
  ArchiveObjectRecord,
  FinalInventory,
  ProviderAttemptReport,
  WorkerCheckpoint,
} from "@transhooter/contracts";
import type { ApplicationOperations, StaffPrincipal } from "./application-operations";
import type { ArchiveService } from "./archives/service";
import type { AuthService } from "./auth/service";
import type { ConsultationService } from "./consultations/service";
import { DomainError, type UUID } from "./domain/model";
import type { CapabilityRefresh, LanguageService } from "./languages/service";
import type {
  InternalPrincipal,
  InternalPrincipalVerifier,
  SessionRecord,
  UserRecord,
} from "./ports/index";

type AuthCommand =
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

type ConsultationCommand =
  | {
      kind: "consultation.create";
      customerUserId: UUID;
      providerProfileId: string;
    }
  | {
      kind: "consultation.createInvitation";
      customerEmail: string;
      customerName: string;
      providerProfileId: string;
    }
  | {
      kind: "consultation.get";
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

type ArchiveCommand =
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
    };

type AdminCommand =
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
      enabled: boolean;
    };

type InternalCommand =
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
      writerEpoch: number;
      causalKey: string;
      object: ArchiveObjectRecord;
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

interface Authenticated {
  session: SessionRecord;
  user: UserRecord;
}
type UnauthenticatedAuthCommand = Extract<
  AuthCommand,
  {
    kind: "auth.requestMagicLink" | "auth.beginExchange" | "auth.verifyExchange";
  }
>;
type AuthenticatedCommand = Exclude<
  WebCommand,
  | InternalCommand
  | { kind: "auth.requestMagicLink" }
  | { kind: "auth.beginExchange" }
  | { kind: "auth.verifyExchange" }
>;
type AuthenticatedAuthCommand = Extract<AuthenticatedCommand, { kind: `auth.${string}` }>;

const readCommandKinds: readonly WebCommand["kind"][] = [
  "auth.authenticate",
  "consultation.get",
  "consultation.list",
  "consultation.options",
  "consultation.room",
  "archive.list",
  "archive.get",
  "archive.objects",
  "archive.download",
  "admin.failures",
  "admin.languages",
];

export function createWebApplication(config: WebApplicationConfig): WebApplication {
  return {
    ready: config.ready,
    async execute(command, context) {
      if (isInternalCommand(command)) {
        return executeVerifiedInternal(config, command, context);
      }

      if (isUnauthenticatedAuthCommand(command)) {
        return executeUnauthenticatedAuth(config, command, context);
      }

      const authenticated = await authenticateForCommand(config, command, context);
      return executeAuthenticated(config, command, context, authenticated);
    },
  };
}

async function executeVerifiedInternal(
  config: WebApplicationConfig,
  command: InternalCommand,
  context: WebExecutionContext,
): Promise<unknown> {
  if (!context.internalHeaders) {
    throw new DomainError("UNAUTHENTICATED_INTERNAL");
  }
  const principal = await config.internalPrincipalVerifier.verify(context.internalHeaders);
  return executeInternal(config, command, principal);
}

async function executeUnauthenticatedAuth(
  config: WebApplicationConfig,
  command: UnauthenticatedAuthCommand,
  context: WebExecutionContext,
): Promise<unknown> {
  switch (command.kind) {
    case "auth.requestMagicLink":
      return config.auth.requestMagicLink({
        email: command.email,
        ip: requestIp(config, context),
        purpose: "sign_in",
        publicBaseUrl: config.publicBaseUrl,
      });
    case "auth.beginExchange":
      return config.auth.beginExchange(command.token);
    case "auth.verifyExchange": {
      if (!context.request || !context.csrfToken) {
        throw new DomainError("REQUEST_CONTEXT_REQUIRED");
      }
      const ip = requestIp(config, context);
      return config.auth.verifyExchange(command.nonce, {
        csrfToken: context.csrfToken,
        origin: context.request.headers.get("origin") ?? "",
        publicBaseUrl: config.publicBaseUrl,
        requestIp: ip,
      });
    }
    default:
      return assertNever(command);
  }
}

async function authenticateForCommand(
  config: WebApplicationConfig,
  command: AuthenticatedCommand,
  context: WebExecutionContext,
): Promise<Authenticated> {
  const sessionToken = context.sessionToken;
  if (!sessionToken) {
    throw new DomainError("UNAUTHENTICATED");
  }
  if (isMutation(command)) {
    return config.auth.authenticateMutation(sessionToken, context.csrfToken ?? "");
  }
  return config.auth.authenticate(sessionToken);
}

async function executeAuthenticated(
  config: WebApplicationConfig,
  command: AuthenticatedCommand,
  context: WebExecutionContext,
  authenticated: Authenticated,
): Promise<unknown> {
  if (isAuthenticatedAuthCommand(command)) {
    return executeAuthenticatedAuth(config, command, context, authenticated);
  }
  if (isConsultationCommand(command)) {
    return executeConsultation(config, command, context, authenticated);
  }
  if (isArchiveCommand(command)) {
    return executeArchive(config, command, authenticated);
  }
  if (isAdminCommand(command)) {
    return executeAdmin(config, command, authenticated);
  }
  return assertNever(command);
}

async function executeAuthenticatedAuth(
  config: WebApplicationConfig,
  command: AuthenticatedAuthCommand,
  context: WebExecutionContext,
  authenticated: Authenticated,
): Promise<unknown> {
  const userId = authenticated.user.id;
  switch (command.kind) {
    case "auth.authenticate":
      return authenticated;
    case "auth.logout":
      return config.operations.logout(authenticated.session.id, userId);
    case "auth.reauthenticate":
      assertAdmin(authenticated.user);
      return config.auth.requestArchiveDeleteReauth(
        context.sessionToken as string,
        command.consultationId,
        requestIp(config, context),
        config.publicBaseUrl,
      );
    case "auth.provisionCustomer":
      assertAdmin(authenticated.user);
      return config.auth.provisionCustomer(command.email, command.displayName);
    default:
      return assertNever(command);
  }
}

async function executeConsultation(
  config: WebApplicationConfig,
  command: ConsultationCommand,
  context: WebExecutionContext,
  authenticated: Authenticated,
): Promise<unknown> {
  const userId = authenticated.user.id;
  switch (command.kind) {
    case "consultation.create":
      assertStaff(authenticated.user);
      return config.consultations.create({
        employeeUserId: userId,
        customerUserId: command.customerUserId,
        providerProfileId: command.providerProfileId,
      });
    case "consultation.createInvitation":
      assertStaff(authenticated.user);
      return createConsultationInvitation(config, command, context, userId);
    case "consultation.get":
      return config.consultations.get(command.consultationId, userId);
    case "consultation.list":
      return config.consultations.list(userId);
    case "consultation.preferences":
      return config.consultations.setPreferences(
        command.consultationId,
        userId,
        command.displayName,
        command.language,
      );
    case "consultation.consent":
      return config.consultations.consent(command.consultationId, userId, command.snapshotHash);
    case "consultation.join":
      return config.consultations.join(command.consultationId, userId);
    case "consultation.token":
      return config.consultations.issueLiveKitToken(command.consultationId, userId);
    case "consultation.end":
      return config.consultations.end(command.consultationId, userId);
    case "consultation.cancel":
      return config.consultations.cancel(command.consultationId, userId);
    case "consultation.resend":
      assertStaff(authenticated.user);
      return resendConsultationInvitation(config, command, context, userId);
    case "consultation.options":
      return config.operations.consultationOptions(command.providerProfileId);
    case "consultation.room":
      return config.operations.consultationRoom(command.consultationId, userId);
    default:
      return assertNever(command);
  }
}

async function createConsultationInvitation(
  config: WebApplicationConfig,
  command: Extract<ConsultationCommand, { kind: "consultation.createInvitation" }>,
  context: WebExecutionContext,
  employeeUserId: UUID,
): Promise<unknown> {
  const customer = await config.auth.provisionCustomer(command.customerEmail, command.customerName);
  const consultation = await config.consultations.create({
    employeeUserId,
    customerUserId: customer.id,
    providerProfileId: command.providerProfileId,
  });
  await config.auth.requestMagicLink({
    email: customer.email,
    ip: requestIp(config, context),
    purpose: "consultation_invite",
    consultationId: consultation.id,
    publicBaseUrl: config.publicBaseUrl,
  });
  return consultation;
}

async function resendConsultationInvitation(
  config: WebApplicationConfig,
  command: Extract<ConsultationCommand, { kind: "consultation.resend" }>,
  context: WebExecutionContext,
  employeeUserId: UUID,
): Promise<unknown> {
  const email = await config.operations.consultationInviteRecipient(
    command.consultationId,
    employeeUserId,
  );
  return config.auth.requestMagicLink({
    email,
    ip: requestIp(config, context),
    purpose: "consultation_invite",
    consultationId: command.consultationId,
    publicBaseUrl: config.publicBaseUrl,
  });
}

async function executeArchive(
  config: WebApplicationConfig,
  command: ArchiveCommand,
  authenticated: Authenticated,
): Promise<unknown> {
  switch (command.kind) {
    case "archive.list":
      return config.operations.archiveList(staffPrincipal(authenticated.user));
    case "archive.get":
      return config.operations.archiveGet(staffPrincipal(authenticated.user), command.archiveId);
    case "archive.objects":
      return config.operations.archiveObjects(
        staffPrincipal(authenticated.user),
        command.archiveId,
        command.cursor,
        command.limit,
      );
    case "archive.download":
      return config.operations.archiveDownload(
        staffPrincipal(authenticated.user),
        command.archiveId,
        command.objectId,
      );
    case "archive.hold":
      assertAdmin(authenticated.user);
      return config.archives.addHold(command.consultationId, authenticated.session, command.reason);
    case "archive.releaseHold":
      assertAdmin(authenticated.user);
      return config.archives.releaseHold(
        command.consultationId,
        command.holdId,
        authenticated.session,
      );
    case "archive.delete":
      assertAdmin(authenticated.user);
      return config.archives.beginDelete(command.consultationId, authenticated.session);
    default:
      return assertNever(command);
  }
}

async function executeAdmin(
  config: WebApplicationConfig,
  command: AdminCommand,
  authenticated: Authenticated,
): Promise<unknown> {
  switch (command.kind) {
    case "admin.failures":
      return config.operations.adminFailures(staffPrincipal(authenticated.user));
    case "admin.languages":
      return config.operations.adminLanguages(
        staffPrincipal(authenticated.user),
        command.providerProfileId,
      );
    case "language.enable":
      assertAdmin(authenticated.user);
      return config.languages.setEnabled(command.capabilityId, command.enabled);
    default:
      return assertNever(command);
  }
}

function isInternalCommand(command: WebCommand): command is InternalCommand {
  return command.kind.startsWith("internal.");
}
function isUnauthenticatedAuthCommand(command: WebCommand): command is UnauthenticatedAuthCommand {
  return (
    command.kind === "auth.requestMagicLink" ||
    command.kind === "auth.beginExchange" ||
    command.kind === "auth.verifyExchange"
  );
}

function isAuthenticatedAuthCommand(
  command: AuthenticatedCommand,
): command is AuthenticatedAuthCommand {
  return command.kind.startsWith("auth.");
}

function isConsultationCommand(command: AuthenticatedCommand): command is ConsultationCommand {
  return command.kind.startsWith("consultation.");
}

function isArchiveCommand(command: AuthenticatedCommand): command is ArchiveCommand {
  return command.kind.startsWith("archive.");
}

function isAdminCommand(command: AuthenticatedCommand): command is AdminCommand {
  return command.kind.startsWith("admin.") || command.kind === "language.enable";
}

async function executeInternal(
  config: WebApplicationConfig,
  command: InternalCommand,
  principal: InternalPrincipal,
): Promise<unknown> {
  switch (command.kind) {
    case "internal.capability":
      authorizeInternal(principal, "capability:write", ["translation-worker", "language-refresh"]);
      return config.languages.publishRevision(command.refresh);
    case "internal.heartbeat":
      authorizeInternal(principal, "heartbeat:write", ["translation-worker"]);
      return config.operations.heartbeat(
        command.consultationId,
        command.generation,
        command.workerId,
        command.epoch,
      );
    case "internal.checkpoint":
      authorizeInternal(principal, "checkpoint:write", ["translation-worker"]);
      return config.operations.checkpoint(command);
    case "internal.providerAttempt":
      authorizeInternal(principal, "checkpoint:write", ["translation-worker"]);
      return config.operations.providerAttempt(command);
    case "internal.archiveObject":
      authorizeInternal(principal, "checkpoint:write", ["translation-worker", "spool-drainer"]);
      return config.archives.recordObject(
        command.consultationId,
        command.writerEpoch,
        command.causalKey,
        command.object,
      );
    case "internal.finalize":
      authorizeInternal(principal, "archive:finalize", ["translation-worker", "control-worker"]);
      return config.archives.finalizeInventory(command.consultationId, command.inventory);
    case "internal.egressLayout":
      authorizeInternal(principal, "egress-layout:read", ["control-worker"]);
      return config.operations.egressLayout(command.consultationId, command.generation);
    case "internal.archiveRecording":
      authorizeInternal(principal, "archive:recording", ["control-worker"]);
      return config.archives.adoptCompositeRecording(command.consultationId);
    case "internal.deleteDrain":
      authorizeInternal(principal, "delete:drain", ["control-worker"]);
      return config.archives.drainDeletion(command.consultationId, command.writeEpoch);
    default:
      return assertNever(command);
  }
}

function isMutation(command: AuthenticatedCommand): boolean {
  return !readCommandKinds.includes(command.kind);
}

function staffPrincipal(user: UserRecord): StaffPrincipal {
  if (user.staffRole !== "employee" && user.staffRole !== "admin") {
    throw new DomainError("FORBIDDEN");
  }
  return { userId: user.id, role: user.staffRole };
}

function assertStaff(user: UserRecord): void {
  staffPrincipal(user);
}

function assertAdmin(user: UserRecord): void {
  if (user.staffRole !== "admin") {
    throw new DomainError("FORBIDDEN");
  }
}

function authorizeInternal(
  principal: InternalPrincipal,
  permission: string,
  services: readonly InternalPrincipal["service"][],
): void {
  if (!services.includes(principal.service) || !principal.permissions.includes(permission)) {
    throw new DomainError("FORBIDDEN_INTERNAL");
  }
}

function requestIp(config: WebApplicationConfig, context: WebExecutionContext): string {
  if (!context.request) {
    throw new DomainError("REQUEST_CONTEXT_REQUIRED");
  }
  const origin = context.request.headers.get("origin");
  if (origin !== new URL(config.publicBaseUrl).origin) {
    throw new DomainError("ORIGIN_INVALID");
  }
  return config.clientIp(context.request);
}

function assertNever(value: never): never {
  throw new DomainError("UNKNOWN_COMMAND", String(value));
}
