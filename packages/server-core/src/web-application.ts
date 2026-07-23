import { DomainError, type UUID } from "./domain/model";
import {
  assertAdmin,
  assertStaff,
  authenticateForCommand,
  executeAuthenticatedAuth,
  executeUnauthenticatedAuth,
  isAuthenticatedAuthCommand,
  isUnauthenticatedAuthCommand,
  requestIp,
  staffPrincipal,
} from "./web-authentication";
import type {
  AdminCommand,
  ArchiveCommand,
  Authenticated,
  AuthenticatedCommand,
  ConsultationCommand,
  WebApplication,
  WebApplicationConfig,
  WebExecutionContext,
} from "./web-command";
import { executeVerifiedInternal, isInternalCommand } from "./web-internal";

export type {
  WebApplication,
  WebApplicationConfig,
  WebCommand,
  WebExecutionContext,
} from "./web-command";

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
        creationIdempotencyKey: command.creationIdempotencyKey,
      });
    case "consultation.createInvitation":
      assertStaff(authenticated.user);
      return createConsultationInvitation(config, command, context, userId);
    case "consultation.get":
      return config.consultations.get(command.consultationId, userId);
    case "consultation.lobby": {
      const consultation = await config.consultations.get(command.consultationId, userId);
      return {
        consultation,
        options: await config.operations.consultationOptions(consultation.providerProfileId),
        viewer: { userId, staffRole: authenticated.user.staffRole },
      };
    }
    case "consultation.list":
      return {
        consultations: await config.consultations.list(userId),
        viewer: { staffRole: authenticated.user.staffRole },
      };
    case "consultation.preferences": {
      const consultation = await config.consultations.setPreferences(
        command.consultationId,
        userId,
        command.displayName,
        command.language,
      );
      return {
        consultation,
        options: await config.operations.consultationOptions(consultation.providerProfileId),
        viewer: { userId, staffRole: authenticated.user.staffRole },
      };
    }
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
    case "consultation.profileMetadata":
      assertStaff(authenticated.user);
      return config.operations.providerProfileMetadata();
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
    creationIdempotencyKey: command.creationIdempotencyKey,
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
      return {
        ...(await config.operations.archivePresentation(
          staffPrincipal(authenticated.user),
          command.archiveId,
          null,
          100,
        )),
        viewer: { staffRole: authenticated.user.staffRole },
      };
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
      return config.archives.beginDelete(
        command.consultationId,
        authenticated.session,
        command.reason,
      );
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
      return config.languages.setEnabled(
        command.capabilityId,
        command.profileId,
        command.profileRevision,
        command.enabled,
      );
    default:
      return assertNever(command);
  }
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

function assertNever(value: never): never {
  throw new DomainError("UNKNOWN_COMMAND", String(value));
}
