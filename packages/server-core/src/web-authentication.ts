import type { StaffPrincipal } from "./application-operations";
import { DomainError } from "./domain/model";
import type { UserRecord } from "./ports/index";
import type {
  Authenticated,
  AuthenticatedAuthCommand,
  AuthenticatedCommand,
  UnauthenticatedAuthCommand,
  WebApplicationConfig,
  WebCommand,
  WebExecutionContext,
} from "./web-command";

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

export async function executeUnauthenticatedAuth(
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
        sessionToken: context.sessionToken ?? null,
      });
    }
    default:
      return assertNever(command);
  }
}

export async function authenticateForCommand(
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

export async function executeAuthenticatedAuth(
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

export function isUnauthenticatedAuthCommand(
  command: WebCommand,
): command is UnauthenticatedAuthCommand {
  return (
    command.kind === "auth.requestMagicLink" ||
    command.kind === "auth.beginExchange" ||
    command.kind === "auth.verifyExchange"
  );
}

export function isAuthenticatedAuthCommand(
  command: AuthenticatedCommand,
): command is AuthenticatedAuthCommand {
  return command.kind.startsWith("auth.");
}

export function staffPrincipal(user: UserRecord): StaffPrincipal {
  if (user.staffRole !== "employee" && user.staffRole !== "admin") {
    throw new DomainError("FORBIDDEN");
  }
  return { userId: user.id, role: user.staffRole };
}

export function assertStaff(user: UserRecord): void {
  staffPrincipal(user);
}

export function assertAdmin(user: UserRecord): void {
  if (user.staffRole !== "admin") {
    throw new DomainError("FORBIDDEN");
  }
}

export function requestIp(config: WebApplicationConfig, context: WebExecutionContext): string {
  if (!context.request) {
    throw new DomainError("REQUEST_CONTEXT_REQUIRED");
  }
  const origin = context.request.headers.get("origin");
  if (origin !== new URL(config.publicBaseUrl).origin) {
    throw new DomainError("ORIGIN_INVALID");
  }
  return config.clientIp(context.request);
}

function isMutation(command: AuthenticatedCommand): boolean {
  return !readCommandKinds.includes(command.kind);
}

function assertNever(value: never): never {
  throw new DomainError("UNKNOWN_COMMAND", String(value));
}
