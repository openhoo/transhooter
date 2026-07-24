import { DomainError } from "./domain/model";
import type { InternalPrincipal } from "./ports/index";
import type {
  InternalCommand,
  WebApplicationConfig,
  WebCommand,
  WebExecutionContext,
} from "./web-command";

export function isInternalCommand(command: WebCommand): command is InternalCommand {
  return command.kind.startsWith("internal.");
}

export async function executeVerifiedInternal(
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
      authorizeInternal(principal, "checkpoint:write", ["spool-drainer"]);
      return config.operations.checkpoint(command);
    case "internal.providerAttempt":
      authorizeInternal(principal, "provider-attempt:write", ["translation-worker"]);
      return config.operations.providerAttempt(command);
    case "internal.archiveObject":
      authorizeInternal(principal, "checkpoint:write", ["spool-drainer"]);
      return config.archives.recordSpoolObject(
        command.consultationId,
        {
          generation: command.generation,
          workerId: command.workerId,
          workerEpoch: command.workerEpoch,
        },
        command.writerEpoch,
        command.causalKey,
        command.object,
      );
    case "internal.expiredWorkerEpochs":
      authorizeInternal(principal, "worker-recovery:read", ["spool-drainer"]);
      return config.operations.expiredWorkerEpochs();
    case "internal.completeWorkerEpoch":
      authorizeInternal(principal, "worker-recovery:write", ["spool-drainer"]);
      return config.operations.completeWorkerEpoch(command);
    case "internal.abandonWorkerEpoch":
      authorizeInternal(principal, "worker-recovery:write", ["spool-drainer"]);
      return config.operations.abandonWorkerEpoch(command);
    case "internal.finalize":
      authorizeInternal(principal, "archive:finalize", ["control-worker"]);
      return config.archives.finalizeInventory(command.consultationId, command.inventory);
    case "internal.egressLayout":
      authorizeInternal(principal, "egress-layout:read", ["control-worker"]);
      return config.operations.egressLayout(command.consultationId, command.generation);
    case "internal.archiveRecording":
      authorizeInternal(principal, "archive:recording", ["control-worker"]);
      return config.archives.adoptCompositeRecording(command.consultationId);
    case "internal.deleteDrain":
      authorizeInternal(principal, "delete:drain", ["control-worker"]);
      return config.archives.drainDeletion(
        command.consultationId,
        command.writeEpoch,
        command.reason,
      );
    default:
      return assertNever(command);
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

function assertNever(value: never): never {
  throw new DomainError("UNKNOWN_COMMAND", String(value));
}
