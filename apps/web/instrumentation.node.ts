import { initializeWebTelemetry, shutdownWebTelemetry } from "./lib/telemetry";

const SHUTDOWN_LISTENERS_KEY = Symbol.for("@transhooter/web.telemetry.shutdown-listeners");
const SHUTDOWN_STARTED_KEY = Symbol.for("@transhooter/web.telemetry.shutdown-started");
const SHUTDOWN_GRACE_MILLIS = 5_000;

const instrumentationGlobal = globalThis as typeof globalThis & {
  [SHUTDOWN_LISTENERS_KEY]?: true;
  [SHUTDOWN_STARTED_KEY]?: true;
};

function shutdownWithinGracePeriod(): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  const timer = setTimeout(resolve, SHUTDOWN_GRACE_MILLIS);
  void shutdownWebTelemetry()
    .catch(() => undefined)
    .then(() => {
      clearTimeout(timer);
      resolve();
    });
  return promise;
}

function shutdownBeforeProcessExit(signal: "SIGINT" | "SIGTERM"): void {
  if (instrumentationGlobal[SHUTDOWN_STARTED_KEY]) return;
  instrumentationGlobal[SHUTDOWN_STARTED_KEY] = true;

  const originalExit = process.exit;
  const shutdown = shutdownWithinGracePeriod();
  const hasApplicationShutdownHandler = process.listenerCount(signal) > 0;
  let exitRequested = false;

  const deferredExit = ((code?: string | number | null): never => {
    if (exitRequested) return undefined as never;
    exitRequested = true;
    void shutdown.then(() => {
      process.exit = originalExit;
      originalExit.call(process, code);
    });
    return undefined as never;
  }) as typeof process.exit;

  process.exit = deferredExit;
  void shutdown.then(() => {
    if (exitRequested || process.exit !== deferredExit) return;
    process.exit = originalExit;
    if (!hasApplicationShutdownHandler) {
      originalExit.call(process, signal === "SIGINT" ? 130 : 143);
    }
  });
}

export function registerNodeTelemetry(): void {
  initializeWebTelemetry();

  if (instrumentationGlobal[SHUTDOWN_LISTENERS_KEY]) return;
  instrumentationGlobal[SHUTDOWN_LISTENERS_KEY] = true;

  // Next.js closes its server asynchronously, then calls process.exit. Run first
  // and defer that final exit until the telemetry SDK has shut down or timed out.
  process.prependOnceListener("SIGINT", () => shutdownBeforeProcessExit("SIGINT"));
  process.prependOnceListener("SIGTERM", () => shutdownBeforeProcessExit("SIGTERM"));
}
