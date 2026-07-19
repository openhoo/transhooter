export async function register(): Promise<void> {
  if (process.env.NEXT_RUNTIME !== "nodejs") return;
  // Keep Node-only SDK and signal lifecycle code out of the Edge bundle.
  const { registerNodeTelemetry } = await import("./instrumentation.node");
  registerNodeTelemetry();
}

export async function onRequestError(error: unknown): Promise<void> {
  if (process.env.NEXT_RUNTIME !== "nodejs") return;
  // Keep the Node SDK out of the Edge instrumentation bundle.
  const { recordFrameworkRequestError } = await import("./lib/telemetry");
  recordFrameworkRequestError(error);
}
