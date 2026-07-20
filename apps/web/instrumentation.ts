export async function register(): Promise<void> {
  if (process.env.NEXT_RUNTIME !== "nodejs") return;
  // Keep Node-only SDK and signal lifecycle code out of the Edge bundle.
  const { registerNodeTelemetry } = await import("./instrumentation.node");
  registerNodeTelemetry();
}

export async function onRequestError(error: unknown): Promise<void> {
  if (process.env.NEXT_RUNTIME !== "nodejs") return;
  // Route framework hooks through the same Node-only module as registration.
  const { recordNodeRequestError } = await import("./instrumentation.node");
  recordNodeRequestError(error);
}
