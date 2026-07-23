import "server-only";
import type { NodeTelemetryHandle } from "@transhooter/telemetry";
import { boundedErrorKind, startNodeTelemetry } from "@transhooter/telemetry";

const SERVICE_NAME = "transhooter-web";
const SERVICE_VERSION = "0.1.0";
const STATE_KEY = Symbol.for("@transhooter/web.telemetry.state");
const OPERATION_DURATION = "transhooter.web.operation.duration";
const OPERATION_TOTAL = "transhooter.web.operation.total";
const FRAMEWORK_ERROR_TOTAL = "transhooter.web.framework.error.total";

const OPERATIONS: Record<string, true> = {
  "auth.magicLink.request": true,
  "auth.exchange.prepare": true,
  "auth.exchange.verify": true,
  "auth.logout": true,
  "auth.archiveDeleteReauth.request": true,
  "consultations.list": true,
  "consultations.create.options": true,
  "consultations.create": true,
  "consultations.get": true,
  "consultations.preferences.update": true,
  "consultations.consent.record": true,
  "consultations.join": true,
  "consultations.livekitToken": true,
  "consultations.room": true,
  "consultations.end": true,
  "consultations.cancel": true,
  "consultations.invitation.resend": true,
  "archives.list": true,
  "archives.get": true,
  "archives.objects.list": true,
  "archives.object.download": true,
  "archives.hold.update": true,
  "archives.delete": true,
  "languages.catalog": true,
  "admin.failures.list": true,
  "admin.languages.list": true,
  "admin.languages.update": true,
  "internal.capabilities.update": true,
  "internal.worker.heartbeat": true,
  "internal.archive.checkpoint": true,
  "internal.providerAttempt": true,
  "internal.failure": true,
  "internal.archiveObject": true,
  "internal.archive.finalize": true,
  "internal.egressLayout.authorize": true,
  "internal.archiveRecording": true,
  "internal.deleteDrain": true,
  "webhooks.livekit.receive": true,
};

const HEALTH_OPERATIONS: Record<string, true> = {
  health: true,
  "health.live": true,
  "health.ready": true,
};

type Surface = "api" | "page";
type Outcome = "success" | "client_error" | "server_error" | "redirect" | "not_found" | "error";
type StatusClass = "2xx" | "3xx" | "4xx" | "5xx" | "none";
type ErrorKind = "aborted" | "timeout" | "validation" | "unavailable" | "other";
type AttributeValue = string | number;
type Attributes = Record<string, AttributeValue>;
type Counter = { add(value: number, attributes?: Attributes): void };
type Histogram = { record(value: number, attributes?: Attributes): void };
type OperationSpan = {
  setAttributes(attributes: Attributes): void;
  setStatus(status: { code: 1 | 2 }): void;
  end(): void;
};

type TelemetryState = {
  handle: NodeTelemetryHandle;
  operationDuration: Histogram | undefined;
  operationTotal: Counter | undefined;
  frameworkErrorTotal: Counter | undefined;
  operationErrors: WeakSet<object>;
};

const telemetryGlobal = globalThis as typeof globalThis & {
  [key: symbol]: TelemetryState | undefined;
};

export function initializeWebTelemetry(): void {
  if (telemetryGlobal[STATE_KEY]) return;

  const environment = process.env.APP_ENV?.trim();
  const serviceName = process.env.OTEL_SERVICE_NAME?.trim() || SERVICE_NAME;
  const handle = startNodeTelemetry({
    serviceName,
    serviceVersion: process.env.OTEL_SERVICE_VERSION?.trim() || SERVICE_VERSION,
    ...(environment ? { environment } : {}),
  });
  const current: TelemetryState = {
    handle,
    operationDuration: undefined,
    operationTotal: undefined,
    frameworkErrorTotal: undefined,
    operationErrors: new WeakSet<object>(),
  };
  telemetryGlobal[STATE_KEY] = current;
  if (!handle.enabled) return;

  try {
    current.operationDuration = handle.meter.createHistogram(OPERATION_DURATION, {
      description: "Duration of web application operations",
      unit: "s",
    });
    current.operationTotal = handle.meter.createCounter(OPERATION_TOTAL, {
      description: "Completed web application operations",
    });
    current.frameworkErrorTotal = handle.meter.createCounter(FRAMEWORK_ERROR_TOTAL, {
      description: "Framework request errors outside application operations",
    });
  } catch {
    current.operationDuration = undefined;
    current.operationTotal = undefined;
    current.frameworkErrorTotal = undefined;
  }
}

export function shutdownWebTelemetry(): Promise<void> {
  return telemetryGlobal[STATE_KEY]?.handle.shutdown() ?? Promise.resolve();
}

function normalizedOperation(operation: string): string {
  return OPERATIONS[operation] === true ? operation : "unknown";
}

function statusClass(status: number): StatusClass {
  if (status >= 200 && status < 300) return "2xx";
  if (status >= 300 && status < 400) return "3xx";
  if (status >= 400 && status < 500) return "4xx";
  if (status >= 500 && status < 600) return "5xx";
  return "none";
}

function responseOutcome(status: number): Outcome {
  if (status === 404) return "not_found";
  if (status >= 200 && status < 300) return "success";
  if (status >= 300 && status < 400) return "redirect";
  if (status >= 400 && status < 500) return "client_error";
  if (status >= 500 && status < 600) return "server_error";
  return "error";
}

function controlFlowOutcome(error: unknown): { outcome: Outcome; statusClass: StatusClass } | null {
  try {
    if (typeof error !== "object" || error === null || !("digest" in error)) return null;
    const digest = typeof error.digest === "string" ? error.digest : "";
    if (digest.startsWith("NEXT_REDIRECT")) {
      return { outcome: "redirect", statusClass: "3xx" };
    }
    if (digest.startsWith("NEXT_HTTP_ERROR_FALLBACK;404")) {
      return { outcome: "not_found", statusClass: "4xx" };
    }
  } catch {
    return null;
  }
  return null;
}

function markOperationError(error: unknown, current: TelemetryState): void {
  if (typeof error === "object" && error !== null) current.operationErrors.add(error);
}

export async function withWebOperation<T>(
  operation: string,
  surface: Surface,
  work: () => Promise<T>,
): Promise<T> {
  if (HEALTH_OPERATIONS[operation] === true) return work();

  const current = telemetryGlobal[STATE_KEY];
  if (!current?.handle.enabled || !current.operationDuration || !current.operationTotal) {
    return work();
  }

  const attributes: Attributes = {
    operation: normalizedOperation(operation),
    surface,
  };
  const started = performance.now();

  let span: OperationSpan | undefined;
  let workStarted = false;
  let workPromise: Promise<T>;
  try {
    workPromise = current.handle.tracer.startActiveSpan(
      "web.operation",
      { attributes },
      (activeSpan) => {
        span = activeSpan;
        workStarted = true;
        return work();
      },
    );
  } catch (error) {
    // Only fall back when span creation failed. A synchronous application error
    // must be observed once rather than causing the operation to run again.
    workPromise = workStarted ? Promise.reject(error) : work();
  }

  let outcome: Outcome = "error";
  let finalStatusClass: StatusClass = "none";
  let responseStatus: number | undefined;
  let errorKind: ErrorKind | undefined;
  try {
    const result = await workPromise;
    if (result instanceof Response) {
      responseStatus = result.status;
      outcome = responseOutcome(responseStatus);
      finalStatusClass = statusClass(responseStatus);
    } else {
      outcome = "success";
      finalStatusClass = "2xx";
    }
    return result;
  } catch (error) {
    markOperationError(error, current);
    const controlFlow = controlFlowOutcome(error);
    if (controlFlow) {
      outcome = controlFlow.outcome;
      finalStatusClass = controlFlow.statusClass;
    } else {
      outcome = surface === "api" && error instanceof SyntaxError ? "client_error" : "server_error";
      finalStatusClass = outcome === "client_error" ? "4xx" : "5xx";
      errorKind = boundedErrorKind(error);
    }
    throw error;
  } finally {
    const completedAttributes: Attributes = {
      operation: normalizedOperation(operation),
      surface,
      outcome,
      status_class: finalStatusClass,
    };
    if (responseStatus !== undefined) {
      completedAttributes["http.response.status_code"] = responseStatus;
    }
    if (errorKind !== undefined) {
      completedAttributes["error.kind"] = errorKind;
    }
    try {
      current.operationTotal.add(1, completedAttributes);
      current.operationDuration.record((performance.now() - started) / 1_000, completedAttributes);
      span?.setAttributes(completedAttributes);
      span?.setStatus({
        code: outcome === "success" || outcome === "redirect" ? 1 : 2,
      });
    } catch {
      // Telemetry failures must not replace the application result or error.
    } finally {
      try {
        span?.end();
      } catch {
        // Ending a span is best-effort during exporter shutdown.
      }
    }
  }
}

export function recordFrameworkRequestError(error: unknown): void {
  const current = telemetryGlobal[STATE_KEY];
  if (!current?.handle.enabled || !current.frameworkErrorTotal) return;
  if (typeof error === "object" && error !== null && current.operationErrors.has(error)) return;
  try {
    current.frameworkErrorTotal.add(1, {
      "error.kind": boundedErrorKind(error),
      result: "error",
    });
  } catch {
    // Framework error reporting is best-effort.
  }
}
