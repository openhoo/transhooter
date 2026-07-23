import {
  boundedErrorKind,
  type NodeTelemetryHandle,
  startNodeTelemetry,
} from "@transhooter/telemetry";

const SERVICE_NAME = "transhooter-control-worker";
const SERVICE_VERSION = "0.1.0";
const HEALTH_ROUTES: Readonly<Record<string, true>> = { "/health/ready": true, other: true };
const HEALTH_STATUSES: Readonly<Record<string, true>> = { "200": true, "404": true, "503": true };
const LOOPS: Readonly<Record<string, true>> = { coordinator: true, effect_runner: true };
const LOOP_OUTCOMES: Readonly<Record<string, true>> = { claimed: true, idle: true, error: true };
const QUEUES: Readonly<Record<string, true>> = {
  coordinator_outbox: true,
  coordinator_deadline: true,
  coordinator_stale_reservation: true,
  effect: true,
};
const OPERATION_FAMILIES: Readonly<Record<string, true>> = {
  consultation: true,
  deadline: true,
  outbox: true,
  webhook: true,
  worker_reservation: true,
  other: true,
};
const OPERATIONS: Readonly<Record<string, true>> = {
  heartbeat: true,
  webhook: true,
  effect_plan: true,
  provision: true,
  finalize: true,
  cancel: true,
  effect_applied: true,
  capture_requested: true,
  archive_failed: true,
  same_language_bypass: true,
  worker_failure: true,
  egress_failure: true,
  participant_joined: true,
  participant_left: true,
  egress_active: true,
  egress_terminal: true,
  ready: true,
  absence: true,
  archive_reconcile: true,
  recover: true,
  other: true,
};
const OPERATION_OUTCOMES: Readonly<Record<string, true>> = {
  claimed: true,
  idle: true,
  error: true,
  done: true,
  failed: true,
  retained: true,
  ignored: true,
  compensated: true,
  lease_lost: true,
  not_owned: true,
  other: true,
};
const EFFECT_KINDS: Readonly<Record<string, true>> = {
  ROOM_CREATE: true,
  ROOM_COMPOSITE_EGRESS: true,
  WORKER_DISPATCH: true,
  PARTICIPANT_EGRESS: true,
  PARTICIPANT_GRANT: true,
  STATUS_PACKET: true,
  ROOM_DRAIN: true,
  DISPATCH_DELETE: true,
  PARTICIPANT_REMOVE: true,
  EGRESS_STOP: true,
  ROOM_DELETE: true,
  ARCHIVE_RECONCILE: true,
  ARCHIVE_DELETE: true,
  other: true,
};
const ERROR_KINDS: Readonly<Record<string, true>> = {
  aborted: true,
  timeout: true,
  validation: true,
  unavailable: true,
  other: true,
};
const SPAN_NAMES: Readonly<Record<string, true>> = {
  "transhooter.control.bootstrap": true,
  "transhooter.control.shutdown.signal": true,
  "control.durable_operation": true,
  "control.effect": true,
};
const ATTRIBUTE_VALUES: Readonly<Record<string, Readonly<Record<string, true>>>> = {
  "control.family": OPERATION_FAMILIES,
  "control.operation": OPERATIONS,
  "control.effect.kind": EFFECT_KINDS,
  "control.outcome": OPERATION_OUTCOMES,
  "control.signal": { SIGINT: true, SIGTERM: true, other: true },
  "error.kind": ERROR_KINDS,
};

type SpanAttribute = string | number | boolean;
type SpanAttributes = Readonly<Record<string, SpanAttribute>>;
type MetricAttributes = Readonly<Record<string, SpanAttribute>>;

interface Counter {
  add(value: number, attributes?: MetricAttributes): void;
}

interface Histogram {
  record(value: number, attributes?: MetricAttributes): void;
}

interface Instruments {
  readonly healthRequests: Counter;
  readonly healthDuration: Histogram;
  readonly loopTicks: Counter;
  readonly loopClaimed: Counter;
  readonly loopDuration: Histogram;
  readonly workClaimed: Counter;
  readonly durableOperations: Counter;
  readonly durableDuration: Histogram;
  readonly effects: Counter;
  readonly effectDuration: Histogram;
}

let telemetry: NodeTelemetryHandle | undefined;
let instruments: Instruments | undefined;
let shutdownPromise: Promise<void> | undefined;

export function initializeControlTelemetry(): void {
  if (telemetry !== undefined) {
    return;
  }
  try {
    const appEnvironment = process.env.APP_ENV?.trim();
    const environment =
      appEnvironment === "development" ||
      appEnvironment === "test" ||
      appEnvironment === "production"
        ? appEnvironment
        : undefined;
    const serviceName = process.env.OTEL_SERVICE_NAME?.trim() || SERVICE_NAME;
    telemetry = startNodeTelemetry({
      serviceName,
      serviceVersion: SERVICE_VERSION,
      ...(environment === undefined ? {} : { environment }),
    });
    const meter = telemetry.meter;
    instruments = {
      healthRequests: meter.createCounter("transhooter.control.health.requests"),
      healthDuration: meter.createHistogram("transhooter.control.health.duration", { unit: "s" }),
      loopTicks: meter.createCounter("transhooter.control.loop.ticks"),
      loopClaimed: meter.createCounter("transhooter.control.loop.claimed", { unit: "{work}" }),
      loopDuration: meter.createHistogram("transhooter.control.loop.duration", { unit: "s" }),
      workClaimed: meter.createCounter("transhooter.control.work.claimed", { unit: "{work}" }),
      durableOperations: meter.createCounter("transhooter.control.durable.operations"),
      durableDuration: meter.createHistogram("transhooter.control.durable.duration", { unit: "s" }),
      effects: meter.createCounter("transhooter.control.effects"),
      effectDuration: meter.createHistogram("transhooter.control.effect.duration", { unit: "s" }),
    };
  } catch {
    // Telemetry initialization is best-effort and must never block bootstrap.
  }
}

export function shutdownControlTelemetry(): Promise<void> {
  if (shutdownPromise !== undefined) {
    return shutdownPromise;
  }
  shutdownPromise = telemetry?.shutdown() ?? Promise.resolve();
  return shutdownPromise;
}

export async function withControlSpan<T>(
  name: string,
  attributes: SpanAttributes,
  work: () => Promise<T>,
): Promise<T> {
  const tracer = telemetry?.tracer;
  if (tracer === undefined) {
    return work();
  }
  return tracer.startActiveSpan(
    normalize(name, SPAN_NAMES, "control.durable_operation"),
    async (span) => {
      safely(() => span.setAttributes(normalizeSpanAttributes(attributes)));
      try {
        const result = await work();
        safely(() => span.setStatus({ code: 1 }));
        return result;
      } catch (error) {
        safely(() => {
          span.setAttribute("error.kind", boundedErrorKind(error));
          span.setStatus({ code: 2 });
        });
        throw error;
      } finally {
        safely(() => span.end());
      }
    },
  );
}

export function recordHealthRequest(
  route: string,
  status: number,
  accepting: boolean,
  durationSeconds: number,
): void {
  safely(() => {
    const attributes = {
      route: normalize(route, HEALTH_ROUTES, "other"),
      status: normalize(String(status), HEALTH_STATUSES, "503"),
      accepting,
    };
    instruments?.healthRequests.add(1, attributes);
    instruments?.healthDuration.record(nonnegative(durationSeconds), attributes);
  });
}

export function recordLoopTick(
  loop: string,
  outcome: string,
  claimed: number,
  durationSeconds: number,
): void {
  safely(() => {
    const attributes = {
      loop: normalize(loop, LOOPS, "coordinator"),
      outcome: normalize(outcome, LOOP_OUTCOMES, "error"),
    };
    instruments?.loopTicks.add(1, attributes);
    instruments?.loopClaimed.add(integerCount(claimed), attributes);
    instruments?.loopDuration.record(nonnegative(durationSeconds), attributes);
  });
}

export function recordWorkClaimed(queue: string, count: number): void {
  safely(() => {
    if (count > 0) {
      instruments?.workClaimed.add(integerCount(count), {
        queue: normalize(queue, QUEUES, "effect"),
      });
    }
  });
}

export function recordDurableOperation(
  family: string,
  operation: string,
  outcome: string,
  durationSeconds: number,
  error?: unknown,
): void {
  safely(() => {
    const attributes: Record<string, string> = {
      family: normalize(family, OPERATION_FAMILIES, "other"),
      operation: normalize(operation, OPERATIONS, "other"),
      outcome: normalize(outcome, OPERATION_OUTCOMES, "other"),
    };
    if (error !== undefined || attributes.outcome === "failed" || attributes.outcome === "error") {
      attributes["error.kind"] = boundedErrorKind(error);
    }
    instruments?.durableOperations.add(1, attributes);
    instruments?.durableDuration.record(nonnegative(durationSeconds), attributes);
  });
}

export function recordEffect(
  kind: string,
  outcome: string,
  durationSeconds: number,
  error?: unknown,
): void {
  safely(() => {
    const attributes: Record<string, string> = {
      kind: normalize(kind, EFFECT_KINDS, "other"),
      outcome: normalize(outcome, OPERATION_OUTCOMES, "other"),
    };
    if (error !== undefined || attributes.outcome === "failed" || attributes.outcome === "error") {
      attributes["error.kind"] = boundedErrorKind(error);
    }
    instruments?.effects.add(1, attributes);
    instruments?.effectDuration.record(nonnegative(durationSeconds), attributes);
  });
}

function normalize(
  value: string,
  allowed: Readonly<Record<string, true>>,
  fallback: string,
): string {
  return allowed[value] === true ? value : fallback;
}

function normalizeSpanAttributes(attributes: SpanAttributes): Record<string, SpanAttribute> {
  const normalized: Record<string, SpanAttribute> = {};
  for (const key in attributes) {
    if (!Object.hasOwn(attributes, key)) continue;
    const value = attributes[key];
    const allowed = ATTRIBUTE_VALUES[key];
    if (allowed !== undefined && typeof value === "string") {
      normalized[key] = normalize(value, allowed, "other");
    }
  }
  return normalized;
}

function integerCount(value: number): number {
  return Number.isSafeInteger(value) && value > 0 ? value : 0;
}

function nonnegative(value: number): number {
  return Number.isFinite(value) && value >= 0 ? value : 0;
}

function safely(record: () => void): void {
  try {
    record();
  } catch {
    // Telemetry must never affect runtime behavior.
  }
}
