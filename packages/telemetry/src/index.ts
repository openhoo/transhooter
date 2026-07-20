import { type Meter, metrics, type Tracer, trace } from "@opentelemetry/api";
import { OTLPMetricExporter } from "@opentelemetry/exporter-metrics-otlp-proto";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { defaultResource, resourceFromAttributes } from "@opentelemetry/resources";
import {
  AggregationType,
  InstrumentType,
  PeriodicExportingMetricReader,
  type ViewOptions,
} from "@opentelemetry/sdk-metrics";
import { NodeSDK } from "@opentelemetry/sdk-node";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace";
import {
  ATTR_DEPLOYMENT_ENVIRONMENT_NAME,
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_NAMESPACE,
  ATTR_SERVICE_VERSION,
} from "@opentelemetry/semantic-conventions";

export interface StartNodeTelemetryOptions {
  serviceName: string;
  serviceVersion: string;
  environment?: string;
  endpoint?: string;
  metricExportIntervalMillis?: number;
}

export interface NodeTelemetryHandle {
  readonly tracer: Tracer;
  readonly meter: Meter;
  readonly enabled: boolean;
  forceFlush(): Promise<void>;
  shutdown(): Promise<void>;
}

const PROCESS_STATE_KEY = Symbol.for("@transhooter/telemetry.node.handle");
// The process registry deliberately survives duplicate ESM package instances.
const telemetryGlobal = globalThis as typeof globalThis & {
  [key: symbol]: unknown;
};
const DEFAULT_METRIC_EXPORT_INTERVAL_MILLIS = 60_000;
const SECONDS_HISTOGRAM_BOUNDARIES = [
  0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
] as const;
const RATIO_HISTOGRAM_BOUNDARIES = [0, 0.1, 0.25, 0.5, 0.7, 0.8, 0.9, 0.95, 1] as const;

const ABORT_CODES: Record<string, true> = {
  ABORT_ERR: true,
  ERR_ABORTED: true,
};
const TIMEOUT_CODES: Record<string, true> = {
  ETIMEDOUT: true,
  UND_ERR_BODY_TIMEOUT: true,
  UND_ERR_CONNECT_TIMEOUT: true,
  UND_ERR_HEADERS_TIMEOUT: true,
};
const VALIDATION_CODES: Record<string, true> = {
  ERR_INVALID_ARG_TYPE: true,
  ERR_INVALID_ARG_VALUE: true,
  ERR_OUT_OF_RANGE: true,
};
const UNAVAILABLE_CODES: Record<string, true> = {
  EAI_AGAIN: true,
  ECONNREFUSED: true,
  ECONNRESET: true,
  EHOSTDOWN: true,
  EHOSTUNREACH: true,
  ENETDOWN: true,
  ENETUNREACH: true,
  ENOTFOUND: true,
  UND_ERR_CONNECT: true,
  UND_ERR_SOCKET: true,
};

function processHandle(): NodeTelemetryHandle | undefined {
  const value = telemetryGlobal[PROCESS_STATE_KEY];
  return isTelemetryHandle(value) ? value : undefined;
}

function isTelemetryHandle(value: unknown): value is NodeTelemetryHandle {
  return (
    typeof value === "object" &&
    value !== null &&
    "tracer" in value &&
    "meter" in value &&
    "enabled" in value &&
    typeof value.enabled === "boolean" &&
    "forceFlush" in value &&
    typeof value.forceFlush === "function" &&
    "shutdown" in value &&
    typeof value.shutdown === "function"
  );
}

function disabledHandle(serviceName: string, serviceVersion: string): NodeTelemetryHandle {
  const handle: NodeTelemetryHandle = {
    tracer: trace.getTracer(serviceName, serviceVersion),
    meter: metrics.getMeter(serviceName, serviceVersion),
    enabled: false,
    async forceFlush(): Promise<void> {},
    async shutdown(): Promise<void> {},
  };
  telemetryGlobal[PROCESS_STATE_KEY] = handle;
  return handle;
}

function metricExportInterval(optionInterval: number | undefined): number {
  if (optionInterval !== undefined) {
    return Number.isSafeInteger(optionInterval) && optionInterval > 0
      ? optionInterval
      : DEFAULT_METRIC_EXPORT_INTERVAL_MILLIS;
  }

  const environmentInterval = Number(process.env.OTEL_METRIC_EXPORT_INTERVAL);
  return Number.isSafeInteger(environmentInterval) && environmentInterval > 0
    ? environmentInterval
    : DEFAULT_METRIC_EXPORT_INTERVAL_MILLIS;
}

function signalEndpoint(baseEndpoint: string, signal: "traces" | "metrics"): string {
  const url = new URL(baseEndpoint);
  const withoutTrailingSlash = url.pathname.replace(/\/+$/u, "");
  const basePath = withoutTrailingSlash.replace(/\/v1\/(?:traces|metrics)$/u, "");
  url.pathname = `${basePath}/v1/${signal}`;
  // Query parameters can carry exporter configuration and must apply to both signals.
  url.hash = "";
  return url.toString();
}

function configuredSignalEndpoints(
  configuredCommonEndpoint: string | undefined,
): Readonly<{ traces?: string; metrics?: string }> {
  const commonEndpoint =
    configuredCommonEndpoint === undefined
      ? process.env.OTEL_EXPORTER_OTLP_ENDPOINT?.trim()
      : configuredCommonEndpoint.trim();
  const traceEndpoint = process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT?.trim();
  const metricEndpoint = process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT?.trim();
  const traces =
    traceEndpoint || (commonEndpoint ? signalEndpoint(commonEndpoint, "traces") : undefined);
  const metrics =
    metricEndpoint || (commonEndpoint ? signalEndpoint(commonEndpoint, "metrics") : undefined);
  return {
    ...(traces ? { traces } : {}),
    ...(metrics ? { metrics } : {}),
  };
}

function resourceAttributes(options: StartNodeTelemetryOptions): Record<string, string> {
  const attributes: Record<string, string> = {
    [ATTR_SERVICE_NAME]: options.serviceName,
    [ATTR_SERVICE_NAMESPACE]: "transhooter",
    [ATTR_SERVICE_VERSION]: options.serviceVersion,
  };
  const environment = options.environment?.trim();
  if (environment) {
    attributes[ATTR_DEPLOYMENT_ENVIRONMENT_NAME] = environment;
  }
  return attributes;
}
function histogramViews(meterName: string): ViewOptions[] {
  return [
    {
      aggregation: {
        type: AggregationType.EXPLICIT_BUCKET_HISTOGRAM,
        options: { boundaries: [...SECONDS_HISTOGRAM_BOUNDARIES] },
      },
      instrumentType: InstrumentType.HISTOGRAM,
      instrumentUnit: "s",
      meterName,
    },
    {
      aggregation: {
        type: AggregationType.EXPLICIT_BUCKET_HISTOGRAM,
        options: { boundaries: [...RATIO_HISTOGRAM_BOUNDARIES] },
      },
      instrumentType: InstrumentType.HISTOGRAM,
      instrumentUnit: "1",
      meterName,
    },
  ];
}

export function startNodeTelemetry(options: StartNodeTelemetryOptions): NodeTelemetryHandle {
  const existingHandle = processHandle();
  if (existingHandle) {
    return existingHandle;
  }

  const signalEndpoints = configuredSignalEndpoints(options.endpoint);
  if (
    process.env.OTEL_SDK_DISABLED?.trim().toLowerCase() === "true" ||
    (!signalEndpoints.traces && !signalEndpoints.metrics)
  ) {
    return disabledHandle(options.serviceName, options.serviceVersion);
  }

  try {
    const spanProcessor = signalEndpoints.traces
      ? new BatchSpanProcessor({
          exporter: new OTLPTraceExporter({ url: signalEndpoints.traces }),
        })
      : undefined;
    const interval = metricExportInterval(options.metricExportIntervalMillis);
    const metricReader = signalEndpoints.metrics
      ? new PeriodicExportingMetricReader({
          exporter: new OTLPMetricExporter({ url: signalEndpoints.metrics }),
          exportIntervalMillis: interval,
          exportTimeoutMillis: Math.min(interval, 30_000),
        })
      : undefined;
    const sdk = new NodeSDK({
      autoDetectResources: true,
      instrumentations: [],
      logRecordProcessors: [],
      metricReaders: metricReader ? [metricReader] : [],
      resource: defaultResource().merge(resourceFromAttributes(resourceAttributes(options))),
      spanProcessors: spanProcessor ? [spanProcessor] : [],
      views: histogramViews(options.serviceName),
    });

    sdk.start();

    let stopped = false;
    let shutdownPromise: Promise<void> | undefined;
    const handle: NodeTelemetryHandle = {
      tracer: trace.getTracer(options.serviceName, options.serviceVersion),
      meter: metrics.getMeter(options.serviceName, options.serviceVersion),
      enabled: true,
      async forceFlush(): Promise<void> {
        if (stopped || shutdownPromise) {
          return;
        }
        await Promise.allSettled(
          [spanProcessor?.forceFlush(), metricReader?.forceFlush()].filter(
            (flush): flush is Promise<void> => flush !== undefined,
          ),
        );
      },
      shutdown(): Promise<void> {
        if (shutdownPromise) {
          return shutdownPromise;
        }
        stopped = true;
        shutdownPromise = sdk.shutdown().catch(() => undefined);
        return shutdownPromise;
      },
    };

    telemetryGlobal[PROCESS_STATE_KEY] = handle;
    return handle;
  } catch {
    return disabledHandle(options.serviceName, options.serviceVersion);
  }
}

export function boundedErrorKind(
  error: unknown,
): "aborted" | "timeout" | "validation" | "unavailable" | "other" {
  if (error instanceof TypeError || error instanceof RangeError) {
    return "validation";
  }

  let name: string | undefined;
  let code: string | undefined;
  try {
    if (typeof error === "object" && error !== null) {
      if ("name" in error) {
        name = typeof error.name === "string" ? error.name : undefined;
      }
      if ("code" in error) {
        code = typeof error.code === "string" ? error.code : undefined;
      }
    }
  } catch {
    return "other";
  }

  if (name === "AbortError" || (code !== undefined && ABORT_CODES[code] === true)) {
    return "aborted";
  }
  if (name === "TimeoutError" || (code !== undefined && TIMEOUT_CODES[code] === true)) {
    return "timeout";
  }
  if (name === "ValidationError" || (code !== undefined && VALIDATION_CODES[code] === true)) {
    return "validation";
  }
  if (code !== undefined && UNAVAILABLE_CODES[code] === true) {
    return "unavailable";
  }
  return "other";
}
