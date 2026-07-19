import { test } from "bun:test";
import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer, type Server } from "node:http";
import { boundedErrorKind, startNodeTelemetry } from "../src/index";

const PROCESS_STATE_KEY = Symbol.for("@transhooter/telemetry.node.handle");
const ENVIRONMENT_KEYS = ["OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_SDK_DISABLED"] as const;
type EnvironmentKey = (typeof ENVIRONMENT_KEYS)[number];
type ErrorKind = "aborted" | "timeout" | "validation" | "unavailable" | "other";

function restoreEnvironment(saved: Readonly<Record<EnvironmentKey, string | undefined>>): void {
  for (const key of ENVIRONMENT_KEYS) {
    const value = saved[key];
    if (value === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  }
}

async function listen(server: Server): Promise<number> {
  const listening = once(server, "listening");
  server.listen(0, "127.0.0.1");
  await listening;
  const address = server.address();
  assert.ok(address && typeof address === "object");
  return address.port;
}

async function close(server: Server): Promise<void> {
  const closed = once(server, "close");
  server.close();
  await closed;
}

test("boundedErrorKind returns only bounded operational classifications", () => {
  const cases: ReadonlyArray<readonly [unknown, ErrorKind]> = [
    [new TypeError("private input"), "validation"],
    [new RangeError("private input"), "validation"],
    [{ name: "AbortError" }, "aborted"],
    [{ code: "ERR_ABORTED" }, "aborted"],
    [{ name: "TimeoutError" }, "timeout"],
    [{ code: "UND_ERR_CONNECT_TIMEOUT" }, "timeout"],
    [{ name: "ValidationError" }, "validation"],
    [{ code: "ERR_INVALID_ARG_VALUE" }, "validation"],
    [{ code: "ECONNREFUSED" }, "unavailable"],
    [new Error("timeout and payload details must not become a classification"), "other"],
    [null, "other"],
    ["ECONNRESET", "other"],
  ];
  const allowed: Record<ErrorKind, true> = {
    aborted: true,
    timeout: true,
    validation: true,
    unavailable: true,
    other: true,
  };

  for (const [error, expected] of cases) {
    const actual = boundedErrorKind(error);
    assert.equal(actual, expected);
    assert.equal(allowed[actual], true);
  }
});

test("disabled telemetry is process-idempotent and never exports during lifecycle calls", async () => {
  const savedEnvironment: Record<EnvironmentKey, string | undefined> = {
    OTEL_EXPORTER_OTLP_ENDPOINT: process.env.OTEL_EXPORTER_OTLP_ENDPOINT,
    OTEL_SDK_DISABLED: process.env.OTEL_SDK_DISABLED,
  };
  const savedRegistry = Object.getOwnPropertyDescriptor(globalThis, PROCESS_STATE_KEY);
  let exporterRequests = 0;
  const server = createServer((_request, response) => {
    exporterRequests += 1;
    response.statusCode = 200;
    response.end();
  });

  try {
    Reflect.deleteProperty(globalThis, PROCESS_STATE_KEY);
    delete process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
    delete process.env.OTEL_SDK_DISABLED;

    const endpointUnset = startNodeTelemetry({
      serviceName: "telemetry-endpoint-unset-test",
      serviceVersion: "test",
    });
    const endpointUnsetAgain = startNodeTelemetry({
      serviceName: "ignored-after-process-start",
      serviceVersion: "test",
    });
    assert.equal(endpointUnset.enabled, false);
    assert.equal(endpointUnsetAgain, endpointUnset);
    await endpointUnset.forceFlush();
    await endpointUnset.forceFlush();
    await endpointUnset.shutdown();
    await endpointUnset.shutdown();

    Reflect.deleteProperty(globalThis, PROCESS_STATE_KEY);
    const port = await listen(server);
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT = `http://127.0.0.1:${port}`;
    process.env.OTEL_SDK_DISABLED = " TrUe ";

    const sdkDisabled = startNodeTelemetry({
      serviceName: "telemetry-sdk-disabled-test",
      serviceVersion: "test",
    });
    const sdkDisabledAgain = startNodeTelemetry({
      serviceName: "ignored-after-process-start",
      serviceVersion: "test",
    });
    assert.equal(sdkDisabled.enabled, false);
    assert.equal(sdkDisabledAgain, sdkDisabled);

    sdkDisabled.meter.createHistogram("transhooter.test.latency", { unit: "s" }).record(0.01);
    sdkDisabled.tracer.startSpan("transhooter.test.span").end();
    await sdkDisabled.forceFlush();
    await sdkDisabled.forceFlush();
    await sdkDisabled.shutdown();
    await sdkDisabled.shutdown();
    assert.equal(exporterRequests, 0);
  } finally {
    if (server.listening) {
      await close(server);
    }
    restoreEnvironment(savedEnvironment);
    if (savedRegistry) {
      Object.defineProperty(globalThis, PROCESS_STATE_KEY, savedRegistry);
    } else {
      Reflect.deleteProperty(globalThis, PROCESS_STATE_KEY);
    }
  }
});
