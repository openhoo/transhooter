import { test } from "bun:test";
import assert from "node:assert/strict";
import { once } from "node:events";
import { readFile } from "node:fs/promises";
import { createServer, type Server } from "node:http";

const OTLP_ENVIRONMENT_KEYS = [
  "OTEL_EXPORTER_OTLP_ENDPOINT",
  "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
  "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
  "OTEL_SDK_DISABLED",
] as const;

const CHILD_PROGRAM = `
  import { startNodeTelemetry } from "./src/index.ts";
  const handle = startNodeTelemetry({
    serviceName: "telemetry-signal-matrix-test",
    serviceVersion: "test",
    metricExportIntervalMillis: 60_000,
  });
  if (!handle.enabled) throw new Error("telemetry unexpectedly disabled");
  handle.tracer.startSpan("transhooter.test.span").end();
  handle.meter.createCounter("transhooter.test.count").add(1);
  await handle.forceFlush();
  await handle.shutdown();
`;

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

function childEnvironment(values: Readonly<Record<string, string>>): Record<string, string> {
  const environment: Record<string, string> = {};
  for (const [name, value] of Object.entries(process.env)) {
    if (value !== undefined) {
      environment[name] = value;
    }
  }
  for (const name of OTLP_ENVIRONMENT_KEYS) {
    delete environment[name];
  }
  return { ...environment, ...values };
}

async function exportedPaths(
  values: (port: number) => Readonly<Record<string, string>>,
): Promise<string[]> {
  const paths: string[] = [];
  const server = createServer((request, response) => {
    paths.push(request.url ?? "");
    request.resume();
    response.statusCode = 200;
    response.end();
  });
  const port = await listen(server);

  try {
    const child = Bun.spawn(["bun", "--eval", CHILD_PROGRAM], {
      cwd: `${import.meta.dir}/..`,
      env: childEnvironment(values(port)),
      stderr: "pipe",
      stdout: "pipe",
    });
    const [exitCode, stderr] = await Promise.all([child.exited, new Response(child.stderr).text()]);
    assert.equal(exitCode, 0, stderr);
    return paths.sort();
  } finally {
    await close(server);
  }
}

test("OTLP common and signal-specific endpoint matrix", async () => {
  const cases = [
    {
      name: "common-only",
      environment: (port: number) => ({
        OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${port}/common`,
      }),
      expected: ["/common/v1/metrics", "/common/v1/traces"],
    },
    {
      name: "traces-only",
      environment: (port: number) => ({
        OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: `http://127.0.0.1:${port}/trace-only`,
      }),
      expected: ["/trace-only"],
    },
    {
      name: "metrics-only",
      environment: (port: number) => ({
        OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: `http://127.0.0.1:${port}/metric-only`,
      }),
      expected: ["/metric-only"],
    },
    {
      name: "signal-specific-overrides",
      environment: (port: number) => ({
        OTEL_EXPORTER_OTLP_ENDPOINT: `http://127.0.0.1:${port}/common`,
        OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: `http://127.0.0.1:${port}/trace-override`,
        OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: `http://127.0.0.1:${port}/metric-override`,
      }),
      expected: ["/metric-override", "/trace-override"],
    },
  ] as const;

  for (const matrixCase of cases) {
    const paths = await exportedPaths(matrixCase.environment);
    const routedEndpoints = [...new Set(paths)].sort();
    assert.deepEqual(routedEndpoints, [...matrixCase.expected], matrixCase.name);
    for (const expectedPath of matrixCase.expected) {
      assert.ok(paths.includes(expectedPath), `${matrixCase.name}: missing ${expectedPath}`);
    }
  }
});

test("application telemetry avoids hot-path attribute temporaries", async () => {
  const [webTelemetry, controlTelemetry] = await Promise.all([
    readFile(new URL("../../../apps/web/lib/server/telemetry.ts", import.meta.url), "utf8"),
    readFile(
      new URL("../../../apps/control-worker/src/runtime/telemetry.ts", import.meta.url),
      "utf8",
    ),
  ]);

  const completionObjects = webTelemetry.match(/const completedAttributes: Attributes =/gu) ?? [];
  assert.equal(completionObjects.length, 1);
  assert.match(
    webTelemetry,
    /const completedAttributes: Attributes = \{\s*operation: normalizedOperation\(operation\),\s*surface,\s*outcome,\s*status_class: finalStatusClass,\s*\};/u,
  );
  assert.match(
    webTelemetry,
    /if \(responseStatus !== undefined\) \{\s*completedAttributes\["http\.response\.status_code"\] = responseStatus;\s*\}/u,
  );
  assert.match(
    webTelemetry,
    /if \(errorKind !== undefined\) \{\s*completedAttributes\["error\.kind"\] = errorKind;\s*\}/u,
  );
  assert.doesNotMatch(
    webTelemetry,
    /const completedAttributes: Attributes = \{[\s\S]*?\.\.\.[\s\S]*?\};/u,
  );

  const normalizer = controlTelemetry.match(/function normalizeSpanAttributes[\s\S]*?\n\}/u)?.[0];
  assert.ok(normalizer, "control span attribute normalizer must remain defined");
  assert.match(normalizer, /for \(const key in attributes\)/u);
  assert.match(normalizer, /Object\.hasOwn\(attributes, key\)/u);
  assert.match(normalizer, /const allowed = ATTRIBUTE_VALUES\[key\]/u);
  assert.doesNotMatch(normalizer, /Object\.entries/u);
});
