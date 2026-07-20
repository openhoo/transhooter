import { createServer, type Server, type ServerResponse } from "node:http";
import { setTimeout as sleepTimer } from "node:timers/promises";
import { boundedErrorKind } from "@transhooter/telemetry";
import { Redis } from "ioredis";
import { LiveKitEffects } from "./adapters/livekit-effects";
import { PostgresStore } from "./adapters/postgres-store";
import { RedisCoordination } from "./adapters/redis-coordination";
import { S3ArchiveVersionDeleter } from "./adapters/s3-archive";
import { Coordinator } from "./orchestration/coordinator";
import { EffectRunner } from "./orchestration/effect-runner";
import { systemClock } from "./orchestration/model";
import { loadConfig, type RuntimeConfig } from "./runtime/config";
import { FileEffectFaultControl, noEffectFaults } from "./runtime/fault-control";
import {
  initializeControlTelemetry,
  recordHealthRequest,
  recordLoopTick,
  shutdownControlTelemetry,
  withControlSpan,
} from "./runtime/telemetry";

interface WorkerRuntime {
  readonly config: RuntimeConfig;
  readonly store: PostgresStore;
  readonly redis: Redis;
  readonly archive: S3ArchiveVersionDeleter;
  readonly remote: LiveKitEffects;
  readonly coordinator: Coordinator;
  readonly effects: EffectRunner;
  readonly abort: AbortController;
  readonly health: HealthService;
}

interface RuntimeResources {
  readonly store: PostgresStore | undefined;
  readonly redis: Redis | undefined;
  readonly archive: S3ArchiveVersionDeleter | undefined;
  readonly health: HealthService | undefined;
}

class HealthService {
  private readonly server: Server;
  private accepting = true;

  constructor(
    private readonly port: number,
    private readonly readiness: () => Promise<void>,
  ) {
    this.server = createServer((request, response) => {
      const startedAt = performance.now();
      const accepting = this.accepting;
      const route =
        request.method === "GET" && request.url === "/health/ready" ? "/health/ready" : "other";
      const finish = (status: 200 | 404 | 503, ready: boolean | undefined): void => {
        sendJson(response, status, ready);
        recordHealthRequest(route, status, accepting, (performance.now() - startedAt) / 1_000);
      };

      if (route === "other") {
        finish(404, undefined);
        return;
      }
      if (!accepting) {
        finish(503, false);
        return;
      }
      void this.readiness()
        .then(() => finish(200, true))
        .catch(() => finish(503, false));
    });
  }

  stopAccepting(): void {
    this.accepting = false;
  }

  async listen(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.server.once("error", reject);
      this.server.listen(this.port, "0.0.0.0", resolve);
    });
  }

  async close(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.server.close((error) => {
        if (error === undefined) {
          resolve();
        } else {
          reject(error);
        }
      });
    });
  }
}

async function bootstrap(): Promise<WorkerRuntime> {
  let store: PostgresStore | undefined;
  let redis: Redis | undefined;
  let archive: S3ArchiveVersionDeleter | undefined;
  let health: HealthService | undefined;
  try {
    const config = await loadConfig(process.env);
    store = PostgresStore.connect(config.databaseUrl);
    redis = new Redis(config.redisUrl, {
      lazyConnect: true,
      enableOfflineQueue: false,
      maxRetriesPerRequest: 1,
    });
    await redis.connect();
    await redis.ping();

    archive = new S3ArchiveVersionDeleter(config.s3.bucket, config.s3);
    const remote = new LiveKitEffects(
      {
        ...config.livekit,
        egressLayoutUrl: config.egressLayoutUrl,
        internalToken: config.internalToken,
        egressLayoutSigningKey: config.egressLayoutSigningKey,
        s3: {
          accessKey: config.s3.accessKey,
          secretKey: config.s3.secretKey,
          endpoint: config.s3.endpoint,
          bucket: config.s3.bucket,
          region: config.s3.region,
          forcePathStyle: config.s3.forcePathStyle,
        },
      },
      archive,
    );
    const owner = config.workerId;
    const coordinator = new Coordinator(
      store,
      systemClock,
      { owner, leaseMs: 30_000, batchSize: 32 },
      remote,
      new RedisCoordination(redis),
    );
    const faultControl =
      config.testFaultControlFile === null
        ? noEffectFaults
        : new FileEffectFaultControl(config.testFaultControlFile);
    const effects = new EffectRunner(
      store,
      remote,
      systemClock,
      { owner, leaseMs: 60_000, batchSize: 16 },
      faultControl,
    );
    const abort = new AbortController();
    const readinessStore = store;
    const readinessRedis = redis;
    health = new HealthService(config.healthPort, async () => {
      await Promise.all([readinessStore.readiness(), readinessRedis.ping(), remote.readiness()]);
    });
    await health.listen();

    return {
      config,
      store,
      redis,
      archive,
      remote,
      coordinator,
      effects,
      abort,
      health,
    };
  } catch (error) {
    await cleanupResources({ store, redis, archive, health }).catch(() => undefined);
    throw error;
  }
}

function sendJson(response: ServerResponse, status: number, ready: boolean | undefined): void {
  if (ready === undefined) {
    response.writeHead(status).end();
    return;
  }
  response
    .writeHead(status, { "content-type": "application/json" })
    .end(ready ? '{"ready":true}' : '{"ready":false}');
}

function installShutdownSignals(runtime: WorkerRuntime): void {
  for (const signal of ["SIGINT", "SIGTERM"] as const) {
    process.once(signal, () => {
      runtime.health.stopAccepting();
      runtime.abort.abort();
      void withControlSpan(
        "transhooter.control.shutdown.signal",
        { "control.signal": signal },
        async () => undefined,
      );
    });
  }
}

async function runLoop(
  name: "coordinator" | "effect_runner",
  tick: () => Promise<number>,
  pollIntervalMs: number,
  signal: AbortSignal,
): Promise<void> {
  while (!signal.aborted) {
    const startedAt = performance.now();
    try {
      const count = await tick();
      recordLoopTick(
        name,
        count === 0 ? "idle" : "claimed",
        count,
        (performance.now() - startedAt) / 1_000,
      );
      if (count === 0) {
        await sleep(pollIntervalMs, signal);
      }
    } catch (error) {
      const errorKind = boundedErrorKind(error);
      recordLoopTick(name, "error", 0, (performance.now() - startedAt) / 1_000);
      console.error(
        JSON.stringify({
          level: "error",
          loop: name,
          "error.kind": errorKind,
        }),
      );
      await sleep(Math.max(pollIntervalMs, 1_000), signal);
    }
  }
}

async function run(runtime: WorkerRuntime): Promise<void> {
  installShutdownSignals(runtime);
  await Promise.all([
    runLoop(
      "coordinator",
      async () => runtime.coordinator.tick(),
      runtime.config.pollIntervalMs,
      runtime.abort.signal,
    ),
    runLoop(
      "effect_runner",
      async () => runtime.effects.tick(),
      runtime.config.pollIntervalMs,
      runtime.abort.signal,
    ),
  ]);
}

async function shutdown(runtime: WorkerRuntime): Promise<void> {
  await cleanupResources(runtime);
}

async function cleanupResources(resources: RuntimeResources): Promise<void> {
  let failure: unknown;
  let failed = false;
  resources.health?.stopAccepting();
  try {
    await resources.health?.close();
  } catch (error) {
    failure = error;
    failed = true;
  }
  try {
    resources.archive?.destroy();
  } catch (error) {
    if (!failed) {
      failure = error;
      failed = true;
    }
  }
  try {
    resources.redis?.disconnect();
  } catch (error) {
    if (!failed) {
      failure = error;
      failed = true;
    }
  }
  try {
    await resources.store?.close();
  } catch (error) {
    if (!failed) {
      failure = error;
      failed = true;
    }
  }
  if (failed) {
    throw failure;
  }
}

async function sleep(milliseconds: number, signal: AbortSignal): Promise<void> {
  try {
    await sleepTimer(milliseconds, undefined, { signal });
  } catch (error) {
    if (!signal.aborted || !(error instanceof Error) || error.name !== "AbortError") {
      throw error;
    }
  }
}

initializeControlTelemetry();
let runtime: WorkerRuntime | undefined;
let failure: unknown;
let failed = false;
try {
  runtime = await withControlSpan("transhooter.control.bootstrap", {}, bootstrap);
  await run(runtime);
} catch (error) {
  failure = error;
  failed = true;
} finally {
  if (runtime !== undefined) {
    try {
      await shutdown(runtime);
    } catch (error) {
      if (!failed) {
        failure = error;
        failed = true;
      }
    }
  }
  try {
    await shutdownControlTelemetry();
  } catch (error) {
    if (!failed) {
      failure = error;
      failed = true;
    }
  }
}
if (failed) {
  throw failure;
}
