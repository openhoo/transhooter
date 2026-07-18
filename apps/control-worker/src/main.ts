import { createServer, type Server, type ServerResponse } from "node:http";
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

class HealthService {
  private readonly server: Server;
  private accepting = true;

  constructor(
    private readonly port: number,
    private readonly readiness: () => Promise<void>,
  ) {
    this.server = createServer((request, response) => {
      if (request.method !== "GET" || request.url !== "/health/ready") {
        sendJson(response, 404, undefined);
        return;
      }
      if (!this.accepting) {
        sendJson(response, 503, false);
        return;
      }
      void this.readiness()
        .then(() => sendJson(response, 200, true))
        .catch(() => sendJson(response, 503, false));
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
  const config = await loadConfig(process.env);
  const store = PostgresStore.connect(config.databaseUrl);
  const redis = new Redis(config.redisUrl, {
    lazyConnect: true,
    enableOfflineQueue: false,
    maxRetriesPerRequest: 1,
  });
  await redis.connect();
  await redis.ping();

  const archive = new S3ArchiveVersionDeleter(config.s3.bucket, config.s3);
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
  const health = new HealthService(config.healthPort, async () => {
    await Promise.all([store.readiness(), redis.ping(), remote.readiness()]);
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
    });
  }
}

async function runLoop(
  name: string,
  tick: () => Promise<number>,
  pollIntervalMs: number,
  signal: AbortSignal,
): Promise<void> {
  while (!signal.aborted) {
    try {
      const count = await tick();
      if (count === 0) {
        await sleep(pollIntervalMs, signal);
      }
    } catch (error) {
      console.error(
        JSON.stringify({
          level: "error",
          loop: name,
          message: error instanceof Error ? error.message : "loop failed",
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
      "effects",
      async () => runtime.effects.tick(),
      runtime.config.pollIntervalMs,
      runtime.abort.signal,
    ),
  ]);
}

async function shutdown(runtime: WorkerRuntime): Promise<void> {
  runtime.health.stopAccepting();
  await runtime.health.close();
  runtime.archive.destroy();
  runtime.redis.disconnect();
  await runtime.store.close();
}

async function sleep(milliseconds: number, signal: AbortSignal): Promise<void> {
  await new Promise<void>((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(resolve, milliseconds);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}

const runtime = await bootstrap();
await run(runtime);
await shutdown(runtime);
