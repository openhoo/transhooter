FROM oven/bun:1.3.14-debian AS development-dependencies
WORKDIR /workspace
COPY package.json bun.lock ./
COPY apps/control-worker/package.json ./apps/control-worker/package.json
COPY apps/web/package.json ./apps/web/package.json
COPY packages/contracts/package.json ./packages/contracts/package.json
COPY packages/telemetry/package.json ./packages/telemetry/package.json
COPY packages/server-core/package.json ./packages/server-core/package.json
COPY tests/e2e/package.json ./tests/e2e/package.json
COPY tests/failure-smoke/package.json ./tests/failure-smoke/package.json
RUN --mount=type=cache,id=bun-runtime-v1,target=/root/.bun/install/cache \
    bun install --frozen-lockfile

FROM development-dependencies AS build
ENV NEXT_TELEMETRY_DISABLED=1
COPY tsconfig.base.json biome.json ./
COPY apps ./apps
COPY packages ./packages
COPY deploy/scripts/migrate.mjs ./deploy/scripts/migrate.mjs
RUN bun run build

FROM oven/bun:1.3.14-debian AS production-dependencies
WORKDIR /workspace
COPY package.json bun.lock ./
COPY apps/control-worker/package.json ./apps/control-worker/package.json
COPY apps/web/package.json ./apps/web/package.json
COPY packages/contracts/package.json ./packages/contracts/package.json
COPY packages/telemetry/package.json ./packages/telemetry/package.json
COPY packages/server-core/package.json ./packages/server-core/package.json
COPY tests/e2e/package.json ./tests/e2e/package.json
COPY tests/failure-smoke/package.json ./tests/failure-smoke/package.json
RUN --mount=type=cache,id=bun-runtime-v1,target=/root/.bun/install/cache \
    bun install --frozen-lockfile --production \
      --filter @transhooter/control-worker \
      --filter @transhooter/web

FROM oven/bun:1.3.14-debian AS runtime
ENV NODE_ENV=production \
    NEXT_TELEMETRY_DISABLED=1
RUN groupadd --gid 10001 transhooter \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin transhooter
WORKDIR /workspace
COPY --from=production-dependencies --chown=10001:10001 /workspace /workspace
COPY --from=build --chown=10001:10001 /workspace/apps/control-worker/dist /workspace/apps/control-worker/dist
COPY --from=build --chown=10001:10001 /workspace/apps/web/.next/standalone /workspace/apps/web/.next/standalone
COPY --from=build --chown=10001:10001 /workspace/apps/web/.next/static /workspace/apps/web/.next/standalone/apps/web/.next/static
COPY --from=build --chown=10001:10001 /workspace/packages/contracts/dist /workspace/packages/contracts/dist
COPY --from=build --chown=10001:10001 /workspace/packages/contracts/generated /workspace/packages/contracts/generated
COPY --from=build --chown=10001:10001 /workspace/packages/telemetry/dist /workspace/packages/telemetry/dist
COPY --from=build --chown=10001:10001 /workspace/packages/server-core/dist /workspace/packages/server-core/dist
COPY --from=build --chown=10001:10001 /workspace/packages/server-core/drizzle /workspace/packages/server-core/drizzle
COPY --from=build --chown=10001:10001 /workspace/deploy/scripts/migrate.mjs /workspace/deploy/scripts/migrate.mjs
USER 10001:10001
EXPOSE 3000 8080
