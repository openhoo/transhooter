FROM oven/bun:1.3.14-debian AS dependencies
WORKDIR /workspace
COPY package.json bun.lock ./
COPY apps/control-worker/package.json ./apps/control-worker/package.json
COPY apps/web/package.json ./apps/web/package.json
COPY packages/contracts/package.json ./packages/contracts/package.json
COPY packages/server-core/package.json ./packages/server-core/package.json
COPY tests/e2e/package.json ./tests/e2e/package.json
COPY tests/failure-smoke/package.json ./tests/failure-smoke/package.json
RUN --mount=type=cache,id=bun-runtime-v1,target=/root/.bun/install/cache \
    bun install --frozen-lockfile

FROM dependencies AS build
ENV NEXT_TELEMETRY_DISABLED=1
COPY tsconfig.base.json biome.json ./
COPY apps ./apps
COPY packages ./packages
COPY deploy/scripts/migrate.mjs ./deploy/scripts/migrate.mjs
RUN bun run build

FROM oven/bun:1.3.14-debian AS runtime
ENV NODE_ENV=production \
    NEXT_TELEMETRY_DISABLED=1
RUN groupadd --gid 10001 transhooter \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin transhooter
WORKDIR /workspace
COPY --from=dependencies --chown=10001:10001 /workspace /workspace
COPY --from=build --chown=10001:10001 /workspace/apps /workspace/apps
COPY --from=build --chown=10001:10001 /workspace/packages /workspace/packages
COPY --from=build --chown=10001:10001 /workspace/deploy /workspace/deploy
USER 10001:10001
EXPOSE 3000 8080
