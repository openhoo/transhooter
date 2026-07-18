FROM oven/bun:1.3.14-debian AS build
WORKDIR /workspace
COPY package.json bun.lock tsconfig.base.json ./
COPY apps/web/package.json apps/web/
COPY packages/contracts/package.json packages/contracts/tsconfig*.json packages/contracts/
COPY packages/server-core/package.json packages/server-core/tsconfig*.json packages/server-core/
COPY apps/control-worker/package.json apps/control-worker/tsconfig.json apps/control-worker/
COPY tests/e2e/package.json tests/e2e/
COPY tests/failure-smoke/package.json tests/failure-smoke/
RUN --mount=type=cache,id=bun-control-worker,target=/root/.bun/install/cache \
    bun install --frozen-lockfile --filter @transhooter/control-worker
COPY packages/contracts packages/contracts
COPY packages/server-core packages/server-core
COPY apps/control-worker apps/control-worker
RUN bun run --filter @transhooter/contracts build \
    && bun run --filter @transhooter/server-core build \
    && bun run --filter @transhooter/control-worker build

FROM oven/bun:1.3.14-debian AS runtime
ENV NODE_ENV=production
RUN groupadd --gid 10001 transhooter && useradd --uid 10001 --gid transhooter --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin transhooter
WORKDIR /workspace
COPY --from=build --chown=10001:10001 /workspace /workspace
USER 10001:10001
CMD ["bun", "apps/control-worker/dist/main.js"]
