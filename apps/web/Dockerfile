FROM oven/bun:1.3.14-debian AS dependencies
ENV NEXT_TELEMETRY_DISABLED=1
WORKDIR /workspace
COPY package.json bun.lock tsconfig.base.json ./
COPY apps/control-worker/package.json apps/control-worker/package.json
COPY apps/web/package.json apps/web/package.json
COPY packages/contracts/package.json packages/contracts/package.json
COPY packages/server-core/package.json packages/server-core/package.json
COPY tests/e2e/package.json tests/e2e/package.json
COPY tests/failure-smoke/package.json tests/failure-smoke/package.json
RUN --mount=type=cache,id=bun-web,target=/root/.bun/install/cache \
    bun install --frozen-lockfile --filter @transhooter/web

FROM dependencies AS build
COPY apps/web apps/web
COPY packages/contracts packages/contracts
COPY packages/server-core packages/server-core
RUN bun run --filter @transhooter/contracts build \
    && bun run --filter @transhooter/server-core build \
    && bun run --filter @transhooter/web build

FROM oven/bun:1.3.14-debian AS runtime
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV HOSTNAME=0.0.0.0
ENV PORT=3000
WORKDIR /app
RUN groupadd --system --gid 10001 transhooter && useradd --system --uid 10001 --gid transhooter --home-dir /app transhooter
COPY --from=build --chown=transhooter:transhooter /workspace/apps/web/.next/standalone ./
COPY --from=build --chown=transhooter:transhooter /workspace/apps/web/.next/static ./apps/web/.next/static
USER 10001:10001
EXPOSE 3000
CMD ["bun", "apps/web/server.js"]
