FROM mcr.microsoft.com/playwright:v1.55.0-noble
COPY --from=oven/bun:1.3.14-debian /usr/local/bin/bun /usr/local/bin/bun
ENV NEXT_TELEMETRY_DISABLED=1
WORKDIR /workspace
COPY package.json bun.lock ./
COPY apps/control-worker/package.json apps/control-worker/package.json
COPY apps/web/package.json apps/web/package.json
COPY packages/contracts/package.json packages/contracts/package.json
COPY packages/server-core/package.json packages/server-core/package.json
COPY tests/e2e/package.json tests/e2e/package.json
COPY tests/failure-smoke/package.json tests/failure-smoke/package.json
RUN --mount=type=cache,id=bun-failure-v1,target=/root/.bun/install/cache \
    bun install --frozen-lockfile --filter @transhooter/failure-smoke
COPY tests/e2e/smoke-consultation.mjs tests/failure-smoke/smoke-consultation.mjs
COPY tests/failure-smoke/failure-smoke.mjs tests/failure-smoke/failure-smoke.mjs
COPY tests/fixtures tests/fixtures
WORKDIR /workspace/tests/failure-smoke
CMD ["bun", "failure-smoke.mjs"]
