FROM mcr.microsoft.com/playwright:v1.55.1-noble
COPY --from=oven/bun:1.3.14-debian /usr/local/bin/bun /usr/local/bin/bun
ENV NEXT_TELEMETRY_DISABLED=1
RUN groupadd --gid 10001 transhooter \
    && useradd --uid 10001 --gid 10001 --create-home transhooter
WORKDIR /workspace
COPY --chown=transhooter:transhooter package.json bun.lock ./
COPY --chown=transhooter:transhooter apps/control-worker/package.json apps/control-worker/package.json
COPY --chown=transhooter:transhooter apps/web/package.json apps/web/package.json
COPY --chown=transhooter:transhooter packages/contracts/package.json packages/contracts/package.json
COPY --chown=transhooter:transhooter packages/telemetry/package.json packages/telemetry/package.json
COPY --chown=transhooter:transhooter packages/server-core/package.json packages/server-core/package.json
COPY --chown=transhooter:transhooter tests/e2e/package.json tests/e2e/package.json
COPY --chown=transhooter:transhooter tests/failure-smoke/package.json tests/failure-smoke/package.json
RUN --mount=type=cache,id=bun-harness-v1,target=/root/.bun/install/cache \
    bun install --frozen-lockfile --filter @transhooter/e2e
COPY --chown=transhooter:transhooter tests/e2e/smoke-consultation.mjs tests/e2e/smoke-consultation.mjs
COPY --chown=transhooter:transhooter tests/e2e/consultation-harness.mjs tests/e2e/consultation-harness.mjs
COPY --chown=transhooter:transhooter tests/e2e/consultation-scenario.mjs tests/e2e/consultation-scenario.mjs
COPY --chown=transhooter:transhooter tests/e2e/consultation-archive-proof.mjs tests/e2e/consultation-archive-proof.mjs
COPY --chown=transhooter:transhooter tests/e2e/harness-contracts.mjs tests/e2e/harness-contracts.mjs
COPY --chown=transhooter:transhooter tests/e2e/harness-contracts.test.mjs tests/e2e/harness-contracts.test.mjs
COPY --chown=transhooter:transhooter tests/fixtures tests/fixtures
WORKDIR /workspace/tests/e2e
USER transhooter
