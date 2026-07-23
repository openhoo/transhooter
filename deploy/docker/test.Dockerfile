FROM oven/bun:1.3.14-debian
COPY --from=ghcr.io/astral-sh/uv:0.8.3 /uv /uvx /bin/
ENV UV_PYTHON_INSTALL_DIR=/opt/uv/python \
    UV_PYTHON=3.13.5 \
    PATH=/workspace/packages/translation-runtime/.venv/bin:$PATH \
    APP_ENV=test \
    PROVIDER_NETWORK_DISABLED=true \
    NEXT_TELEMETRY_DISABLED=1
RUN groupadd --gid 10001 transhooter \
    && useradd --uid 10001 --gid 10001 --create-home transhooter
WORKDIR /workspace
COPY --chown=transhooter:transhooter package.json bun.lock ./
COPY --chown=transhooter:transhooter apps/control-worker/package.json apps/control-worker/package.json
COPY --chown=transhooter:transhooter apps/web/package.json apps/web/package.json
COPY --chown=transhooter:transhooter packages/contracts/package.json packages/contracts/package.json
COPY --chown=transhooter:transhooter packages/server-core/package.json packages/server-core/package.json
COPY --chown=transhooter:transhooter packages/telemetry/package.json packages/telemetry/package.json
COPY --chown=transhooter:transhooter tests/e2e/package.json tests/e2e/package.json
COPY --chown=transhooter:transhooter tests/failure-smoke/package.json tests/failure-smoke/package.json
COPY --chown=transhooter:transhooter packages/translation-runtime/pyproject.toml packages/translation-runtime/uv.lock packages/translation-runtime/
RUN --mount=type=cache,id=bun-test-v1,target=/root/.bun/install/cache \
    --mount=type=cache,target=/root/.cache/uv \
    bun install --frozen-lockfile \
    && uv sync --project packages/translation-runtime --frozen --no-install-project \
    && chown -R transhooter:transhooter packages/translation-runtime/.venv \
    && chmod -R a+rX /opt/uv
COPY --chown=transhooter:transhooter . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --project packages/translation-runtime --frozen --offline \
    && chown -R transhooter:transhooter packages/translation-runtime/.venv
USER transhooter
ENTRYPOINT ["/workspace/deploy/scripts/run-tests.sh"]
