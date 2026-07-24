FROM oven/bun:1.3.14-debian
COPY --from=ghcr.io/astral-sh/uv:0.8.3 /uv /uvx /bin/
ENV UV_PYTHON_INSTALL_DIR=/opt/uv/python \
    UV_PYTHON=3.13.5 \
    APP_ENV=test \
    PROVIDER_NETWORK_DISABLED=true \
    NEXT_TELEMETRY_DISABLED=1 \
    PATH=/workspace/services/spool-drainer/.venv/bin:$PATH
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
COPY --chown=transhooter:transhooter packages/spool-store/pyproject.toml packages/spool-store/uv.lock packages/spool-store/
COPY --chown=transhooter:transhooter services/translation-worker/pyproject.toml services/translation-worker/uv.lock services/translation-worker/
COPY --chown=transhooter:transhooter services/spool-drainer/pyproject.toml services/spool-drainer/uv.lock services/spool-drainer/
COPY --chown=transhooter:transhooter packages/spool-store/src packages/spool-store/src
COPY --chown=transhooter:transhooter services/translation-worker/src services/translation-worker/src
COPY --chown=transhooter:transhooter services/spool-drainer/src services/spool-drainer/src
RUN --mount=type=cache,id=bun-test-v1,target=/root/.bun/install/cache \
    --mount=type=cache,id=uv-test-spool-store-v1,target=/root/.cache/uv-spool-store \
    --mount=type=cache,id=uv-test-translation-worker-v1,target=/root/.cache/uv-translation-worker \
    --mount=type=cache,id=uv-test-spool-drainer-v1,target=/root/.cache/uv-spool-drainer \
    bun install --frozen-lockfile \
    && UV_CACHE_DIR=/root/.cache/uv-spool-store uv sync --project packages/spool-store --frozen \
    && UV_CACHE_DIR=/root/.cache/uv-translation-worker uv sync --project services/translation-worker --frozen \
    && UV_CACHE_DIR=/root/.cache/uv-spool-drainer uv sync --project services/spool-drainer --frozen \
    && chown -R transhooter:transhooter packages/spool-store/.venv services/translation-worker/.venv services/spool-drainer/.venv \
    && chmod -R a+rX /opt/uv
COPY --chown=transhooter:transhooter . .
RUN --mount=type=cache,id=uv-test-spool-store-v1,target=/root/.cache/uv-spool-store \
    --mount=type=cache,id=uv-test-translation-worker-v1,target=/root/.cache/uv-translation-worker \
    --mount=type=cache,id=uv-test-spool-drainer-v1,target=/root/.cache/uv-spool-drainer \
    UV_CACHE_DIR=/root/.cache/uv-spool-store uv sync --project packages/spool-store --frozen --offline \
    && UV_CACHE_DIR=/root/.cache/uv-translation-worker uv sync --project services/translation-worker --frozen --offline \
    && UV_CACHE_DIR=/root/.cache/uv-spool-drainer uv sync --project services/spool-drainer --frozen --offline \
    && chown -R transhooter:transhooter packages/spool-store/.venv services/translation-worker/.venv services/spool-drainer/.venv
USER transhooter
ENTRYPOINT ["/workspace/deploy/scripts/run-tests.sh"]
