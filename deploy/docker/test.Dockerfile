FROM oven/bun:1.3.14-debian
COPY --from=ghcr.io/astral-sh/uv:0.8.3 /uv /uvx /bin/
ENV UV_PYTHON_INSTALL_DIR=/opt/uv/python \
    UV_PYTHON=3.13.5 \
    PATH=/workspace/services/translation-worker/.venv/bin:$PATH \
    APP_ENV=test \
    PROVIDER_NETWORK_DISABLED=true \
    NEXT_TELEMETRY_DISABLED=1
RUN groupadd --gid 10001 transhooter \
    && useradd --uid 10001 --gid 10001 --create-home transhooter
WORKDIR /workspace
COPY --chown=transhooter:transhooter . .
RUN --mount=type=cache,id=bun-test-v1,target=/root/.bun/install/cache \
    --mount=type=cache,target=/root/.cache/uv \
    bun install --frozen-lockfile \
    && uv sync --project services/translation-worker --frozen \
    && chown -R transhooter:transhooter services/translation-worker/.venv \
    && chmod -R a+rX /opt/uv
COPY --chown=transhooter:transhooter deploy/scripts/run-tests.sh /opt/transhooter/run-tests
USER transhooter
ENTRYPOINT ["/opt/transhooter/run-tests"]
