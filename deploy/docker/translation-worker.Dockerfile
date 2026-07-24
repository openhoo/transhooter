FROM python:3.13.5-slim-bookworm AS dependencies
COPY --from=ghcr.io/astral-sh/uv:0.8.3 /uv /uvx /bin/
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
RUN groupadd --gid 10001 transhooter && useradd --uid 10001 --gid 10001 --create-home transhooter
WORKDIR /workspace
COPY packages/spool-store/pyproject.toml packages/spool-store/uv.lock ./packages/spool-store/
COPY services/translation-worker/pyproject.toml services/translation-worker/uv.lock ./services/translation-worker/
RUN --mount=type=cache,id=uv-translation-worker-v1,target=/root/.cache/uv \
    uv sync --project services/translation-worker --frozen --no-dev --no-editable \
      --no-install-project --no-install-package transhooter-spool-store
COPY packages/spool-store/src ./packages/spool-store/src
COPY services/translation-worker/src ./services/translation-worker/src
RUN --mount=type=cache,id=uv-translation-worker-v1,target=/root/.cache/uv \
    uv sync --project services/translation-worker --frozen --no-dev --no-editable

FROM python:3.13.5-slim-bookworm AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH=/workspace/services/translation-worker/.venv/bin:$PATH \
    CONTRACTS_SCHEMA_FILE=/workspace/contracts/contracts.schema.json
RUN groupadd --gid 10001 transhooter && useradd --uid 10001 --gid 10001 --create-home transhooter
WORKDIR /workspace
COPY --from=dependencies --chown=transhooter:transhooter /workspace/services/translation-worker/.venv ./services/translation-worker/.venv
COPY --chown=transhooter:transhooter packages/contracts/generated/contracts.schema.json ./contracts/contracts.schema.json
USER transhooter
CMD ["transhooter-translation-worker", "start"]
