FROM python:3.13.5-slim-bookworm AS dependencies
COPY --from=ghcr.io/astral-sh/uv:0.8.3 /uv /uvx /bin/
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH=/workspace/packages/translation-runtime/.venv/bin:$PATH \
    CONTRACTS_SCHEMA_FILE=/workspace/contracts/contracts.schema.json
RUN groupadd --gid 10001 transhooter && useradd --uid 10001 --gid 10001 --create-home transhooter
WORKDIR /workspace
COPY packages/translation-runtime/pyproject.toml packages/translation-runtime/uv.lock ./packages/translation-runtime/
RUN --mount=type=cache,id=uv-translation-runtime-v1,target=/root/.cache/uv \
    uv sync --project packages/translation-runtime --frozen --no-dev --no-editable --no-install-project
COPY packages/translation-runtime/src ./packages/translation-runtime/src
RUN --mount=type=cache,id=uv-translation-runtime-v1,target=/root/.cache/uv \
    uv sync --project packages/translation-runtime --frozen --no-dev --no-editable

FROM python:3.13.5-slim-bookworm AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH=/workspace/packages/translation-runtime/.venv/bin:$PATH \
    CONTRACTS_SCHEMA_FILE=/workspace/contracts/contracts.schema.json
RUN groupadd --gid 10001 transhooter && useradd --uid 10001 --gid 10001 --create-home transhooter
WORKDIR /workspace
COPY --from=dependencies --chown=transhooter:transhooter /workspace/packages/translation-runtime/.venv ./packages/translation-runtime/.venv
COPY --chown=transhooter:transhooter packages/contracts/generated/contracts.schema.json ./contracts/contracts.schema.json
COPY --chown=transhooter:transhooter apps/translation-worker/main.py ./apps/translation-worker/main.py
COPY --chown=transhooter:transhooter apps/spool-drainer/main.py ./apps/spool-drainer/main.py
USER transhooter
WORKDIR /workspace
CMD ["python", "apps/translation-worker/main.py", "start"]
