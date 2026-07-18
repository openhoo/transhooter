FROM python:3.13.5-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.8.3 /uv /uvx /bin/
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH=/workspace/services/translation-worker/.venv/bin:$PATH \
    CONTRACTS_SCHEMA_FILE=/workspace/contracts/contracts.schema.json
RUN groupadd --gid 10001 transhooter && useradd --uid 10001 --gid 10001 --create-home transhooter
WORKDIR /workspace
COPY services/translation-worker/pyproject.toml services/translation-worker/uv.lock ./services/translation-worker/
RUN --mount=type=cache,target=/root/.cache/uv uv sync --project services/translation-worker --frozen --no-install-project
COPY --chown=transhooter:transhooter services/translation-worker ./services/translation-worker
COPY --chown=transhooter:transhooter packages/contracts/generated/contracts.schema.json ./contracts/contracts.schema.json
RUN --mount=type=cache,target=/root/.cache/uv uv sync --project services/translation-worker --frozen
USER transhooter
WORKDIR /workspace/services/translation-worker
EXPOSE 8080
CMD ["python", "-m", "transhooter_worker.runtime"]
