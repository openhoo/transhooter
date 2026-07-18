FROM ghcr.io/astral-sh/uv:0.8.4-python3.13-bookworm-slim AS build
WORKDIR /workspace
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev
COPY src ./src
RUN uv sync --frozen --no-dev

FROM python:3.13.5-slim-bookworm
RUN groupadd --gid 10001 transhooter && useradd --uid 10001 --gid 10001 --home-dir /var/lib/transhooter --create-home transhooter
WORKDIR /workspace
COPY --from=build --chown=10001:10001 /workspace/.venv /workspace/.venv
COPY --from=build --chown=10001:10001 /workspace/src /workspace/src
ENV PATH="/workspace/.venv/bin:$PATH" PYTHONPATH=/workspace/src PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 10001:10001
ENTRYPOINT ["python", "-m", "transhooter_worker"]
