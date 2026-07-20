FROM golang:1.24.5-bookworm AS build
ARG MINIO_VERSION=RELEASE.2025-07-23T15-54-02Z
RUN git clone --branch "${MINIO_VERSION}" --depth 1 https://github.com/minio/minio.git /src/minio \
    && cd /src/minio \
    && CGO_ENABLED=0 go build -tags kqueue -trimpath \
        --ldflags "$(MINIO_RELEASE=RELEASE go run buildscripts/gen-ldflags.go)" \
        -o /out/minio

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 minio \
    && useradd --uid 1000 --gid 10001 --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin minio
COPY --from=build /out/minio /usr/local/bin/minio
USER 1000:10001
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
