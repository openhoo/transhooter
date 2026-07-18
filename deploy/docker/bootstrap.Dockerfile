FROM python:3.13.5-slim-bookworm
RUN groupadd --gid 10001 transhooter && useradd --uid 10001 --gid 10001 --create-home transhooter \
    && pip install --no-cache-dir boto3==1.39.14 awscrt==0.27.6 livekit-api==1.1.1
COPY --chown=transhooter:transhooter deploy/scripts/minio_bootstrap.py /opt/transhooter/minio_bootstrap.py
COPY --chown=transhooter:transhooter deploy/scripts/egress_readiness.py /opt/transhooter/egress_readiness.py
USER transhooter
