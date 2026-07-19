from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from uuid import UUID

import boto3  # type: ignore[import-untyped]
import httpx
from opentelemetry import metrics, trace
from opentelemetry.trace import Span, Status, StatusCode

from transhooter_worker.adapters.s3_archive import S3Archive
from transhooter_worker.adapters.spool import EncryptedSpool
from transhooter_worker.application.compactor import PcmCompactor
from transhooter_worker.ports.archive import ArchiveStore, ObjectRecord
from transhooter_worker.runtime.control_client import PermanentControlRequestError
from transhooter_worker.telemetry import bounded_error_kind, configure_telemetry

logger = logging.getLogger(__name__)

_METER = metrics.get_meter(__name__)
_TRACER = trace.get_tracer(__name__)
_DRAIN_DURATION = _METER.create_histogram(
    "transhooter.worker.spool.drain.duration",
    unit="s",
)
_REGISTRATIONS = _METER.create_counter(
    "transhooter.worker.spool.registrations.total",
    unit="{registration}",
)
_OBJECTS = _METER.create_counter(
    "transhooter.worker.spool.objects.total",
    unit="{object}",
)
_SPOOL_UTILIZATION = _METER.create_histogram(
    "transhooter.worker.spool.utilization",
    description="Encrypted spool utilization ratio observed by worker heartbeats.",
    unit="1",
)
_OBJECT_CLASSES = frozenset(
    {"provider_terminal", "checkpoint", "caption_ledger", "pipeline_exchange"}
)


def _normalized_object_class(object_class: str) -> str:
    return object_class if object_class in _OBJECT_CLASSES else "pipeline_exchange"


@contextmanager
def _span(
    name: str,
    attributes: dict[str, str] | None = None,
) -> Iterator[Span]:
    try:
        context = _TRACER.start_as_current_span(
            name,
            attributes=attributes,
            record_exception=False,
            set_status_on_exception=False,
        )
        span = context.__enter__()
    except Exception:
        yield trace.NonRecordingSpan(trace.INVALID_SPAN_CONTEXT)
        return

    try:
        yield span
    except BaseException as error:
        try:
            context.__exit__(type(error), error, error.__traceback__)
        except Exception:
            pass
        raise
    else:
        try:
            context.__exit__(None, None, None)
        except Exception:
            pass


def _finish_span(
    span: Span,
    result: str,
    error: BaseException | None = None,
) -> None:
    try:
        span.set_attribute("result", result)
        if error is None:
            span.set_status(Status(StatusCode.OK))
            return
        span.set_attribute("error.kind", bounded_error_kind(error))
        span.set_status(Status(StatusCode.ERROR))
    except Exception:
        pass


def _record_registration(
    result: str,
    object_class: str,
    error: BaseException | None = None,
) -> None:
    attributes = {
        "result": result,
        "object.class": _normalized_object_class(object_class),
    }
    if error is not None:
        attributes["error.kind"] = bounded_error_kind(error)
    try:
        _REGISTRATIONS.add(1, attributes)
    except Exception:
        pass


def _record_object(result: str, object_class: str) -> None:
    try:
        _OBJECTS.add(
            1,
            {
                "result": result,
                "object.class": _normalized_object_class(object_class),
            },
        )
    except Exception:
        pass


def _record_drain(
    result: str,
    duration_seconds: float,
    error: BaseException | None = None,
) -> None:
    attributes = {"result": result}
    if error is not None:
        attributes["error.kind"] = bounded_error_kind(error)
    try:
        _DRAIN_DURATION.record(duration_seconds, attributes)
    except Exception:
        pass


def _record_utilization(spool: EncryptedSpool) -> None:
    try:
        ratio = spool.usage_ratio()
        _SPOOL_UTILIZATION.record(
            max(0.0, min(1.0, ratio)),
            {"role": "drainer"},
        )
    except Exception:
        pass


def _metric_export_interval() -> int | None:
    raw = os.environ.get("OTEL_METRIC_EXPORT_INTERVAL", "").strip()
    try:
        interval = int(raw)
    except ValueError:
        return None
    return interval if interval > 0 else None


def _secret(name: str) -> str:
    path = os.environ.get(name + "_FILE")
    if path:
        value = Path(path).read_text("utf-8").strip()
    else:
        value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name}_FILE is required")
    return value


def _build_spool(root: Path) -> EncryptedSpool:
    database = Path(os.environ.get("SPOOL_DATABASE", str(root / "journal.sqlite3")))
    return EncryptedSpool.from_keyring(
        root,
        database,
        Path(os.environ["SPOOL_KEYRING_FILE"]),
    )


def build_archive(root: Path) -> S3Archive:
    credentials_file = os.environ.get("S3_CREDENTIALS_FILE")
    if not credentials_file:
        raise RuntimeError("S3_CREDENTIALS_FILE is required")
    credentials = json.loads(Path(credentials_file).read_text("utf-8"))
    access_key = credentials.get("accessKeyId")
    secret_key = credentials.get("secretAccessKey")
    if (
        not isinstance(access_key, str)
        or not access_key
        or not isinstance(secret_key, str)
        or not secret_key
    ):
        raise RuntimeError("S3 credential file is invalid")
    client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT"),
        region_name=os.environ.get("S3_REGION", "eu-central-1"),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    return S3Archive(
        client,
        _secret("S3_BUCKET"),
        os.environ.get("S3_KMS_KEY_ID"),
        os.environ.get("ARCHIVE_REQUIRE_KMS", "true").lower() == "true",
        root / "multipart.sqlite3",
    )


class RetryableRegistrationError(RuntimeError):
    pass


class PermanentRegistrationError(RuntimeError):
    pass


class ArchiveObjectRegistrationClient:
    def __init__(
        self,
        base_url: str,
        bearer_file: Path,
        client: httpx.Client | None = None,
    ) -> None:
        self._url = f"{base_url.rstrip('/')}/api/internal/archive-object"
        self._bearer_file = bearer_file
        self._client = client or httpx.Client(timeout=10, follow_redirects=False)

    def register(
        self,
        meeting_id: UUID,
        spool_object_id: UUID,
        object_class: str,
        record: ObjectRecord,
    ) -> None:
        bearer = self._bearer_file.read_text("utf-8").strip()
        if not bearer:
            raise RuntimeError("spool drainer internal bearer is empty")
        response = self._client.post(
            self._url,
            headers={
                "Authorization": f"Bearer {bearer}",
                "Content-Type": "application/json",
            },
            json={
                "consultationId": str(meeting_id),
                "causalKey": str(spool_object_id),
                "object": {
                    "objectId": str(spool_object_id),
                    "class": object_class,
                    "key": record.key,
                    "versionId": record.version_id,
                    "size": record.size,
                    "sha256": record.sha256,
                    "s3Checksum": record.s3_checksum,
                    "contentType": record.content_type,
                    "sampleRange": None,
                    "attempt": None,
                    "sequence": None,
                },
            },
        )
        if response.status_code // 100 != 2:
            error_type = (
                RetryableRegistrationError
                if response.status_code in {408, 425, 429} or response.status_code >= 500
                else PermanentRegistrationError
            )
            response_code = "unknown"
            try:
                response_body = response.json()
                if isinstance(response_body, dict) and isinstance(response_body.get("code"), str):
                    response_code = response_body["code"]
            except ValueError:
                pass
            raise error_type(
                "archive object registration rejected with "
                f"HTTP {response.status_code} ({response_code})"
            )


def _object_class(stage: str) -> str:
    if stage == "terminal" or stage.endswith("-terminal"):
        return "provider_terminal"
    if stage == "checkpoint":
        return "checkpoint"
    if stage == "caption":
        return "caption_ledger"
    return "pipeline_exchange"


def _compact_pcm_scopes(spool: EncryptedSpool, archive: S3Archive) -> None:
    for meeting, stage, direction in spool.pcm_scopes():
        sample_rate = 16_000 if stage == "stt-input" else 48_000
        compactor = PcmCompactor(spool, archive, meeting, sample_rate)
        terminal_checkpoint = spool.covering_checkpoint(meeting, stage, direction, 0, True)
        closed_objects = compactor.compact(
            stage,
            direction,
            drain=terminal_checkpoint is not None,
        )
        for closed_pcm_object in closed_objects:
            checkpoint = spool.covering_checkpoint(
                meeting,
                stage,
                direction,
                closed_pcm_object.samples.end,
            )
            if checkpoint is not None:
                compactor.acknowledge_covering_checkpoint(
                    closed_pcm_object,
                    str(checkpoint),
                )


def _register_object(
    register: Callable[[UUID, UUID, str, ObjectRecord], None],
    meeting: UUID,
    object_id: UUID,
    object_class: str,
    record: ObjectRecord,
) -> None:
    normalized_class = _normalized_object_class(object_class)
    with _span(
        "transhooter.worker.spool.register",
        {"object.class": normalized_class},
    ) as span:
        try:
            register(meeting, object_id, object_class, record)
        except PermanentRegistrationError as error:
            _record_registration("permanent", normalized_class, error)
            _finish_span(span, "permanent", error)
            raise
        except BaseException as error:
            _record_registration("retryable", normalized_class, error)
            _finish_span(span, "retryable", error)
            raise
        _record_registration("ok", normalized_class)
        _finish_span(span, "ok")


async def _register_object_async(
    register: Callable[[UUID, UUID, str, ObjectRecord], Awaitable[None]],
    meeting: UUID,
    object_id: UUID,
    object_class: str,
    record: ObjectRecord,
) -> None:
    normalized_class = _normalized_object_class(object_class)
    with _span(
        "transhooter.worker.spool.register",
        {"object.class": normalized_class},
    ) as span:
        try:
            await register(meeting, object_id, object_class, record)
        except PermanentControlRequestError as error:
            _record_registration("permanent", normalized_class, error)
            _finish_span(span, "permanent", error)
            raise
        except BaseException as error:
            _record_registration("retryable", normalized_class, error)
            _finish_span(span, "retryable", error)
            raise
        _record_registration("ok", normalized_class)
        _finish_span(span, "ok")


def upload_committed_objects(
    spool: EncryptedSpool,
    archive: ArchiveStore,
    register: Callable[[UUID, UUID, str, ObjectRecord], None],
    meeting_id: UUID | None = None,
) -> None:
    pcm_stages = {"stt-input", "tts-output", "livekit-output"}
    for spool_ref, _ in spool.committed():
        meeting, attempt, stage, ordinal, media_type = spool.context(spool_ref.object_id)
        if (
            (meeting_id is not None and meeting != meeting_id)
            or stage in pcm_stages
            or stage == "checkpoint"
        ):
            continue
        suffix = "json" if "json" in media_type else "bin"
        key = f"v1/meetings/{meeting}/pipeline/{stage}/raw/{attempt}/{ordinal:020d}.{suffix}"
        body = spool.read(spool_ref.object_id)
        archive_record = archive.put_create_once(
            key,
            body,
            media_type,
            hashlib.sha256(body).hexdigest(),
        )
        object_class = _object_class(stage)
        try:
            _register_object(
                register,
                meeting,
                spool_ref.object_id,
                object_class,
                archive_record,
            )
        except PermanentRegistrationError as error:
            spool.quarantine(spool_ref.object_id)
            _record_object("quarantined", object_class)
            logger.error(
                "spool object result=quarantined object.class=%s error.kind=%s",
                _normalized_object_class(object_class),
                bounded_error_kind(error),
            )
            continue
        spool.mark_uploaded(
            spool_ref.object_id,
            archive_record.version_id,
            archive_record.s3_checksum,
        )
        _record_object("uploaded", object_class)


async def upload_committed_objects_async(
    spool: EncryptedSpool,
    archive: ArchiveStore,
    register: Callable[[UUID, UUID, str, ObjectRecord], Awaitable[None]],
    meeting_id: UUID,
) -> None:
    pcm_stages = {"stt-input", "tts-output", "livekit-output"}
    for spool_ref, _ in spool.committed():
        meeting, attempt, stage, ordinal, media_type = spool.context(spool_ref.object_id)
        if meeting != meeting_id or stage in pcm_stages or stage == "checkpoint":
            continue
        suffix = "json" if "json" in media_type else "bin"
        key = f"v1/meetings/{meeting}/pipeline/{stage}/raw/{attempt}/{ordinal:020d}.{suffix}"
        body = spool.read(spool_ref.object_id)
        archive_record = archive.put_create_once(
            key,
            body,
            media_type,
            hashlib.sha256(body).hexdigest(),
        )
        object_class = _object_class(stage)
        try:
            await _register_object_async(
                register,
                meeting,
                spool_ref.object_id,
                object_class,
                archive_record,
            )
        except PermanentControlRequestError as error:
            spool.quarantine(spool_ref.object_id)
            _record_object("quarantined", object_class)
            logger.error(
                "spool object result=quarantined object.class=%s error.kind=%s",
                _normalized_object_class(object_class),
                bounded_error_kind(error),
            )
            continue
        spool.mark_uploaded(
            spool_ref.object_id,
            archive_record.version_id,
            archive_record.s3_checksum,
        )
        _record_object("uploaded", object_class)


def _drain_once(
    spool: EncryptedSpool,
    archive: S3Archive,
    register: Callable[[UUID, UUID, str, ObjectRecord], None],
) -> None:
    started = time.monotonic()
    result = "failed"
    drain_error: BaseException | None = None
    with _span("transhooter.worker.spool.drain") as span:
        try:
            _compact_pcm_scopes(spool, archive)
            upload_committed_objects(spool, archive, register)
        except (httpx.TransportError, RetryableRegistrationError) as error:
            result = "deferred"
            drain_error = error
            raise
        except BaseException as error:
            drain_error = error
            raise
        else:
            result = "ok"
        finally:
            _record_drain(result, time.monotonic() - started, drain_error)
            _record_utilization(spool)
            _finish_span(span, result, drain_error)


def main() -> None:
    telemetry = configure_telemetry(
        os.environ.get("OTEL_SERVICE_NAME", "").strip() or "transhooter-spool-drainer",
        endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"),
        environment=os.environ.get("APP_ENV"),
        metric_export_interval_millis=_metric_export_interval(),
    )
    try:
        token_file = (
            os.environ.get("INTERNAL_TOKEN_FILE")
            or os.environ.get("WORKER_INTERNAL_BEARER_FILE")
            or os.environ.get("INTERNAL_SERVICE_ACCOUNT_TOKEN_FILE")
        )
        if not token_file:
            raise RuntimeError("spool drainer internal bearer file is required")
        root = Path(os.environ.get("SPOOL_PATH", os.environ.get("SPOOL_DIR", "")))
        spool = _build_spool(root)
        archive = build_archive(root)
        registration = ArchiveObjectRegistrationClient(
            os.environ["CONTROL_INTERNAL_URL"],
            Path(token_file),
        )
        drain_once = os.environ.get("DRAIN_ONCE", "false").lower() == "true"
        while True:
            try:
                _drain_once(spool, archive, registration.register)
            except (httpx.TransportError, RetryableRegistrationError) as error:
                if drain_once:
                    raise
                logger.warning(
                    "spool drain result=deferred error.kind=%s",
                    bounded_error_kind(error),
                )
            else:
                if drain_once:
                    return
            time.sleep(1)
    finally:
        telemetry.shutdown()


if __name__ == "__main__":
    main()
