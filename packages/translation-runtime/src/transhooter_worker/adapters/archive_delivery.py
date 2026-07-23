from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable
from concurrent.futures import Future, ThreadPoolExecutor
from functools import partial
from pathlib import Path
from types import TracebackType
from typing import ParamSpec, TypeVar
from uuid import UUID

import boto3  # type: ignore[import-untyped]
import httpx
from opentelemetry import metrics, trace
from opentelemetry.trace import Span, Status, StatusCode

from transhooter_worker.adapters.s3_archive import S3Archive
from transhooter_worker.adapters.spool import EncryptedSpool
from transhooter_worker.ports.archive import ArchiveStore, ObjectRecord
from transhooter_worker.runtime.control_client import PermanentControlRequestError
from transhooter_worker.telemetry import bounded_error_kind

logger = logging.getLogger(__name__)

_METER = metrics.get_meter(__name__)
_TRACER = trace.get_tracer(__name__)
_REGISTRATIONS = _METER.create_counter(
    "transhooter.worker.spool.registrations.total",
    unit="{registration}",
)
_OBJECTS = _METER.create_counter(
    "transhooter.worker.spool.objects.total",
    unit="{object}",
)
_OBJECT_CLASSES = frozenset(
    {"provider_terminal", "checkpoint", "caption_ledger", "pipeline_exchange"}
)

_P = ParamSpec("_P")
_T = TypeVar("_T")


class ArchiveDeliveryExecutor:
    """Single thread owner for a terminal spool/archive delivery sequence."""

    def __init__(self) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="transhooter-archive-delivery",
        )
        self._loop: asyncio.AbstractEventLoop | None = None
        self._cancel_requested = False

    async def __aenter__(self) -> ArchiveDeliveryExecutor:
        self._cancel_requested = False
        self._loop = asyncio.get_running_loop()
        return self

    async def __aexit__(
        self,
        _error_type: type[BaseException] | None,
        _error: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        self._executor.shutdown(wait=True, cancel_futures=True)
        self._loop = None

    async def run(
        self,
        operation: Callable[_P, _T],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T:
        loop = asyncio.get_running_loop()
        if self._loop is None or loop is not self._loop:
            raise RuntimeError("archive delivery executor used outside its owning event loop")
        if self._cancel_requested:
            raise asyncio.CancelledError
        future: asyncio.Future[_T] = loop.run_in_executor(
            self._executor,
            partial(operation, *args, **kwargs),
        )
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            self._cancel_requested = True
            await future
            raise


def _normalized_object_class(object_class: str) -> str:
    return object_class if object_class in _OBJECT_CLASSES else "pipeline_exchange"


def _secret(name: str) -> str:
    path = os.environ.get(name + "_FILE")
    if path:
        value = Path(path).read_text("utf-8").strip()
    else:
        value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name}_FILE is required")
    return value


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
    def __init__(self, message: str, failures: tuple[Exception, ...] = ()) -> None:
        super().__init__(message)
        self.failures = failures


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
                if response.status_code in {401, 403, 408, 425, 429} or response.status_code >= 500
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


def _record_registration(
    result: str,
    object_class: str,
    error: BaseException | None = None,
) -> None:
    attributes = {"result": result, "object.class": _normalized_object_class(object_class)}
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
            {"result": result, "object.class": _normalized_object_class(object_class)},
        )
    except Exception:
        pass


def _finish_span(span: Span, result: str, error: BaseException | None = None) -> None:
    try:
        span.set_attribute("result", result)
        if error is None:
            span.set_status(Status(StatusCode.OK))
            return
        span.set_attribute("error.kind", bounded_error_kind(error))
        span.set_status(Status(StatusCode.ERROR))
    except Exception:
        pass


def _register_object(
    register: Callable[[UUID, UUID, str, ObjectRecord], None],
    meeting: UUID,
    object_id: UUID,
    object_class: str,
    record: ObjectRecord,
) -> None:
    normalized_class = _normalized_object_class(object_class)
    with _TRACER.start_as_current_span(
        "transhooter.worker.spool.register",
        attributes={"object.class": normalized_class},
        record_exception=False,
        set_status_on_exception=False,
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


def _is_transient_auth_rejection(error: PermanentControlRequestError) -> bool:
    message = str(error)
    return message.endswith("HTTP 401") or message.endswith("HTTP 403")


def _raise_registration_failures(failures: list[Exception]) -> None:
    if not failures:
        return
    if len(failures) == 1:
        raise failures[0]
    raise RetryableRegistrationError(
        f"{len(failures)} archive object registrations remain retryable",
        tuple(failures),
    )


def upload_committed_objects(
    spool: EncryptedSpool,
    archive: ArchiveStore,
    register: Callable[[UUID, UUID, str, ObjectRecord], None],
    meeting_id: UUID | None = None,
) -> None:
    retryable_failures: list[Exception] = []
    records = (
        spool.committed_drainable(meeting_id)
        if meeting_id is not None
        else (
            (spool_ref, sample_range, spool.context(spool_ref.object_id))
            for spool_ref, sample_range in spool.committed()
        )
    )
    for spool_ref, _, context in records:
        meeting, attempt, stage, ordinal, media_type = context
        if stage in {"stt-input", "tts-output", "livekit-output", "checkpoint"}:
            continue
        suffix = "json" if "json" in media_type else "bin"
        key = f"v1/meetings/{meeting}/pipeline/{stage}/raw/{attempt}/{ordinal:020d}.{suffix}"
        body = spool.read(spool_ref.object_id)
        archive_record = archive.put_create_once(
            key,
            body,
            media_type,
            spool_ref.sha256,
        )
        object_class = _object_class(stage)
        try:
            _register_object(register, meeting, spool_ref.object_id, object_class, archive_record)
        except PermanentRegistrationError as error:
            spool.quarantine(spool_ref.object_id)
            _record_object("quarantined", object_class)
            logger.error(
                "spool object result=quarantined object.class=%s error.kind=%s",
                _normalized_object_class(object_class),
                bounded_error_kind(error),
            )
            continue
        except Exception as error:
            retryable_failures.append(error)
            continue
        spool.mark_uploaded(
            spool_ref.object_id,
            archive_record.version_id,
            archive_record.s3_checksum,
        )
        _record_object("uploaded", object_class)
    spool.compact_uploaded_envelopes()
    _raise_registration_failures(retryable_failures)


async def _registration_coroutine(
    register: Callable[[UUID, UUID, str, ObjectRecord], Awaitable[None]],
    meeting: UUID,
    object_id: UUID,
    object_class: str,
    record: ObjectRecord,
) -> None:
    await register(meeting, object_id, object_class, record)


def _run_registration_on_loop(
    loop: asyncio.AbstractEventLoop,
    register: Callable[[UUID, UUID, str, ObjectRecord], Awaitable[None]],
    meeting: UUID,
    object_id: UUID,
    object_class: str,
    record: ObjectRecord,
) -> None:
    registration = _registration_coroutine(register, meeting, object_id, object_class, record)
    future: Future[None] = asyncio.run_coroutine_threadsafe(registration, loop)
    try:
        future.result()
    except PermanentControlRequestError as error:
        if _is_transient_auth_rejection(error):
            raise RetryableRegistrationError(str(error), (error,)) from error
        raise PermanentRegistrationError(str(error)) from error


def _upload_committed_objects_with_async_registration(
    spool: EncryptedSpool,
    archive: ArchiveStore,
    register: Callable[[UUID, UUID, str, ObjectRecord], Awaitable[None]],
    meeting_id: UUID,
    loop: asyncio.AbstractEventLoop,
) -> None:
    upload_committed_objects(
        spool,
        archive,
        lambda meeting, object_id, object_class, record: _run_registration_on_loop(
            loop,
            register,
            meeting,
            object_id,
            object_class,
            record,
        ),
        meeting_id,
    )


async def upload_committed_objects_async(
    spool: EncryptedSpool,
    archive: ArchiveStore,
    register: Callable[[UUID, UUID, str, ObjectRecord], Awaitable[None]],
    meeting_id: UUID,
    executor: ArchiveDeliveryExecutor | None = None,
) -> None:
    async def upload(owner: ArchiveDeliveryExecutor) -> None:
        await owner.run(
            _upload_committed_objects_with_async_registration,
            spool,
            archive,
            register,
            meeting_id,
            asyncio.get_running_loop(),
        )

    if executor is not None:
        await upload(executor)
        return
    async with ArchiveDeliveryExecutor() as owner:
        await upload(owner)
