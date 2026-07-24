from __future__ import annotations

import asyncio
import concurrent.futures
import errno
import os
import socket
import threading
from importlib.metadata import PackageNotFoundError, version
from typing import Any, Literal
from urllib.parse import urlsplit, urlunsplit

from opentelemetry import metrics, trace
from opentelemetry.metrics import Meter
from opentelemetry.trace import Tracer

ErrorKind = Literal["aborted", "timeout", "validation", "unavailable", "other"]

_SERVICE_NAMESPACE = "transhooter"
_DEFAULT_METRIC_EXPORT_INTERVAL_MILLIS = 60_000
_LATENCY_BUCKET_BOUNDARIES = (
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1,
    2.5,
    5,
    10,
    30,
)
_RATIO_BUCKET_BOUNDARIES = (0, 0.1, 0.25, 0.5, 0.7, 0.8, 0.9, 0.95, 1)
_UNAVAILABLE_ERRNOS = frozenset(
    {
        socket.EAI_AGAIN,
        errno.ECONNABORTED,
        errno.ECONNREFUSED,
        errno.ECONNRESET,
        errno.EHOSTUNREACH,
        errno.ENETDOWN,
        errno.ENETUNREACH,
        errno.EPIPE,
    }
)
_ABORTED_ERROR_NAMES = frozenset({"AbortError", "CancelledError"})
_UNAVAILABLE_ERROR_NAMES = frozenset({"ConnectError", "NetworkError", "ServiceUnavailableError"})
_handle: TelemetryHandle | None = None
_configure_lock = threading.Lock()


class TelemetryHandle:
    def __init__(
        self,
        *,
        tracer: Tracer,
        meter: Meter,
        enabled: bool,
        tracer_provider: Any = None,
        meter_provider: Any = None,
    ) -> None:
        self.tracer = tracer
        self.meter = meter
        self.enabled = enabled
        self._tracer_provider = tracer_provider
        self._meter_provider = meter_provider
        self._closed = False
        self._lifecycle_lock = threading.Lock()

    def force_flush(self) -> None:
        if not self.enabled:
            return
        with self._lifecycle_lock:
            if self._closed:
                return
            for provider in (self._tracer_provider, self._meter_provider):
                if provider is None:
                    continue
                try:
                    provider.force_flush()
                except Exception:
                    pass

    def shutdown(self) -> None:
        if not self.enabled:
            return
        with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            for provider in (self._meter_provider, self._tracer_provider):
                if provider is None:
                    continue
                try:
                    provider.shutdown()
                except Exception:
                    pass


def bounded_error_kind(error: BaseException) -> ErrorKind:
    if isinstance(error, asyncio.CancelledError | concurrent.futures.CancelledError):
        return "aborted"
    if isinstance(error, TimeoutError):
        return "timeout"
    error_name = type(error).__name__
    if isinstance(error, TypeError | ValueError) or error_name == "ValidationError":
        return "validation"
    if isinstance(error, ConnectionError):
        return "unavailable"
    if isinstance(error, OSError):
        if error.errno == errno.ECANCELED:
            return "aborted"
        if error.errno == errno.ETIMEDOUT:
            return "timeout"
        if error.errno == errno.EINVAL:
            return "validation"
        if error.errno in _UNAVAILABLE_ERRNOS:
            return "unavailable"
    if error_name in _ABORTED_ERROR_NAMES:
        return "aborted"
    if error_name in _UNAVAILABLE_ERROR_NAMES:
        return "unavailable"
    return "other"


def configure_telemetry(
    service_name: str,
    endpoint: str | None = None,
    environment: str | None = None,
    metric_export_interval_millis: int | None = None,
) -> TelemetryHandle:
    global _handle

    with _configure_lock:
        if _handle is not None:
            return _handle

        signal_endpoints = _configured_signal_endpoints(endpoint)
        if _sdk_disabled() or not any(signal_endpoints):
            _handle = _disabled_handle(service_name)
            return _handle

        try:
            _handle = _enabled_handle(
                service_name=service_name,
                signal_endpoints=signal_endpoints,
                environment=environment,
                metric_export_interval_millis=metric_export_interval_millis,
            )
        except Exception:
            _handle = _disabled_handle(service_name)
        return _handle


def _sdk_disabled() -> bool:
    return os.environ.get("OTEL_SDK_DISABLED", "").strip().lower() == "true"


def _disabled_handle(service_name: str) -> TelemetryHandle:
    service_version = _service_version()
    return TelemetryHandle(
        tracer=trace.NoOpTracerProvider().get_tracer(service_name, service_version),
        meter=metrics.NoOpMeterProvider().get_meter(service_name, service_version),
        enabled=False,
    )


def _metric_views() -> list[Any]:
    from opentelemetry.sdk.metrics.view import (
        ExplicitBucketHistogramAggregation,
        View,
    )

    return [
        View(
            instrument_name="transhooter.*",
            instrument_type=metrics.Histogram,
            instrument_unit="s",
            aggregation=ExplicitBucketHistogramAggregation(boundaries=_LATENCY_BUCKET_BOUNDARIES),
        ),
        View(
            instrument_name="transhooter.*",
            instrument_type=metrics.Histogram,
            instrument_unit="1",
            aggregation=ExplicitBucketHistogramAggregation(boundaries=_RATIO_BUCKET_BOUNDARIES),
        ),
    ]


def _enabled_handle(
    *,
    service_name: str,
    signal_endpoints: tuple[str | None, str | None],
    environment: str | None,
    metric_export_interval_millis: int | None,
) -> TelemetryHandle:
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    service_version = _service_version()
    attributes: dict[str, str] = {
        "service.name": service_name,
        "service.namespace": _SERVICE_NAMESPACE,
        "service.version": service_version,
    }
    if environment and environment.strip():
        attributes["deployment.environment.name"] = environment.strip()
    resource = Resource.create(attributes)
    trace_endpoint, metric_endpoint = signal_endpoints
    export_interval = _metric_export_interval(metric_export_interval_millis)

    span_processor: Any = None
    tracer_provider: Any = None
    metric_reader: Any = None
    meter_provider: Any = None
    try:
        if trace_endpoint is not None:
            span_processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=trace_endpoint))
            tracer_provider = TracerProvider(resource=resource)
            tracer_provider.add_span_processor(span_processor)

        if metric_endpoint is not None:
            metric_reader = PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint=metric_endpoint),
                export_interval_millis=export_interval,
            )
            meter_provider = MeterProvider(
                resource=resource,
                metric_readers=[metric_reader],
                views=_metric_views(),
            )
        if tracer_provider is not None:
            trace.set_tracer_provider(tracer_provider)
        if meter_provider is not None:
            metrics.set_meter_provider(meter_provider)
        return TelemetryHandle(
            tracer=(
                tracer_provider.get_tracer(service_name, service_version)
                if tracer_provider is not None
                else trace.NoOpTracerProvider().get_tracer(service_name, service_version)
            ),
            meter=(
                meter_provider.get_meter(service_name, service_version)
                if meter_provider is not None
                else metrics.NoOpMeterProvider().get_meter(service_name, service_version)
            ),
            enabled=True,
            tracer_provider=tracer_provider,
            meter_provider=meter_provider,
        )
    except Exception:
        if meter_provider is not None:
            _quiet_shutdown(meter_provider)
        elif metric_reader is not None:
            _quiet_shutdown(metric_reader)
        if tracer_provider is not None:
            _quiet_shutdown(tracer_provider)
        elif span_processor is not None:
            _quiet_shutdown(span_processor)
        raise


def _configured_signal_endpoints(
    configured_common_endpoint: str | None,
) -> tuple[str | None, str | None]:
    common_endpoint = (
        configured_common_endpoint.strip()
        if configured_common_endpoint is not None
        else os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    )
    trace_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "").strip()
    metric_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "").strip()
    common_signal_endpoints = (
        _signal_endpoints(common_endpoint) if common_endpoint else (None, None)
    )
    return (
        trace_endpoint or common_signal_endpoints[0],
        metric_endpoint or common_signal_endpoints[1],
    )


def _signal_endpoints(endpoint: str) -> tuple[str, str]:
    parsed = urlsplit(endpoint.strip())
    path = parsed.path.rstrip("/")
    for suffix in ("/v1/traces", "/v1/metrics"):
        if path.endswith(suffix):
            path = path[: -len(suffix)].rstrip("/")
            break

    def signal_url(signal: str) -> str:
        signal_path = f"{path}/v1/{signal}" if path else f"/v1/{signal}"
        return urlunsplit(
            (parsed.scheme, parsed.netloc, signal_path, parsed.query, parsed.fragment)
        )

    return signal_url("traces"), signal_url("metrics")


def _metric_export_interval(configured: int | None) -> int:
    if configured is not None:
        valid = isinstance(configured, int) and not isinstance(configured, bool) and configured > 0
        return configured if valid else _DEFAULT_METRIC_EXPORT_INTERVAL_MILLIS
    raw = os.environ.get("OTEL_METRIC_EXPORT_INTERVAL", "").strip()
    try:
        interval = int(raw)
    except ValueError:
        return _DEFAULT_METRIC_EXPORT_INTERVAL_MILLIS
    return interval if interval > 0 else _DEFAULT_METRIC_EXPORT_INTERVAL_MILLIS


def _service_version() -> str:
    try:
        return version("transhooter-spool-drainer")
    except PackageNotFoundError:
        return "0.1.0"


def _quiet_shutdown(component: Any) -> None:
    try:
        component.shutdown()
    except Exception:
        pass
