from __future__ import annotations

import asyncio
import concurrent.futures
import errno
from collections.abc import Iterator

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from transhooter_spool_drainer import telemetry


class AbortError(Exception):
    pass


class NetworkError(Exception):
    pass


class ValidationError(Exception):
    pass


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (asyncio.CancelledError(), "aborted"),
        (concurrent.futures.CancelledError(), "aborted"),
        (OSError(errno.ECANCELED, "cancelled"), "aborted"),
        (AbortError(), "aborted"),
        (TimeoutError(), "timeout"),
        (OSError(errno.ETIMEDOUT, "timed out"), "timeout"),
        (TypeError(), "validation"),
        (ValueError(), "validation"),
        (OSError(errno.EINVAL, "invalid"), "validation"),
        (ValidationError(), "validation"),
        (ConnectionError(), "unavailable"),
        (OSError(errno.ECONNREFUSED, "refused"), "unavailable"),
        (NetworkError(), "unavailable"),
        (RuntimeError(), "other"),
    ],
)
def test_bounded_error_kind_has_a_bounded_classification(
    error: BaseException, expected: telemetry.ErrorKind
) -> None:
    assert telemetry.bounded_error_kind(error) == expected


def test_metric_views_apply_operational_buckets_only_to_matching_histograms(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OTEL_SDK_DISABLED", raising=False)
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader], views=telemetry._metric_views())
    meter = provider.get_meter("telemetry-test")

    meter.create_histogram("transhooter.test.latency", unit="s").record(0.012)
    meter.create_histogram("transhooter.test.ratio", unit="1").record(0.75)
    meter.create_histogram("transhooter.test.count", unit="{item}").record(3)
    meter.create_histogram("external.test.latency", unit="s").record(0.012)

    try:
        metrics_data = reader.get_metrics_data()
        assert metrics_data is not None
        exported = {
            metric.name: metric
            for resource_metrics in metrics_data.resource_metrics
            for scope_metrics in resource_metrics.scope_metrics
            for metric in scope_metrics.metrics
        }

        latency_bounds = exported["transhooter.test.latency"].data.data_points[0].explicit_bounds
        ratio_bounds = exported["transhooter.test.ratio"].data.data_points[0].explicit_bounds
        count_bounds = exported["transhooter.test.count"].data.data_points[0].explicit_bounds
        external_bounds = exported["external.test.latency"].data.data_points[0].explicit_bounds

        assert latency_bounds == telemetry._LATENCY_BUCKET_BOUNDARIES
        assert ratio_bounds == telemetry._RATIO_BUCKET_BOUNDARIES
        assert count_bounds not in (latency_bounds, ratio_bounds)
        assert external_bounds != latency_bounds
    finally:
        provider.shutdown()


@pytest.fixture
def isolated_disabled_telemetry(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    enabled_attempts: list[str] = []

    def unexpected_enabled_handle(**_kwargs: object) -> telemetry.TelemetryHandle:
        enabled_attempts.append("attempted")
        raise AssertionError("disabled telemetry must not construct exporters")

    monkeypatch.setattr(telemetry, "_handle", None)
    monkeypatch.setattr(telemetry, "_enabled_handle", unexpected_enabled_handle)
    for name in (
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("OTEL_SDK_DISABLED", raising=False)
    yield enabled_attempts


def assert_repeated_disabled_lifecycle_is_a_noop(handle: telemetry.TelemetryHandle) -> None:
    handle.force_flush()
    handle.force_flush()
    handle.shutdown()
    handle.shutdown()
    handle.force_flush()


def test_configure_telemetry_is_idempotently_disabled_without_endpoint(
    isolated_disabled_telemetry: list[str],
) -> None:
    first = telemetry.configure_telemetry("test-worker")
    second = telemetry.configure_telemetry(
        "ignored-after-first-configuration", endpoint="https://collector.invalid"
    )

    assert first is second
    assert first.enabled is False
    assert isolated_disabled_telemetry == []
    assert_repeated_disabled_lifecycle_is_a_noop(first)


def test_configure_telemetry_is_idempotently_disabled_when_sdk_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
    isolated_disabled_telemetry: list[str],
) -> None:
    monkeypatch.setenv("OTEL_SDK_DISABLED", " TRUE ")

    first = telemetry.configure_telemetry("test-worker", endpoint="https://collector.invalid")
    second = telemetry.configure_telemetry("test-worker")

    assert first is second
    assert first.enabled is False
    assert isolated_disabled_telemetry == []
    assert_repeated_disabled_lifecycle_is_a_noop(first)


def test_enabled_handle_lifecycle_skips_an_unconfigured_signal() -> None:
    class Provider:
        def __init__(self) -> None:
            self.flushes = 0
            self.shutdowns = 0

        def force_flush(self) -> None:
            self.flushes += 1

        def shutdown(self) -> None:
            self.shutdowns += 1

    provider = Provider()
    handle = telemetry.TelemetryHandle(
        tracer=telemetry.trace.NoOpTracerProvider().get_tracer("test"),
        meter=telemetry.metrics.NoOpMeterProvider().get_meter("test"),
        enabled=True,
        tracer_provider=provider,
    )

    handle.force_flush()
    handle.force_flush()
    handle.shutdown()
    handle.shutdown()
    handle.force_flush()

    assert provider.flushes == 2
    assert provider.shutdowns == 1


@pytest.mark.parametrize(
    ("environment", "configured_common", "expected"),
    [
        (
            {"OTEL_EXPORTER_OTLP_ENDPOINT": "https://common.invalid/root"},
            None,
            (
                "https://common.invalid/root/v1/traces",
                "https://common.invalid/root/v1/metrics",
            ),
        ),
        (
            {"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "https://traces.invalid/custom"},
            None,
            ("https://traces.invalid/custom", None),
        ),
        (
            {"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "https://metrics.invalid/custom"},
            None,
            (None, "https://metrics.invalid/custom"),
        ),
        (
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": "https://environment-common.invalid",
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "https://traces.invalid/override",
                "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "https://metrics.invalid/override",
            },
            "https://option-common.invalid/base",
            (
                "https://traces.invalid/override",
                "https://metrics.invalid/override",
            ),
        ),
    ],
    ids=["common-only", "traces-only", "metrics-only", "signal-overrides"],
)
def test_signal_endpoints_are_configured_independently(
    monkeypatch: pytest.MonkeyPatch,
    environment: dict[str, str],
    configured_common: str | None,
    expected: tuple[str | None, str | None],
) -> None:
    for name in (
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    ):
        monkeypatch.delenv(name, raising=False)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    assert telemetry._configured_signal_endpoints(configured_common) == expected


@pytest.mark.parametrize(
    ("environment", "expected"),
    [
        (
            {"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "https://traces.invalid/v1/traces"},
            ("https://traces.invalid/v1/traces", None),
        ),
        (
            {"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "https://metrics.invalid/v1/metrics"},
            (None, "https://metrics.invalid/v1/metrics"),
        ),
    ],
    ids=["traces-only", "metrics-only"],
)
def test_configure_passes_only_configured_signals_to_enabled_handle(
    monkeypatch: pytest.MonkeyPatch,
    environment: dict[str, str],
    expected: tuple[str | None, str | None],
) -> None:
    captured: list[tuple[str | None, str | None]] = []
    sentinel = telemetry._disabled_handle("sentinel")

    def capture_enabled_handle(**kwargs: object) -> telemetry.TelemetryHandle:
        signal_endpoints = kwargs["signal_endpoints"]
        assert isinstance(signal_endpoints, tuple) and len(signal_endpoints) == 2
        trace_endpoint, metric_endpoint = signal_endpoints
        assert trace_endpoint is None or isinstance(trace_endpoint, str)
        assert metric_endpoint is None or isinstance(metric_endpoint, str)
        captured.append((trace_endpoint, metric_endpoint))
        return sentinel

    monkeypatch.setattr(telemetry, "_handle", None)
    monkeypatch.setattr(telemetry, "_enabled_handle", capture_enabled_handle)
    monkeypatch.delenv("OTEL_SDK_DISABLED", raising=False)
    for name in (
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    ):
        monkeypatch.delenv(name, raising=False)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    assert telemetry.configure_telemetry("test-worker") is sentinel
    assert captured == [expected]


def test_service_version_uses_drainer_distribution(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(telemetry, "version", lambda name: "9.8.7" if name == "transhooter-spool-drainer" else "wrong")
    assert telemetry._service_version() == "9.8.7"
