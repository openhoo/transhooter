from __future__ import annotations

import asyncio
import concurrent.futures
import errno
from collections.abc import Iterator

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from transhooter_worker import telemetry


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
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
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
