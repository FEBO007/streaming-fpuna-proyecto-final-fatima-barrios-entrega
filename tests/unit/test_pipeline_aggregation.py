"""Unit tests for event-time windowing and velocity aggregation."""

from __future__ import annotations

from typing import Any

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import (
    TestPipeline as BeamTestPipeline,
)
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from src.pipeline.aggregation import (
    AggregateVelocity,
    VelocityCombineFn,
    WindowVelocityEvents,
)


def _event(
    *,
    event_id: str,
    customer_id: str,
    amount: int,
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "event_id": event_id,
        "customer_id": customer_id,
        "event_time": "1970-01-01T00:02:05.000Z",
        "payload": {
            "amount": amount,
            "currency": "PYG",
            "transaction_type": "transfer",
            "channel": "mobile",
        },
    }


def _event_window_metadata(
    event: dict[str, Any],
    window=beam.DoFn.WindowParam,  # noqa: B008
) -> tuple[str, int, int]:
    return (
        event["event_id"],
        int(float(window.start)),
        int(float(window.end)),
    )


def _aggregate_window_metadata(
    element: tuple[str, dict[str, int]],
    window=beam.DoFn.WindowParam,  # noqa: B008
) -> tuple[str, int, int, int, int]:
    customer_id, aggregate = element

    return (
        customer_id,
        aggregate["transaction_count"],
        aggregate["total_amount"],
        int(float(window.start)),
        int(float(window.end)),
    )


@pytest.mark.parametrize(
    ("arguments", "expected_message"),
    [
        (
            {"window_size_seconds": 0},
            "window_size_seconds must be positive",
        ),
        (
            {"window_period_seconds": 0},
            "window_period_seconds must be positive",
        ),
        (
            {
                "window_size_seconds": 10,
                "window_period_seconds": 20,
            },
            "window_period_seconds must not exceed window_size_seconds",
        ),
        (
            {"allowed_lateness_seconds": -1},
            "allowed_lateness_seconds must not be negative",
        ),
        (
            {"early_firing_delay_seconds": 0},
            "early_firing_delay_seconds must be positive",
        ),
    ],
)
def test_invalid_window_configuration_is_rejected(
    arguments: dict[str, int],
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        WindowVelocityEvents(**arguments)


def test_non_boolean_early_firing_switch_is_rejected() -> None:
    with pytest.raises(
        TypeError,
        match="enable_processing_time_early_firings must be a boolean",
    ):
        WindowVelocityEvents(  # type: ignore[arg-type]
            enable_processing_time_early_firings="false"
        )


def test_velocity_combine_fn_accumulates_and_merges() -> None:
    combine_fn = VelocityCombineFn()

    first_partial = combine_fn.create_accumulator()
    first_partial = combine_fn.add_input(first_partial, 2_500_000)
    first_partial = combine_fn.add_input(first_partial, 3_000_000)

    second_partial = combine_fn.create_accumulator()
    second_partial = combine_fn.add_input(second_partial, 4_500_000)

    merged = combine_fn.merge_accumulators([first_partial, second_partial])

    assert combine_fn.extract_output(merged) == {
        "transaction_count": 3,
        "total_amount": 10_000_000,
    }


def test_default_policy_assigns_six_sliding_windows() -> None:
    event = _event(
        event_id="evt-window-001",
        customer_id="cust-001",
        amount=2_500_000,
    )

    expected = [
        ("evt-window-001", window_start, window_start + 60)
        for window_start in [70, 80, 90, 100, 110, 120]
    ]

    with BeamTestPipeline(runner="FnApiRunner") as pipeline:
        result = (
            pipeline
            | "CreateWindowEvent" >> beam.Create([TimestampedValue(event, 125)])
            | "ApplyDefaultWindowPolicy" >> WindowVelocityEvents()
            | "AttachEventWindowMetadata" >> beam.Map(_event_window_metadata)
        )

        assert_that(result, equal_to(expected))


def test_aggregation_is_isolated_by_customer() -> None:
    inputs = [
        (
            "cust-001",
            _event(
                event_id="evt-aggregate-001",
                customer_id="cust-001",
                amount=2_000_000,
            ),
        ),
        (
            "cust-001",
            _event(
                event_id="evt-aggregate-002",
                customer_id="cust-001",
                amount=3_000_000,
            ),
        ),
        (
            "cust-002",
            _event(
                event_id="evt-aggregate-003",
                customer_id="cust-002",
                amount=7_000_000,
            ),
        ),
    ]

    expected = [
        (
            "cust-001",
            {
                "transaction_count": 2,
                "total_amount": 5_000_000,
            },
        ),
        (
            "cust-002",
            {
                "transaction_count": 1,
                "total_amount": 7_000_000,
            },
        ),
    ]

    with BeamTestPipeline(runner="FnApiRunner") as pipeline:
        result = (
            pipeline
            | "CreateKeyedEvents" >> beam.Create(inputs)
            | "AggregateVelocityByCustomer" >> AggregateVelocity()
        )

        assert_that(result, equal_to(expected))


def test_bounded_smoke_policy_aggregates_without_processing_time_clock() -> None:
    event = _event(
        event_id="evt-smoke-001",
        customer_id="cust-001",
        amount=10_000_000,
    )
    expected = [
        ("cust-001", 1, 10_000_000, window_start, window_start + 60)
        for window_start in [70, 80, 90, 100, 110, 120]
    ]

    with BeamTestPipeline(runner="FnApiRunner") as pipeline:
        result = (
            pipeline
            | "CreateBoundedSmokeEvent" >> beam.Create([TimestampedValue(("cust-001", event), 125)])
            | "ApplyBoundedSmokeWindowPolicy"
            >> WindowVelocityEvents(enable_processing_time_early_firings=False)
            | "AggregateBoundedSmokeEvent" >> AggregateVelocity()
            | "AttachBoundedSmokeMetadata" >> beam.Map(_aggregate_window_metadata)
        )

        assert_that(result, equal_to(expected))
