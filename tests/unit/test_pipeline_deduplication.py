"""Unit tests for window-scoped Beam event deduplication."""

from __future__ import annotations

from typing import Any

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import SlidingWindows, TimestampedValue

from src.pipeline.deduplication import DeduplicateEvents

WINDOW_SIZE_SECONDS = 60
WINDOW_PERIOD_SECONDS = 10
ALLOWED_LATENESS_SECONDS = 30


def _event(
    *,
    event_id: str,
    customer_id: str,
    event_time: str,
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "event_id": event_id,
        "customer_id": customer_id,
        "event_time": event_time,
        "payload": {
            "amount": 2_500_000,
            "currency": "PYG",
            "transaction_type": "transfer",
            "channel": "mobile",
        },
    }


def _with_window_metadata(
    element: tuple[str, dict[str, Any]],
    window=beam.DoFn.WindowParam,  # noqa: B008
) -> tuple[str, str, int, int]:
    customer_id, event = element

    return (
        customer_id,
        event["event_id"],
        int(float(window.start)),
        int(float(window.end)),
    )


def _apply_deduplication(pipeline, inputs):
    return (
        pipeline
        | "CreateTimestampedEvents" >> beam.Create(inputs)
        | "AssignSlidingWindows"
        >> beam.WindowInto(
            SlidingWindows(
                size=WINDOW_SIZE_SECONDS,
                period=WINDOW_PERIOD_SECONDS,
            ),
            allowed_lateness=ALLOWED_LATENESS_SECONDS,
        )
        | "DeduplicateEvents"
        >> DeduplicateEvents(allowed_lateness_seconds=ALLOWED_LATENESS_SECONDS)
        | "AttachWindowMetadata" >> beam.Map(_with_window_metadata)
    )


def _expected_rows(
    customer_ids: list[str],
    event_id: str,
    window_starts: list[int],
) -> list[tuple[str, str, int, int]]:
    return [
        (
            customer_id,
            event_id,
            window_start,
            window_start + WINDOW_SIZE_SECONDS,
        )
        for customer_id in customer_ids
        for window_start in window_starts
    ]


def test_negative_allowed_lateness_is_rejected() -> None:
    with pytest.raises(
        ValueError,
        match="allowed_lateness_seconds must not be negative",
    ):
        DeduplicateEvents(allowed_lateness_seconds=-1)


def test_duplicate_is_emitted_once_per_sliding_window() -> None:
    event = _event(
        event_id="evt-duplicate-001",
        customer_id="cust-001",
        event_time="1970-01-01T00:02:05.000Z",
    )

    inputs = [
        TimestampedValue(event, 125),
        TimestampedValue(dict(event), 125),
    ]

    expected = _expected_rows(
        ["cust-001"],
        "evt-duplicate-001",
        [70, 80, 90, 100, 110, 120],
    )

    with BeamTestPipeline() as pipeline:
        result = _apply_deduplication(pipeline, inputs)
        assert_that(result, equal_to(expected))


def test_same_event_id_is_isolated_between_customers() -> None:
    inputs = [
        TimestampedValue(
            _event(
                event_id="evt-shared-001",
                customer_id="cust-001",
                event_time="1970-01-01T00:02:05.000Z",
            ),
            125,
        ),
        TimestampedValue(
            _event(
                event_id="evt-shared-001",
                customer_id="cust-002",
                event_time="1970-01-01T00:02:05.000Z",
            ),
            125,
        ),
    ]

    expected = _expected_rows(
        ["cust-001", "cust-002"],
        "evt-shared-001",
        [70, 80, 90, 100, 110, 120],
    )

    with BeamTestPipeline() as pipeline:
        result = _apply_deduplication(pipeline, inputs)
        assert_that(result, equal_to(expected))


def test_same_event_id_is_allowed_in_non_overlapping_windows() -> None:
    inputs = [
        TimestampedValue(
            _event(
                event_id="evt-reused-001",
                customer_id="cust-001",
                event_time="1970-01-01T00:02:05.000Z",
            ),
            125,
        ),
        TimestampedValue(
            _event(
                event_id="evt-reused-001",
                customer_id="cust-001",
                event_time="1970-01-01T00:03:15.000Z",
            ),
            195,
        ),
    ]

    expected = _expected_rows(
        ["cust-001"],
        "evt-reused-001",
        [
            70,
            80,
            90,
            100,
            110,
            120,
            140,
            150,
            160,
            170,
            180,
            190,
        ],
    )

    with BeamTestPipeline() as pipeline:
        result = _apply_deduplication(pipeline, inputs)
        assert_that(result, equal_to(expected))
