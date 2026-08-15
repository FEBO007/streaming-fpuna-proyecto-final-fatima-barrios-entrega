"""Integration test for event-time, watermark and late-data behavior."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
)
from apache_beam.testing.test_pipeline import (
    TestPipeline as BeamTestPipeline,
)
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.windowed_value import PaneInfoTiming

from src.pipeline.aggregation import (
    AggregateVelocity,
    WindowVelocityEvents,
)
from src.pipeline.deduplication import DeduplicateEvents

WINDOW_START = int(datetime(2026, 7, 12, 10, 0, tzinfo=UTC).timestamp())
WINDOW_END = WINDOW_START + 60


def _event(
    *,
    event_id: str,
    event_time: str,
    amount: int,
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "event_id": event_id,
        "customer_id": "cust-9009",
        "event_time": event_time,
        "payload": {
            "amount": amount,
            "currency": "PYG",
            "transaction_type": "transfer",
            "channel": "mobile",
        },
    }


def _target_window_snapshot(
    element: tuple[str, dict[str, int]],
    window=beam.DoFn.WindowParam,
    pane_info=beam.DoFn.PaneInfoParam,
) -> Iterator[tuple[str, int, int, int, int, int]]:
    """Expose revisions emitted for the target sliding window."""
    if int(window.start) != WINDOW_START:
        return

    customer_id, aggregate = element

    yield (
        customer_id,
        aggregate["transaction_count"],
        aggregate["total_amount"],
        int(window.start),
        int(window.end),
        pane_info.timing,
    )


def test_watermark_late_data_deduplication_and_final_closure() -> None:
    """Verify ON_TIME, LATE, deduplication and definitive closure."""
    first_event = _event(
        event_id="txn-001",
        event_time="2026-07-12T10:00:05.000Z",
        amount=2_000_000,
    )
    duplicated_event = _event(
        event_id="txn-002",
        event_time="2026-07-12T10:00:20.000Z",
        amount=2_500_000,
    )
    third_event = _event(
        event_id="txn-004",
        event_time="2026-07-12T10:00:50.000Z",
        amount=3_000_000,
    )
    accepted_late_event = _event(
        event_id="txn-003",
        event_time="2026-07-12T10:00:35.000Z",
        amount=2_500_000,
    )
    rejected_too_late_event = _event(
        event_id="txn-006",
        event_time="2026-07-12T10:00:45.000Z",
        amount=99_000_000,
    )

    stream = (
        BeamTestStream()
        .advance_watermark_to(WINDOW_START)
        .add_elements(
            [
                TimestampedValue(first_event, WINDOW_START + 5),
                TimestampedValue(
                    duplicated_event,
                    WINDOW_START + 20,
                ),
                TimestampedValue(
                    duplicated_event,
                    WINDOW_START + 20,
                ),
                TimestampedValue(third_event, WINDOW_START + 50),
            ]
        )
        .advance_watermark_to(WINDOW_END)
        .add_elements(
            [
                TimestampedValue(
                    accepted_late_event,
                    WINDOW_START + 35,
                )
            ]
        )
        .advance_watermark_to(WINDOW_END + 31)
        .add_elements(
            [
                TimestampedValue(
                    rejected_too_late_event,
                    WINDOW_START + 45,
                )
            ]
        )
        .advance_watermark_to_infinity()
    )

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    expected = [
        (
            "cust-9009",
            3,
            7_500_000,
            WINDOW_START,
            WINDOW_END,
            PaneInfoTiming.ON_TIME,
        ),
        (
            "cust-9009",
            4,
            10_000_000,
            WINDOW_START,
            WINDOW_END,
            PaneInfoTiming.LATE,
        ),
    ]

    with BeamTestPipeline(
        runner="DirectRunner",
        options=options,
    ) as pipeline:
        snapshots = (
            pipeline
            | "CreateTemporalStream" >> stream
            | "ApplyTemporalPolicy" >> WindowVelocityEvents()
            | "DeduplicateTemporalEvents" >> DeduplicateEvents()
            | "AggregateTemporalVelocity" >> AggregateVelocity()
            | "KeepTargetWindowAndPaneMetadata" >> beam.FlatMap(_target_window_snapshot)
        )

        assert_that(snapshots, equal_to(expected))
