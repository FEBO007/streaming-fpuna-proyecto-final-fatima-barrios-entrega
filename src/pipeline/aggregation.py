"""Event-time windowing and incremental velocity aggregation."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

import apache_beam as beam
from apache_beam.transforms.trigger import (
    AccumulationMode,
    AfterCount,
    AfterProcessingTime,
    AfterWatermark,
    Repeatedly,
)
from apache_beam.transforms.window import SlidingWindows
from apache_beam.typehints import KV, Dict

WINDOW_SIZE_SECONDS = 60
WINDOW_PERIOD_SECONDS = 10
ALLOWED_LATENESS_SECONDS = 30
EARLY_FIRING_DELAY_SECONDS = 10

VelocityAccumulator = tuple[int, int]


def _amount_by_customer(
    element: tuple[str, Mapping[str, Any]],
) -> tuple[str, int]:
    """Extract the validated amount while preserving the customer key."""
    customer_id, event = element

    if not isinstance(customer_id, str) or not customer_id.strip():
        raise TypeError("validated customer_id must be a non-empty string")

    payload = event.get("payload")
    if not isinstance(payload, Mapping):
        raise TypeError("validated payload must be a mapping")

    amount = payload.get("amount")
    if isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
        raise TypeError("validated amount must be a positive integer")

    return customer_id, amount


class VelocityCombineFn(beam.CombineFn):
    """Incrementally count transactions and sum their amounts."""

    def create_accumulator(self) -> VelocityAccumulator:
        """Return an empty count-and-total accumulator."""
        return 0, 0

    def add_input(
        self,
        accumulator: VelocityAccumulator,
        amount: int,
    ) -> VelocityAccumulator:
        """Add one transaction amount to the partial aggregate."""
        transaction_count, total_amount = accumulator
        return transaction_count + 1, total_amount + amount

    def merge_accumulators(
        self,
        accumulators: Iterable[VelocityAccumulator],
    ) -> VelocityAccumulator:
        """Merge partial aggregates produced by separate workers."""
        transaction_count = 0
        total_amount = 0

        for partial_count, partial_total in accumulators:
            transaction_count += partial_count
            total_amount += partial_total

        return transaction_count, total_amount

    def extract_output(
        self,
        accumulator: VelocityAccumulator,
    ) -> dict[str, int]:
        """Expose the aggregate using domain-oriented field names."""
        transaction_count, total_amount = accumulator

        return {
            "transaction_count": transaction_count,
            "total_amount": total_amount,
        }


class WindowVelocityEvents(beam.PTransform):
    """Apply the project's sliding-window and trigger policy."""

    def __init__(
        self,
        *,
        window_size_seconds: int = WINDOW_SIZE_SECONDS,
        window_period_seconds: int = WINDOW_PERIOD_SECONDS,
        allowed_lateness_seconds: int = ALLOWED_LATENESS_SECONDS,
        early_firing_delay_seconds: int = EARLY_FIRING_DELAY_SECONDS,
        enable_processing_time_early_firings: bool = True,
    ) -> None:
        if window_size_seconds <= 0:
            raise ValueError("window_size_seconds must be positive")
        if window_period_seconds <= 0:
            raise ValueError("window_period_seconds must be positive")
        if window_period_seconds > window_size_seconds:
            raise ValueError("window_period_seconds must not exceed window_size_seconds")
        if allowed_lateness_seconds < 0:
            raise ValueError("allowed_lateness_seconds must not be negative")
        if early_firing_delay_seconds <= 0:
            raise ValueError("early_firing_delay_seconds must be positive")
        if not isinstance(enable_processing_time_early_firings, bool):
            raise TypeError("enable_processing_time_early_firings must be a boolean")

        self._window_size_seconds = window_size_seconds
        self._window_period_seconds = window_period_seconds
        self._allowed_lateness_seconds = allowed_lateness_seconds
        self._early_firing_delay_seconds = early_firing_delay_seconds
        self._enable_processing_time_early_firings = enable_processing_time_early_firings

    def expand(self, events):
        """Assign sliding windows with accumulating early/on-time/late panes."""
        if self._enable_processing_time_early_firings:
            trigger = AfterWatermark(
                early=Repeatedly(AfterProcessingTime(self._early_firing_delay_seconds)),
                late=AfterCount(1),
            )
        else:
            # FnApiRunner/DirectRunner does not provide a real-time clock for
            # this bounded Kafka smoke test. The full policy remains the
            # default; the smoke path emits the on-time pane at source close.
            trigger = AfterWatermark(late=AfterCount(1))

        return events | "ApplyVelocityWindowPolicy" >> beam.WindowInto(
            SlidingWindows(
                size=self._window_size_seconds,
                period=self._window_period_seconds,
            ),
            trigger=trigger,
            accumulation_mode=AccumulationMode.ACCUMULATING,
            allowed_lateness=self._allowed_lateness_seconds,
        )


class AggregateVelocity(beam.PTransform):
    """Aggregate unique customer-keyed events in each assigned window."""

    def expand(self, keyed_events):
        """Return count and total amount per customer and window."""
        return (
            keyed_events
            | "ExtractAmountByCustomer"
            >> beam.Map(_amount_by_customer).with_output_types(KV[str, int])
            | "CombineVelocityByCustomer"
            >> beam.CombinePerKey(VelocityCombineFn()).with_output_types(KV[str, Dict[str, int]])
        )
