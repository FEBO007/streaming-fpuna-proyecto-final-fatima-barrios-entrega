"""Window-scoped deduplication for validated transaction events."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import apache_beam as beam
from apache_beam.coders import StrUtf8Coder
from apache_beam.metrics.metric import Metrics
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import SetStateSpec, TimerSpec, on_timer
from apache_beam.typehints import KV, Dict
from apache_beam.utils.timestamp import Duration


def _key_by_customer_id(
    event: Mapping[str, Any],
) -> tuple[str, dict[str, Any]]:
    customer_id = event["customer_id"]
    if not isinstance(customer_id, str):
        raise TypeError("validated customer_id must be a string")
    return customer_id, dict(event)


class DeduplicateByEventId(beam.DoFn):
    """Keep the first event_id for each customer and Beam window."""

    SEEN_IDS = SetStateSpec("seen_event_ids", StrUtf8Coder())
    STATE_EXPIRY = TimerSpec("state_expiry", TimeDomain.WATERMARK)

    def __init__(self, allowed_lateness_seconds: int = 30) -> None:
        if allowed_lateness_seconds < 0:
            raise ValueError("allowed_lateness_seconds must not be negative")

        self._allowed_lateness_seconds = allowed_lateness_seconds
        self._unique_events = Metrics.counter(self.__class__, "unique_window_assignments")
        self._duplicate_events = Metrics.counter(self.__class__, "duplicate_window_assignments")

    def process(
        self,
        element: tuple[str, Mapping[str, Any]],
        window=beam.DoFn.WindowParam,
        seen_ids=beam.DoFn.StateParam(SEEN_IDS),  # noqa: B008
        state_expiry=beam.DoFn.TimerParam(STATE_EXPIRY),  # noqa: B008
    ):
        """Emit only the first occurrence and schedule bounded-state cleanup."""
        customer_id, event = element
        event_id = event["event_id"]

        if not isinstance(event_id, str):
            raise TypeError("validated event_id must be a string")

        state_expiry.set(window.end + Duration(seconds=self._allowed_lateness_seconds))

        if event_id in seen_ids.read():
            self._duplicate_events.inc()
            return

        seen_ids.add(event_id)
        self._unique_events.inc()
        yield customer_id, dict(event)

    @on_timer(STATE_EXPIRY)
    def clear_seen_ids(
        self,
        seen_ids=beam.DoFn.StateParam(SEEN_IDS),  # noqa: B008
    ) -> None:
        """Release event IDs after the window's accepted-lateness horizon."""
        seen_ids.clear()


class DeduplicateEvents(beam.PTransform):
    """Key validated events by customer and deduplicate per window."""

    def __init__(self, allowed_lateness_seconds: int = 30) -> None:
        if allowed_lateness_seconds < 0:
            raise ValueError("allowed_lateness_seconds must not be negative")
        self._allowed_lateness_seconds = allowed_lateness_seconds

    def expand(self, events):
        """Return customer-keyed unique events for downstream aggregation."""
        return (
            events
            | "KeyByCustomerId"
            >> beam.Map(_key_by_customer_id).with_output_types(KV[str, Dict[str, Any]])
            | "DeduplicateByEventId"
            >> beam.ParDo(DeduplicateByEventId(self._allowed_lateness_seconds)).with_output_types(
                KV[str, Dict[str, Any]]
            )
        )
