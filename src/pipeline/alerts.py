"""Velocity-alert contract for accumulated customer-window revisions."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.typehints import KV, Dict
from apache_beam.utils.windowed_value import PaneInfoTiming

TRANSACTION_COUNT_THRESHOLD = 4
TOTAL_AMOUNT_THRESHOLD = 10_000_000
CURRENCY = "PYG"

_PANE_TIMING_NAMES = {
    PaneInfoTiming.EARLY: "EARLY",
    PaneInfoTiming.ON_TIME: "ON_TIME",
    PaneInfoTiming.LATE: "LATE",
    PaneInfoTiming.UNKNOWN: "UNKNOWN",
}


def _require_non_negative_int(name: str, value: object) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{name} must be a non-negative integer")

    if isinstance(value, int):
        normalized = value
    elif isinstance(value, float) and value.is_integer():
        normalized = int(value)
    else:
        raise TypeError(f"{name} must be a non-negative integer")

    if normalized < 0:
        raise TypeError(f"{name} must be a non-negative integer")

    return normalized


def _iso_utc(timestamp_millis: int) -> str:
    return (
        datetime.fromtimestamp(timestamp_millis / 1_000, tz=UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def velocity_alert_key(
    customer_id: str,
    window_start_millis: int,
    window_end_millis: int,
) -> str:
    """Return the stable logical key shared by every pane revision."""
    if not isinstance(customer_id, str) or not customer_id.strip():
        raise TypeError("customer_id must be a non-empty string")
    if window_end_millis <= window_start_millis:
        raise ValueError("window end must be greater than window start")

    return f"velocity|{customer_id}|{window_start_millis}|{window_end_millis}"


def build_velocity_alert(
    *,
    customer_id: str,
    aggregate: dict[str, int],
    window_start_millis: int,
    window_end_millis: int,
    pane_timing: str,
    pane_index: int,
    pane_nonspeculative_index: int,
    pane_is_first: bool,
    pane_is_last: bool,
    transaction_count_threshold: int = TRANSACTION_COUNT_THRESHOLD,
    total_amount_threshold: int = TOTAL_AMOUNT_THRESHOLD,
) -> dict[str, Any] | None:
    """Build one accumulated alert revision, or None below thresholds."""
    transaction_count = _require_non_negative_int(
        "transaction_count",
        aggregate.get("transaction_count"),
    )
    total_amount = _require_non_negative_int(
        "total_amount",
        aggregate.get("total_amount"),
    )

    if transaction_count == 0 or total_amount == 0:
        if transaction_count == 0 and total_amount == 0:
            # Some runners may materialize the CombineFn's neutral accumulator
            # for an empty pane. It represents no transactions, not an error.
            return None
        raise ValueError(
            "aggregate must contain positive count and amount or the neutral state (0, 0)"
        )

    triggered_conditions: list[str] = []
    if transaction_count >= transaction_count_threshold:
        triggered_conditions.append("transaction_count")
    if total_amount >= total_amount_threshold:
        triggered_conditions.append("total_amount")

    if not triggered_conditions:
        return None

    alert_id = velocity_alert_key(
        customer_id,
        window_start_millis,
        window_end_millis,
    )

    return {
        "schema_version": "1.0",
        "alert_id": alert_id,
        "idempotency_key": alert_id,
        "alert_type": "velocity",
        "rule_version": "velocity-v1",
        "customer_id": customer_id,
        "window_start": _iso_utc(window_start_millis),
        "window_end": _iso_utc(window_end_millis),
        "transaction_count": transaction_count,
        "total_amount": total_amount,
        "currency": CURRENCY,
        "thresholds": {
            "transaction_count": transaction_count_threshold,
            "total_amount": total_amount_threshold,
        },
        "triggered_conditions": triggered_conditions,
        "revision": pane_index,
        "pane_timing": pane_timing,
        "pane_nonspeculative_index": pane_nonspeculative_index,
        "pane_is_first": pane_is_first,
        "pane_is_last": pane_is_last,
        "accumulation_mode": "ACCUMULATING",
    }


class BuildVelocityAlert(beam.DoFn):
    """Convert one customer-window aggregate into an alert revision."""

    def __init__(
        self,
        *,
        transaction_count_threshold: int = TRANSACTION_COUNT_THRESHOLD,
        total_amount_threshold: int = TOTAL_AMOUNT_THRESHOLD,
    ) -> None:
        if transaction_count_threshold <= 0:
            raise ValueError("transaction_count_threshold must be positive")
        if total_amount_threshold <= 0:
            raise ValueError("total_amount_threshold must be positive")

        self._transaction_count_threshold = transaction_count_threshold
        self._total_amount_threshold = total_amount_threshold
        self._alerts_emitted = Metrics.counter(
            self.__class__,
            "alerts_emitted",
        )
        self._below_threshold = Metrics.counter(
            self.__class__,
            "below_threshold_evaluations",
        )

    def process(
        self,
        element: tuple[str, dict[str, int]],
        window=beam.DoFn.WindowParam,
        pane_info=beam.DoFn.PaneInfoParam,
    ):
        """Emit a stable-keyed accumulated revision when the rule fires."""
        customer_id, aggregate = element
        alert = build_velocity_alert(
            customer_id=customer_id,
            aggregate=aggregate,
            window_start_millis=window.start.micros // 1_000,
            window_end_millis=window.end.micros // 1_000,
            pane_timing=_PANE_TIMING_NAMES.get(
                pane_info.timing,
                "UNKNOWN",
            ),
            pane_index=pane_info.index,
            pane_nonspeculative_index=pane_info.nonspeculative_index,
            pane_is_first=pane_info.is_first,
            pane_is_last=pane_info.is_last,
            transaction_count_threshold=self._transaction_count_threshold,
            total_amount_threshold=self._total_amount_threshold,
        )

        if alert is None:
            self._below_threshold.inc()
            return

        self._alerts_emitted.inc()
        yield alert["idempotency_key"], alert


class CreateVelocityAlerts(beam.PTransform):
    """Emit stable-keyed velocity alerts from accumulated aggregates."""

    def __init__(
        self,
        *,
        transaction_count_threshold: int = TRANSACTION_COUNT_THRESHOLD,
        total_amount_threshold: int = TOTAL_AMOUNT_THRESHOLD,
    ) -> None:
        if transaction_count_threshold <= 0:
            raise ValueError("transaction_count_threshold must be positive")
        if total_amount_threshold <= 0:
            raise ValueError("total_amount_threshold must be positive")

        self._transaction_count_threshold = transaction_count_threshold
        self._total_amount_threshold = total_amount_threshold

    def expand(self, aggregates):
        """Return logical records as string key and alert dictionary."""
        return aggregates | "BuildVelocityAlertRevisions" >> beam.ParDo(
            BuildVelocityAlert(
                transaction_count_threshold=self._transaction_count_threshold,
                total_amount_threshold=self._total_amount_threshold,
            )
        ).with_output_types(KV[str, Dict[str, Any]])
