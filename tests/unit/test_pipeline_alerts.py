"""Tests for the accumulated velocity-alert contract."""

from __future__ import annotations

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import FixedWindows, TimestampedValue

from src.pipeline.alerts import (
    TOTAL_AMOUNT_THRESHOLD,
    TRANSACTION_COUNT_THRESHOLD,
    BuildVelocityAlert,
    CreateVelocityAlerts,
    build_velocity_alert,
    velocity_alert_key,
)


def _alert(
    *,
    transaction_count: int,
    total_amount: int,
    pane_timing: str = "ON_TIME",
    pane_index: int = 0,
) -> dict[str, object] | None:
    return build_velocity_alert(
        customer_id="cust-9009",
        aggregate={
            "transaction_count": transaction_count,
            "total_amount": total_amount,
        },
        window_start_millis=120_000,
        window_end_millis=180_000,
        pane_timing=pane_timing,
        pane_index=pane_index,
        pane_nonspeculative_index=pane_index,
        pane_is_first=pane_index == 0,
        pane_is_last=False,
    )


@pytest.mark.parametrize(
    ("transform_type", "arguments", "message"),
    [
        (
            BuildVelocityAlert,
            {"transaction_count_threshold": 0},
            "transaction_count_threshold must be positive",
        ),
        (
            BuildVelocityAlert,
            {"total_amount_threshold": 0},
            "total_amount_threshold must be positive",
        ),
        (
            CreateVelocityAlerts,
            {"transaction_count_threshold": -1},
            "transaction_count_threshold must be positive",
        ),
        (
            CreateVelocityAlerts,
            {"total_amount_threshold": -1},
            "total_amount_threshold must be positive",
        ),
    ],
)
def test_invalid_thresholds_are_rejected(
    transform_type: type,
    arguments: dict[str, int],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        transform_type(**arguments)


def test_below_threshold_aggregate_does_not_create_alert() -> None:
    assert _alert(transaction_count=3, total_amount=9_999_999) is None


def test_neutral_aggregate_does_not_create_alert() -> None:
    """A runner-emitted empty pane must be ignored instead of failing the job."""
    assert _alert(transaction_count=0, total_amount=0) is None


@pytest.mark.parametrize(
    ("transaction_count", "total_amount"),
    [
        (0, 1_000_000),
        (1, 0),
    ],
)
def test_inconsistent_zero_aggregate_is_rejected(
    transaction_count: int,
    total_amount: int,
) -> None:
    with pytest.raises(ValueError, match="positive count and amount or the neutral state"):
        _alert(
            transaction_count=transaction_count,
            total_amount=total_amount,
        )


def test_count_condition_can_trigger_independently() -> None:
    alert = _alert(transaction_count=4, total_amount=1_000_000)

    assert alert is not None
    assert alert["triggered_conditions"] == ["transaction_count"]


def test_amount_condition_can_trigger_independently() -> None:
    alert = _alert(transaction_count=1, total_amount=10_000_000)

    assert alert is not None
    assert alert["triggered_conditions"] == ["total_amount"]


def test_contract_exposes_rule_window_and_pane_metadata() -> None:
    alert = _alert(transaction_count=4, total_amount=10_000_000)

    assert alert is not None
    assert alert == {
        "schema_version": "1.0",
        "alert_id": "velocity|cust-9009|120000|180000",
        "idempotency_key": "velocity|cust-9009|120000|180000",
        "alert_type": "velocity",
        "rule_version": "velocity-v1",
        "customer_id": "cust-9009",
        "window_start": "1970-01-01T00:02:00.000Z",
        "window_end": "1970-01-01T00:03:00.000Z",
        "transaction_count": 4,
        "total_amount": 10_000_000,
        "currency": "PYG",
        "thresholds": {
            "transaction_count": TRANSACTION_COUNT_THRESHOLD,
            "total_amount": TOTAL_AMOUNT_THRESHOLD,
        },
        "triggered_conditions": [
            "transaction_count",
            "total_amount",
        ],
        "revision": 0,
        "pane_timing": "ON_TIME",
        "pane_nonspeculative_index": 0,
        "pane_is_first": True,
        "pane_is_last": False,
        "accumulation_mode": "ACCUMULATING",
    }


def test_late_revision_reuses_the_same_idempotency_key() -> None:
    on_time = _alert(transaction_count=4, total_amount=10_000_000)
    late = _alert(
        transaction_count=5,
        total_amount=10_500_000,
        pane_timing="LATE",
        pane_index=1,
    )

    assert on_time is not None
    assert late is not None
    assert late["idempotency_key"] == on_time["idempotency_key"]
    assert late["revision"] == 1
    assert late["transaction_count"] == 5
    assert late["total_amount"] == 10_500_000
    assert late["pane_timing"] == "LATE"


def test_stable_key_changes_for_a_different_window() -> None:
    first_window = velocity_alert_key("cust-9009", 120_000, 180_000)
    next_window = velocity_alert_key("cust-9009", 130_000, 190_000)

    assert first_window != next_window


def _alert_summary(
    element: tuple[str, dict[str, object]],
) -> tuple[str, str, str, str, int, int]:
    key, alert = element
    return (
        key,
        str(alert["customer_id"]),
        str(alert["window_start"]),
        str(alert["window_end"]),
        int(alert["transaction_count"]),
        int(alert["total_amount"]),
    )


def test_transform_emits_one_stable_keyed_alert_for_the_window() -> None:
    expected = [
        (
            "velocity|cust-9009|120000|180000",
            "cust-9009",
            "1970-01-01T00:02:00.000Z",
            "1970-01-01T00:03:00.000Z",
            4,
            10_000_000,
        )
    ]

    with BeamTestPipeline() as pipeline:
        summaries = (
            pipeline
            | "CreateAggregate"
            >> beam.Create(
                [
                    TimestampedValue(
                        (
                            "cust-9009",
                            {
                                "transaction_count": 4,
                                "total_amount": 10_000_000,
                            },
                        ),
                        125,
                    )
                ]
            )
            | "AssignFixedWindow" >> beam.WindowInto(FixedWindows(60))
            | "CreateVelocityAlert" >> CreateVelocityAlerts()
            | "ProjectAlertContract" >> beam.Map(_alert_summary)
        )

        assert_that(summaries, equal_to(expected))


def test_transform_ignores_neutral_runner_pane() -> None:
    """Ignore a neutral aggregate emitted at the runner boundary."""
    with BeamTestPipeline() as pipeline:
        alerts = (
            pipeline
            | "CreateNeutralAggregate"
            >> beam.Create(
                [
                    TimestampedValue(
                        (
                            "cust-9009",
                            {
                                "transaction_count": 0,
                                "total_amount": 0,
                            },
                        ),
                        125,
                    )
                ]
            )
            | "AssignNeutralAggregateWindow" >> beam.WindowInto(FixedWindows(60))
            | "IgnoreNeutralAggregate" >> CreateVelocityAlerts()
        )

        assert_that(alerts, equal_to([]))
