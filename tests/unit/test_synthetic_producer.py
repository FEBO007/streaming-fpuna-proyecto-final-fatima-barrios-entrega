"""Unit tests for the reproducible synthetic producer."""

from datetime import UTC, datetime

from src.common.event_contract import parse_event_time, transaction_kafka_key
from src.producer.skew_analysis import (
    HOT_CUSTOMER_ID,
    build_reference_scenarios,
    partition_for_key,
    summarize_scenario,
)
from src.producer.synthetic_producer import build_demo_scenario


def make_scenario():
    """Build the fixed scenario used by the producer tests."""
    return build_demo_scenario(datetime(2026, 8, 8, 18, 0, tzinfo=UTC))


def test_demo_scenario_contains_expected_publication_order() -> None:
    scenario = make_scenario()

    assert [item.label for item in scenario] == [
        "normal",
        "velocity-1",
        "velocity-2",
        "out-of-order-newer-first",
        "out-of-order-older-after",
        "intentional-duplicate",
        "advance-event-time",
        "late-candidate",
    ]


def test_demo_scenario_contains_eight_events() -> None:
    assert len(make_scenario()) == 8


def test_velocity_events_reach_threshold_within_sixty_seconds() -> None:
    scenario = make_scenario()
    velocity_events = [scenario[index].event for index in (1, 2, 3, 4)]

    event_times = [parse_event_time(event["event_time"]) for event in velocity_events]
    total_amount = sum(event["payload"]["amount"] for event in velocity_events)

    assert len(velocity_events) == 4
    assert total_amount == 10_000_000
    assert max(event_times) - min(event_times) <= (
        datetime(2026, 8, 8, 18, 1, tzinfo=UTC) - datetime(2026, 8, 8, 18, 0, tzinfo=UTC)
    )


def test_disordered_event_is_published_after_a_newer_event() -> None:
    scenario = make_scenario()

    newer_time = parse_event_time(scenario[3].event["event_time"])
    older_time = parse_event_time(scenario[4].event["event_time"])

    assert newer_time > older_time


def test_intentional_duplicate_reuses_event_id_and_content() -> None:
    scenario = make_scenario()

    original = scenario[2].event
    duplicate = scenario[5].event

    assert duplicate == original
    assert duplicate["event_id"] == original["event_id"]


def test_late_candidate_follows_event_time_advance() -> None:
    scenario = make_scenario()

    advanced_time = parse_event_time(scenario[6].event["event_time"])
    late_time = parse_event_time(scenario[7].event["event_time"])

    assert advanced_time > late_time


def test_customer_key_supports_locality_and_reproducible_skew_analysis() -> None:
    scenario = make_scenario()

    assert transaction_kafka_key(scenario[0].event) == b"cust-normal-001"
    assert all(transaction_kafka_key(item.event) == b"cust-velocity-001" for item in scenario[1:])

    balanced, hot_key = build_reference_scenarios()
    balanced_summary = summarize_scenario(balanced, partition_count=3)
    hot_key_summary = summarize_scenario(hot_key, partition_count=3)

    assert balanced_summary.total_events == hot_key_summary.total_events == 300
    assert balanced_summary.unique_keys == 300
    assert hot_key_summary.unique_keys == 61
    assert [item.event_count for item in balanced_summary.partitions] == [92, 116, 92]
    assert [item.event_count for item in hot_key_summary.partitions] == [258, 20, 22]
    assert hot_key_summary.max_partition_share == 0.86
    assert partition_for_key(HOT_CUSTOMER_ID, 3) == 0
    assert {
        partition_for_key(record.customer_id, 3)
        for record in hot_key.records
        if record.customer_id == HOT_CUSTOMER_ID
    } == {0}
