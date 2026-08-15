"""Reproducible partition-skew analysis for keyed transaction events."""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from zlib import crc32

from src.common.event_contract import build_transaction_event, encode_transaction_event

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "bank.transactions.raw"
DEFAULT_PARTITIONS = 3
DEFAULT_TOTAL_EVENTS = 300
DEFAULT_HOT_KEY_SHARE = 0.80
DEFAULT_BASE_TIME = datetime(2026, 8, 15, 12, 0, tzinfo=UTC)
HOT_CUSTOMER_ID = "cust-hot-001"


@dataclass(frozen=True)
class SkewRecord:
    """One deterministic record used by a skew scenario."""

    sequence: int
    customer_id: str


@dataclass(frozen=True)
class SkewScenario:
    """A named collection of keyed records."""

    name: str
    records: tuple[SkewRecord, ...]


@dataclass(frozen=True)
class PartitionSummary:
    """Observed event and key counts for one partition."""

    partition: int
    event_count: int
    unique_key_count: int


@dataclass(frozen=True)
class ScenarioSummary:
    """Distribution metrics for one scenario."""

    name: str
    total_events: int
    unique_keys: int
    partitions: tuple[PartitionSummary, ...]
    max_partition: int
    max_partition_share: float
    max_to_mean_ratio: float


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _hot_key_share(value: str) -> float:
    parsed = float(value)
    if not 0 < parsed < 1:
        raise argparse.ArgumentTypeError("must be greater than 0 and lower than 1")
    return parsed


def partition_for_key(customer_id: str, partition_count: int) -> int:
    """Mirror librdkafka's default CRC32 assignment for a non-empty key."""
    if not customer_id.strip():
        raise ValueError("customer_id must be non-empty")
    if partition_count <= 0:
        raise ValueError("partition_count must be positive")
    return crc32(customer_id.encode("utf-8")) % partition_count


def build_reference_scenarios(
    *,
    total_events: int = DEFAULT_TOTAL_EVENTS,
    hot_key_share: float = DEFAULT_HOT_KEY_SHARE,
) -> tuple[SkewScenario, SkewScenario]:
    """Build equal-volume balanced and hot-key scenarios."""
    if total_events <= 0:
        raise ValueError("total_events must be positive")
    if not 0 < hot_key_share < 1:
        raise ValueError("hot_key_share must be greater than 0 and lower than 1")

    hot_event_count = round(total_events * hot_key_share)
    if not 0 < hot_event_count < total_events:
        raise ValueError("hot_key_share must leave at least one cold-key event")

    balanced_records = tuple(
        SkewRecord(sequence=index, customer_id=f"cust-balanced-{index:03d}")
        for index in range(total_events)
    )
    hot_records = tuple(
        SkewRecord(sequence=index, customer_id=HOT_CUSTOMER_ID)
        if index < hot_event_count
        else SkewRecord(
            sequence=index,
            customer_id=f"cust-cold-{index - hot_event_count:03d}",
        )
        for index in range(total_events)
    )

    return (
        SkewScenario(name="balanced", records=balanced_records),
        SkewScenario(name="hot_key", records=hot_records),
    )


def summarize_scenario(
    scenario: SkewScenario,
    *,
    partition_count: int,
    observed_partitions: Sequence[int] | None = None,
) -> ScenarioSummary:
    """Calculate distribution metrics from simulated or observed partitions."""
    if partition_count <= 0:
        raise ValueError("partition_count must be positive")
    if not scenario.records:
        raise ValueError("scenario must contain at least one record")

    assignments = (
        [partition_for_key(record.customer_id, partition_count) for record in scenario.records]
        if observed_partitions is None
        else list(observed_partitions)
    )
    if len(assignments) != len(scenario.records):
        raise ValueError("observed_partitions must match the number of scenario records")
    if any(partition < 0 or partition >= partition_count for partition in assignments):
        raise ValueError("observed partition is outside the configured range")

    event_counts = Counter(assignments)
    unique_keys: dict[int, set[str]] = {partition: set() for partition in range(partition_count)}
    for record, partition in zip(scenario.records, assignments, strict=True):
        unique_keys[partition].add(record.customer_id)

    partition_summaries = tuple(
        PartitionSummary(
            partition=partition,
            event_count=event_counts[partition],
            unique_key_count=len(unique_keys[partition]),
        )
        for partition in range(partition_count)
    )
    busiest = max(partition_summaries, key=lambda item: (item.event_count, -item.partition))
    total_events = len(scenario.records)
    mean_events = total_events / partition_count

    return ScenarioSummary(
        name=scenario.name,
        total_events=total_events,
        unique_keys=len({record.customer_id for record in scenario.records}),
        partitions=partition_summaries,
        max_partition=busiest.partition,
        max_partition_share=busiest.event_count / total_events,
        max_to_mean_ratio=busiest.event_count / mean_events,
    )


def _event_for_record(
    *,
    scenario: SkewScenario,
    record: SkewRecord,
    base_time: datetime,
) -> dict[str, Any]:
    event_time = base_time + timedelta(milliseconds=record.sequence)
    return build_transaction_event(
        event_id=f"evt-skew-{scenario.name}-{record.sequence:04d}",
        customer_id=record.customer_id,
        event_time=event_time,
        amount=100_000,
        transaction_type="transfer",
        channel="api",
    )


def publish_scenarios(
    scenarios: Sequence[SkewScenario],
    *,
    bootstrap_servers: str,
    topic: str,
    client_id: str,
    base_time: datetime,
) -> dict[str, list[int]]:
    """Publish scenarios and return the partition reported for each record."""
    from confluent_kafka import Producer

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "delivery.timeout.ms": 30_000,
            "partitioner": "consistent_random",
        }
    )
    observed: dict[str, list[int | None]] = {
        scenario.name: [None] * len(scenario.records) for scenario in scenarios
    }
    delivery_errors: list[str] = []

    def callback_for(scenario_name: str, record_index: int):
        def on_delivery(error: Any, message: Any) -> None:
            if error is not None:
                delivery_errors.append(str(error))
                return
            observed[scenario_name][record_index] = message.partition()

        return on_delivery

    for scenario_number, scenario in enumerate(scenarios):
        scenario_base_time = base_time + timedelta(hours=scenario_number)
        for index, record in enumerate(scenario.records):
            event = _event_for_record(
                scenario=scenario,
                record=record,
                base_time=scenario_base_time,
            )
            while True:
                try:
                    producer.produce(
                        topic=topic,
                        key=record.customer_id.encode("utf-8"),
                        value=encode_transaction_event(event),
                        timestamp=int(
                            datetime.fromisoformat(
                                event["event_time"].replace("Z", "+00:00")
                            ).timestamp()
                            * 1_000
                        ),
                        on_delivery=callback_for(scenario.name, index),
                    )
                    break
                except BufferError:
                    producer.poll(0.5)
            producer.poll(0)

    remaining = producer.flush(30.0)
    if remaining:
        delivery_errors.append(f"{remaining} message(s) were not delivered before timeout")
    if delivery_errors:
        raise RuntimeError("; ".join(delivery_errors))

    completed: dict[str, list[int]] = {}
    for scenario in scenarios:
        assignments = observed[scenario.name]
        if any(partition is None for partition in assignments):
            raise RuntimeError(f"missing delivery report for scenario {scenario.name}")
        completed[scenario.name] = [int(partition) for partition in assignments]
    return completed


def render_report(
    summaries: Sequence[ScenarioSummary],
    *,
    partition_count: int,
    hot_key_share: float,
    mode: str,
) -> str:
    """Render stable key-value output suitable for evidence and review."""
    lines = [
        "SKEW_ANALYSIS_START "
        f"mode={mode} partitions={partition_count} "
        f"events_per_scenario={summaries[0].total_events} "
        f"hot_key_share={hot_key_share:.2f}"
    ]

    for summary in summaries:
        lines.append(
            f"SCENARIO name={summary.name} total_events={summary.total_events} "
            f"unique_keys={summary.unique_keys}"
        )
        for partition in summary.partitions:
            share = partition.event_count / summary.total_events
            lines.append(
                f"PARTITION scenario={summary.name} id={partition.partition} "
                f"events={partition.event_count} unique_keys={partition.unique_key_count} "
                f"share={share:.2%}"
            )
        lines.append(
            f"SUMMARY scenario={summary.name} max_partition={summary.max_partition} "
            f"max_share={summary.max_partition_share:.2%} "
            f"max_to_mean={summary.max_to_mean_ratio:.2f}"
        )

    balanced, hot_key = summaries
    share_delta = (hot_key.max_partition_share - balanced.max_partition_share) * 100
    hot_partition = partition_for_key(HOT_CUSTOMER_ID, partition_count)
    hot_events = round(hot_key.total_events * hot_key_share)
    lines.extend(
        [
            f"HOT_KEY key={HOT_CUSTOMER_ID} events={hot_events} "
            f"partition={hot_partition} share={hot_events / hot_key.total_events:.2%}",
            f"COMPARISON max_share_delta_pp={share_delta:.2f}",
            "CONCLUSION more_partitions_distribute_distinct_keys_but_do_not_split_one_hot_key",
            "SKEW_ANALYSIS_EXIT_CODE=0",
        ]
    )
    return "\n".join(lines)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command-line options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("simulate", "publish"), default="simulate")
    parser.add_argument("--partitions", type=_positive_int, default=DEFAULT_PARTITIONS)
    parser.add_argument("--events", type=_positive_int, default=DEFAULT_TOTAL_EVENTS)
    parser.add_argument("--hot-key-share", type=_hot_key_share, default=DEFAULT_HOT_KEY_SHARE)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--client-id", default="skew-analysis-v1")
    parser.add_argument(
        "--base-time",
        default=DEFAULT_BASE_TIME.isoformat().replace("+00:00", "Z"),
        help="ISO 8601 timestamp used only by publish mode",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the selected skew analysis mode."""
    args = parse_args(argv)
    scenarios = build_reference_scenarios(
        total_events=args.events,
        hot_key_share=args.hot_key_share,
    )
    observed = None
    if args.mode == "publish":
        base_time = datetime.fromisoformat(args.base_time.replace("Z", "+00:00"))
        if base_time.tzinfo is None or base_time.utcoffset() is None:
            raise ValueError("--base-time must include timezone information")
        observed = publish_scenarios(
            scenarios,
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            client_id=args.client_id,
            base_time=base_time.astimezone(UTC),
        )

    summaries = [
        summarize_scenario(
            scenario,
            partition_count=args.partitions,
            observed_partitions=None if observed is None else observed[scenario.name],
        )
        for scenario in scenarios
    ]
    print(
        render_report(
            summaries,
            partition_count=args.partitions,
            hot_key_share=args.hot_key_share,
            mode=args.mode,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
