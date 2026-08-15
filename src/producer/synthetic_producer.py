"""Reproducible synthetic producer for transaction demo scenarios."""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from confluent_kafka import Producer

from src.common.event_contract import (
    build_transaction_event,
    encode_transaction_event,
    parse_event_time,
    transaction_kafka_key,
)

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "bank.transactions.raw"


@dataclass(frozen=True)
class ScenarioEvent:
    """One labeled event in publication order."""

    label: str
    event: dict[str, Any]


def build_demo_scenario(base_time: datetime) -> list[ScenarioEvent]:
    """Build a deterministic scenario with normal, duplicate and disordered events."""
    if base_time.tzinfo is None or base_time.utcoffset() is None:
        raise ValueError("base_time must include timezone information")

    base_time = base_time.astimezone(UTC).replace(microsecond=0)
    run_token = base_time.strftime("%Y%m%dT%H%M%SZ")

    def make_event(
        *,
        name: str,
        customer_id: str,
        seconds: int,
        amount: int,
        transaction_type: str = "transfer",
        channel: str = "mobile",
    ) -> dict[str, Any]:
        return build_transaction_event(
            event_id=f"evt-{name}-{run_token}",
            customer_id=customer_id,
            event_time=base_time + timedelta(seconds=seconds),
            amount=amount,
            transaction_type=transaction_type,
            channel=channel,
        )

    velocity_2 = make_event(
        name="velocity-002",
        customer_id="cust-velocity-001",
        seconds=20,
        amount=2_500_000,
    )

    return [
        ScenarioEvent(
            "normal",
            make_event(
                name="normal-001",
                customer_id="cust-normal-001",
                seconds=5,
                amount=350_000,
                transaction_type="card_purchase",
                channel="web",
            ),
        ),
        ScenarioEvent(
            "velocity-1",
            make_event(
                name="velocity-001",
                customer_id="cust-velocity-001",
                seconds=0,
                amount=2_500_000,
            ),
        ),
        ScenarioEvent("velocity-2", velocity_2),
        ScenarioEvent(
            "out-of-order-newer-first",
            make_event(
                name="velocity-004",
                customer_id="cust-velocity-001",
                seconds=50,
                amount=2_500_000,
                channel="api",
            ),
        ),
        ScenarioEvent(
            "out-of-order-older-after",
            make_event(
                name="velocity-003",
                customer_id="cust-velocity-001",
                seconds=40,
                amount=2_500_000,
                channel="atm",
            ),
        ),
        ScenarioEvent("intentional-duplicate", velocity_2),
        ScenarioEvent(
            "advance-event-time",
            make_event(
                name="velocity-005",
                customer_id="cust-velocity-001",
                seconds=120,
                amount=100_000,
                channel="branch",
            ),
        ),
        ScenarioEvent(
            "late-candidate",
            make_event(
                name="velocity-late-001",
                customer_id="cust-velocity-001",
                seconds=10,
                amount=500_000,
                channel="api",
            ),
        ),
    ]


def create_producer(bootstrap_servers: str, client_id: str) -> Producer:
    """Create a Kafka producer with safe retry semantics."""
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "delivery.timeout.ms": 30_000,
        }
    )


def publish_scenario(
    *,
    scenario: list[ScenarioEvent],
    producer: Producer | None,
    topic: str,
    interval_seconds: float,
    cycle: int,
) -> None:
    """Publish one scenario or display it without contacting Kafka."""
    delivery_errors: list[str] = []

    def on_delivery(error: Any, message: Any) -> None:
        if error is not None:
            delivery_errors.append(str(error))
            print(f"DELIVERY_ERROR error={error}", file=sys.stderr)
            return

        print(
            "DELIVERED "
            f"topic={message.topic()} partition={message.partition()} "
            f"offset={message.offset()}"
        )

    for index, item in enumerate(scenario):
        event = item.event
        key = transaction_kafka_key(event)
        value = encode_transaction_event(event)
        event_timestamp_ms = int(parse_event_time(event["event_time"]).timestamp() * 1_000)

        if producer is None:
            print(
                f"DRY_RUN cycle={cycle} scenario={item.label} "
                f"key={key.decode('utf-8')} value={value.decode('utf-8')}"
            )
        else:
            producer.produce(
                topic=topic,
                key=key,
                value=value,
                timestamp=event_timestamp_ms,
                on_delivery=on_delivery,
            )
            producer.poll(0)
            print(
                f"QUEUED cycle={cycle} scenario={item.label} "
                f"event_id={event['event_id']} event_time={event['event_time']}"
            )

        if interval_seconds and index < len(scenario) - 1:
            time.sleep(interval_seconds)

    if producer is not None:
        remaining = producer.flush(15.0)
        if remaining:
            delivery_errors.append(f"{remaining} message(s) were not delivered before timeout")

    if delivery_errors:
        raise RuntimeError("; ".join(delivery_errors))


def parse_args() -> argparse.Namespace:
    """Parse command-line options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--client-id", default="synthetic-transactions-v1")
    parser.add_argument(
        "--base-time",
        help="ISO 8601 event time; defaults to the current UTC second",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=0.25,
        help="Pause between publications",
    )
    parser.add_argument(
        "--cycles",
        type=int,
        default=1,
        help="Number of scenarios; use 0 to repeat until Ctrl+C",
    )
    parser.add_argument(
        "--cycle-spacing-seconds",
        type=float,
        default=180.0,
        help="Event-time distance between repeated scenarios",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.interval_seconds < 0:
        parser.error("--interval-seconds must be non-negative")
    if args.cycles < 0:
        parser.error("--cycles must be non-negative")
    if args.cycle_spacing_seconds <= 0:
        parser.error("--cycle-spacing-seconds must be positive")

    return args


def main() -> int:
    """Run the configured synthetic production scenario."""
    args = parse_args()
    base_time = (
        parse_event_time(args.base_time)
        if args.base_time
        else datetime.now(UTC).replace(microsecond=0)
    )
    producer = None if args.dry_run else create_producer(args.bootstrap_servers, args.client_id)
    cycle = 0

    try:
        while args.cycles == 0 or cycle < args.cycles:
            cycle += 1
            cycle_base_time = base_time + timedelta(
                seconds=(cycle - 1) * args.cycle_spacing_seconds
            )
            publish_scenario(
                scenario=build_demo_scenario(cycle_base_time),
                producer=producer,
                topic=args.topic,
                interval_seconds=args.interval_seconds,
                cycle=cycle,
            )
    except KeyboardInterrupt:
        print("Stopped by user.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
