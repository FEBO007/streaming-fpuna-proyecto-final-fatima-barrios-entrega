"""Read velocity alerts and materialize the highest revision per stable key."""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "risk.alerts.velocity"
DEFAULT_STATE_DB = Path("data/materialized-alerts.sqlite3")


def _validated_revision(alert: Mapping[str, Any]) -> int:
    revision = alert.get("revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        raise ValueError("alert revision must be a non-negative integer")
    return revision


def _validated_candidate(key: str, alert: Mapping[str, Any]) -> tuple[dict[str, Any], int]:
    if not isinstance(key, str) or not key.strip():
        raise ValueError("Kafka alert key must be a non-empty string")
    if not isinstance(alert, Mapping):
        raise TypeError("alert must be a mapping")
    if alert.get("idempotency_key") != key:
        raise ValueError("Kafka alert key must match idempotency_key")

    candidate = dict(alert)
    return candidate, _validated_revision(candidate)


def materialize_latest_alerts(
    records: Iterable[tuple[str, Mapping[str, Any]]],
) -> dict[str, dict[str, Any]]:
    """Keep the highest observed revision for every idempotency key."""
    latest: dict[str, dict[str, Any]] = {}

    for key, alert in records:
        candidate, candidate_revision = _validated_candidate(key, alert)
        current = latest.get(key)

        if current is None or candidate_revision > _validated_revision(current):
            latest[key] = candidate

    return latest


class SQLiteAlertStore:
    """Durable latest-revision view keyed by the Kafka idempotency key."""

    def __init__(self, database: str | Path = DEFAULT_STATE_DB) -> None:
        self.path = Path(database)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.path)
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS velocity_alerts (
                idempotency_key TEXT PRIMARY KEY,
                revision INTEGER NOT NULL CHECK (revision >= 0),
                alert_json TEXT NOT NULL
            )
            """
        )
        self._connection.commit()

    def upsert(self, key: str, alert: Mapping[str, Any]) -> bool:
        """Persist only a revision newer than the one already materialized."""
        candidate, revision = _validated_candidate(key, alert)
        serialized = json.dumps(
            candidate,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        changes_before = self._connection.total_changes

        with self._connection:
            self._connection.execute(
                """
                INSERT INTO velocity_alerts (idempotency_key, revision, alert_json)
                VALUES (?, ?, ?)
                ON CONFLICT(idempotency_key) DO UPDATE SET
                    revision = excluded.revision,
                    alert_json = excluded.alert_json
                WHERE excluded.revision > velocity_alerts.revision
                """,
                (key, revision, serialized),
            )

        return self._connection.total_changes > changes_before

    def snapshot(self) -> dict[str, dict[str, Any]]:
        """Return the complete materialized view ordered by stable key."""
        rows = self._connection.execute(
            """
            SELECT idempotency_key, alert_json
            FROM velocity_alerts
            ORDER BY idempotency_key
            """
        )
        return {key: json.loads(serialized) for key, serialized in rows}

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> SQLiteAlertStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _decode_message(message: Any) -> tuple[str, dict[str, Any]]:
    raw_key = message.key()
    raw_value = message.value()
    if raw_key is None or raw_value is None:
        raise ValueError("alert record must contain a non-null key and value")

    key = raw_key.decode("utf-8")
    alert = json.loads(raw_value.decode("utf-8"))
    if not isinstance(alert, dict):
        raise ValueError("alert value must be a JSON object")
    return key, alert


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--consumer-group", default="velocity-alert-materializer-v1")
    parser.add_argument("--max-messages", type=int)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument(
        "--state-db",
        type=Path,
        default=DEFAULT_STATE_DB,
        help="SQLite file used for the durable latest-revision view",
    )
    args = parser.parse_args()

    if args.max_messages is not None and args.max_messages <= 0:
        parser.error("--max-messages must be positive")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")
    return args


def main() -> int:
    args = parse_args()
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": args.consumer_group,
            "auto.offset.reset": "earliest" if args.from_beginning else "latest",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    consumer.subscribe([args.topic])
    messages_read = 0
    materialized_updates = 0

    with SQLiteAlertStore(args.state_db) as store:
        try:
            while args.max_messages is None or messages_read < args.max_messages:
                message = consumer.poll(args.timeout_seconds)
                if message is None:
                    break
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(str(message.error()))

                key, alert = _decode_message(message)
                applied = store.upsert(key, alert)
                consumer.commit(message=message, asynchronous=False)
                messages_read += 1
                materialized_updates += int(applied)
                print(
                    "ALERT "
                    f"partition={message.partition()} offset={message.offset()} "
                    f"key={key} materialized={'updated' if applied else 'unchanged'} "
                    f"value={json.dumps(alert, ensure_ascii=False, sort_keys=True)}"
                )
        finally:
            consumer.close()

        snapshot = store.snapshot()

    print(json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True))
    print(
        f"MATERIALIZED_KEYS={len(snapshot)} MESSAGES_READ={messages_read} "
        f"MATERIALIZED_UPDATES={materialized_updates} STATE_DB={args.state_db}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
