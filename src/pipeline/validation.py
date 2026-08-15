"""Beam ingestion validation for Kafka transaction records."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.pvalue import TaggedOutput
from apache_beam.transforms.window import TimestampedValue

from src.common.event_contract import (
    parse_event_time,
    validate_transaction_event,
)

INVALID_EVENTS_TAG = "invalid_events"


def _diagnostic_text(value: object) -> str | None:
    """Convert raw Kafka data into readable diagnostic text."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return repr(value)


def _build_invalid_record(
    *,
    raw_key: object,
    raw_value: object,
    event: object,
    errors: list[str],
) -> dict[str, Any]:
    """Build a serializable record for the invalid-events output."""
    event_id = event.get("event_id") if isinstance(event, Mapping) else None

    return {
        "error_stage": "ingestion_validation",
        "errors": errors,
        "event_id": event_id if isinstance(event_id, str) else None,
        "kafka_key": _diagnostic_text(raw_key),
        "raw_value": _diagnostic_text(raw_value),
    }


class ParseAndValidateKafkaRecord(beam.DoFn):
    """Decode, validate and timestamp one Kafka transaction record."""

    def __init__(self) -> None:
        self._valid_records = Metrics.counter(
            self.__class__,
            "valid_records",
        )
        self._invalid_records = Metrics.counter(
            self.__class__,
            "invalid_records",
        )

    def process(self, record: object):
        """Emit valid events normally and invalid records by tagged output."""
        if not isinstance(record, tuple) or len(record) != 2:
            self._invalid_records.inc()
            yield TaggedOutput(
                INVALID_EVENTS_TAG,
                _build_invalid_record(
                    raw_key=None,
                    raw_value=record,
                    event=None,
                    errors=["Kafka record must be a key-value tuple"],
                ),
            )
            return

        raw_key, raw_value = record
        errors: list[str] = []
        kafka_key: str | None = None
        decoded_value: str | None = None
        event: object = None
        json_parsed = False

        if raw_key is None:
            errors.append("Kafka key must not be null")
        elif not isinstance(raw_key, bytes):
            errors.append("Kafka key must be UTF-8 bytes")
        else:
            try:
                kafka_key = raw_key.decode("utf-8")
            except UnicodeDecodeError:
                errors.append("Kafka key must contain valid UTF-8")
            else:
                if not kafka_key.strip():
                    errors.append("Kafka key must be a non-empty string")

        if not isinstance(raw_value, bytes):
            errors.append("Kafka value must be UTF-8 bytes")
        else:
            try:
                decoded_value = raw_value.decode("utf-8")
            except UnicodeDecodeError:
                errors.append("Kafka value must contain valid UTF-8")

        if decoded_value is not None:
            try:
                event = json.loads(decoded_value)
                json_parsed = True
            except json.JSONDecodeError as exc:
                errors.append(
                    "Kafka value must be valid JSON: "
                    f"{exc.msg} at line {exc.lineno} column {exc.colno}"
                )

        if json_parsed:
            errors.extend(validate_transaction_event(event))

            if kafka_key is not None and isinstance(event, Mapping):
                customer_id = event.get("customer_id")
                if (
                    isinstance(customer_id, str)
                    and customer_id.strip()
                    and customer_id != kafka_key
                ):
                    errors.append("Kafka key must match customer_id")

        if errors:
            self._invalid_records.inc()
            yield TaggedOutput(
                INVALID_EVENTS_TAG,
                _build_invalid_record(
                    raw_key=raw_key,
                    raw_value=raw_value,
                    event=event,
                    errors=errors,
                ),
            )
            return

        if not isinstance(event, Mapping):
            raise RuntimeError("validated event must be a mapping")

        event_timestamp = parse_event_time(event["event_time"]).timestamp()
        self._valid_records.inc()

        yield TimestampedValue(
            dict(event),
            event_timestamp,
        )
