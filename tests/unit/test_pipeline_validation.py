"""Unit tests for Beam Kafka ingestion validation."""

from __future__ import annotations

import copy
import json

import pytest
from apache_beam.pvalue import TaggedOutput
from apache_beam.transforms.window import TimestampedValue

from src.common.event_contract import parse_event_time
from src.pipeline.validation import (
    INVALID_EVENTS_TAG,
    ParseAndValidateKafkaRecord,
)

VALID_EVENT = {
    "schema_version": "1.0",
    "event_id": "evt-test-001",
    "customer_id": "cust-test-001",
    "event_time": "2026-08-08T19:14:11.000Z",
    "payload": {
        "amount": 2_500_000,
        "currency": "PYG",
        "transaction_type": "transfer",
        "channel": "mobile",
    },
}


def _encode_event(event: object) -> bytes:
    return json.dumps(
        event,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _process(record: object) -> list[object]:
    return list(ParseAndValidateKafkaRecord().process(record))


def _single_invalid(record: object) -> dict[str, object]:
    outputs = _process(record)

    assert len(outputs) == 1
    tagged = outputs[0]
    assert isinstance(tagged, TaggedOutput)
    assert tagged.tag == INVALID_EVENTS_TAG

    return tagged.value


def test_valid_record_is_emitted_with_event_time_timestamp() -> None:
    outputs = _process(
        (
            b"cust-test-001",
            _encode_event(VALID_EVENT),
        )
    )

    assert len(outputs) == 1
    output = outputs[0]
    assert isinstance(output, TimestampedValue)
    assert output.value == VALID_EVENT
    assert float(output.timestamp) == pytest.approx(
        parse_event_time(VALID_EVENT["event_time"]).timestamp()
    )


def test_invalid_json_is_routed_to_invalid_output() -> None:
    invalid = _single_invalid(
        (
            b"cust-test-001",
            b'{"schema_version":',
        )
    )

    assert invalid["error_stage"] == "ingestion_validation"
    assert invalid["kafka_key"] == "cust-test-001"
    assert invalid["raw_value"] == '{"schema_version":'
    assert any(error.startswith("Kafka value must be valid JSON") for error in invalid["errors"])


def test_contract_errors_are_accumulated() -> None:
    event = copy.deepcopy(VALID_EVENT)
    event["payload"]["amount"] = 0
    event["payload"]["currency"] = "USD"

    invalid = _single_invalid(
        (
            b"cust-test-001",
            _encode_event(event),
        )
    )

    assert invalid["event_id"] == "evt-test-001"
    assert "payload.amount must be a positive integer" in invalid["errors"]
    assert "payload.currency must be 'PYG'" in invalid["errors"]


def test_kafka_key_must_match_customer_id() -> None:
    invalid = _single_invalid(
        (
            b"another-customer",
            _encode_event(VALID_EVENT),
        )
    )

    assert invalid["event_id"] == "evt-test-001"
    assert "Kafka key must match customer_id" in invalid["errors"]


def test_null_kafka_key_is_rejected() -> None:
    invalid = _single_invalid(
        (
            None,
            _encode_event(VALID_EVENT),
        )
    )

    assert "Kafka key must not be null" in invalid["errors"]


def test_invalid_utf8_key_and_value_are_reported() -> None:
    invalid = _single_invalid(
        (
            b"\xff",
            b"\xff",
        )
    )

    assert "Kafka key must contain valid UTF-8" in invalid["errors"]
    assert "Kafka value must contain valid UTF-8" in invalid["errors"]


def test_non_pair_record_is_rejected() -> None:
    invalid = _single_invalid(b"not-a-key-value-pair")

    assert invalid["errors"] == ["Kafka record must be a key-value tuple"]
    assert invalid["kafka_key"] is None
    assert invalid["raw_value"] == "not-a-key-value-pair"
