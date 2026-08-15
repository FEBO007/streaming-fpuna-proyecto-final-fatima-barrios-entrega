"""Unit tests for the transaction event contract."""

from datetime import UTC, datetime

import pytest

from src.common.event_contract import (
    build_transaction_event,
    encode_transaction_event,
    parse_event_time,
    transaction_kafka_key,
    validate_transaction_event,
)


def make_valid_event() -> dict:
    """Return a valid transaction event for testing."""
    return build_transaction_event(
        event_id="evt-001",
        customer_id="cust-001",
        event_time=datetime(2026, 8, 8, 18, 0, tzinfo=UTC),
        amount=2_500_000,
        transaction_type="transfer",
        channel="mobile",
    )


def test_build_transaction_event_uses_expected_contract() -> None:
    event = make_valid_event()

    assert event == {
        "schema_version": "1.0",
        "event_id": "evt-001",
        "customer_id": "cust-001",
        "event_time": "2026-08-08T18:00:00.000Z",
        "payload": {
            "amount": 2_500_000,
            "currency": "PYG",
            "transaction_type": "transfer",
            "channel": "mobile",
        },
    }


def test_parse_event_time_normalizes_offset_to_utc() -> None:
    parsed = parse_event_time("2026-08-08T15:00:00-03:00")

    assert parsed == datetime(2026, 8, 8, 18, 0, tzinfo=UTC)


def test_encode_transaction_event_is_deterministic() -> None:
    event = make_valid_event()
    reordered = {
        "payload": event["payload"],
        "event_time": event["event_time"],
        "customer_id": event["customer_id"],
        "event_id": event["event_id"],
        "schema_version": event["schema_version"],
    }

    assert encode_transaction_event(event) == encode_transaction_event(reordered)


def test_transaction_kafka_key_uses_customer_id() -> None:
    assert transaction_kafka_key(make_valid_event()) == b"cust-001"


def test_validate_transaction_event_reports_invalid_fields() -> None:
    invalid_event = {
        "schema_version": "2.0",
        "event_id": "",
        "customer_id": "",
        "event_time": "not-a-date",
        "payload": {
            "amount": True,
            "currency": "USD",
            "transaction_type": "unknown",
            "channel": "unknown",
        },
    }

    errors = validate_transaction_event(invalid_event)

    assert len(errors) == 8
    assert "schema_version must be '1.0'" in errors
    assert "event_id must be a non-empty string" in errors
    assert "customer_id must be a non-empty string" in errors
    assert "payload.amount must be a positive integer" in errors
    assert "payload.currency must be 'PYG'" in errors


def test_build_transaction_event_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="timezone"):
        build_transaction_event(
            event_id="evt-001",
            customer_id="cust-001",
            event_time=datetime(2026, 8, 8, 18, 0),
            amount=2_500_000,
            transaction_type="transfer",
            channel="mobile",
        )
