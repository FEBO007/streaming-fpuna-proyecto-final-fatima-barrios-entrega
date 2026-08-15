"""Versioned contract for transaction events."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

SCHEMA_VERSION = "1.0"

ALLOWED_TRANSACTION_TYPES = {
    "card_purchase",
    "cash_deposit",
    "cash_withdrawal",
    "transfer",
}

ALLOWED_CHANNELS = {
    "api",
    "atm",
    "branch",
    "mobile",
    "web",
}


def format_event_time(value: datetime) -> str:
    """Convert an aware datetime to an ISO 8601 UTC value."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("event_time must include timezone information")

    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_event_time(value: object) -> datetime:
    """Parse an ISO 8601 event-time value and normalize it to UTC."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("event_time must be a non-empty ISO 8601 string")

    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("event_time must include timezone information")

    return parsed.astimezone(UTC)


def build_transaction_event(
    *,
    event_id: str,
    customer_id: str,
    event_time: datetime,
    amount: int,
    transaction_type: str,
    channel: str,
    currency: str = "PYG",
) -> dict[str, Any]:
    """Build and validate one transaction event using schema version 1.0."""
    event = {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id,
        "customer_id": customer_id,
        "event_time": format_event_time(event_time),
        "payload": {
            "amount": amount,
            "currency": currency,
            "transaction_type": transaction_type,
            "channel": channel,
        },
    }

    errors = validate_transaction_event(event)
    if errors:
        raise ValueError("; ".join(errors))

    return event


def validate_transaction_event(event: object) -> list[str]:
    """Return every contract validation error found in an event."""
    if not isinstance(event, Mapping):
        return ["event must be a JSON object"]

    errors: list[str] = []

    if event.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version must be {SCHEMA_VERSION!r}")

    for field in ("event_id", "customer_id"):
        value = event.get(field)
        if not isinstance(value, str) or not value.strip():
            errors.append(f"{field} must be a non-empty string")

    try:
        parse_event_time(event.get("event_time"))
    except (TypeError, ValueError) as exc:
        errors.append(str(exc))

    payload = event.get("payload")
    if not isinstance(payload, Mapping):
        errors.append("payload must be a JSON object")
        return errors

    amount = payload.get("amount")
    if isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
        errors.append("payload.amount must be a positive integer")

    if payload.get("currency") != "PYG":
        errors.append("payload.currency must be 'PYG'")

    transaction_type = payload.get("transaction_type")
    if transaction_type not in ALLOWED_TRANSACTION_TYPES:
        errors.append(
            f"payload.transaction_type must be one of {sorted(ALLOWED_TRANSACTION_TYPES)}"
        )

    channel = payload.get("channel")
    if channel not in ALLOWED_CHANNELS:
        errors.append(f"payload.channel must be one of {sorted(ALLOWED_CHANNELS)}")

    return errors


def encode_transaction_event(event: Mapping[str, Any]) -> bytes:
    """Validate and serialize an event as deterministic UTF-8 JSON."""
    errors = validate_transaction_event(event)
    if errors:
        raise ValueError("; ".join(errors))

    return json.dumps(
        event,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def transaction_kafka_key(event: Mapping[str, Any]) -> bytes:
    """Use customer_id as the Kafka record key."""
    customer_id = event.get("customer_id")
    if not isinstance(customer_id, str) or not customer_id.strip():
        raise ValueError("customer_id must be a non-empty string")

    return customer_id.encode("utf-8")
