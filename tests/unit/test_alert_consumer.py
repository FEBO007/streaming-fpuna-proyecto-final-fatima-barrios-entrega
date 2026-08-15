"""Tests for the alert materializer's revision policy."""

from __future__ import annotations

import pytest

from src.materializer.alert_consumer import SQLiteAlertStore, materialize_latest_alerts


def _alert(key: str, revision: int, count: int) -> dict[str, object]:
    return {
        "idempotency_key": key,
        "revision": revision,
        "transaction_count": count,
    }


def test_materializer_keeps_highest_revision_and_persists_after_restart(tmp_path) -> None:
    key = "velocity|cust-1|1000|2000"
    result = materialize_latest_alerts(
        [
            (key, _alert(key, 0, 4)),
            (key, _alert(key, 1, 5)),
        ]
    )

    assert result[key]["revision"] == 1
    assert result[key]["transaction_count"] == 5

    database = tmp_path / "materialized-alerts.sqlite3"
    with SQLiteAlertStore(database) as store:
        assert store.upsert(key, _alert(key, 0, 4)) is True
        assert store.upsert(key, _alert(key, 1, 5)) is True

    with SQLiteAlertStore(database) as restarted_store:
        persisted = restarted_store.snapshot()

    assert persisted[key]["revision"] == 1
    assert persisted[key]["transaction_count"] == 5


def test_materializer_ignores_stale_and_duplicate_revisions(tmp_path) -> None:
    key = "velocity|cust-1|1000|2000"
    result = materialize_latest_alerts(
        [
            (key, _alert(key, 2, 6)),
            (key, _alert(key, 1, 5)),
        ]
    )

    assert result[key]["revision"] == 2

    database = tmp_path / "materialized-alerts.sqlite3"
    with SQLiteAlertStore(database) as store:
        assert store.upsert(key, _alert(key, 2, 6)) is True
        assert store.upsert(key, _alert(key, 1, 5)) is False
        assert store.upsert(key, _alert(key, 2, 999)) is False
        persisted = store.snapshot()

    assert persisted[key]["revision"] == 2
    assert persisted[key]["transaction_count"] == 6


def test_materializer_rejects_mismatched_kafka_key() -> None:
    with pytest.raises(ValueError, match="must match idempotency_key"):
        materialize_latest_alerts([("wrong-key", _alert("expected-key", 0, 4))])


@pytest.mark.parametrize("revision", [-1, True, "1"])
def test_materializer_rejects_invalid_revision(revision: object) -> None:
    key = "velocity|cust-1|1000|2000"
    alert = _alert(key, 0, 4)
    alert["revision"] = revision

    with pytest.raises(ValueError, match="revision must be"):
        materialize_latest_alerts([(key, alert)])
