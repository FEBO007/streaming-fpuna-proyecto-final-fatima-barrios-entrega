"""Tests for Kafka pipeline configuration and deterministic serialization."""

from __future__ import annotations

import json

import pytest

from src.pipeline.streaming_pipeline import (
    DEFAULT_ALERT_TOPIC,
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_CONSUMER_GROUP,
    DEFAULT_INPUT_TOPIC,
    DEFAULT_INVALID_TOPIC,
    DEFAULT_KAFKA_ENVIRONMENT_CONFIG,
    DEFAULT_KAFKA_ENVIRONMENT_TYPE,
    PipelineConfig,
    alert_to_kafka_record,
    invalid_to_kafka_record,
    parse_args,
)


@pytest.mark.parametrize(
    "field",
    [
        "bootstrap_servers",
        "input_topic",
        "alert_topic",
        "invalid_topic",
        "consumer_group",
        "kafka_environment_config",
    ],
)
def test_config_rejects_empty_text_fields(field: str) -> None:
    with pytest.raises(
        ValueError,
        match=f"{field} must be a non-empty string",
    ):
        PipelineConfig(**{field: "  "})


@pytest.mark.parametrize("value", [0, True])
def test_config_rejects_invalid_max_num_records(
    value: object,
) -> None:
    with pytest.raises(
        ValueError,
        match="max_num_records must be a positive integer",
    ):
        PipelineConfig(max_num_records=value)  # type: ignore[arg-type]


def test_config_rejects_unknown_kafka_environment_type() -> None:
    with pytest.raises(
        ValueError,
        match="kafka_environment_type must be one of",
    ):
        PipelineConfig(kafka_environment_type="UNKNOWN")


def test_config_rejects_non_boolean_directrunner_smoke_mode() -> None:
    with pytest.raises(
        TypeError,
        match="directrunner_smoke_mode must be a boolean",
    ):
        PipelineConfig(  # type: ignore[arg-type]
            directrunner_smoke_mode="true"
        )


def test_alert_serialization_is_deterministic_and_preserves_key() -> None:
    key, value = alert_to_kafka_record(
        (
            "velocity|cust-001|120000|180000",
            {"revision": 1, "customer_id": "cust-001"},
        )
    )

    assert key == b"velocity|cust-001|120000|180000"
    assert value == b'{"customer_id":"cust-001","revision":1}'


def test_invalid_record_uses_event_id_when_available() -> None:
    key, value = invalid_to_kafka_record(
        {
            "errors": ["invalid amount"],
            "event_id": "evt-invalid-001",
        }
    )

    assert key == b"invalid|evt-invalid-001"
    assert json.loads(value) == {
        "errors": ["invalid amount"],
        "event_id": "evt-invalid-001",
    }


def test_invalid_record_without_event_id_uses_stable_content_hash() -> None:
    first = invalid_to_kafka_record(
        {
            "errors": ["invalid JSON"],
            "raw_value": "{",
        }
    )
    second = invalid_to_kafka_record(
        {
            "raw_value": "{",
            "errors": ["invalid JSON"],
        }
    )

    assert first == second
    assert first[0].startswith(b"invalid|sha256:")


def test_parser_keeps_defaults_and_separates_beam_arguments() -> None:
    args, beam_args = parse_args(
        [
            "--runner=DirectRunner",
            "--max-num-records",
            "8",
        ]
    )

    assert args.bootstrap_servers == DEFAULT_BOOTSTRAP_SERVERS
    assert args.input_topic == DEFAULT_INPUT_TOPIC
    assert args.alert_topic == DEFAULT_ALERT_TOPIC
    assert args.invalid_topic == DEFAULT_INVALID_TOPIC
    assert args.consumer_group == DEFAULT_CONSUMER_GROUP
    assert args.max_num_records == 8
    assert args.kafka_environment_type == DEFAULT_KAFKA_ENVIRONMENT_TYPE
    assert args.kafka_environment_config == DEFAULT_KAFKA_ENVIRONMENT_CONFIG
    assert beam_args == ["--runner=DirectRunner"]


def test_parser_accepts_docker_java_worker_for_directrunner_smoke() -> None:
    args, beam_args = parse_args(
        [
            "--runner=DirectRunner",
            "--environment_type=LOOPBACK",
            "--kafka-environment-type=DOCKER",
            "--kafka-environment-config=apache/beam_java17_sdk:2.75.0",
            "--directrunner-smoke-mode",
        ]
    )

    assert args.kafka_environment_type == "DOCKER"
    assert args.kafka_environment_config == "apache/beam_java17_sdk:2.75.0"
    assert args.directrunner_smoke_mode is True
    assert beam_args == [
        "--runner=DirectRunner",
        "--environment_type=LOOPBACK",
    ]
