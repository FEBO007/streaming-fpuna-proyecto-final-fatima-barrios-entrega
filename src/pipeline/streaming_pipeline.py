"""Executable Kafka-to-Beam-to-Kafka velocity pipeline."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import apache_beam as beam
from apache_beam.io.kafka import (
    ReadFromKafka,
    WriteToKafka,
    default_io_expansion_service,
)
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)
from apache_beam.typehints import KV

from src.pipeline.aggregation import (
    ALLOWED_LATENESS_SECONDS,
    AggregateVelocity,
    WindowVelocityEvents,
)
from src.pipeline.alerts import CreateVelocityAlerts
from src.pipeline.deduplication import DeduplicateEvents
from src.pipeline.validation import (
    INVALID_EVENTS_TAG,
    ParseAndValidateKafkaRecord,
)

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_INPUT_TOPIC = "bank.transactions.raw"
DEFAULT_ALERT_TOPIC = "risk.alerts.velocity"
DEFAULT_INVALID_TOPIC = "bank.transactions.invalid"
DEFAULT_CONSUMER_GROUP = "velocity-beam-v1"
DEFAULT_KAFKA_ENVIRONMENT_TYPE = "PROCESS"
DEFAULT_KAFKA_ENVIRONMENT_CONFIG = '{"command":"/opt/apache/beam/boot"}'
SUPPORTED_KAFKA_ENVIRONMENT_TYPES = frozenset({"DOCKER", "EXTERNAL", "PROCESS"})


def _require_non_empty_text(name: str, value: object) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


@dataclass(frozen=True)
class PipelineConfig:
    """Runtime configuration for Kafka sources and sinks."""

    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS
    input_topic: str = DEFAULT_INPUT_TOPIC
    alert_topic: str = DEFAULT_ALERT_TOPIC
    invalid_topic: str = DEFAULT_INVALID_TOPIC
    consumer_group: str = DEFAULT_CONSUMER_GROUP
    max_num_records: int | None = None
    kafka_environment_type: str = DEFAULT_KAFKA_ENVIRONMENT_TYPE
    kafka_environment_config: str = DEFAULT_KAFKA_ENVIRONMENT_CONFIG
    directrunner_smoke_mode: bool = False

    def __post_init__(self) -> None:
        for name in (
            "bootstrap_servers",
            "input_topic",
            "alert_topic",
            "invalid_topic",
            "consumer_group",
            "kafka_environment_config",
        ):
            _require_non_empty_text(name, getattr(self, name))

        if self.kafka_environment_type not in SUPPORTED_KAFKA_ENVIRONMENT_TYPES:
            supported = ", ".join(sorted(SUPPORTED_KAFKA_ENVIRONMENT_TYPES))
            raise ValueError(f"kafka_environment_type must be one of: {supported}")

        if not isinstance(self.directrunner_smoke_mode, bool):
            raise TypeError("directrunner_smoke_mode must be a boolean")

        if self.max_num_records is not None and (
            isinstance(self.max_num_records, bool)
            or not isinstance(self.max_num_records, int)
            or self.max_num_records <= 0
        ):
            raise ValueError("max_num_records must be a positive integer")


def _json_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def alert_to_kafka_record(
    element: tuple[str, Mapping[str, Any]],
) -> tuple[bytes, bytes]:
    """Encode a stable-keyed alert for Kafka's byte serializers."""
    key, alert = element
    _require_non_empty_text("alert key", key)
    if not isinstance(alert, Mapping):
        raise TypeError("alert must be a mapping")
    return key.encode("utf-8"), _json_bytes(alert)


def invalid_to_kafka_record(
    record: Mapping[str, Any],
) -> tuple[bytes, bytes]:
    """Encode an invalid record with a deterministic diagnostic key."""
    if not isinstance(record, Mapping):
        raise TypeError("invalid record must be a mapping")

    value = _json_bytes(record)
    event_id = record.get("event_id")
    if isinstance(event_id, str) and event_id.strip():
        key = f"invalid|{event_id}"
    else:
        key = f"invalid|sha256:{hashlib.sha256(value).hexdigest()}"

    return key.encode("utf-8"), value


def build_kafka_pipeline(
    pipeline: beam.Pipeline,
    config: PipelineConfig,
) -> None:
    """Attach Kafka input, domain processing and both Kafka outputs."""
    consumer_config = {
        "bootstrap.servers": config.bootstrap_servers,
        "group.id": config.consumer_group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
        "allow.auto.create.topics": "false",
    }
    producer_config = {
        "bootstrap.servers": config.bootstrap_servers,
        "acks": "all",
        "enable.idempotence": "true",
        "max.in.flight.requests.per.connection": "5",
    }

    kafka_expansion_service = default_io_expansion_service(
        append_args=[
            f"--defaultEnvironmentType={config.kafka_environment_type}",
            f"--defaultEnvironmentConfig={config.kafka_environment_config}",
        ]
    )
    raw_records = pipeline | "ReadTransactionsFromKafka" >> ReadFromKafka(
        consumer_config=consumer_config,
        topics=[config.input_topic],
        max_num_records=config.max_num_records,
        commit_offset_in_finalize=True,
        timestamp_policy="ProcessingTime",
        expansion_service=kafka_expansion_service,
    )

    parsed = raw_records | "ParseValidateAndAssignEventTime" >> beam.ParDo(
        ParseAndValidateKafkaRecord()
    ).with_outputs(
        INVALID_EVENTS_TAG,
        main="valid_events",
    )

    alerts = (
        parsed.valid_events
        | "WindowVelocityEvents"
        >> WindowVelocityEvents(
            enable_processing_time_early_firings=(not config.directrunner_smoke_mode)
        )
        | "DeduplicateVelocityEvents"
        >> DeduplicateEvents(allowed_lateness_seconds=ALLOWED_LATENESS_SECONDS)
        | "AggregateVelocity" >> AggregateVelocity()
        | "CreateVelocityAlerts" >> CreateVelocityAlerts()
    )

    alert_records = alerts | "EncodeVelocityAlerts" >> beam.Map(
        alert_to_kafka_record
    ).with_output_types(KV[bytes, bytes])
    invalid_records = parsed[INVALID_EVENTS_TAG] | "EncodeInvalidTransactions" >> beam.Map(
        invalid_to_kafka_record
    ).with_output_types(KV[bytes, bytes])

    _ = alert_records | "WriteVelocityAlertsToKafka" >> WriteToKafka(
        producer_config=producer_config,
        topic=config.alert_topic,
        expansion_service=kafka_expansion_service,
    )
    _ = invalid_records | "WriteInvalidTransactionsToKafka" >> WriteToKafka(
        producer_config=producer_config,
        topic=config.invalid_topic,
        expansion_service=kafka_expansion_service,
    )


def parse_args(
    argv: Sequence[str] | None = None,
) -> tuple[argparse.Namespace, list[str]]:
    """Parse application arguments and preserve Beam runner arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
    )
    parser.add_argument("--input-topic", default=DEFAULT_INPUT_TOPIC)
    parser.add_argument("--alert-topic", default=DEFAULT_ALERT_TOPIC)
    parser.add_argument("--invalid-topic", default=DEFAULT_INVALID_TOPIC)
    parser.add_argument(
        "--consumer-group",
        default=DEFAULT_CONSUMER_GROUP,
    )
    parser.add_argument(
        "--max-num-records",
        type=int,
        help="Stop after reading this many records; useful for the smoke test",
    )
    parser.add_argument(
        "--kafka-environment-type",
        default=DEFAULT_KAFKA_ENVIRONMENT_TYPE,
        choices=sorted(SUPPORTED_KAFKA_ENVIRONMENT_TYPES),
        help="Java SDK environment used by the cross-language KafkaIO transforms",
    )
    parser.add_argument(
        "--kafka-environment-config",
        default=DEFAULT_KAFKA_ENVIRONMENT_CONFIG,
        help="Configuration for the Java SDK environment used by KafkaIO",
    )
    parser.add_argument(
        "--directrunner-smoke-mode",
        action="store_true",
        help=(
            "Use the bounded DirectRunner demo policy without processing-time "
            "early firings; the full temporal policy remains the default"
        ),
    )
    return parser.parse_known_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Build and run the configured streaming pipeline."""
    args, beam_args = parse_args(argv)
    config = PipelineConfig(
        bootstrap_servers=args.bootstrap_servers,
        input_topic=args.input_topic,
        alert_topic=args.alert_topic,
        invalid_topic=args.invalid_topic,
        consumer_group=args.consumer_group,
        max_num_records=args.max_num_records,
        kafka_environment_type=args.kafka_environment_type,
        kafka_environment_config=args.kafka_environment_config,
        directrunner_smoke_mode=args.directrunner_smoke_mode,
    )

    options = PipelineOptions(beam_args)
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.info(
        "Starting velocity pipeline input=%s alerts=%s invalid=%s group=%s "
        "max_records=%s directrunner_smoke_mode=%s",
        config.input_topic,
        config.alert_topic,
        config.invalid_topic,
        config.consumer_group,
        config.max_num_records,
        config.directrunner_smoke_mode,
    )

    pipeline = beam.Pipeline(options=options)
    build_kafka_pipeline(pipeline, config)
    result = pipeline.run()

    try:
        result.wait_until_finish()
    except KeyboardInterrupt:
        logging.info("Cancellation requested; stopping the pipeline.")
        result.cancel()
        return 130

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
