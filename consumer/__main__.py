from __future__ import annotations

import logging
import os
import sys
from io import BytesIO

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from consumer.azure_storage import upload_message_value
from enrichment.validate import EnrichmentValidationError
from _lib.email_notify import (
    notify_consumer_kafka_failure,
    notify_consumer_message_failure,
    notify_enrichment_validation_failure,
    notify_consumer_startup_failure,
)
from producer.kafka_broker import broker_connection_kwargs

log = logging.getLogger(__name__)


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").lower() in ("1", "true", "yes")


def _parquet_row_count(data: bytes) -> int | None:
    """Best-effort row count from Parquet footer (for logs); returns None if not Parquet."""
    try:
        import pyarrow.parquet as pq

        return pq.ParquetFile(BytesIO(data)).metadata.num_rows
    except Exception:
        return None


def _configure_logging() -> None:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _consumer_kwargs() -> tuple[str, dict]:
    topic = os.environ.get("KAFKA_TOPIC", "").strip()
    if not topic:
        raise ValueError("KAFKA_TOPIC is required (same Event Hub name as the producer uses)")

    base = broker_connection_kwargs(default_client_id="wsc-careers-consumer")
    group = os.environ.get("KAFKA_CONSUMER_GROUP", "careers-parquet-consumer").strip()
    auto_offset = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest").strip()
    if auto_offset not in ("earliest", "latest"):
        raise ValueError("KAFKA_AUTO_OFFSET_RESET must be earliest or latest")

    kwargs = {
        **base,
        "group_id": group,
        "auto_offset_reset": auto_offset,
        "enable_auto_commit": False,
        "consumer_timeout_ms": int(os.environ.get("KAFKA_CONSUMER_TIMEOUT_MS", "-1")),
        "value_deserializer": lambda v: v,
        "key_deserializer": lambda k: k,
    }
    return topic, kwargs


def main() -> None:
    """
    Read Parquet messages from Kafka / Event Hubs and upload each to Azure Blob Storage.

    Environment (Kafka — same as producer):
        EVENTHUB_CONNECTION_STRING: Namespace connection string.
        KAFKA_TOPIC: Event Hub name (topic).
        KAFKA_BOOTSTRAP_SERVERS: Optional override.
        KAFKA_CONSUMER_GROUP: Consumer group id (default ``careers-parquet-consumer``).
        KAFKA_AUTO_OFFSET_RESET: ``earliest`` or ``latest`` (default ``earliest``).
        KAFKA_CLIENT_ID: Optional client id override.
        KAFKA_CONSUMER_TIMEOUT_MS: Poll loop timeout; ``-1`` = block forever (default).

    Environment (Azure Blob):
        AZURE_STORAGE_CONNECTION_STRING: Required.
        AZURE_STORAGE_CONTAINER: Default ``careers-parquet``.
        AZURE_BLOB_PREFIX: Default ``careers-parquet/`` (logical folder under container).
        AUTO_CREATE_CONTAINER: Set ``1`` to create the container if it does not exist.

    Optional:
        CONSUMER_MAX_MESSAGES: Stop after N messages (handy for tests; unset = run forever).

        ENRICH_WITH_DUCKDB: ``1`` to run DuckDB enrichment on each Parquet payload before
            Blob upload (adds ``complexity_score``, ``category``, ``seniority_level``).
            Logic lives in ``enrichment/duckdb_enrich.py`` (reusable in stream processors).

        Failure email (see ``_lib/email_notify.py``):
            ALERT_ON_FAILURE, ALERT_EMAIL_TO (default ``shaharpriel@gmail.com``),
            ALERT_EMAIL_FROM, SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, SMTP_SSL.
            Enrichment validation failures use ``notify_enrichment_validation_failure`` (subset of
            ``ENRICH_WITH_DUCKDB=1`` path).
    """
    _configure_logging()
    try:
        topic, consumer_kwargs = _consumer_kwargs()
    except ValueError as e:
        print(str(e), file=sys.stderr)
        notify_consumer_startup_failure(message=str(e))
        sys.exit(1)

    max_msg_env = os.environ.get("CONSUMER_MAX_MESSAGES", "").strip()
    max_messages = int(max_msg_env) if max_msg_env else None

    consumer = KafkaConsumer(topic, **consumer_kwargs)
    processed = 0
    log.info("Subscribed to topic %s; uploading to Azure Blob Storage", topic)

    try:
        for msg in consumer:
            if msg.value is None or len(msg.value) == 0:
                log.warning("Skipping empty message partition=%s offset=%s", msg.partition, msg.offset)
                consumer.commit()
                continue
            try:
                payload = msg.value
                enriched = _truthy("ENRICH_WITH_DUCKDB")
                if enriched:
                    from enrichment.duckdb_enrich import enrich_parquet_bytes

                    payload = enrich_parquet_bytes(payload)
                nrows = _parquet_row_count(payload)
                if nrows is not None:
                    log.info(
                        "Processing Kafka message partition=%s offset=%s (%s rows in Parquet payload)",
                        msg.partition,
                        msg.offset,
                        nrows,
                    )
                upload_message_value(
                    payload,
                    topic=msg.topic or topic,
                    partition=msg.partition,
                    offset=msg.offset,
                    key=msg.key,
                    enriched=enriched,
                )
                consumer.commit()
                processed += 1
                if max_messages is not None and processed >= max_messages:
                    log.info("Stopping after CONSUMER_MAX_MESSAGES=%s", max_messages)
                    break
            except EnrichmentValidationError as exc:
                log.exception(
                    "Enrichment validation failed partition=%s offset=%s: %s",
                    msg.partition,
                    msg.offset,
                    exc,
                )
                notify_enrichment_validation_failure(
                    topic=msg.topic or topic,
                    partition=msg.partition,
                    offset=msg.offset,
                    exc=exc,
                )
                sys.exit(1)
            except Exception as exc:
                log.exception(
                    "Failed to process partition=%s offset=%s: %s",
                    msg.partition,
                    msg.offset,
                    exc,
                )
                notify_consumer_message_failure(
                    topic=msg.topic or topic,
                    partition=msg.partition,
                    offset=msg.offset,
                    exc=exc,
                )
                sys.exit(1)
    except KafkaError as e:
        log.error("Kafka consumer error: %s", e)
        notify_consumer_kafka_failure(exc=e)
        sys.exit(1)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
