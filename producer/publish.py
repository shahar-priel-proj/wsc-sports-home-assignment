from __future__ import annotations

import os
import sys
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

from producer.kafka_broker import broker_connection_kwargs


def _using_event_hubs() -> bool:
    return bool(os.environ.get("EVENTHUB_CONNECTION_STRING", "").strip())


def _kafka_compression_type_for_producer() -> str | None:
    """
    Value for ``KafkaProducer(compression_type=...)``.

    kafka-python only accepts ``None`` for *no* compression (not the string ``\"none\"``); see
    ``KafkaProducer._COMPRESSORS`` in kafka-python.
    """
    raw = os.environ.get("KAFKA_COMPRESSION_TYPE", "").strip()
    if raw:
        low = raw.lower()
        if low in ("none", "off", "false", "uncompressed", ""):
            return None
        return low
    if _using_event_hubs():
        return None
    return "gzip"


def _event_hubs_producer_tuning() -> dict[str, Any]:
    """
    Azure Event Hubs rejects some Kafka client defaults; see MS Learn
    "Apache Kafka client configurations for Azure Event Hubs".

    - ``max.request.size`` must stay *below* 1,046,528 bytes (default Java 1,048,576 is too high).
    - Shorter idle/metadata refresh avoids broker closing idle connections (~240s).
    - ``compression.type`` other than ``none`` / ``gzip`` is unsupported; gzip batches can
      trigger ``UnsupportedForMessageFormatError`` (error 43) with some kafka-python versions.
    """
    if not _using_event_hubs():
        return {}
    return {
        "max_request_size": int(os.environ.get("KAFKA_MAX_REQUEST_SIZE", "1000000")),
        "metadata_max_age_ms": int(os.environ.get("KAFKA_METADATA_MAX_AGE_MS", "180000")),
        "connections_max_idle_ms": int(os.environ.get("KAFKA_CONNECTIONS_MAX_IDLE_MS", "180000")),
        "request_timeout_ms": int(os.environ.get("KAFKA_REQUEST_TIMEOUT_MS", "60000")),
    }


def _producer_record_headers() -> list[tuple[str, bytes]] | None:
    """Event Hubs Kafka can reject record headers on produce; default off for EH unless overridden."""
    explicit = os.environ.get("KAFKA_INCLUDE_HEADERS", "").strip().lower()
    if explicit in ("1", "true", "yes", "on"):
        return [
            ("format", b"parquet"),
            ("content-type", b"application/vnd.apache.parquet"),
        ]
    if explicit in ("0", "false", "no", "off"):
        return None
    if _using_event_hubs():
        return None
    return [
        ("format", b"parquet"),
        ("content-type", b"application/vnd.apache.parquet"),
    ]


def _max_kafka_payload_bytes() -> int:
    """Upper bound checked before send; align with broker ``message.max.bytes`` / Event Hubs limits."""
    return max(1024, int(os.environ.get("KAFKA_MAX_MESSAGE_BYTES", str(1024 * 1024))))


def _ensure_payload_fits(name: str, payload: bytes) -> None:
    mx = _max_kafka_payload_bytes()
    if len(payload) > mx:
        raise ValueError(
            f"{name}: payload is {len(payload)} bytes; exceeds KAFKA_MAX_MESSAGE_BYTES ({mx}). "
            "Lower row counts per batch (PARQUET_BATCH_MAX_ROWS), raise the broker limit, "
            "or increase KAFKA_MAX_MESSAGE_BYTES if the broker allows it."
        )


def _kafka_producer_kwargs() -> dict[str, Any]:
    """
    Build keyword arguments for ``KafkaProducer`` from environment variables.

    See ``broker_connection_kwargs`` for broker auth env vars.
    """
    return broker_connection_kwargs(default_client_id="wsc-careers-producer")


def _kafka_producer_kwargs_tuned_for_throughput() -> dict[str, Any]:
    """
    Same broker auth as ``_kafka_producer_kwargs`` plus client batching for high send rates.

    Environment (all optional):
        KAFKA_BATCH_SIZE: Producer batch size in bytes (default 262144).
        KAFKA_LINGER_MS: Wait time to fill batches in ms (default 50).
        KAFKA_COMPRESSION_TYPE: ``gzip``, ``snappy``, ``lz4``, ``zstd``, or ``none`` / empty for
            uncompressed (stored as ``None`` for kafka-python). Default: uncompressed on Event Hubs,
            ``gzip`` elsewhere.
        KAFKA_INCLUDE_HEADERS: ``1`` to send Parquet metadata headers; ``0`` to omit.
            Default: omitted for Event Hubs (avoids broker error 43), included for other Kafka.
        KAFKA_MAX_REQUEST_SIZE / KAFKA_METADATA_MAX_AGE_MS / KAFKA_CONNECTIONS_MAX_IDLE_MS /
        KAFKA_REQUEST_TIMEOUT_MS: Event Hubs–safe overrides (see Microsoft Learn) when using EH.
        KAFKA_BUFFER_MEMORY: Producer buffer cap in bytes (default 67108864).
        KAFKA_MAX_IN_FLIGHT: ``max_in_flight_requests_per_connection`` (default 5).
        KAFKA_ACKS: ``acks`` string (default all).

    Returns:
        Keyword args for ``KafkaProducer``.
    """
    kwargs = _kafka_producer_kwargs()
    kwargs.update(
        {
            "acks": os.environ.get("KAFKA_ACKS", "all"),
            "batch_size": int(os.environ.get("KAFKA_BATCH_SIZE", str(256 * 1024))),
            "linger_ms": int(os.environ.get("KAFKA_LINGER_MS", "50")),
            "compression_type": _kafka_compression_type_for_producer(),
            "buffer_memory": int(os.environ.get("KAFKA_BUFFER_MEMORY", str(64 * 1024 * 1024))),
            "max_in_flight_requests_per_connection": int(
                os.environ.get("KAFKA_MAX_IN_FLIGHT", "5")
            ),
        }
    )
    kwargs.update(_event_hubs_producer_tuning())
    return kwargs


def publish_parquet_bytes(
    payload: bytes,
    *,
    topic: str | None = None,
    key: bytes | None = None,
) -> None:
    """
    Send one Kafka message whose value is the Parquet file bytes.

    Topic defaults to ``KAFKA_TOPIC``; broker settings come from ``broker_connection_kwargs``.
    Uses ``KAFKA_FLUSH_TIMEOUT_SECONDS`` (default 60) as the record send timeout.

    Args:
        payload: Raw Parquet bytes (same as written to disk by the producer).
        topic: Destination topic / Event Hub name; if omitted, ``KAFKA_TOPIC`` must be set.
        key: Optional Kafka record key; defaults to ``b"open_positions"``.

    Raises:
        ValueError: If topic is missing and ``KAFKA_TOPIC`` is unset or empty.
        RuntimeError: If the broker returns an error for the produce request.

    """
    topic = topic or os.environ.get("KAFKA_TOPIC", "").strip()
    if not topic:
        raise ValueError("KAFKA_TOPIC is required (Event Hub name when using Kafka API)")

    _ensure_payload_fits("publish_parquet_bytes", payload)

    kwargs = _kafka_producer_kwargs()
    kwargs.update(_event_hubs_producer_tuning())
    timeout_s = float(os.environ.get("KAFKA_FLUSH_TIMEOUT_SECONDS", "60"))
    producer = KafkaProducer(**kwargs)
    headers = _producer_record_headers()
    try:
        future = producer.send(
            topic,
            value=payload,
            key=key or b"open_positions",
            headers=headers,
        )
        future.get(timeout=timeout_s)
    except KafkaError as e:
        print(str(e), file=sys.stderr)
        raise RuntimeError("Kafka delivery failed") from e
    finally:
        producer.close()


def publish_parquet_batches(
    payloads: list[bytes],
    *,
    topic: str | None = None,
) -> None:
    """
    Send many Parquet payloads using batched, compressed Kafka production.

    Blocks until all records are acknowledged (or raises). Uses
    ``_kafka_producer_kwargs_tuned_for_throughput`` for ``linger_ms`` / ``batch_size``.

    Args:
        payloads: Non-empty or empty list; empty is a no-op (no broker calls).
        topic: Destination topic; default ``KAFKA_TOPIC``.

    Raises:
        ValueError: If topic is missing when needed and payloads is non-empty.
        RuntimeError: If any delivery fails.
    """
    if not payloads:
        return
    topic = topic or os.environ.get("KAFKA_TOPIC", "").strip()
    if not topic:
        raise ValueError("KAFKA_TOPIC is required (Event Hub name when using Kafka API)")

    kwargs = _kafka_producer_kwargs_tuned_for_throughput()
    timeout_s = float(os.environ.get("KAFKA_FLUSH_TIMEOUT_SECONDS", "60"))
    producer = KafkaProducer(**kwargs)
    headers = _producer_record_headers()
    futures: list[Any] = []
    try:
        for i, payload in enumerate(payloads):
            _ensure_payload_fits(f"publish_parquet_batches[{i}]", payload)
            key = f"parquet-batch-{i}".encode()
            futures.append(
                producer.send(topic, value=payload, key=key, headers=headers)
            )
        producer.flush(timeout=int(timeout_s))
        for f in futures:
            f.get(timeout=timeout_s)
    except KafkaError as e:
        print(str(e), file=sys.stderr)
        raise RuntimeError("Kafka delivery failed") from e
    finally:
        producer.close()
