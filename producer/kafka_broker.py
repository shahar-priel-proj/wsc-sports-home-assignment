from __future__ import annotations

import os
import re
from typing import Any


def parse_eventhub_bootstrap(connection_string: str) -> str:
    """
    Derive ``host:9093`` from an Azure Event Hubs connection string.

    Args:
        connection_string: Namespace connection string containing
            ``Endpoint=sb://<namespace>.servicebus.windows.net/``.

    Returns:
        Bootstrap server string in the form ``<namespace>.servicebus.windows.net:9093``.

    Raises:
        ValueError: If no ``Endpoint=sb://`` segment is present.
    """
    m = re.search(r"Endpoint=sb://([^/]+)/", connection_string, re.IGNORECASE)
    if not m:
        raise ValueError(
            "EVENTHUB_CONNECTION_STRING must contain Endpoint=sb://<namespace>.servicebus.windows.net/"
        )
    return f"{m.group(1)}:9093"


def parse_bootstrap_servers(raw: str) -> list[str]:
    """Split comma-separated ``host:port`` bootstrap list."""
    return [h.strip() for h in raw.split(",") if h.strip()]


def broker_connection_kwargs(*, default_client_id: str) -> dict[str, Any]:
    """
    Shared Kafka client connection settings for Event Hubs or generic Kafka (SASL).

    Environment:
        EVENTHUB_CONNECTION_STRING: Azure Event Hubs namespace connection string.
        KAFKA_BOOTSTRAP_SERVERS: Optional override or required when no Event Hubs string.
        KAFKA_CLIENT_ID: Overrides ``default_client_id`` when set.
        KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_MECHANISM, KAFKA_SASL_USERNAME,
        KAFKA_SASL_PASSWORD: Used for non-Event-Hubs Kafka.

    Args:
        default_client_id: Used when ``KAFKA_CLIENT_ID`` is unset.

    Returns:
        Keyword arguments suitable for ``KafkaProducer`` / ``KafkaConsumer`` (connection only).

    Raises:
        ValueError: If broker cannot be resolved from environment.
    """
    conn = os.environ.get("EVENTHUB_CONNECTION_STRING", "").strip()
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    client_id = os.environ.get("KAFKA_CLIENT_ID", "").strip() or default_client_id

    if conn:
        if not bootstrap:
            bootstrap = parse_eventhub_bootstrap(conn)
        return {
            "bootstrap_servers": parse_bootstrap_servers(bootstrap),
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "$ConnectionString",
            "sasl_plain_password": conn,
            "client_id": client_id,
        }

    if not bootstrap:
        raise ValueError(
            "Set EVENTHUB_CONNECTION_STRING (Azure Event Hubs) or KAFKA_BOOTSTRAP_SERVERS"
        )

    kwargs: dict[str, Any] = {
        "bootstrap_servers": parse_bootstrap_servers(bootstrap),
        "client_id": client_id,
    }
    proto = os.environ.get("KAFKA_SECURITY_PROTOCOL", "").strip()
    if proto:
        kwargs["security_protocol"] = proto
    mech = os.environ.get("KAFKA_SASL_MECHANISM", "").strip()
    user = os.environ.get("KAFKA_SASL_USERNAME", "").strip()
    password = os.environ.get("KAFKA_SASL_PASSWORD", "").strip()
    if mech:
        kwargs["sasl_mechanism"] = mech
    if user:
        kwargs["sasl_plain_username"] = user
    if password:
        kwargs["sasl_plain_password"] = password
    return kwargs
