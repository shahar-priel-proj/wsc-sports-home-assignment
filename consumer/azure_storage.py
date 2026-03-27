from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContentSettings

log = logging.getLogger(__name__)


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").lower() in ("1", "true", "yes")


def _blob_prefix() -> str:
    p = os.environ.get("AZURE_BLOB_PREFIX", "careers-parquet/").strip()
    if p and not p.endswith("/"):
        p += "/"
    return p


def ensure_container(client: BlobServiceClient, container_name: str) -> None:
    """Create container if missing (when ``AUTO_CREATE_CONTAINER=1``)."""
    if not _truthy("AUTO_CREATE_CONTAINER"):
        return
    try:
        client.create_container(container_name)
        log.info("Created container %s", container_name)
    except ResourceExistsError:
        pass


def upload_message_value(
    data: bytes,
    *,
    topic: str,
    partition: int,
    offset: int,
    key: bytes | None,
    enriched: bool = False,
) -> str:
    """
    Upload Parquet bytes to Azure Blob Storage.

    Args:
        data: Message value (Parquet file bytes).
        topic: Kafka topic / Event Hub name (used in blob path).
        partition: Kafka partition.
        offset: Kafka offset.
        key: Optional message key (sanitized for filename).

    Returns:
        Blob URL of the uploaded object.

    Environment:
        AZURE_STORAGE_CONNECTION_STRING: Storage account connection string (required).
        AZURE_STORAGE_CONTAINER: Container name (default ``careers-parquet``).
        AZURE_BLOB_PREFIX: Virtual folder prefix (default ``careers-parquet/``).
        AUTO_CREATE_CONTAINER: If ``1``, create container when missing.
        enriched: If ``True``, append ``_enriched`` before ``.parquet`` in the blob name.

    Raises:
        ValueError: If connection string or container is missing.
        azure.core.exceptions.AzureError: On upload failures.
    """
    conn = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "").strip()
    if not conn:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING is required for Azure Blob upload")

    container = os.environ.get("AZURE_STORAGE_CONTAINER", "careers-parquet").strip()
    if not container:
        raise ValueError("AZURE_STORAGE_CONTAINER must be non-empty")

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")[:-3] + "Z"
    raw_key = (key or b"message").decode("utf-8", errors="replace")
    safe_key = re.sub(r"[^a-zA-Z0-9._-]+", "_", raw_key).strip("_")[:100] or "message"
    safe_topic = re.sub(r"[^a-zA-Z0-9._-]+", "_", topic)[:80] or "topic"

    suffix = "_enriched" if enriched else ""
    blob_name = f"{_blob_prefix()}{safe_topic}/{ts}_p{partition}_o{offset}_{safe_key}{suffix}.parquet"

    client = BlobServiceClient.from_connection_string(conn)
    ensure_container(client, container)

    blob = client.get_blob_client(container=container, blob=blob_name)
    blob.upload_blob(
        data,
        overwrite=True,
        content_settings=ContentSettings(content_type="application/vnd.apache.parquet"),
    )
    url = blob.url
    log.info("Uploaded %d bytes to %s", len(data), url)
    return url
