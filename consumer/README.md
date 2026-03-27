# Consumer: Event Hubs → Azure Blob Storage

Reads Parquet payloads from the same Kafka-compatible **Azure Event Hub** the producer writes to, then uploads each message to **Azure Blob Storage**.

## Azure Storage (one-time setup)

1. In [Azure Portal](https://portal.azure.com), create a **Storage account** (or use an existing one).
2. Open **Containers** → **+ Container** → name e.g. `careers-parquet` (must match `AZURE_STORAGE_CONTAINER` or set the env var to your name).
3. Under **Security + networking** → **Access keys**, copy **Connection string** (key1 or key2). This is `AZURE_STORAGE_CONNECTION_STRING`.

Optional: set `AUTO_CREATE_CONTAINER=1` so the app creates the container if it is missing (dev only; production usually pre-create with least privilege).

## Environment variables

### Kafka / Event Hubs (same as producer)

| Variable | Required | Description |
|----------|----------|-------------|
| `EVENTHUB_CONNECTION_STRING` | Yes* | Namespace connection string (Kafka API). |
| `KAFKA_TOPIC` | Yes | Event Hub name (= topic). |
| `KAFKA_BOOTSTRAP_SERVERS` | No | Override bootstrap; otherwise parsed from the connection string. |
| `KAFKA_CONSUMER_GROUP` | No | Default `careers-parquet-consumer`. |
| `KAFKA_AUTO_OFFSET_RESET` | No | `earliest` (default) or `latest`. |
| `KAFKA_CLIENT_ID` | No | Client id (default `wsc-careers-consumer`). |

\* Or use generic Kafka with `KAFKA_BOOTSTRAP_SERVERS` + SASL envs (see `producer/kafka_broker.py`).

**Important:** The shared access policy behind `EVENTHUB_CONNECTION_STRING` must allow **Listen** for consumers. A **Send-only** policy works for the producer but causes **`TopicAuthorizationFailedError` (Kafka error 29)** on the consumer. Use **Listen** (or **Manage** / `RootManageSharedAccessKey` in dev) for the consumer’s connection string.

### Azure Blob

| Variable | Required | Description |
|----------|----------|-------------|
| `AZURE_STORAGE_CONNECTION_STRING` | Yes | Storage account connection string. |
| `AZURE_STORAGE_CONTAINER` | No | Default `careers-parquet`. |
| `AZURE_BLOB_PREFIX` | No | Default `careers-parquet/` (path prefix inside the container). |
| `AUTO_CREATE_CONTAINER` | No | `1` to auto-create the container. |

### Optional

| Variable | Description |
|----------|-------------|
| `CONSUMER_MAX_MESSAGES` | Stop after N messages (testing). |
| `LOG_LEVEL` | e.g. `DEBUG`, `INFO`. |
| `ENRICH_WITH_DUCKDB` | Set `1` / `true` / `yes` to run DuckDB enrichment on each Parquet payload before Blob upload (adds `complexity_score`, `category`, `seniority_level`). See `enrichment/README.md`. After enrichment, **validation** ensures row counts and `Index` / `Position title` match the producer payload and scores/categories are in range; on failure the consumer logs and exits (that offset is not committed). |

### Failure alerts (email)

On **startup error** (bad config), **per-message processing error** (enrichment validation, blob upload, …), or **Kafka consumer error**, the consumer can send a plain-text email via SMTP.

**Turning on `ALERT_ON_FAILURE` is not enough:** you must also set **`SMTP_HOST`** (and usually `SMTP_USER` / `SMTP_PASSWORD`) below. Without SMTP, no email is delivered (a warning is logged).

| Variable | Default | Description |
|----------|---------|-------------|
| `ALERT_ON_FAILURE` | `0` (off) | Set `1` / `true` / `yes` / `on` to enable sending (requires SMTP vars). |
| `ALERT_EMAIL_TO` | `shaharpriel@gmail.com` | Comma-separated recipients. |
| `ALERT_EMAIL_FROM` | First recipient | SMTP `From` header (use a mailbox your provider allows). |
| `SMTP_HOST` | *(empty)* | If unset, no email is sent (warning logged). E.g. `smtp.gmail.com`. |
| `SMTP_PORT` | `587` | Use `465` with `SMTP_SSL=1` if your provider requires implicit TLS. |
| `SMTP_USER` / `SMTP_PASSWORD` | — | SMTP auth (e.g. Gmail [App Password](https://support.google.com/accounts/answer/185833)). |
| `SMTP_SSL` | `0` | Set `1` for `SMTP_SSL` instead of `STARTTLS`. |

Shared implementation: `_lib/email_notify.py` (not under `consumer/` so the producer or other jobs can reuse the same SMTP helpers without importing the consumer package).

## Run locally

From the repository root (so `producer` and `consumer` resolve):

```bash
pip install -r requirements-consumer.txt
export EVENTHUB_CONNECTION_STRING='...'
export KAFKA_TOPIC='your-event-hub-name'
export AZURE_STORAGE_CONNECTION_STRING='...'
export AZURE_STORAGE_CONTAINER='careers-parquet'
# optional: add DuckDB columns before upload
# export ENRICH_WITH_DUCKDB=1
python -m consumer
```

Run from the **repository root** so `producer`, `consumer`, and `enrichment` resolve on `PYTHONPATH`.

## Docker

```bash
docker build -f Dockerfile.consumer -t careers-consumer .
docker run --rm \
  -e EVENTHUB_CONNECTION_STRING='...' \
  -e KAFKA_TOPIC='your-event-hub-name' \
  -e AZURE_STORAGE_CONNECTION_STRING='...' \
  careers-consumer
```

## Blob path layout

Objects are stored as:

`{AZURE_BLOB_PREFIX}{KAFKA_TOPIC}/{timestamp}_p{partition}_o{offset}_{sanitized-key}.parquet`

With `ENRICH_WITH_DUCKDB=1`, the object name ends with `_enriched.parquet` instead of `.parquet`.

Content-Type is `application/vnd.apache.parquet`.

## Troubleshooting

| Symptom | Likely cause |
|--------|----------------|
| `[Error 29] TopicAuthorizationFailedError` | Connection string uses a policy with **Send** but not **Listen**. Create a policy with **Listen** (or use **Manage**) for the consumer. |
| No messages | `KAFKA_AUTO_OFFSET_RESET=latest` and producer already ran → only new messages are read; try `earliest` once or produce again. |
| Wrong topic | `KAFKA_TOPIC` must match the Event Hub **name** exactly (case-sensitive). |
| Fewer rows than on the careers site | **Enrichment does not drop rows** — row count equals that Parquet file. The producer may send **several Kafka messages** (e.g. `HIGH_THROUGHPUT=1` with `PARQUET_BATCH_MAX_ROWS`), and the consumer uploads **one blob per message**. Open every blob (or merge them), or raise `PARQUET_BATCH_MAX_ROWS` so one message holds all jobs. Logs show `N rows in Parquet payload` per message. |
| Producer: `[Error 43] UnsupportedForMessageFormatError` | Common on **Azure Event Hubs** with aggressive producer settings. Ensure `EVENTHUB_CONNECTION_STRING` is set so the producer applies Event-Hubs-safe defaults (`max_request_size` &lt; 1,046,528, `compression_type=none`, no record headers by default). Copy a **fresh** namespace connection string after recreating the namespace. Override with `KAFKA_COMPRESSION_TYPE=none`, `KAFKA_INCLUDE_HEADERS=0` if needed. |

## Other clouds (AWS S3, GCS, Google Drive)

Only **Azure Blob** is implemented here. Adding S3 or GCS would follow the same pattern: replace `consumer/azure_storage.py` with another backend (or add a `STORAGE_BACKEND` switch). Google Drive is OAuth-oriented and less suited to headless services than object storage.

## Delivery semantics

Offsets are committed **after** a successful blob upload. If the process crashes after upload but before commit, the message may be processed again; blob names include partition and offset with `overwrite=True`, so the same blob path is overwritten (idempotent for a given offset).
