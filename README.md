# WSC careers pipeline (home assignment)

## What this is

End-to-end flow: **scrape** WSC careers listings → **Parquet** (`Index`, `Position title`) → **Azure Event Hubs** (Kafka protocol) → **consumer** reads messages → optional **DuckDB enrichment** → **Azure Blob Storage**. The producer is a batch-style job; the consumer runs until stopped. Details vary by mode (`SKIP_KAFKA`, `ENRICH_WITH_DUCKDB`, `HIGH_THROUGHPUT`, etc.).

| Area | Location |
|------|----------|
| Producer (scrape + publish) | `producer/` — run `python -m producer` |
| Consumer (Blob upload) | `consumer/` — run `python -m consumer` |
| Enrichment (DuckDB, validation) | `enrichment/` |
| Shared SMTP alerts | `_lib/` |
| Tests | `tests/` |
| Producer edge cases | `producer/EDGE_CASES.md` |
| Consumer / Blob / Kafka env reference | [`consumer/README.md`](consumer/README.md) |
| Enrichment | [`enrichment/README.md`](enrichment/README.md) |
| **Kubernetes / AKS (bonus)** | [`k8s/README.md`](k8s/README.md) |

---

## Prerequisites

- **Python 3.12+** recommended (matches `Dockerfile`); 3.11 may work if dependencies resolve.
- **Repository root** as the working directory so `producer`, `consumer`, `enrichment`, and `_lib` import correctly.

---

## Run locally (venv)

```bash
cd /path/to/WSCHomeAssignment
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
```

### Producer

Install runtime deps only: `pip install -r requirements.txt`.

**Required (to publish to Event Hubs):** `EVENTHUB_CONNECTION_STRING`, `KAFKA_TOPIC`  
**Optional:** `SKIP_KAFKA` (`1` to skip broker and only write Parquet), `CAREERS_URL`, `CAREERS_MERGE_URLS`, `PARQUET_PATH`, `HIGH_THROUGHPUT`, `LOG_LEVEL`, scraper tuning (`SCRAPER_*`, etc. — see `producer/scrape.py` / `EDGE_CASES.md`).

```bash
export EVENTHUB_CONNECTION_STRING='<your-namespace-connection-string>'
export KAFKA_TOPIC='<your-event-hub-name>'
python -m producer
```

### Consumer

Install: `pip install -r requirements-consumer.txt`.

**Required:** `EVENTHUB_CONNECTION_STRING`, `KAFKA_TOPIC`, `AZURE_STORAGE_CONNECTION_STRING`  
**Optional:** `AZURE_STORAGE_CONTAINER`, `ENRICH_WITH_DUCKDB`, `KAFKA_CONSUMER_GROUP`, `CONSUMER_MAX_MESSAGES`, SMTP / alert vars — full tables in [`consumer/README.md`](consumer/README.md). **Failure emails** are off by default (`ALERT_ON_FAILURE` defaults to `0`). To receive them, set `ALERT_ON_FAILURE=1` and configure SMTP (`SMTP_HOST`, etc.); alerts are not sent without SMTP.

```bash
export EVENTHUB_CONNECTION_STRING='<your-namespace-connection-string>'
export KAFKA_TOPIC='<your-event-hub-name>'
export AZURE_STORAGE_CONNECTION_STRING='<your-storage-connection-string>'
# optional:
# export ENRICH_WITH_DUCKDB=1
python -m consumer
```

---

## Docker

From the **repository root** (where `Dockerfile` and `Dockerfile.consumer` live):

```bash
docker build -t careers-producer .
docker run --rm \
  -e EVENTHUB_CONNECTION_STRING='...' \
  -e KAFKA_TOPIC='...' \
  -e SKIP_KAFKA=0 \
  careers-producer

docker build -f Dockerfile.consumer -t careers-consumer .
docker run --rm \
  -e EVENTHUB_CONNECTION_STRING='...' \
  -e KAFKA_TOPIC='...' \
  -e AZURE_STORAGE_CONNECTION_STRING='...' \
  -e AZURE_STORAGE_CONTAINER='careers-parquet' \
  -e ENRICH_WITH_DUCKDB=1 \
  careers-consumer
```

Use the same variable **names** as locally; do not commit real connection strings.

**AKS / ACR:** On Apple Silicon, build images for cluster nodes with `--platform linux/amd64` (see [`k8s/README.md`](k8s/README.md)).

---

## Kubernetes (bonus)

Manifests (`Job`, `CronJob`, `Deployment`, `ConfigMap`) and step-by-step AKS + ACR + `kubectl apply -k k8s/` are in **[`k8s/README.md`](k8s/README.md)**.

Secrets are **not** in git: copy `k8s/secret-env.example` → `k8s/secret.env` (gitignored), then `kubectl create secret ... --from-env-file=k8s/secret.env`.

---

## Tests

Use the same environment as development (`requirements-dev.txt`).

```bash
source .venv/bin/activate
python -m pytest
```

Or without activating the venv:

```bash
.venv/bin/python -m pytest
```

Tests use **mocked HTTP** for scraping/multi-source where applicable; they do not require Azure or Kafka to be up. Coverage includes broker helpers, enrichment, scrape/sanitize, publish/Event Hubs tuning, email notify, and validation.

---

## Assumptions and trade-offs

- **Azure-first:** Event Hubs + Blob are first-class; other clouds would need replacing `consumer/azure_storage.py` (not implemented here).
- **Raw Parquet in Kafka:** Producer emits two-column Parquet; optional **DuckDB enrichment** runs in the **consumer** when `ENRICH_WITH_DUCKDB=1`, with validation against producer keys.
- **Long-running consumer:** Poll loop with optional `CONSUMER_MAX_MESSAGES` for tests; offsets commit after successful Blob upload (see `consumer/README.md`).
- **Shared `_lib`:** SMTP / failure alerts live under **`_lib/`** so producer and consumer do not depend on each other’s packages circularly.
- **Homework scope:** Kubernetes docs target a small AKS demo (single node, optional CronJob); not a production multi-cluster design.
