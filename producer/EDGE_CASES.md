# Edge cases and how the producer handles them

This document describes non-ideal inputs, runtime conditions, and the **observable behavior** of the careers producer. For environment variables referenced here, see module docstrings in `__main__.py`, `scrape.py`, `multisource.py`, and `publish.py`.

## HTTP and listing pages

| Situation | Behavior |
|-----------|----------|
| **Response larger than cap** | Sync (`fetch_page`) and async (`fetch_page_async`) stop reading and raise `ValueError` mentioning `MAX_RESPONSE_BYTES` (default 15 MiB). Legacy `main` exits with code 1; high-throughput logs retries then drops that source. |
| **Non-HTTP(S) listing URL** | `load_source_configs` / `site_origin_from_url` raise `ValueError` for `CAREERS_SOURCES` / `CAREERS_URL` / `origin`. |
| **Invalid or empty `CAREERS_SOURCES` JSON** | Raises `ValueError` with a short reason (including invalid JSON). High-throughput `main` exits with code 1. |
| **Transient network errors** (high-throughput) | Retries up to `HTTP_FETCH_RETRIES` (default 3) with exponential backoff from `HTTP_RETRY_BACKOFF_SEC` (default 0.5s) plus small jitter. If all attempts fail, the source contributes **no rows**; other sources still run. |
| **Slow listing host** | `sock_read=120` on the aiohttp session; per-request `timeout_s` defaults to 60s in `fetch_page_async`. |

## HTML parsing (WSC-style listings)

| Situation | Behavior |
|-----------|----------|
| **Empty or whitespace-only HTML** | `scrape_wsc_job_records` returns `[]` → run may exit 1 if no rows overall. |
| **Mixed-case paths (``/Career/`` vs ``/career/``)** | Parser always scans all ``<a href>`` and treats ``/career/`` case-insensitively (CSS-only matching used to miss links and return partial counts). |
| **Job URLs only in scripts, JSON, or non-anchor text** | After anchors, a **raw HTML** pass finds unique ``/career/<slug>/`` paths and adds any missing jobs (title is **slug → Title Case** when no anchor text exists). |
| **Fewer rows than the public careers page** | Default fetch uses a **Chrome-like User-Agent** (many hosts return only a handful of links to bot UAs). Logs show ``Careers HTML: N chars, S unique /career/ slugs…`` — if ``N`` is small (~tens of kB vs ~90k) the HTML is truncated/bot-mode. Set ``SCRAPER_DUMP_HTML`` and compare to browser View Source; override with ``SCRAPER_USER_AGENT``; do **not** set ``SCRAPER_USE_BOT_UA=1`` unless you need the old cooperative bot string. |
| **`javascript:`, `mailto:`, `tel:`, `data:` links** | Ignored. |
| **Markup no longer uses `/career/<slug>/`** | No rows (or partial); operator should update the parser for a new site shape. |

## Job URLs and deduplication

| Situation | Behavior |
|-----------|----------|
| **Same job, different query strings** | `canonical_job_url` normalizes scheme/host/path and **strips** common tracking parameters (`utm_*`, `fbclid`, …) unless `KEEP_ALL_QUERY_PARAMS=1`. |
| **Trailing slash vs no slash** | Trailing slash removed from path (except `/`) so URLs dedupe consistently. |
| **Relative `href`** | Resolved with `urljoin(site_origin + "/", href)`; wrong `origin` in config yields wrong absolute URLs or skipped links after validation. |
| **Cross-source duplicate job URL** | After gather, `sanitize_job_records_for_parquet` keeps the **first** occurrence (order follows async completion order). |
| **Duplicate title, different URLs** | ``sanitize_job_records_for_parquet`` keeps **both** rows when ``job_url`` differs (same label can appear on multiple postings). Legacy ``positions_to_parquet_bytes`` (title-only path) still dedupes by title case-insensitively. |
| **Missing or invalid `job_url` on a record** | Dedupe key falls back to `nourl:{source}:{title}`; avoids unbounded duplicate rows from unstable `id()`. |

## Titles (`Position title`)

| Situation | Behavior |
|-----------|----------|
| **NBSP, extra whitespace** | Normalized to single spaces. |
| **Trailing “View Position”** | Removed (case-insensitive). |
| **Unicode** | NFC normalization applied; title truncated to `TITLE_MAX_CHARS` (default 2000). |
| **Non-printable / control characters** | Stripped (except tab/newline during intermediate steps; final line is single-line trimmed). |
| **Empty after normalization** | Row dropped. |

## Parquet and sorting

| Situation | Behavior |
|-----------|----------|
| **Global sort** | `str.casefold()` for A→Z ordering (not locale-aware; e.g. Swedish `å` may not match local phonebook rules). |
| **High-throughput batch index** | Each Kafka Parquet chunk has `Index` **1…k within that file only**, not global across chunks. Merged file on disk uses global `Index` 1…n. |
| **Empty dataset** | Legacy and high-throughput exit with code 1 when there are zero rows after fetch/parse. |

## Kafka / Event Hubs

| Situation | Behavior |
|-----------|----------|
| **Payload larger than configured max** | Before send, `publish_parquet_bytes` / each batch in `publish_parquet_batches` call `_ensure_payload_fits`. Raises `ValueError` with hint to reduce `PARQUET_BATCH_MAX_ROWS` or raise `KAFKA_MAX_MESSAGE_BYTES` if the broker allows it (default check: 1 MiB). |
| **Misconfigured broker / topic** | `KafkaError` → `RuntimeError` after logging. |

## Kafka / Azure Event Hubs produce

| Situation | Behavior |
|-----------|----------|
| **`UnsupportedForMessageFormatError` (Kafka error 43)** on produce | Event Hubs’ Kafka surface is stricter than Apache Kafka. This project applies [Microsoft’s recommended](https://learn.microsoft.com/en-us/azure/event-hubs/apache-kafka-configurations) producer limits when ``EVENTHUB_CONNECTION_STRING`` is set: lower ``max.request.size``, idle/metadata refresh, ``request.timeout.ms`` ≥ 60s, **uncompressed** batches by default (kafka-python ``compression_type=None``; env ``KAFKA_COMPRESSION_TYPE=none`` is mapped to that), and **no** record headers unless ``KAFKA_INCLUDE_HEADERS=1``. After recreating a namespace, use a **new** connection string. |

## Operational limits

- **Parser scope**: All sources share the **WSC careers-style** link pattern. Other career sites need a different extractor (or adapter), not just another URL.
- **Legal / polite use**: The code does not read `robots.txt` or enforce crawl budgets; rate limiting is only via `MAX_CONCURRENT_SOURCES` and host `limit_per_host`.

## Quick reference — environment knobs

| Variable | Default | Purpose |
|----------|---------|--------|
| `MAX_RESPONSE_BYTES` | 15728640 | Cap download size per listing page |
| `TITLE_MAX_CHARS` | 2000 | Max stored title length |
| `KEEP_ALL_QUERY_PARAMS` | off | Preserve `utm_*` etc. in canonical URLs |
| `HTTP_FETCH_RETRIES` | 3 | Listing fetch retries (high-throughput) |
| `HTTP_RETRY_BACKOFF_SEC` | 0.5 | Backoff base for retries |
| `KAFKA_MAX_MESSAGE_BYTES` | 1048576 | Client-side max Parquet payload size check |
| `PARQUET_BATCH_MAX_ROWS` | 2000 | Rows per Kafka Parquet chunk (high-throughput) |
