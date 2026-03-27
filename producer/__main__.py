from __future__ import annotations

import os
import sys
from pathlib import Path
from urllib.parse import urlparse

import requests


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").lower() in ("1", "true", "yes")


def main_high_throughput() -> None:
    """
    Multi-source, concurrent scrape → merged Parquet on disk + batched Kafka messages.

    Parquet columns: ``Index`` and ``Position title`` only (raw). Run DuckDB enrichment
    in the consumer (``ENRICH_WITH_DUCKDB=1``) or in a stream processor — see ``enrichment/``.

    Environment:
        CAREERS_SOURCES: JSON array of ``{"id", "url", "origin?"}`` (see ``load_source_configs``).
            If unset, same as legacy: single ``CAREERS_URL`` / ``https://wsc-sports.com/Careers``.
        MAX_CONCURRENT_SOURCES: Max parallel listing fetches (default ``32``).
        PARQUET_BATCH_MAX_ROWS: Rows per Parquet Kafka message (default ``2000``).
        PARQUET_PATH: Merged output path (all sources, A–Z by title).
        SKIP_KAFKA, KAFKA_TOPIC, broker vars: same as legacy; use ``publish_parquet_batches``.
        LOG_LEVEL: logging level for stderr (default ``INFO``).

    Kafka tuning (optional): ``KAFKA_BATCH_SIZE``, ``KAFKA_LINGER_MS``,
    ``KAFKA_COMPRESSION_TYPE``, ``KAFKA_BUFFER_MEMORY``, ``KAFKA_MAX_IN_FLIGHT``, ``KAFKA_ACKS``.

    See ``producer/EDGE_CASES.md`` for edge-case behavior and safety limits.
    """
    from producer.multisource import configure_logging, gather_all_records, load_source_configs
    from producer.publish import publish_parquet_batches
    from producer.scrape import (
        job_records_to_parquet_batches,
        job_records_to_sorted_parquet_bytes,
        sanitize_job_records_for_parquet,
    )

    configure_logging()
    out = Path(os.environ.get("PARQUET_PATH", "open_positions.parquet"))
    try:
        sources = load_source_configs()
    except ValueError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
    max_conc = int(os.environ.get("MAX_CONCURRENT_SOURCES", "32"))
    records = gather_all_records(sources, max_concurrent=max_conc)
    if not records:
        print(
            "No open positions from any source; check CAREERS_SOURCES / CAREERS_URL and page markup.",
            file=sys.stderr,
        )
        sys.exit(1)

    records = sanitize_job_records_for_parquet(records)
    merged_parquet = job_records_to_sorted_parquet_bytes(records)
    batch_rows = int(os.environ.get("PARQUET_BATCH_MAX_ROWS", "2000"))
    kafka_chunks = job_records_to_parquet_batches(records, max_rows_per_file=batch_rows)

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(merged_parquet)

    if _truthy("SKIP_KAFKA"):
        print(
            f"High-throughput run: {len(records)} rows → {out.resolve()} "
            f"({len(kafka_chunks)} batched Parquet payload(s); SKIP_KAFKA set)"
        )
        return

    publish_parquet_batches(kafka_chunks)
    print(
        f"High-throughput run: {len(records)} rows → {out.resolve()} "
        f"and {len(kafka_chunks)} Parquet message(s) to Kafka"
    )


def _dedupe_str_sequence(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in values:
        v = raw.strip()
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _legacy_listing_urls(primary: str) -> list[str]:
    urls = [primary]
    extra = os.environ.get("CAREERS_MERGE_URLS", "").strip()
    if extra:
        urls.extend(part.strip() for part in extra.split(",") if part.strip())
    return _dedupe_str_sequence(urls)


def main() -> None:
    """
    Scrape careers, write Parquet, optionally publish to Kafka / Event Hubs.

    Output is **raw** two-column Parquet. Enrichment runs in the **consumer** (see
    ``ENRICH_WITH_DUCKDB`` there) or in an external stream processor — ``enrichment/`` package.

    Set ``HIGH_THROUGHPUT=1`` for concurrent multi-source ingestion (see ``main_high_throughput``).

    Environment (legacy / default):
        CAREERS_URL: Listing page URL (default ``https://wsc-sports.com/Careers``).
        CAREERS_MERGE_URLS: Optional comma-separated extra listing URLs; fetched in order and
            merged (dedupe by job URL / title) so a partial primary HTML can be supplemented.
        PARQUET_PATH: Output Parquet file path (default ``open_positions.parquet``).
        SKIP_KAFKA: If ``1`` / ``true`` / ``yes``, skip broker publish after writing Parquet.

    Broker publish (when ``SKIP_KAFKA`` is not set) uses ``publish_parquet_bytes`` /
    ``broker_connection_kwargs``.

    Raises:
        SystemExit: Code ``1`` if no job titles were parsed from the listing.

    Edge cases: ``producer/EDGE_CASES.md``.
    """
    if _truthy("HIGH_THROUGHPUT"):
        main_high_throughput()
        return

    from producer.scrape import (
        fetch_page,
        job_records_to_sorted_parquet_bytes,
        maybe_dump_listing_html,
        sanitize_job_records_for_parquet,
        scrape_wsc_job_records,
        site_origin_from_url,
    )

    url = os.environ.get("CAREERS_URL", "https://wsc-sports.com/Careers").strip()
    listing_urls = _legacy_listing_urls(url)
    out = Path(os.environ.get("PARQUET_PATH", "open_positions.parquet"))

    raw_records: list[dict[str, str]] = []
    for i, listing_url in enumerate(listing_urls):
        try:
            html = fetch_page(listing_url)
        except ValueError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)
        except requests.RequestException as e:
            print(f"Failed to download listing {listing_url!r}: {e}", file=sys.stderr)
            sys.exit(1)

        label = urlparse(listing_url).netloc or "listing"
        if i:
            label = f"{label}-merge{i}"
        maybe_dump_listing_html(html, label=label)
        origin = site_origin_from_url(listing_url)
        raw_records.extend(scrape_wsc_job_records(html, origin))

    records = sanitize_job_records_for_parquet(raw_records)
    if not records:
        print("No open positions found; page structure may have changed.", file=sys.stderr)
        sys.exit(1)

    print(
        f"Parsed {len(records)} open position(s) from {len(listing_urls)} listing URL(s)",
        file=sys.stderr,
    )

    parquet_bytes = job_records_to_sorted_parquet_bytes(records)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(parquet_bytes)

    if _truthy("SKIP_KAFKA"):
        print(f"Wrote {len(records)} rows to {out.resolve()} (SKIP_KAFKA set)")
        return

    from producer.publish import publish_parquet_bytes

    publish_parquet_bytes(parquet_bytes)
    print(f"Wrote {len(records)} rows to {out.resolve()} and sent parquet to Kafka topic")


if __name__ == "__main__":
    main()
