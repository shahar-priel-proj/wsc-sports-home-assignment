from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import sys
from typing import Any

import aiohttp

from producer.scrape import (
    fetch_page_async,
    listing_request_headers,
    maybe_dump_listing_html,
    parse_url_loose,
    scrape_wsc_job_records,
    site_origin_from_url,
)

log = logging.getLogger(__name__)


def _validate_listing_url(url: str, *, context: str) -> None:
    p = parse_url_loose(url.strip())
    if not p.netloc:
        raise ValueError(f"{context}: URL missing host: {url!r}")
    scheme = (p.scheme or "https").lower()
    if scheme not in ("http", "https"):
        raise ValueError(f"{context}: only http(s) listing URLs allowed, got {url!r}")


def load_source_configs() -> list[dict[str, str]]:
    """
    Load listing sources from ``CAREERS_SOURCES`` JSON or fall back to ``CAREERS_URL``.

    ``CAREERS_SOURCES`` format: JSON array of objects, each with ``id``, ``url``,
    and optional ``origin`` (scheme+host for resolving relative links).

    Returns:
        Non-empty list of source config dicts.

    Raises:
        ValueError: On invalid JSON shape, missing ``url``, or non-http(s) URLs.
        json.JSONDecodeError: Propagated with invalid JSON (wrap in message at call site if needed).
    """
    raw = os.environ.get("CAREERS_SOURCES", "").strip()
    if raw:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(
                "CAREERS_SOURCES must be valid JSON array of objects: " + str(e)
            ) from e
        if not isinstance(data, list) or not data:
            raise ValueError("CAREERS_SOURCES must be a non-empty JSON array")
        out: list[dict[str, str]] = []
        for i, item in enumerate(data):
            if not isinstance(item, dict) or "url" not in item:
                raise ValueError(f"CAREERS_SOURCES[{i}] must be an object with a string url")
            url = str(item["url"]).strip()
            if not url:
                raise ValueError(f"CAREERS_SOURCES[{i}].url must be non-empty")
            _validate_listing_url(url, context=f"CAREERS_SOURCES[{i}]")
            sid = str(item.get("id") or site_origin_from_url(url)).strip()
            origin_raw = item.get("origin")
            origin = str(origin_raw).strip() if origin_raw else site_origin_from_url(url)
            _validate_listing_url(origin, context=f"CAREERS_SOURCES[{i}].origin")
            out.append({"id": sid, "url": url, "origin": origin.rstrip("/")})
        return out

    u = os.environ.get("CAREERS_URL", "https://wsc-sports.com/Careers").strip()
    if not u:
        raise ValueError("CAREERS_URL is empty")
    _validate_listing_url(u, context="CAREERS_URL")
    o = site_origin_from_url(u)
    return [{"id": o, "url": u, "origin": o.rstrip("/")}]


def _fetch_attempts() -> int:
    return max(1, int(os.environ.get("HTTP_FETCH_RETRIES", "3")))


def _fetch_backoff_sec() -> float:
    return max(0.0, float(os.environ.get("HTTP_RETRY_BACKOFF_SEC", "0.5")))


async def _fetch_source_records(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    cfg: dict[str, str],
) -> list[dict[str, str]]:
    """
    Fetch one listing URL and return job rows tagged with ``source``.

    Retries transient failures up to ``HTTP_FETCH_RETRIES`` with exponential backoff
    (base ``HTTP_RETRY_BACKOFF_SEC``).

    Args:
        session: Shared HTTP session.
        sem: Concurrency limiter (per-task acquire).
        cfg: Must include ``id``, ``url``, ``origin``.

    Returns:
        Records with keys ``source``, ``position_title``, ``job_url``.
    """
    async with sem:
        html: str | None = None
        attempts = _fetch_attempts()
        base_backoff = _fetch_backoff_sec()
        for attempt in range(attempts):
            try:
                html = await fetch_page_async(session, cfg["url"])
                break
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                if attempt + 1 >= attempts:
                    log.warning(
                        "Giving up on source %s (%s) after %s attempts: %s",
                        cfg["id"],
                        cfg["url"],
                        attempts,
                        exc,
                    )
                else:
                    delay = base_backoff * (2**attempt) + random.uniform(0, 0.15)
                    log.info(
                        "Retrying source %s (%s) in %.2fs after error: %s",
                        cfg["id"],
                        cfg["url"],
                        delay,
                        exc,
                    )
                    await asyncio.sleep(delay)

        if html is None:
            return []

        maybe_dump_listing_html(html, label=str(cfg.get("id") or "listing"))

        try:
            rows = scrape_wsc_job_records(html, cfg["origin"])
        except Exception as exc:
            log.warning("Parse failed for source %s: %s", cfg["id"], exc)
            return []

        out: list[dict[str, str]] = []
        for r in rows:
            out.append(
                {
                    "source": cfg["id"],
                    "position_title": r["position_title"],
                    "job_url": r["job_url"],
                }
            )
        return out


async def gather_all_records_async(
    sources: list[dict[str, str]],
    *,
    max_concurrent: int,
    connector_limit: int | None = None,
) -> list[dict[str, str]]:
    """
    Concurrently fetch and parse all sources.

    Args:
        sources: Listing configs (``id``, ``url``, ``origin``).
        max_concurrent: Max simultaneous in-flight source fetches.
        connector_limit: Optional aiohttp connector limit; defaults to ``max_concurrent``.

    Returns:
        Flat list of job records from all successful sources.
    """
    if max_concurrent < 1:
        raise ValueError("max_concurrent must be >= 1")
    limit = connector_limit or max_concurrent
    sem = asyncio.Semaphore(max_concurrent)
    connector = aiohttp.TCPConnector(limit=limit, limit_per_host=max(1, min(max_concurrent, 32)))
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=120)
    async with aiohttp.ClientSession(
        headers=listing_request_headers(), connector=connector, timeout=timeout
    ) as session:
        tasks = [_fetch_source_records(session, sem, c) for c in sources]
        nested = await asyncio.gather(*tasks)
    flat: list[dict[str, str]] = []
    for chunk in nested:
        flat.extend(chunk)
    return flat


def gather_all_records(
    sources: list[dict[str, str]],
    *,
    max_concurrent: int,
) -> list[dict[str, Any]]:
    """
    Sync entrypoint for async multi-source gather (for use from non-async code).

    Args:
        sources: Listing configs.
        max_concurrent: Max parallel fetches.

    Returns:
        Combined job records from all sources.
    """
    return asyncio.run(gather_all_records_async(sources, max_concurrent=max_concurrent))


def configure_logging() -> None:
    """Set log level from ``LOG_LEVEL`` (default INFO)."""
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
