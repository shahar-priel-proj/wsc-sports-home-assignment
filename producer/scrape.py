from __future__ import annotations

import logging
import os
import re
import unicodedata
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import aiohttp
import pandas as pd
import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# Many career sites (incl. CDNs / WordPress) serve a **short** listing to non‑browser User-Agents.
# Default mimics a current desktop Chrome; override with ``SCRAPER_USER_AGENT`` or set
# ``SCRAPER_USE_BOT_UA=1`` for the old cooperative bot string.
_CHROME_LIKE_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
_BOT_UA = "Mozilla/5.0 (compatible; WSC-CareersProducer/1.0; +https://wsc-sports.com/)"

DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": _CHROME_LIKE_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    # Some CDNs / WAFs vary markup for requests that do not look like a browser navigation.
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}


def listing_request_headers() -> dict[str, str]:
    """HTTP headers for listing fetches; override User-Agent with ``SCRAPER_USER_AGENT``."""
    h = {**DEFAULT_HEADERS}
    if os.environ.get("SCRAPER_USE_BOT_UA", "").lower() in ("1", "true", "yes"):
        h["User-Agent"] = _BOT_UA
    ua = os.environ.get("SCRAPER_USER_AGENT", "").strip()
    if ua:
        h["User-Agent"] = ua
    return h

_DEFAULT_MAX_RESPONSE_BYTES = 15 * 1024 * 1024
_DEFAULT_TITLE_MAX_CHARS = 2000
_TRACKING_QUERY_PREFIXES = ("utm_",)
_TRACKING_QUERY_KEYS = frozenset({"fbclid", "gclid", "_ga", "mc_eid"})


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").lower() in ("1", "true", "yes")


def _max_response_bytes() -> int:
    return max(1024, int(os.environ.get("MAX_RESPONSE_BYTES", str(_DEFAULT_MAX_RESPONSE_BYTES))))


def _title_max_chars() -> int:
    return max(1, int(os.environ.get("TITLE_MAX_CHARS", str(_DEFAULT_TITLE_MAX_CHARS))))


def _strip_tracking_query_params(query: str) -> str:
    """Drop common marketing/analytics query keys so URLs dedupe reliably."""
    if _truthy_env("KEEP_ALL_QUERY_PARAMS"):
        return query
    if not query:
        return ""
    pairs = []
    for key, value in parse_qsl(query, keep_blank_values=True):
        kl = key.lower()
        if kl in _TRACKING_QUERY_KEYS or any(kl.startswith(p) for p in _TRACKING_QUERY_PREFIXES):
            continue
        pairs.append((key, value))
    pairs.sort()
    return urlencode(pairs)


def canonical_job_url(url: str) -> str:
    """
    Normalize a job URL for stable deduplication and storage.

    - Lowercases scheme and host.
    - Removes fragment.
    - Trims trailing slash on path (except root).
    - Optionally strips tracking query parameters (default) unless ``KEEP_ALL_QUERY_PARAMS=1``.

    Args:
        url: Absolute job URL.

    Returns:
        Canonical string form.

    Raises:
        ValueError: If URL cannot be parsed into a network location.
    """
    raw = (url or "").strip()
    if not raw or raw.lower().startswith(("javascript:", "mailto:", "tel:", "data:")):
        raise ValueError(f"Not a HTTP(S) job URL: {raw!r}")
    if raw.startswith("//"):
        p = urlparse("https:" + raw)
    else:
        p = urlparse(raw)
    if not p.netloc:
        raise ValueError(f"URL missing host: {raw!r}")
    scheme = (p.scheme or "https").lower()
    if scheme not in ("http", "https"):
        raise ValueError(f"Only http(s) URLs supported, got scheme {scheme!r}")
    netloc = p.netloc.lower()
    path = p.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    query = _strip_tracking_query_params(p.query)
    return urlunparse((scheme, netloc, path, "", query, ""))


def fetch_page(url: str, timeout_s: float = 60.0) -> str:
    """
    Download a URL and return the response body as text.

    Aborts if the body exceeds ``MAX_RESPONSE_BYTES`` (default 15 MiB) to avoid memory abuse.
    Decodes with the declared charset, falling back to UTF-8 with replacement for bad bytes.

    Args:
        url: Fully qualified page URL (e.g. the WSC careers listing).
        timeout_s: Maximum time in seconds to wait for the server to respond.

    Returns:
        HTML document as a string.

    Raises:
        requests.HTTPError: If the HTTP status is not successful.
        ValueError: If the response body exceeds the configured size cap.
    """
    max_b = _max_response_bytes()
    with requests.get(
        url, headers=listing_request_headers(), timeout=timeout_s, stream=True
    ) as resp:
        resp.raise_for_status()
        enc = resp.encoding or "utf-8"
        final_url = str(resp.url)
        status_code = resp.status_code
        chunks: list[bytes] = []
        total = 0
        for chunk in resp.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            total += len(chunk)
            if total > max_b:
                raise ValueError(
                    f"Response larger than MAX_RESPONSE_BYTES ({max_b}); "
                    "increase env MAX_RESPONSE_BYTES if this is expected."
                )
            chunks.append(chunk)
        data = b"".join(chunks)
    text = data.decode(enc, errors="replace")
    log.info(
        "HTTP listing: requested=%r final_url=%r status=%s decoded_chars=%s",
        url,
        final_url,
        status_code,
        len(text),
    )
    return text


async def fetch_page_async(
    session: aiohttp.ClientSession,
    url: str,
    *,
    timeout_s: float = 60.0,
) -> str:
    """
    Async variant of ``fetch_page`` for concurrent multi-source ingestion.

    Enforces ``MAX_RESPONSE_BYTES`` like the sync path.

    Args:
        session: Shared ``aiohttp`` client session (connection pooling).
        url: Fully qualified listing page URL.
        timeout_s: Total timeout for the request.

    Returns:
        Response body as text.

    Raises:
        aiohttp.ClientResponseError: On non-success HTTP status.
        aiohttp.ClientError: On network-level failures.
        ValueError: If the body exceeds ``MAX_RESPONSE_BYTES``.
    """
    max_b = _max_response_bytes()
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    async with session.get(url, timeout=timeout) as resp:
        resp.raise_for_status()
        body = await resp.content.read(max_b + 1)
        if len(body) > max_b:
            raise ValueError(
                f"Response larger than MAX_RESPONSE_BYTES ({max_b}); "
                "increase env MAX_RESPONSE_BYTES if this is expected."
            )
        enc = resp.charset or "utf-8"
        text = body.decode(enc, errors="replace")
        log.info(
            "HTTP listing (async): requested=%r final_url=%r status=%s decoded_chars=%s",
            url,
            str(resp.url),
            resp.status,
            len(text),
        )
        return text


def _normalize_job_title(raw: str) -> str:
    """
    Clean anchor text from the listing into a single-line position title.

    Applies Unicode NFC, collapses whitespace, strips trailing "View Position", and
    truncates to ``TITLE_MAX_CHARS`` (default 2000).

    Args:
        raw: Raw text from a job link (may include non-breaking spaces and a trailing
            ``View Position`` label).

    Returns:
        Normalized title string, or empty string if nothing remains after cleaning.
    """
    if raw is None:
        return ""
    text = str(raw).replace("\xa0", " ")
    text = "".join(ch for ch in text if ch.isprintable() or ch in "\t\n\r")
    text = unicodedata.normalize("NFC", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\s*View\s+Position\s*$", "", text, flags=re.IGNORECASE).strip()
    limit = _title_max_chars()
    if len(text) > limit:
        log.debug("Truncating title from %d to %d chars", len(text), limit)
        text = text[:limit].rstrip()
    return text


def _job_slug_from_href(href: str) -> str | None:
    """
    Parse a job listing URL and return the path slug if it is a single-role page.

    Args:
        href: A relative or absolute ``href`` (e.g. ``/career/backend-engineer/`` or
            a full URL). Query strings and fragments are ignored.

    Returns:
        The slug segment after ``/career/``, or ``None`` if this is not a job-detail
        URL (e.g. ``/careers/`` or ``/career`` only).
    """
    path = href.split("#")[0].split("?")[0]
    parts = [p for p in path.split("/") if p]
    try:
        idx = [p.lower() for p in parts].index("career")
    except ValueError:
        return None
    if idx + 1 >= len(parts):
        return None
    slug = parts[idx + 1]
    if slug.lower() in ("", "careers", "career"):
        return None
    return slug


def _unique_career_slugs_from_raw_html(html: str) -> list[str]:
    """
    Find ``/career/<slug>/`` segments anywhere in the HTML (not only ``<a href>``).

    Some environments serve markup where job URLs appear only in JSON, comments, or
    attributes the parser does not walk, yielding too few ``<a>`` tags. This list is
    used to supplement anchor-based discovery.
    """
    # Allow underscores for uncommon slugs; `[a-z0-9-]+` misses segments embedded in minified JSON.
    slugs = re.findall(r"/career/([a-z0-9](?:[a-z0-9_-]*[a-z0-9])?)/?", html, flags=re.I)
    out: list[str] = []
    seen: set[str] = set()
    for s in slugs:
        ls = s.lower()
        if ls in ("", "career", "careers") or ls in seen:
            continue
        seen.add(ls)
        out.append(s)
    return out


def _title_from_career_slug(slug: str) -> str:
    """Readable title when we only know the URL slug (last resort)."""
    return re.sub(r"[-_]+", " ", slug).strip().title()


def _href_might_be_job_link(href: str) -> bool:
    """
    True if href likely points at a single-job page under .../career/<slug>/.

    Uses case-folding so variants like ``/Career/`` match. Accepts site-root-relative
    ``career/<slug>`` (no leading slash) as well as absolute or protocol-relative URLs.
    """
    h = (href or "").strip().lower()
    if not h or h.startswith(("javascript:", "mailto:", "tel:", "#", "data:")):
        return False
    if "/career/" in h:
        return True
    return h.startswith("career/") and not h.startswith("careers/")


def scrape_wsc_job_records(html: str, site_origin: str = "https://wsc-sports.com") -> list[dict[str, str]]:
    """
    Parse one careers listing HTML page into job rows (title + absolute job URL).

    Handles empty HTML, skips ``javascript:`` / ``mailto:`` links, case-insensitive
    ``/career/`` path matching, and dedupes by canonical job URL.

    Args:
        html: Full HTML source of the careers listing page.
        site_origin: Scheme and host used to resolve relative ``href`` values (no trailing slash).

    Returns:
        One dict per unique canonical job URL: ``position_title``, ``job_url``.
    """
    if not (html or "").strip():
        return []

    soup = BeautifulSoup(html, "html.parser")
    seen_keys: set[str] = set()
    rows: list[dict[str, str]] = []

    # Always scan all anchors. Case-sensitive CSS ``[href*="/career/"]`` can match only a
    # subset of links (e.g. mixed ``/Career/`` vs ``/career/``), which previously skipped
    # the fallback path and yielded too few rows.
    candidates: list[Any] = []
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if _href_might_be_job_link(href):
            candidates.append(a)

    for a in candidates:
        href = (a.get("href") or "").strip()
        if not href or _job_slug_from_href(href) is None:
            continue
        if href.lower().startswith(("javascript:", "mailto:", "tel:", "#")):
            continue

        try:
            abs_raw = urljoin(site_origin.rstrip("/") + "/", href)
            canon = canonical_job_url(abs_raw)
        except ValueError as e:
            log.debug("Skipping non-canonical href %r: %s", href, e)
            continue

        key = canon.lower()
        if key in seen_keys:
            continue
        seen_keys.add(key)

        title = _normalize_job_title(a.get_text(separator=" ", strip=True))
        if title:
            rows.append({"position_title": title, "job_url": canon})

    rows_from_anchors = len(rows)
    origin_base = site_origin.rstrip("/")
    for slug in _unique_career_slugs_from_raw_html(html):
        try:
            abs_raw = urljoin(origin_base + "/", f"career/{slug}/")
            canon = canonical_job_url(abs_raw)
        except ValueError:
            continue
        key = canon.lower()
        if key in seen_keys:
            continue
        seen_keys.add(key)
        rows.append({"position_title": _title_from_career_slug(slug), "job_url": canon})

    if len(rows) > rows_from_anchors:
        log.info(
            "Supplemented %s job row(s) from raw HTML /career/ slug scan (anchors yielded %s)",
            len(rows) - rows_from_anchors,
            rows_from_anchors,
        )
    slug_scan = len(_unique_career_slugs_from_raw_html(html))
    html_len = len(html or "")
    log.info(
        "Careers HTML: %s chars, %s unique /career/ slugs in raw HTML, %s anchor link(s), "
        "%s row(s) from anchors, %s row(s) total",
        html_len,
        slug_scan,
        len(candidates),
        rows_from_anchors,
        len(rows),
    )
    if html_len < 50000 and len(rows) < 12:
        log.warning(
            "Only %s job row(s) and HTML is %s chars (full WSC listing is often ~90k+). "
            "Check producer logs for 'HTTP listing: … decoded_chars=' — if chars are low, the server "
            "gave a short document (network / region / WAF). Use SCRAPER_DUMP_HTML and compare to "
            "View Source in a browser; set SCRAPER_USER_AGENT to match your browser; or set "
            "CAREERS_MERGE_URLS (comma-separated listing URLs) to merge rows from multiple fetches.",
            len(rows),
            html_len,
        )
    return rows


def maybe_dump_listing_html(html: str, *, label: str = "listing") -> None:
    """
    If ``SCRAPER_DUMP_HTML`` is set, write HTML for debugging (encoding UTF-8).

    If the path ends with ``/`` or exists as a directory, writes
    ``{dir}/listing-{label}.html``; otherwise overwrites the given file path.
    """
    raw = os.environ.get("SCRAPER_DUMP_HTML", "").strip()
    if not raw or not html:
        return
    safe_label = re.sub(r"[^a-zA-Z0-9._-]+", "_", label).strip("_")[:80] or "listing"
    path = Path(raw).expanduser()
    try:
        is_dir_target = raw.endswith(("/", "\\")) or (path.exists() and path.is_dir())
        if is_dir_target:
            path.mkdir(parents=True, exist_ok=True)
            out = path / f"listing-{safe_label}.html"
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            out = path
        out.write_text(html, encoding="utf-8")
        log.info("Wrote listing HTML dump (%s chars) to %s", len(html), out.resolve())
    except OSError as e:
        log.warning("Skipping SCRAPER_DUMP_HTML: %s", e)


def scrape_wsc_open_positions(html: str, site_origin: str = "https://wsc-sports.com") -> list[str]:
    """
    Collect open role titles from one HTML document (titles only; see ``scrape_wsc_job_records``).

    Args:
        html: Full HTML source of the careers listing page.
        site_origin: Scheme and host used to resolve relative ``href`` values (no trailing slash).

    Returns:
        One human-readable title per unique job URL, in page discovery order (not yet A–Z).
    """
    return [r["position_title"] for r in scrape_wsc_job_records(html, site_origin)]


def site_origin_from_url(url: str) -> str:
    """
    Args:
        url: Any HTTP(S) URL.

    Returns:
        ``{scheme}://{netloc}`` suitable for resolving relative links.

    Raises:
        ValueError: If the URL has no scheme or host.
    """
    raw = (url or "").strip()
    p = parse_url_loose(raw)
    if not p.netloc:
        raise ValueError(f"Cannot derive site origin from URL (missing host): {url!r}")
    scheme = (p.scheme or "https").lower()
    if scheme not in ("http", "https"):
        raise ValueError(f"Expected http(s) listing URL, got scheme {scheme!r} for {url!r}")
    return f"{scheme}://{p.netloc.lower()}"


def parse_url_loose(raw: str):
    """Parse URL; scheme-relative ``//host`` becomes ``https://host``."""
    if raw.startswith("//"):
        return urlparse("https:" + raw)
    return urlparse(raw)


def sanitize_job_records_for_parquet(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Coerce titles, drop empty rows, dedupe by canonical ``job_url`` (or nourl/source/title key).

    Rows with different ``job_url`` values are kept even when ``position_title`` matches
    case-insensitively (two postings can legitimately share a label).

    Args:
        records: Items with ``position_title`` and ideally ``job_url``.

    Returns:
        Sanitized list safe for sorting and Parquet.
    """
    seen_key: set[str] = set()
    out: list[dict[str, Any]] = []
    for r in records:
        title = _normalize_job_title(
            r.get("position_title") if r.get("position_title") is not None else ""
        )
        if not title:
            continue
        jid = r.get("job_url") or ""
        src = str(r.get("source") or "")
        try:
            key = (
                canonical_job_url(str(jid)).lower()
                if jid
                else f"nourl:{src}:{title.casefold()}"
            )
        except ValueError:
            key = f"{str(jid).strip().lower()}|{src}|{title.casefold()}"
        if key in seen_key:
            continue
        seen_key.add(key)
        row = {"position_title": title, "job_url": str(jid)}
        if "source" in r:
            row["source"] = str(r["source"])
        out.append(row)
    return out


def positions_to_parquet_bytes(titles: list[str]) -> bytes:
    """
    Build a Parquet file in memory: ``Index`` (1…n) and ``Position title``, A–Z by title.

    Duplicate titles (after normalization, compared case-insensitively) are removed; the
    first occurrence wins.

    Args:
        titles: Role names as scraped (order does not matter; output is sorted case-insensitively).

    Returns:
        Parquet file bytes (Apache Arrow / PyArrow), two columns, no DataFrame index.
    """
    cleaned = [_normalize_job_title(t) for t in titles]
    cleaned = [t for t in cleaned if t]
    seen_cf: set[str] = set()
    unique: list[str] = []
    for t in cleaned:
        k = t.casefold()
        if k in seen_cf:
            continue
        seen_cf.add(k)
        unique.append(t)
    ordered = sorted(unique, key=str.casefold)
    df = pd.DataFrame(
        {
            "Index": range(1, len(ordered) + 1),
            "Position title": ordered,
        }
    )
    buf = BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    return buf.getvalue()


def job_records_to_sorted_parquet_bytes(records: list[dict[str, Any]]) -> bytes:
    """
    Build one Parquet file from normalized job records (multi-source ingest).

    Same schema as legacy mode: ``Index`` and ``Position title`` only, sorted A–Z by
    title (case-insensitive). Records are passed through ``sanitize_job_records_for_parquet``.

    Args:
        records: Dicts with at least ``position_title`` (and ``job_url`` for deduplication).

    Returns:
        Parquet bytes.
    """
    records = sanitize_job_records_for_parquet(records)
    if not records:
        df = pd.DataFrame(
            {
                "Index": pd.Series(dtype="int64"),
                "Position title": pd.Series(dtype="object"),
            }
        )
        buf = BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        return buf.getvalue()
    df = pd.DataFrame(records)
    df = df.sort_values("position_title", key=lambda s: s.astype(str).str.casefold()).reset_index(
        drop=True
    )
    out = pd.DataFrame(
        {
            "Index": range(1, len(df) + 1),
            "Position title": df["position_title"].astype(str),
        }
    )
    buf = BytesIO()
    out.to_parquet(buf, index=False, engine="pyarrow")
    return buf.getvalue()


def job_records_to_parquet_batches(
    records: list[dict[str, Any]],
    *,
    max_rows_per_file: int,
) -> list[bytes]:
    """
    Split sorted job records into multiple Parquet files (for bounded Kafka message size).

    Schema matches legacy: ``Index`` and ``Position title`` only. Each batch is sorted
    by title globally first; ``Index`` is **per batch** (1…k), not global across messages.

    Args:
        records: Dicts with at least ``position_title``.
        max_rows_per_file: Maximum rows per Parquet payload; must be >= 1.

    Returns:
        List of Parquet byte strings, one per batch, in order.
    """
    if max_rows_per_file < 1:
        raise ValueError("max_rows_per_file must be >= 1")
    records = sanitize_job_records_for_parquet(records)
    if not records:
        return []

    df = pd.DataFrame(records)
    df = df.sort_values("position_title", key=lambda s: s.astype(str).str.casefold()).reset_index(
        drop=True
    )

    batches: list[bytes] = []
    for start in range(0, len(df), max_rows_per_file):
        chunk = df.iloc[start : start + max_rows_per_file]
        part = pd.DataFrame(
            {
                "Index": range(1, len(chunk) + 1),
                "Position title": chunk["position_title"].astype(str).values,
            }
        )
        buf = BytesIO()
        part.to_parquet(buf, index=False, engine="pyarrow")
        batches.append(buf.getvalue())
    return batches
