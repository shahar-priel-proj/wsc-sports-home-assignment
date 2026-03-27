from __future__ import annotations

from io import BytesIO

import pyarrow.parquet as pq

from producer.scrape import (
    canonical_job_url,
    positions_to_parquet_bytes,
    sanitize_job_records_for_parquet,
    scrape_wsc_job_records,
)


def test_scrape_wsc_job_records_mixed_case_hrefs():
    html = """
    <html><body>
    <a href="https://wsc-sports.com/career/one/">Alpha View Position</a>
    <a href="https://wsc-sports.com/Career/two/">Beta View Position</a>
    <a href="career/three/">Gamma View Position</a>
    </body></html>
    """
    recs = scrape_wsc_job_records(html, "https://wsc-sports.com")
    assert len(recs) == 3
    titles = {r["position_title"] for r in recs}
    assert titles == {"Alpha", "Beta", "Gamma"}


def test_scrape_wsc_job_records_supplements_slugs_without_anchors():
    html = """
    <html><body><script type="application/json">
    {"x":"https://wsc-sports.com/career/json-only-role/"}
    </script></body></html>
    """
    recs = scrape_wsc_job_records(html, "https://wsc-sports.com")
    assert len(recs) == 1
    assert recs[0]["position_title"] == "Json Only Role"


def test_scrape_supplements_slugs_with_underscores_in_raw_html():
    html = """
    <html><body><script>"/career/lead_engineer_role/"</script></body></html>
    """
    recs = scrape_wsc_job_records(html, "https://wsc-sports.com")
    assert len(recs) == 1
    assert recs[0]["position_title"] == "Lead Engineer Role"


def test_positions_to_parquet_bytes_dedupes_titles_case_insensitive():
    raw = positions_to_parquet_bytes(["Engineer", "engineer", "Designer"])
    df = pq.read_table(BytesIO(raw)).to_pandas()
    assert len(df) == 2
    assert set(df["Position title"]) == {"Engineer", "Designer"}


def test_sanitize_keeps_distinct_urls_even_when_titles_match_case_insensitively():
    records = [
        {"position_title": "Same", "job_url": "https://wsc-sports.com/career/a/"},
        {"position_title": "SAME", "job_url": "https://wsc-sports.com/career/b/"},
        {"position_title": "Other", "job_url": "https://wsc-sports.com/career/c/"},
    ]
    out = sanitize_job_records_for_parquet(records)
    assert len(out) == 3


def test_sanitize_dedupes_duplicate_canonical_urls():
    records = [
        {"position_title": "First", "job_url": "https://wsc-sports.com/career/a/"},
        {"position_title": "Second pass", "job_url": "https://WSC-Sports.com/career/a?utm_source=x"},
    ]
    out = sanitize_job_records_for_parquet(records)
    assert len(out) == 1
    assert out[0]["position_title"] == "First"


def test_canonical_job_url_strips_tracking_params():
    u = canonical_job_url(
        "https://WSC-Sports.com/career/foo/?utm_source=Email&utm_medium=mail"
    )
    assert u == "https://wsc-sports.com/career/foo"


def test_canonical_job_url_trailing_slash_normalized():
    a = canonical_job_url("https://wsc-sports.com/career/foo/")
    b = canonical_job_url("https://wsc-sports.com/career/foo")
    assert a == b
