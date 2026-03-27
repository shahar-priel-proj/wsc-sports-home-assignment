from __future__ import annotations

import asyncio
import json

import aiohttp
import pytest

from producer.multisource import (
    gather_all_records,
    gather_all_records_async,
    load_source_configs,
)


def test_load_source_configs_uses_default_careers_url(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("CAREERS_SOURCES", raising=False)
    monkeypatch.delenv("CAREERS_URL", raising=False)
    cfgs = load_source_configs()
    assert len(cfgs) == 1
    assert cfgs[0]["url"] == "https://wsc-sports.com/Careers"
    assert cfgs[0]["id"] == "https://wsc-sports.com"
    assert cfgs[0]["origin"] == "https://wsc-sports.com"


def test_load_source_configs_careers_url_override(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("CAREERS_SOURCES", raising=False)
    monkeypatch.setenv("CAREERS_URL", "https://example.com/jobs")
    cfgs = load_source_configs()
    assert len(cfgs) == 1
    assert cfgs[0]["url"] == "https://example.com/jobs"
    assert cfgs[0]["origin"] == "https://example.com"


def test_load_source_configs_json_array(monkeypatch: pytest.MonkeyPatch):
    sources = [
        {"id": "s1", "url": "https://a.example.com/care", "origin": "https://a.example.com"},
        {"url": "https://b.example.com/care"},
    ]
    monkeypatch.setenv("CAREERS_SOURCES", json.dumps(sources))
    cfgs = load_source_configs()
    assert len(cfgs) == 2
    assert cfgs[0]["id"] == "s1"
    assert cfgs[0]["origin"] == "https://a.example.com"
    assert cfgs[1]["id"] == "https://b.example.com"
    assert cfgs[1]["origin"] == "https://b.example.com"


def test_load_source_configs_rejects_empty_sources(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CAREERS_SOURCES", "[]")
    with pytest.raises(ValueError, match="non-empty"):
        load_source_configs()


def test_load_source_configs_rejects_bad_json(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CAREERS_SOURCES", "not json")
    with pytest.raises(ValueError, match="valid JSON"):
        load_source_configs()


def test_load_source_configs_rejects_missing_url(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CAREERS_SOURCES", json.dumps([{"id": "x"}]))
    with pytest.raises(ValueError, match="url"):
        load_source_configs()


def test_load_source_configs_rejects_ftp(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(
        "CAREERS_SOURCES",
        json.dumps([{"url": "ftp://evil.com/list"}]),
    )
    with pytest.raises(ValueError, match="http"):
        load_source_configs()


@pytest.fixture
def sample_html_jobs() -> tuple[str, str]:
    a = '<a href="https://a.example.com/career/role-a/">Role A View Position</a>'
    b = '<a href="https://b.example.com/career/role-b/">Role B View Position</a>'
    return a, b


def test_gather_all_records_async_merges_sources(
    monkeypatch: pytest.MonkeyPatch,
    sample_html_jobs: tuple[str, str],
):
    html_a, html_b = sample_html_jobs

    async def fake_fetch(
        session: aiohttp.ClientSession,
        url: str,
        *,
        timeout_s: float = 60.0,
    ) -> str:
        if "a.example.com" in url:
            return html_a
        if "b.example.com" in url:
            return html_b
        raise AssertionError(f"unexpected url {url!r}")

    monkeypatch.setattr("producer.multisource.fetch_page_async", fake_fetch)

    sources = [
        {"id": "src-a", "url": "https://a.example.com/care", "origin": "https://a.example.com"},
        {"id": "src-b", "url": "https://b.example.com/care", "origin": "https://b.example.com"},
    ]

    async def run():
        return await gather_all_records_async(sources, max_concurrent=2)

    records = asyncio.run(run())
    assert len(records) == 2
    by_src = {r["source"]: r for r in records}
    assert by_src["src-a"]["position_title"] == "Role A"
    assert by_src["src-b"]["position_title"] == "Role B"


def test_gather_all_records_async_failed_fetch_returns_no_rows_for_source(
    monkeypatch: pytest.MonkeyPatch,
):
    async def fail_fetch(
        session: aiohttp.ClientSession,
        url: str,
        *,
        timeout_s: float = 60.0,
    ) -> str:
        raise aiohttp.ClientConnectionError("offline")

    monkeypatch.setattr("producer.multisource.fetch_page_async", fail_fetch)
    monkeypatch.setenv("HTTP_FETCH_RETRIES", "1")

    sources = [
        {"id": "down", "url": "https://down.example.com/", "origin": "https://down.example.com"},
    ]

    async def run():
        return await gather_all_records_async(sources, max_concurrent=1)

    assert asyncio.run(run()) == []


def test_gather_all_records_async_max_concurrent_invalid():
    with pytest.raises(ValueError, match="max_concurrent"):
        asyncio.run(gather_all_records_async([], max_concurrent=0))


def test_gather_all_records_sync_wraps_async(
    monkeypatch: pytest.MonkeyPatch,
    sample_html_jobs: tuple[str, str],
):
    html_a, _ = sample_html_jobs

    async def fake_fetch(
        session: aiohttp.ClientSession,
        url: str,
        *,
        timeout_s: float = 60.0,
    ) -> str:
        return html_a

    monkeypatch.setattr("producer.multisource.fetch_page_async", fake_fetch)
    sources = [
        {"id": "only", "url": "https://a.example.com/care", "origin": "https://a.example.com"},
    ]
    records = gather_all_records(sources, max_concurrent=1)
    assert len(records) == 1
    assert records[0]["source"] == "only"
    assert records[0]["position_title"] == "Role A"
