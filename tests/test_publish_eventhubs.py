"""Producer publish.py behaviour when ``EVENTHUB_CONNECTION_STRING`` is set."""

from __future__ import annotations

import pytest

from producer import publish


def test_event_hubs_tuning_empty_without_connection_string(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("EVENTHUB_CONNECTION_STRING", raising=False)
    assert publish._event_hubs_producer_tuning() == {}


def test_event_hubs_tuning_applied_with_connection_string(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://x.servicebus.windows.net/;key=v")
    d = publish._event_hubs_producer_tuning()
    assert d["max_request_size"] == 1000000
    assert d["metadata_max_age_ms"] == 180000
    assert d["request_timeout_ms"] == 60000


def test_headers_omitted_for_event_hubs_by_default(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://x.servicebus.windows.net/;key=v")
    monkeypatch.delenv("KAFKA_INCLUDE_HEADERS", raising=False)
    assert publish._producer_record_headers() is None


def test_headers_included_when_explicit(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://x.servicebus.windows.net/;key=v")
    monkeypatch.setenv("KAFKA_INCLUDE_HEADERS", "1")
    h = publish._producer_record_headers()
    assert h is not None and any(x[0] == "format" for x in h)


def test_tuned_compression_defaults_none_for_event_hubs(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://x.servicebus.windows.net/;key=v")
    monkeypatch.delenv("KAFKA_COMPRESSION_TYPE", raising=False)
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    kw = publish._kafka_producer_kwargs_tuned_for_throughput()
    assert kw["compression_type"] is None
    assert kw["max_request_size"] == 1000000


def test_compression_env_none_string_becomes_python_none(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://x.servicebus.windows.net/;key=v")
    monkeypatch.setenv("KAFKA_COMPRESSION_TYPE", "none")
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    kw = publish._kafka_producer_kwargs_tuned_for_throughput()
    assert kw["compression_type"] is None
