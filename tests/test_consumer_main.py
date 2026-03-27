from __future__ import annotations

import pytest

from consumer.__main__ import _consumer_kwargs, _parquet_row_count


def test_consumer_kwargs_requires_topic(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("KAFKA_TOPIC", raising=False)
    monkeypatch.setenv(
        "EVENTHUB_CONNECTION_STRING",
        "Endpoint=sb://eh.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccessKey=y",
    )
    with pytest.raises(ValueError, match="KAFKA_TOPIC"):
        _consumer_kwargs()


def test_consumer_kwargs_invalid_auto_offset_reset(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("KAFKA_TOPIC", "myhub")
    monkeypatch.setenv("KAFKA_AUTO_OFFSET_RESET", "middle")
    monkeypatch.setenv(
        "EVENTHUB_CONNECTION_STRING",
        "Endpoint=sb://eh.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccessKey=y",
    )
    with pytest.raises(ValueError, match="earliest or latest"):
        _consumer_kwargs()


def test_consumer_kwargs_returns_topic_and_kafka_settings(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("KAFKA_TOPIC", "positions")
    monkeypatch.setenv("KAFKA_CONSUMER_GROUP", "my-group")
    monkeypatch.setenv("KAFKA_AUTO_OFFSET_RESET", "latest")
    monkeypatch.setenv(
        "EVENTHUB_CONNECTION_STRING",
        "Endpoint=sb://eh.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccessKey=y",
    )
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.delenv("KAFKA_CONSUMER_TIMEOUT_MS", raising=False)

    topic, kwargs = _consumer_kwargs()
    assert topic == "positions"
    assert kwargs["group_id"] == "my-group"
    assert kwargs["auto_offset_reset"] == "latest"
    assert kwargs["enable_auto_commit"] is False
    assert kwargs["bootstrap_servers"] == ["eh.servicebus.windows.net:9093"]


def test_parquet_row_count_non_parquet_returns_none():
    assert _parquet_row_count(b"not a parquet file") is None
