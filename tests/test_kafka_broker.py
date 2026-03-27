from __future__ import annotations

import pytest

from producer.kafka_broker import (
    broker_connection_kwargs,
    parse_bootstrap_servers,
    parse_eventhub_bootstrap,
)


def test_parse_eventhub_bootstrap():
    cs = (
        "Endpoint=sb://myns.servicebus.windows.net/;"
        "SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=secret="
    )
    assert parse_eventhub_bootstrap(cs) == "myns.servicebus.windows.net:9093"


def test_parse_eventhub_bootstrap_case_insensitive():
    cs = "endpoint=sb://ABC.servicebus.windows.net/;SharedAccessKey=key"
    assert parse_eventhub_bootstrap(cs) == "ABC.servicebus.windows.net:9093"


def test_parse_eventhub_bootstrap_missing_raises():
    with pytest.raises(ValueError, match="Endpoint=sb://"):
        parse_eventhub_bootstrap("NoEndpointHere=foo;")


def test_parse_bootstrap_servers():
    assert parse_bootstrap_servers("a:1,b:2") == ["a:1", "b:2"]
    assert parse_bootstrap_servers(" host:9093 ") == ["host:9093"]
    assert parse_bootstrap_servers("") == []


def test_broker_connection_kwargs_eventhub(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(
        "EVENTHUB_CONNECTION_STRING",
        "Endpoint=sb://eh.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccessKey=y",
    )
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.delenv("KAFKA_CLIENT_ID", raising=False)
    kw = broker_connection_kwargs(default_client_id="default-client")
    assert kw["bootstrap_servers"] == ["eh.servicebus.windows.net:9093"]
    assert kw["security_protocol"] == "SASL_SSL"
    assert kw["sasl_mechanism"] == "PLAIN"
    assert kw["sasl_plain_username"] == "$ConnectionString"
    assert kw["client_id"] == "default-client"


def test_broker_connection_kwargs_bootstrap_override(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(
        "EVENTHUB_CONNECTION_STRING",
        "Endpoint=sb://eh.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccessKey=y",
    )
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "custom:9093,other:9093")
    monkeypatch.setenv("KAFKA_CLIENT_ID", "my-id")
    kw = broker_connection_kwargs(default_client_id="ignored")
    assert kw["bootstrap_servers"] == ["custom:9093", "other:9093"]
    assert kw["client_id"] == "my-id"


def test_broker_connection_kwargs_plain_kafka(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("EVENTHUB_CONNECTION_STRING", raising=False)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kw = broker_connection_kwargs(default_client_id="c")
    assert kw["bootstrap_servers"] == ["kafka:9092"]
    assert kw["client_id"] == "c"
    assert "security_protocol" not in kw


def test_broker_connection_kwargs_missing_broker(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("EVENTHUB_CONNECTION_STRING", raising=False)
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    with pytest.raises(ValueError, match="EVENTHUB_CONNECTION_STRING"):
        broker_connection_kwargs(default_client_id="x")
