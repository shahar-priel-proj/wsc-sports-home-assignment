from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from azure.core.exceptions import ResourceExistsError

import consumer.azure_storage as az


def test_blob_prefix_default(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("AZURE_BLOB_PREFIX", raising=False)
    assert az._blob_prefix() == "careers-parquet/"


def test_blob_prefix_strips_and_appends_slash(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AZURE_BLOB_PREFIX", "  pre/fix  ")
    assert az._blob_prefix() == "pre/fix/"


def test_upload_requires_connection_string(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    with pytest.raises(ValueError, match="AZURE_STORAGE_CONNECTION_STRING"):
        az.upload_message_value(b"x", topic="t", partition=0, offset=0, key=b"k")


def test_upload_requires_non_empty_container(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(
        "AZURE_STORAGE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=https;AccountName=fake;AccountKey=a2V5Cg==;EndpointSuffix=core.windows.net",
    )
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER", "   ")
    with pytest.raises(ValueError, match="AZURE_STORAGE_CONTAINER"):
        az.upload_message_value(b"x", topic="t", partition=0, offset=0, key=None)


def test_ensure_container_skipped_without_flag(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("AUTO_CREATE_CONTAINER", raising=False)
    client = MagicMock()
    az.ensure_container(client, "c")
    client.create_container.assert_not_called()


def test_ensure_container_creates_when_enabled(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AUTO_CREATE_CONTAINER", "1")
    client = MagicMock()
    az.ensure_container(client, "mycontainer")
    client.create_container.assert_called_once_with("mycontainer")


def test_ensure_container_ignores_already_exists(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AUTO_CREATE_CONTAINER", "1")
    client = MagicMock()
    client.create_container.side_effect = ResourceExistsError()
    az.ensure_container(client, "x")


@patch("consumer.azure_storage.datetime")
@patch.object(az.BlobServiceClient, "from_connection_string")
def test_upload_message_value_uploads_with_expected_path(
    mock_from_cs: MagicMock,
    mock_datetime: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(
        "AZURE_STORAGE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=https;AccountName=fake;AccountKey=a2V5Cg==;EndpointSuffix=core.windows.net",
    )
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER", "c1")
    monkeypatch.setenv("AZURE_BLOB_PREFIX", "prefix/")
    mock_blob = MagicMock()
    mock_blob.url = "https://fake.blob.core.windows.net/c1/blob"
    mock_client = MagicMock()
    mock_client.get_blob_client.return_value = mock_blob
    mock_from_cs.return_value = mock_client

    inst = MagicMock()
    inst.strftime.return_value = "20250101T120000123456"
    mock_datetime.now.return_value = inst

    url = az.upload_message_value(
        b"parquet-bytes",
        topic="my-topic",
        partition=2,
        offset=99,
        key=b"key-1",
        enriched=False,
    )

    assert url == "https://fake.blob.core.windows.net/c1/blob"
    mock_client.get_blob_client.assert_called_once()
    _, kwargs = mock_client.get_blob_client.call_args
    assert kwargs["container"] == "c1"
    blob_name = kwargs["blob"]
    assert blob_name.startswith("prefix/my-topic/")
    assert "_p2_o99_" in blob_name
    assert blob_name.endswith("_key-1.parquet")
    assert "_enriched" not in blob_name

    mock_blob.upload_blob.assert_called_once()
    u_args, u_kwargs = mock_blob.upload_blob.call_args
    assert (u_args[0] if u_args else u_kwargs.get("data")) == b"parquet-bytes"
    assert u_kwargs.get("overwrite") is True
    assert "parquet" in u_kwargs["content_settings"].content_type


@patch("consumer.azure_storage.datetime")
@patch.object(az.BlobServiceClient, "from_connection_string")
def test_upload_message_value_enriched_suffix(
    mock_from_cs: MagicMock,
    mock_datetime: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(
        "AZURE_STORAGE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=https;AccountName=fake;AccountKey=a2V5Cg==;EndpointSuffix=core.windows.net",
    )
    mock_blob = MagicMock()
    mock_blob.url = "https://x/blob"
    mock_client = MagicMock()
    mock_client.get_blob_client.return_value = mock_blob
    mock_from_cs.return_value = mock_client

    inst = MagicMock()
    inst.strftime.return_value = "20250101T120000123456"
    mock_datetime.now.return_value = inst
    az.upload_message_value(
        b"d",
        topic="t",
        partition=0,
        offset=0,
        key=None,
        enriched=True,
    )

    blob_name = mock_client.get_blob_client.call_args.kwargs["blob"]
    assert blob_name.endswith("_message_enriched.parquet")


@patch("consumer.azure_storage.datetime")
@patch.object(az.BlobServiceClient, "from_connection_string")
def test_upload_sanitizes_topic_and_key_in_blob_name(
    mock_from_cs: MagicMock,
    mock_datetime: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(
        "AZURE_STORAGE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=https;AccountName=fake;AccountKey=a2V5Cg==;EndpointSuffix=core.windows.net",
    )
    mock_blob = MagicMock()
    mock_blob.url = "https://x/blob"
    mock_client = MagicMock()
    mock_client.get_blob_client.return_value = mock_blob
    mock_from_cs.return_value = mock_client

    inst = MagicMock()
    inst.strftime.return_value = "20250101T120000123456"
    mock_datetime.now.return_value = inst
    az.upload_message_value(
        b"d",
        topic="bad/name!topic",
        partition=0,
        offset=0,
        key=b"weird key!",
        enriched=False,
    )

    blob_name = mock_client.get_blob_client.call_args.kwargs["blob"]
    assert "bad_name_topic" in blob_name
    assert "weird_key" in blob_name
