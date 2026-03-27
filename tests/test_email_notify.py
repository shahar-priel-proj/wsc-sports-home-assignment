from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import _lib.email_notify as email_notify


def test_alert_recipients_default():
    assert email_notify._DEFAULT_TO == "shaharpriel@gmail.com"


def test_send_failure_alert_skipped_when_disabled(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ALERT_ON_FAILURE", "0")
    monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
    assert email_notify.send_failure_alert(subject="s", body="b") is False


def test_send_failure_alert_skipped_when_alert_disabled_by_default(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("ALERT_ON_FAILURE", raising=False)
    monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
    assert email_notify.send_failure_alert(subject="s", body="b") is False


def test_send_failure_alert_skipped_without_smtp(monkeypatch: pytest.MonkeyPatch, caplog):
    monkeypatch.delenv("SMTP_HOST", raising=False)
    monkeypatch.setenv("ALERT_ON_FAILURE", "1")
    with caplog.at_level("WARNING"):
        assert email_notify.send_failure_alert(subject="s", body="b") is False
    assert "SMTP_HOST" in caplog.text


def test_send_failure_alert_sends_with_starttls(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ALERT_ON_FAILURE", "1")
    monkeypatch.setenv("SMTP_HOST", "smtp.test")
    monkeypatch.setenv("SMTP_PORT", "587")
    monkeypatch.setenv("SMTP_USER", "u")
    monkeypatch.setenv("SMTP_PASSWORD", "p")
    monkeypatch.setenv("ALERT_EMAIL_TO", "a@example.com,b@example.com")

    mock_smtp = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=mock_smtp)
    mock_ctx.__exit__ = MagicMock(return_value=False)

    with patch("_lib.email_notify.smtplib.SMTP", return_value=mock_ctx):
        assert email_notify.send_failure_alert(subject="Subj", body="Body text") is True

    mock_smtp.starttls.assert_called_once()
    mock_smtp.login.assert_called_once_with("u", "p")
    mock_smtp.send_message.assert_called_once()
    sent = mock_smtp.send_message.call_args[0][0]
    assert sent["Subject"] == "Subj"
    assert "a@example.com" in sent["To"]


def test_notify_enrichment_validation_failure_calls_send(monkeypatch: pytest.MonkeyPatch):
    called = {}

    def capture(**kwargs):
        called["subject"] = kwargs["subject"]
        called["body"] = kwargs["body"]
        return True

    monkeypatch.setenv("SMTP_HOST", "h")
    monkeypatch.setattr(email_notify, "send_failure_alert", lambda **kw: capture(**kw))
    email_notify.notify_enrichment_validation_failure(
        topic="jobs",
        partition=1,
        offset=42,
        exc=ValueError("row count mismatch"),
    )
    assert "enrichment validation failed" in called["subject"]
    assert "jobs" in called["subject"]
    assert "partition=1" in called["subject"] or "p=1" in called["subject"]
    assert "row count mismatch" in called["body"]


def test_notify_consumer_message_failure_calls_send(monkeypatch: pytest.MonkeyPatch):
    called = {}

    def capture(**kwargs):
        called["subject"] = kwargs["subject"]
        called["body"] = kwargs["body"]
        return True

    monkeypatch.setenv("SMTP_HOST", "h")
    monkeypatch.setattr(email_notify, "send_failure_alert", lambda **kw: capture(**kw))
    email_notify.notify_consumer_message_failure(
        topic="t1", partition=3, offset=99, exc=ValueError("bad parquet")
    )
    assert "[careers-consumer]" in called["subject"]
    assert "t1" in called["subject"]
    assert "bad parquet" in called["body"]
