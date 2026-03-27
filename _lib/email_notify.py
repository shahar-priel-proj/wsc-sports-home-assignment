from __future__ import annotations

import logging
import os
import smtplib
import traceback
from email.message import EmailMessage
from typing import Any

log = logging.getLogger(__name__)

_DEFAULT_TO = "shaharpriel@gmail.com"


def _truthy(name: str, *, default: str = "1") -> bool:
    v = os.environ.get(name, default).strip().lower()
    return v in ("1", "true", "yes", "on")


def _alert_recipients() -> list[str]:
    raw = os.environ.get("ALERT_EMAIL_TO", _DEFAULT_TO).strip()
    if not raw:
        return [_DEFAULT_TO]
    return [a.strip() for a in raw.split(",") if a.strip()]


def _smtp_settings() -> dict[str, Any] | None:
    host = os.environ.get("SMTP_HOST", "").strip()
    if not host:
        return None
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER", "").strip()
    password = os.environ.get("SMTP_PASSWORD", "")
    use_ssl = _truthy("SMTP_SSL", default="0")
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "use_ssl": use_ssl,
    }


def _from_addr() -> str:
    return (
        os.environ.get("ALERT_EMAIL_FROM", "").strip()
        or _alert_recipients()[0]
        or _DEFAULT_TO
    )


def send_failure_alert(*, subject: str, body: str) -> bool:
    """
    Send a plain-text alert email if ``ALERT_ON_FAILURE`` is enabled and SMTP is configured.

    Environment:
        ALERT_ON_FAILURE: ``0`` / ``false`` / ``no`` to disable (default: on).
        ALERT_EMAIL_TO: Comma-separated recipients (default ``shaharpriel@gmail.com``).
        ALERT_EMAIL_FROM: Sender address (defaults to first recipient).
        SMTP_HOST: If unset, no email is sent (a warning is logged once per call).
        SMTP_PORT: Default ``587``.
        SMTP_USER / SMTP_PASSWORD: Auth for SMTP (optional for local relay).
        SMTP_SSL: Set ``1`` to use SMTP_SSL on the port (e.g. 465).

    Returns:
        True if an SMTP transaction was attempted and completed without raising.
    """
    if not _truthy("ALERT_ON_FAILURE", default="1"):
        log.debug("Skipping failure email: ALERT_ON_FAILURE is disabled")
        return False

    cfg = _smtp_settings()
    if cfg is None:
        log.warning(
            "Failure alert not emailed: set SMTP_HOST (and typically SMTP_USER, SMTP_PASSWORD, "
            "SMTP_PORT) to deliver mail; ALERT_EMAIL_TO=%s",
            _alert_recipients(),
        )
        return False

    recipients = _alert_recipients()
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = _from_addr()
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    try:
        if cfg["use_ssl"]:
            with smtplib.SMTP_SSL(cfg["host"], cfg["port"], timeout=30) as smtp:
                if cfg["user"]:
                    smtp.login(cfg["user"], cfg["password"])
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(cfg["host"], cfg["port"], timeout=30) as smtp:
                smtp.starttls()
                if cfg["user"]:
                    smtp.login(cfg["user"], cfg["password"])
                smtp.send_message(msg)
        log.info("Sent failure alert email to %s", recipients)
        return True
    except OSError as exc:
        log.error("Failure alert email could not be sent: %s", exc, exc_info=True)
        return False


def notify_consumer_message_failure(
    *,
    topic: str,
    partition: int,
    offset: int,
    exc: BaseException,
) -> None:
    """Format and send consumer per-message failure (enrichment, blob upload, etc.)."""
    subject = f"[careers-consumer] message failed topic={topic} p={partition} o={offset}"
    body = (
        f"The consumer failed while processing a Kafka message.\n\n"
        f"topic: {topic}\n"
        f"partition: {partition}\n"
        f"offset: {offset}\n\n"
        f"Exception: {type(exc).__name__}: {exc}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )
    send_failure_alert(subject=subject, body=body)


def notify_consumer_kafka_failure(*, exc: BaseException) -> None:
    subject = "[careers-consumer] Kafka consumer error"
    body = (
        f"KafkaConsumer raised an error.\n\n"
        f"Exception: {type(exc).__name__}: {exc}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )
    send_failure_alert(subject=subject, body=body)


def notify_consumer_startup_failure(*, message: str) -> None:
    subject = "[careers-consumer] startup configuration error"
    body = f"The consumer exited during startup.\n\n{message}\n"
    send_failure_alert(subject=subject, body=body)


def notify_enrichment_validation_failure(
    *,
    exc: BaseException,
    topic: str | None = None,
    partition: int | None = None,
    offset: int | None = None,
) -> None:
    """
    Alert when ``validate_producer_vs_enrichment`` fails (row mismatch, key drift, bad scores, etc.).

    Uses the same ``ALERT_ON_FAILURE`` / SMTP settings as other consumer alerts. Callers may pass
    Kafka coordinates when the failure happened while processing a message.
    """
    subject = "[careers-consumer] enrichment validation failed"
    if topic is not None and partition is not None and offset is not None:
        subject += f" topic={topic} p={partition} o={offset}"
    meta = ""
    if topic is not None:
        meta += f"topic: {topic}\n"
    if partition is not None:
        meta += f"partition: {partition}\n"
    if offset is not None:
        meta += f"offset: {offset}\n"
    if meta:
        meta = meta + "\n"
    body = (
        "Producer vs enrichment validation failed.\n\n"
        f"{meta}"
        f"Exception: {type(exc).__name__}: {exc}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )
    send_failure_alert(subject=subject, body=body)
