"""Leased, at-least-once HTTP delivery worker."""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import UTC, datetime

import httpx

from .config import Settings
from .signing import sign
from .storage import Database

logger = logging.getLogger("relayqueue.worker")


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        for key in ("event_id", "outcome", "status_code"):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value
        return json.dumps(payload, separators=(",", ":"))


def _epoch() -> float:
    return time.time()


def process_one(
    database: Database,
    settings: Settings,
    *,
    client: httpx.Client | None = None,
    now: float | None = None,
) -> bool:
    """Claim and deliver one event. Returns false when the queue has no work."""
    claimed_at = _epoch() if now is None else now
    claim = database.claim_one(
        now=claimed_at, lease_seconds=settings.lease_seconds, max_attempts=settings.max_attempts
    )
    if claim is None:
        return False

    owns_client = client is None
    http = client or httpx.Client(timeout=settings.request_timeout_seconds, follow_redirects=False)
    event_id = claim["id"]
    body = bytes(claim["payload"])
    timestamp = str(int(claimed_at))
    signature = sign(settings.destination_secret, event_id, timestamp, body)
    headers = {
        "Webhook-Id": event_id,
        "Webhook-Timestamp": timestamp,
        "Webhook-Signature": signature,
        "Content-Type": "application/json",
    }
    outcome: str
    error: str | None = None
    status_code: int | None = None
    retry_at: float | None = None
    try:
        with http.stream(
            "POST",
            settings.destination_url,
            content=body,
            headers=headers,
            timeout=settings.request_timeout_seconds,
            follow_redirects=False,
        ) as response:
            status_code = response.status_code
        event_finished_at = _epoch() if now is None else now
        if 200 <= status_code < 300:
            outcome = "succeeded"
        elif status_code in (408, 425, 429) or status_code >= 500:
            if claim["attempt_count"] >= settings.max_attempts:
                outcome = "permanent_error"
                error = f"HTTP{status_code}"
            else:
                outcome = "retryable_error"
                error = f"HTTP{status_code}"
                delay = min(
                    settings.retry_base_seconds * (2 ** (claim["attempt_count"] - 1)),
                    settings.retry_max_seconds,
                )
                retry_at = event_finished_at + delay
        else:
            outcome = "permanent_error"
            error = f"HTTP{status_code}"
    except httpx.HTTPError as exc:
        event_finished_at = _epoch() if now is None else now
        error = type(exc).__name__
        if claim["attempt_count"] >= settings.max_attempts:
            outcome = "permanent_error"
        else:
            outcome = "retryable_error"
            delay = min(
                settings.retry_base_seconds * (2 ** (claim["attempt_count"] - 1)),
                settings.retry_max_seconds,
            )
            retry_at = event_finished_at + delay
    finally:
        if owns_client:
            http.close()

    finished_at = event_finished_at
    saved = database.finish(
        event_id,
        claim["lease_token"],
        outcome=outcome,
        now=finished_at,
        error=error,
        status_code=status_code,
        retry_at=retry_at,
    )
    logger.info(
        "delivery completed",
        extra={
            "event_id": event_id,
            "outcome": outcome if saved else "stale_completion",
            "status_code": status_code,
        },
    )
    return True


def main() -> None:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, force=True)
    logging.getLogger().handlers[0].setFormatter(_JsonFormatter())
    settings = Settings.from_env()
    database = Database(settings.database_url)
    database.ensure_schema()
    client = httpx.Client(timeout=settings.request_timeout_seconds, follow_redirects=False)
    try:
        while True:
            if not process_one(database, settings, client=client):
                time.sleep(settings.poll_interval_seconds)
    except KeyboardInterrupt:
        logger.info("worker stopping")
    finally:
        client.close()
        database.close()


if __name__ == "__main__":
    main()
