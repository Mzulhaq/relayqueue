from __future__ import annotations

import hashlib
import hmac
import os
import threading
from pathlib import Path

import httpx
import pytest

from relayqueue.config import Settings
from relayqueue.signing import sign
from relayqueue.storage import ConflictError, Database, metadata
from relayqueue.worker import process_one


@pytest.fixture
def db(tmp_path):
    url = os.environ.get("TEST_DATABASE_URL") or f"sqlite:///{Path(tmp_path) / 'queue.sqlite'}"
    database = Database(url)
    if os.environ.get("TEST_DATABASE_URL"):
        metadata.drop_all(database.engine)
    database.ensure_schema()
    yield database
    database.close()


def settings(**overrides):
    values = dict(
        database_url="sqlite:///:memory:",
        ingress_secret="i" * 32,
        destination_secret="d" * 32,
        admin_token="a" * 32,
        destination_url="https://receiver.example/hook",
        allow_insecure_http=False,
        max_attempts=3,
        retry_base_seconds=2,
        retry_max_seconds=60,
        request_timeout_seconds=5,
        lease_seconds=30,
        poll_interval_seconds=0.5,
        max_body_bytes=65536,
        signature_tolerance_seconds=300,
    )
    values.update(overrides)
    return Settings(**values)


def test_ingest_persists_deduplicates_and_hides_payload(db):
    body = b'{"topic":"orders","data":{"n":1}}'
    created, is_new = db.ingest("evt.1", "orders", body, 10)
    again, is_new_again = db.ingest("evt.1", "orders", body, 20)
    assert is_new and not is_new_again
    assert created["id"] == again["id"] == "evt.1"
    assert "payload" not in created and "body" not in created
    assert db.get_event("evt.1")["attempts"] == []
    with pytest.raises(ConflictError):
        db.ingest("evt.1", "orders", body + b" ", 30)


def test_ingest_concurrent_duplicate_is_one_event(db):
    barrier = threading.Barrier(8)
    results = []
    errors = []

    def ingest():
        try:
            barrier.wait()
            results.append(db.ingest("same", "t", b"{}", 1)[1])
        except Exception as exc:  # captured to assert thread failures in main thread
            errors.append(exc)

    threads = [threading.Thread(target=ingest) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    assert sum(results) == 1
    assert len(db.list_events()) == 1


def test_worker_sends_raw_body_and_signature_and_marks_success(db):
    body = b'{ "data": {"x": 2}, "topic":"orders" }'
    db.ingest("evt-1", "orders", body, 100)
    seen = []

    def handler(request):
        seen.append(request)
        assert request.content == body
        assert request.headers["Webhook-Id"] == "evt-1"
        timestamp = request.headers["Webhook-Timestamp"]
        assert request.headers["Webhook-Signature"] == sign("d" * 32, "evt-1", timestamp, body)
        expected = hmac.new(
            b"d" * 32,
            timestamp.encode() + b"." + b"evt-1." + body,
            hashlib.sha256,
        ).hexdigest()
        assert request.headers["Webhook-Signature"] == f"v1={expected}"
        return httpx.Response(204)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    assert process_one(db, settings(), client=client, now=100)
    event = db.get_event("evt-1")
    assert event["status"] == "succeeded"
    assert event["attempt_count"] == 1
    assert event["attempts"][0]["outcome"] == "succeeded"
    assert len(seen) == 1


def test_retryable_status_schedules_backoff_then_succeeds(db):
    db.ingest("retry", "t", b"{}", 100)
    client = httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(503)))
    assert process_one(db, settings(), client=client, now=100)
    failed = db.get_event("retry")
    assert failed["status"] == "retry_wait"
    assert failed["available_at"] == 102
    assert failed["attempts"][0]["outcome"] == "retryable_error"

    client = httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(200)))
    assert process_one(db, settings(), client=client, now=102)
    assert db.get_event("retry")["status"] == "succeeded"


@pytest.mark.parametrize("status", [400, 401, 404, 302])
def test_nonretryable_status_and_redirect_are_terminal(db, status):
    db.ingest("terminal", "t", b"{}", 1)
    client = httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(status)))
    assert process_one(db, settings(), client=client, now=1)
    event = db.get_event("terminal")
    assert event["status"] == "dead"
    assert len(event["attempts"]) == 1


def test_attempt_budget_exhaustion_is_dead(db):
    db.ingest("exhaust", "t", b"{}", 0)
    client = httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(503)))
    for now in (0, 2, 6):
        assert process_one(db, settings(), client=client, now=now)
    event = db.get_event("exhaust")
    assert event["status"] == "dead"
    assert event["attempt_count"] == 3
    assert len(event["attempts"]) == 3


def test_timeout_is_a_retryable_sanitized_error(db):
    db.ingest("timeout", "t", b"{}", 0)

    def fail(_request):
        raise httpx.ReadTimeout("secret URL https://private.example/token")

    client = httpx.Client(transport=httpx.MockTransport(fail))
    assert process_one(db, settings(), client=client, now=0)
    event = db.get_event("timeout")
    assert event["status"] == "retry_wait"
    assert "private.example" not in event["last_error"]
    assert "ReadTimeout" in event["last_error"]


def test_expired_lease_recovery_fences_stale_completion(db):
    db.ingest("lease", "t", b"{}", 0)
    first = db.claim_one(now=0, lease_seconds=5, max_attempts=3)
    assert first is not None
    second = db.claim_one(now=6, lease_seconds=5, max_attempts=3)
    assert second is not None
    assert second["lease_token"] != first["lease_token"]
    assert not db.finish(first["id"], first["lease_token"], outcome="succeeded", now=7)
    assert db.finish(second["id"], second["lease_token"], outcome="succeeded", now=7)


def test_expired_but_unreclaimed_lease_cannot_be_finished(db):
    db.ingest("expired", "t", b"{}", 0)
    claim = db.claim_one(now=0, lease_seconds=5, max_attempts=3)
    assert claim is not None
    assert not db.finish(claim["id"], claim["lease_token"], outcome="succeeded", now=6)
    assert db.get_event("expired")["status"] == "inflight"
    replacement = db.claim_one(now=6, lease_seconds=5, max_attempts=3)
    assert replacement is not None
    assert replacement["lease_token"] != claim["lease_token"]
    assert db.get_event("expired")["attempts"][0]["outcome"] == "abandoned"


def test_concurrent_finish_and_reclaim_preserve_audit_fence(db):
    db.ingest("race", "t", b"{}", 0)
    first = db.claim_one(now=0, lease_seconds=5, max_attempts=3)
    assert first is not None
    barrier = threading.Barrier(2)
    result = {}

    def finish():
        barrier.wait()
        result["finish"] = db.finish(first["id"], first["lease_token"], outcome="succeeded", now=6)

    def reclaim():
        barrier.wait()
        result["claim"] = db.claim_one(now=6, lease_seconds=5, max_attempts=3)

    threads = [threading.Thread(target=finish), threading.Thread(target=reclaim)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert result["finish"] is False
    assert result["claim"] is not None
    history = db.get_event("race")["attempts"]
    assert [attempt["outcome"] for attempt in history] == ["abandoned", "inflight"]


def test_concurrent_reclaimers_create_only_one_new_attempt(db):
    db.ingest("reclaim", "t", b"{}", 0)
    assert db.claim_one(now=0, lease_seconds=5, max_attempts=3) is not None
    barrier = threading.Barrier(2)
    claims = []

    def reclaim():
        barrier.wait()
        claims.append(db.claim_one(now=6, lease_seconds=5, max_attempts=3))

    threads = [threading.Thread(target=reclaim), threading.Thread(target=reclaim)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    winners = [claim for claim in claims if claim]
    assert len(winners) == 1
    assert len(db.get_event("reclaim")["attempts"]) == 2


def test_repeated_crashes_consume_attempt_budget(db):
    db.ingest("crash", "t", b"{}", 0)
    for now in (0, 2, 4):
        claim = db.claim_one(now=now, lease_seconds=1, max_attempts=3)
        assert claim is not None
    assert db.claim_one(now=6, lease_seconds=1, max_attempts=3) is None
    event = db.get_event("crash")
    assert event["status"] == "dead"
    assert event["attempt_count"] == 3
    assert [a["outcome"] for a in event["attempts"]] == ["abandoned"] * 3


def test_ready_event_becomes_dead_when_attempt_budget_is_lowered(db):
    db.ingest("lowered", "t", b"{}", 0)
    claim = db.claim_one(now=0, lease_seconds=30, max_attempts=3)
    assert claim is not None
    assert db.finish(
        claim["id"], claim["lease_token"], outcome="retryable_error", now=1, retry_at=1
    )
    assert db.claim_one(now=1, lease_seconds=30, max_attempts=1) is None
    assert db.get_event("lowered")["status"] == "dead"


def test_two_workers_claim_distinct_events_concurrently(db):
    db.ingest("a", "t", b"{}", 0)
    db.ingest("b", "t", b"{}", 0)
    barrier = threading.Barrier(2)
    claims = []

    def claim():
        barrier.wait()
        claims.append(db.claim_one(now=0, lease_seconds=30, max_attempts=3))

    threads = [threading.Thread(target=claim) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert {claim["id"] for claim in claims if claim} == {"a", "b"}


def test_metrics_include_state_counts_and_total_attempts(db):
    db.ingest("m", "t", b"{}", 0)
    assert db.metrics()["total_attempts"] == 0
    client = httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(200)))
    process_one(db, settings(), client=client, now=0)
    metrics = db.metrics()
    assert metrics["total_attempts"] == 1
    assert metrics["by_status"]["succeeded"] == 1
