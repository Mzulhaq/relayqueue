import hashlib
import hmac
import time
from dataclasses import replace

import pytest
from fastapi.testclient import TestClient

from relayqueue.api import create_app
from relayqueue.config import Settings

BODY = b'{"topic":"order.created","data":{"order_id":"synthetic-123"}}'


def headers(body=BODY, event_id="evt-1", timestamp=None):
    timestamp = str(int(time.time())) if timestamp is None else timestamp
    signature = hmac.new(
        b"i" * 32, f"{timestamp}.{event_id}.".encode() + body, hashlib.sha256
    ).hexdigest()
    return {
        "Webhook-Id": event_id,
        "Webhook-Timestamp": timestamp,
        "Webhook-Signature": "v1=" + signature,
        "Content-Type": "application/json",
    }


@pytest.fixture
def config(tmp_path):
    return Settings(
        f"sqlite:///{tmp_path / 'api.db'}",
        "i" * 32,
        "d" * 32,
        "a" * 32,
        "https://receiver.example/hook",
    )


@pytest.fixture
def client(config):
    with TestClient(create_app(config)) as client:
        yield client


def admin():
    return {"Authorization": "Bearer " + "a" * 32}


def test_ingress_persists_before_ack_and_deduplicates(client):
    first = client.post("/webhooks", content=BODY, headers=headers())
    assert first.status_code == 202
    assert first.json()["created"] is True
    again = client.post("/webhooks", content=BODY, headers=headers())
    assert again.status_code == 200
    assert again.json()["created"] is False
    saved = client.get("/events/evt-1", headers=admin()).json()
    assert saved["topic"] == "order.created"
    assert saved["attempt_count"] == 0
    assert "body" not in saved and "data" not in saved


def test_same_id_different_payload_conflicts(client):
    client.post("/webhooks", content=BODY, headers=headers())
    changed = BODY.replace(b"synthetic-123", b"synthetic-456")
    assert client.post("/webhooks", content=changed, headers=headers(changed)).status_code == 409


@pytest.mark.parametrize("change", ["body", "id", "signature", "past", "future", "timestamp"])
def test_tampering_and_stale_signatures_rejected(client, change):
    signed = headers()
    body = BODY
    if change == "body":
        body += b" "
    elif change == "id":
        signed["Webhook-Id"] = "other-id"
    elif change == "signature":
        signed["Webhook-Signature"] = "v1=" + "0" * 64
    elif change in {"past", "future"}:
        signed = headers(timestamp=str(int(time.time()) + (-601 if change == "past" else 601)))
    else:
        signed["Webhook-Timestamp"] = "nan"
    assert client.post("/webhooks", content=body, headers=signed).status_code == 401
    assert client.get("/events", headers=admin()).json() == []


@pytest.mark.parametrize(
    "body",
    [
        b"null",
        b"[]",
        b"{}",
        b'{"topic":"x","data":[]}',
        b'{"topic":"","data":{}}',
        b"\xff",
        b'{"topic":"x","data":{},"extra":true}',
        b'{"topic":"x","data":{"value":NaN}}',
    ],
)
def test_invalid_json_and_schema_rejected(client, body):
    assert client.post("/webhooks", content=body, headers=headers(body)).status_code == 422


@pytest.mark.parametrize("event_id", ["bad/id", "x" * 129, ""])
def test_invalid_ids_rejected(client, event_id):
    assert (
        client.post("/webhooks", content=BODY, headers=headers(event_id=event_id)).status_code
        == 400
    )


def test_oversized_body_rejected_even_without_content_length(config):
    body = b" " * 101
    with TestClient(create_app(replace(config, max_body_bytes=100))) as client:
        response = client.post(
            "/webhooks", content=iter([body[:50], body[50:]]), headers=headers(body)
        )
        assert response.status_code == 413


def test_wrong_content_type_rejected(client):
    signed = headers()
    signed["Content-Type"] = "text/plain"
    assert client.post("/webhooks", content=BODY, headers=signed).status_code == 415


@pytest.mark.parametrize("route", ["/events", "/events/evt-1", "/metrics"])
def test_admin_endpoints_require_token(client, route):
    assert client.get(route).status_code == 401
    assert client.get(route, headers={"Authorization": "Bearer wrong"}).status_code == 401


def test_health_readiness_metrics_and_filters(client):
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/ready").status_code == 200
    assert client.get("/metrics", headers=admin()).status_code == 200
    assert client.get("/events/missing", headers=admin()).status_code == 404
    assert client.get("/events?limit=1000", headers=admin()).status_code == 422
    assert client.get("/events?status=wrong", headers=admin()).status_code == 422


@pytest.mark.parametrize("status", ["pending", "inflight", "retry_wait", "succeeded", "dead"])
def test_all_queue_states_can_be_filtered(client, status):
    assert client.get(f"/events?status={status}", headers=admin()).status_code == 200
