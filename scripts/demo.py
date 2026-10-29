"""Run a synthetic, real-HTTP end-to-end demonstration without external services."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import uvicorn

from relayqueue.api import create_app
from relayqueue.config import Settings
from relayqueue.signing import sign
from relayqueue.storage import Database


@contextmanager
def serve(app):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level="error", access_log=False))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 10
        while not server.started:
            if not thread.is_alive() or time.monotonic() >= deadline:
                raise RuntimeError("Demo API failed to start")
            time.sleep(0.02)
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        sock.close()


def run_demo() -> dict:
    ingress_secret, destination_secret, admin_token = [secrets.token_hex(32) for _ in range(3)]
    received = Counter()

    class Receiver(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            event_id = self.headers.get("Webhook-Id", "")
            timestamp = self.headers.get("Webhook-Timestamp", "")
            expected = (
                "v1="
                + hmac.new(
                    destination_secret.encode(),
                    f"{timestamp}.{event_id}.".encode() + body,
                    hashlib.sha256,
                ).hexdigest()
            )
            if not hmac.compare_digest(self.headers.get("Webhook-Signature", ""), expected):
                status = 401
            else:
                received[event_id] += 1
                status = 503 if event_id == "demo-dead" else 204
                if event_id == "demo-retry" and received[event_id] < 3:
                    status = 503
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

    receiver = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
    receiver_thread = threading.Thread(target=receiver.serve_forever, daemon=True)
    receiver_thread.start()
    worker = None
    try:
        with tempfile.TemporaryDirectory(prefix="relayqueue-demo-") as directory:
            settings = Settings(
                database_url=f"sqlite:///{Path(directory) / 'queue.db'}",
                ingress_secret=ingress_secret,
                destination_secret=destination_secret,
                admin_token=admin_token,
                destination_url=f"http://127.0.0.1:{receiver.server_port}/receive",
                allow_insecure_http=True,
                max_attempts=3,
                retry_base_seconds=0.1,
                retry_max_seconds=1,
                poll_interval_seconds=0.05,
            )
            db = Database(settings.database_url)
            db.ensure_schema()  # Complete schema initialization before starting API/worker.
            try:
                with (
                    serve(create_app(settings, db)) as base_url,
                    httpx.Client(base_url=base_url, timeout=5, trust_env=False) as client,
                ):
                    body = b'{"topic":"order.created","data":{"order_id":"synthetic-123"}}'

                    def submit(event_id, valid=True):
                        timestamp = str(int(time.time()))
                        signature = sign(ingress_secret, event_id, timestamp, body)
                        return client.post(
                            "/webhooks",
                            content=body,
                            headers={
                                "Content-Type": "application/json",
                                "Webhook-Id": event_id,
                                "Webhook-Timestamp": timestamp,
                                "Webhook-Signature": signature if valid else "v1=" + "0" * 64,
                            },
                        )

                    assert submit("demo-invalid", valid=False).status_code == 401
                    for event_id in ("demo-success", "demo-retry", "demo-dead"):
                        assert submit(event_id).status_code == 202
                    for _ in range(25):
                        assert submit("demo-success").status_code == 200

                    env = os.environ.copy()
                    for field in settings.__dataclass_fields__:
                        value = getattr(settings, field)
                        env["RQ_" + field.upper()] = (
                            str(value).lower() if isinstance(value, bool) else str(value)
                        )
                    worker = subprocess.Popen(
                        [sys.executable, "-m", "relayqueue.worker"],
                        env=env,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                    )
                    auth = {"Authorization": f"Bearer {admin_token}"}
                    deadline = time.monotonic() + 20
                    while True:
                        response = client.get("/events", headers=auth)
                        response.raise_for_status()
                        events = response.json()
                        if len(events) == 3 and all(
                            event["status"] in {"succeeded", "dead"} for event in events
                        ):
                            break
                        if worker.poll() is not None:
                            raise RuntimeError("Demo worker exited before completing delivery")
                        if time.monotonic() >= deadline:
                            raise RuntimeError("Demo deliveries did not finish within 20 seconds")
                        time.sleep(0.05)
                    actual = {event["id"]: event for event in events}
                    assert actual["demo-success"]["status"] == "succeeded"
                    assert actual["demo-success"]["attempt_count"] == 1
                    assert actual["demo-retry"]["status"] == "succeeded"
                    assert actual["demo-retry"]["attempt_count"] == 3
                    assert actual["demo-dead"]["status"] == "dead"
                    assert actual["demo-dead"]["attempt_count"] == 3
                    assert received == {"demo-success": 1, "demo-retry": 3, "demo-dead": 3}
                    result = {
                        "unique_events": len(events),
                        "duplicate_requests_deduplicated": 25,
                        "invalid_signatures_rejected": 1,
                        "successful_events": 2,
                        "dead_events": 1,
                        "delivery_attempts": client.get("/metrics", headers=auth).json()[
                            "total_attempts"
                        ],
                        "receiver_requests": dict(sorted(received.items())),
                        "transport": "real loopback HTTP; separate worker process",
                    }
                    assert result["delivery_attempts"] == 7
                    return result
            finally:
                if worker is not None and worker.poll() is None:
                    worker.terminate()
                    try:
                        worker.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        worker.kill()
                        worker.wait(timeout=5)
                db.close()
    finally:
        receiver.shutdown()
        receiver.server_close()
        receiver_thread.join(timeout=5)


if __name__ == "__main__":
    print(json.dumps(run_demo(), indent=2))
