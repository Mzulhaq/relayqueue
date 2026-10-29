"""Webhook signature protocol shared by ingress and delivery."""

import hashlib
import hmac


def sign(secret: str, event_id: str, timestamp: str, body: bytes) -> str:
    message = f"{timestamp}.{event_id}.".encode("ascii") + body
    return "v1=" + hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
