"""Explicit runtime configuration; secrets are never included in representations."""

import math
import os
from dataclasses import dataclass, field
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Settings:
    database_url: str
    ingress_secret: str = field(repr=False)
    destination_secret: str = field(repr=False)
    admin_token: str = field(repr=False)
    destination_url: str
    allow_insecure_http: bool = False
    max_attempts: int = 5
    retry_base_seconds: float = 2
    retry_max_seconds: float = 60
    request_timeout_seconds: float = 5
    lease_seconds: float = 30
    poll_interval_seconds: float = 0.5
    max_body_bytes: int = 65536
    signature_tolerance_seconds: int = 300

    def __post_init__(self):
        secrets = (self.ingress_secret, self.destination_secret, self.admin_token)
        if any(
            len(value) < 32 or not value.isascii() or any(c.isspace() for c in value)
            for value in secrets
        ):
            raise ValueError("Secrets must contain at least 32 non-whitespace ASCII characters")
        if len(set(secrets)) != 3:
            raise ValueError("Use distinct ingress, destination, and admin secrets")
        url = urlsplit(self.destination_url)
        allowed = {"https", "http"} if self.allow_insecure_http else {"https"}
        if (
            url.scheme not in allowed
            or not url.hostname
            or url.username
            or url.password
            or url.fragment
            or any(c.isspace() for c in self.destination_url)
        ):
            raise ValueError("Destination must be HTTPS with no credentials or fragment")
        _ = url.port  # Validate malformed/out-of-range ports before worker startup.
        if not 1 <= self.max_attempts <= 100:
            raise ValueError("max_attempts must be between 1 and 100")
        for name in (
            "retry_base_seconds",
            "retry_max_seconds",
            "request_timeout_seconds",
            "lease_seconds",
            "poll_interval_seconds",
            "max_body_bytes",
            "signature_tolerance_seconds",
        ):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if self.lease_seconds <= self.request_timeout_seconds + 5:
            raise ValueError("lease_seconds must exceed request_timeout_seconds by more than 5")
        if self.retry_max_seconds < self.retry_base_seconds:
            raise ValueError("retry_max_seconds must be at least retry_base_seconds")

    @classmethod
    def from_env(cls) -> "Settings":
        allow_http = os.getenv("RQ_ALLOW_INSECURE_HTTP", "false").lower()
        if allow_http not in {"true", "false"}:
            raise ValueError("RQ_ALLOW_INSECURE_HTTP must be true or false")
        values = {
            "database_url": os.getenv("RQ_DATABASE_URL", "sqlite:///./relayqueue.db"),
            "destination_url": os.getenv("RQ_DESTINATION_URL", ""),
            "ingress_secret": os.getenv("RQ_INGRESS_SECRET", ""),
            "destination_secret": os.getenv("RQ_DESTINATION_SECRET", ""),
            "admin_token": os.getenv("RQ_ADMIN_TOKEN", ""),
            "allow_insecure_http": allow_http == "true",
        }
        for name in (
            "max_attempts",
            "retry_base_seconds",
            "retry_max_seconds",
            "request_timeout_seconds",
            "lease_seconds",
            "poll_interval_seconds",
            "max_body_bytes",
            "signature_tolerance_seconds",
        ):
            raw = os.getenv("RQ_" + name.upper())
            if raw is not None:
                cast = (
                    int
                    if name in {"max_attempts", "max_body_bytes", "signature_tolerance_seconds"}
                    else float
                )
                values[name] = cast(raw)
        return cls(**values)
