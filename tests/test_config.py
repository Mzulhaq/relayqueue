from dataclasses import replace

import pytest

from relayqueue.config import Settings


def settings():
    return Settings("sqlite://", "i" * 32, "d" * 32, "a" * 32, "https://example.com/hook")


@pytest.mark.parametrize("field", ["ingress_secret", "destination_secret", "admin_token"])
def test_short_secrets_rejected(field):
    with pytest.raises(ValueError, match="32"):
        replace(settings(), **{field: "short"})


def test_distinct_secrets_required():
    with pytest.raises(ValueError, match="distinct"):
        replace(settings(), destination_secret="i" * 32)


@pytest.mark.parametrize(
    "url",
    [
        "ftp://host",
        "https://user:pass@host",
        "https://host/#fragment",
        "https://",
        "https://host:bad",
        "http://localhost:8001/hook",
    ],
)
def test_invalid_or_insecure_destinations_rejected(url):
    with pytest.raises(ValueError):
        replace(settings(), destination_url=url)


def test_explicit_http_opt_in():
    assert replace(settings(), destination_url="http://127.0.0.1:8001", allow_insecure_http=True)


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_attempts", 0),
        ("max_attempts", 101),
        ("retry_base_seconds", 0),
        ("request_timeout_seconds", -1),
        ("lease_seconds", 5),
        ("poll_interval_seconds", 0),
        ("retry_max_seconds", float("inf")),
        ("retry_base_seconds", float("nan")),
        ("max_body_bytes", 0),
        ("signature_tolerance_seconds", -1),
    ],
)
def test_invalid_bounds_rejected(field, value):
    with pytest.raises(ValueError):
        replace(settings(), **{field: value})


def test_environment_loading(monkeypatch):
    for key, value in {
        "RQ_INGRESS_SECRET": "i" * 32,
        "RQ_DESTINATION_SECRET": "d" * 32,
        "RQ_ADMIN_TOKEN": "a" * 32,
        "RQ_DESTINATION_URL": "https://example.com/hook",
        "RQ_MAX_ATTEMPTS": "3",
        "RQ_RETRY_BASE_SECONDS": "0.25",
    }.items():
        monkeypatch.setenv(key, value)
    config = Settings.from_env()
    assert config.max_attempts == 3
    assert config.retry_base_seconds == 0.25
    assert config.database_url == "sqlite:///./relayqueue.db"
    assert "i" * 32 not in repr(config)


def test_invalid_boolean_rejected(monkeypatch):
    monkeypatch.setenv("RQ_ALLOW_INSECURE_HTTP", "tru")
    with pytest.raises(ValueError, match="RQ_ALLOW_INSECURE_HTTP"):
        Settings.from_env()
