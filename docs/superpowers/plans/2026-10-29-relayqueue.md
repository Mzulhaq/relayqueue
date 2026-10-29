# RelayQueue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Build, verify and publish the authorized webhook reliability portfolio project.

**Architecture:** FastAPI validates signed ingress and persists events. A separate worker claims SQL-backed leases and delivers over HTTP; fencing prevents stale workers from overwriting recovery. A loopback demo exercises the complete flow.

**Tech Stack:** Python 3.11+, FastAPI, SQLAlchemy 2, SQLite/PostgreSQL, httpx, pytest, Ruff, Docker and GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-10-29-relayqueue.md`

## Global Constraints

- 65536-byte maximum request body; five-minute signature window.
- 1-128-character event IDs; distinct secrets of at least 32 characters.
- No remote write except the new public Mzulhaq/relayqueue repository.
- Separate project repository; preserve the weekly scheduler and its ledger.

## Review Focus

- Concurrent duplicate ingestion must persist one event (storage test).
- Reclaiming an expired lease must fence the previous owner (delivery test).
- Repeated worker crashes must consume the attempt budget (delivery test).
- Streamed/chunked oversized bodies and invalid UTF-8 must be rejected (API test).
- HTTP redirects must never send signed payloads to an unconfigured destination (delivery test).

## Task 1: Signed API and configuration (primary agent)

Files: `src/relayqueue/config.py`, `signing.py`, `api.py`, `tests/test_api.py`, `tests/test_config.py`.

Interfaces: `Settings` frozen dataclass, `Settings.from_env()`; `sign(secret, event_id, timestamp, body) -> str`; `create_app(settings=None, database=None) -> FastAPI`.

- [x] Write failing tests for valid signed ingestion, tampering, stale timestamps, invalid schema/IDs, body limits, duplicate conflict, and admin auth.
- [x] Run tests and observe missing behavior; implement the smallest complete API against the storage contract below.
- [x] Run API/configuration tests and the full suite.

## Task 2: Durable delivery (delegated worker)

Files: `src/relayqueue/storage.py`, `worker.py`, `tests/test_delivery.py`.

Interfaces: `Database(url)`, `ensure_schema()`, `ingest(event_id, topic, body: bytes, now: float) -> tuple[dict, bool]`, `get_event(event_id) -> dict | None`, `list_events(status=None, limit=50) -> list[dict]`, `metrics() -> dict`, `close()`; `ConflictError`; `process_one(database, settings, *, client=None, now=None) -> bool`. Dictionary snapshots include `id`, `topic`, `status`, `attempt_count`, `last_error`, timestamps and attempt history; exclude raw payload. Settings fields are recorded in the worker brief.

- [x] Write and run failing tests for persistence, ingestion deduplication/conflicts, retries/exhaustion, permanent errors, lease races/recovery/fencing, crash budgets, and outbound signatures.
- [x] Implement transactional storage and delivery; confirm every claimed attempt is recorded and bounded.
- [x] Run tests with SQLite, then reuse the suite with PostgreSQL in CI.

## Task 3: Demonstration and delivery (primary agent)

Files: `scripts/demo.py`, `README.md`, `docs/interview.md`, `docs/validation.md`, `.github/workflows/ci.yml`, `Dockerfile`, `compose.yaml`, `.env.example`, `requirements.lock`.

- [x] Add a synthetic loopback HTTP demo showing invalid signature rejection, duplicates, 503-to-success recovery, and dead-letter exhaustion.
- [x] Run full tests, lint, format checks, package build, locked clean install and demo; record actual evidence.
- [x] Obtain independent review, fix material findings with regression tests, and rerun affected checks.
- [x] Create/push the new public repository; verify remote SHA and GitHub CI, including PostgreSQL and container demo.
- [x] Update the weekly ledger and deliver repository, verified resume drafts and exercise.

## Execution decisions

The user's working agreement and explicit request authorize execution without another plan approval. PM mode delegates the bounded storage/worker task; the primary agent owns API, integration, and publication. New standalone repository provides isolation without a worktree of the empty parent repository.
