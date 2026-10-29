# Validation evidence

Validated on October 29, 2026. This is a functional reliability demonstration using synthetic data, not a throughput benchmark or production deployment.

## Local checks

Windows, Python 3.11.9, installed from this repository's locked dependencies:

- `python -m pytest --cov=relayqueue --cov-report=term-missing -q`: 72 passed, 90% statement coverage.
- `python -m ruff check .` and `python -m ruff format --check .`: passed.
- `python -m pip check`: no broken requirements.
- `python -m build`: source distribution and wheel built successfully.
- A fresh virtual environment installed `requirements.lock` and the built wheel; the same HTTP demo and `pip check` passed there.
- `python scripts/demo.py`: real loopback HTTP API and receiver, separate worker subprocess, temporary SQLite database, generated runtime secrets. Passed with 3 unique events, 25 duplicate submissions deduplicated, 1 invalid signature rejected, 2 successful events, 1 dead event, and 7 delivery attempts. The receiver confirmed exactly 1, 3, and 3 requests for the respective events.

Coverage includes the API, configuration, signatures, and storage/worker logic. The separate-process demo also exercises startup and the polling loop; its subprocess coverage is not included in the 90% figure.

## Cross-platform verification

[GitHub CI run 40905906621](https://github.com/Mzulhaq/relayqueue/actions/runs/40905906621) passed all four jobs for source commit `e0c2b0a735803378fbfcd4bbc0e165a56ab8e757`:

- Python 3.11 with SQLite: checks, full test suite, HTTP demo and package build passed.
- Python 3.13 with SQLite: the same checks passed.
- Python 3.11 with PostgreSQL 17: the full suite passed, with the delivery/storage tests targeting PostgreSQL (API tests use isolated SQLite databases); demo and package build also passed.
- Container: image build, in-container HTTP demo, Docker Compose startup, API readiness and worker access to PostgreSQL passed.

Docker is not installed on the development machine; the container evidence comes from the GitHub-hosted Linux runner. See the repository's CI badge for subsequent runs.

## Guarantees and limits

An independent review examined the API, delivery/lease transitions, configuration, tests, and deployment setup. It identified a Compose environment-forwarding mismatch, which was corrected; it reported no remaining material findings. The reviewer separately ran all 18 delivery tests successfully.

Tests exercise conflicting and concurrent duplicate submissions, concurrent claims, expired-lease recovery, stale completion fencing, retry exhaustion, changed attempt limits, redirect rejection, body limits, and signature tampering. The demo measures behavior on a small synthetic scenario; it does not establish capacity, availability, or exactly-once side effects. Receiver-side idempotency remains necessary, and terminal failures are exposed for operator review.
