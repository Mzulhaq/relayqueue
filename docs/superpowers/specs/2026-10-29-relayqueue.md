# RelayQueue design

Build a complete, locally runnable Python/backend portfolio project and publish it under Mzulhaq/relayqueue. This week's project is authorized by the user's request to create it now and the accepted weekly defaults. The owner delegated routine implementation choices.

## Behavior

A FastAPI ingress accepts JSON objects with `topic` and object-valued `data`, signed with HMAC-SHA256 over `timestamp.event_id.raw_body`. Headers are `Webhook-Id`, `Webhook-Timestamp`, and `Webhook-Signature` (`v1=<hex>`). Accept a five-minute timestamp window, cap streamed bodies at 65536 bytes, and compare signatures in constant time. Event IDs are 1-128 ASCII letters/digits, underscore, dash, or dot. Persist before acknowledging. A repeated ID with identical bytes returns the existing event; changed bytes return 409. Invalid requests never enter the queue.

A separate synchronous worker delivers the exact body to one operator-configured destination. SQLAlchemy supports SQLite for a zero-service demo and PostgreSQL for integration CI. Atomic conditional UPDATE claims and random lease tokens ensure one current owner. Workers record attempts durably, recover expired leases after crashes, and reject stale completion writes. Requests have bounded timeouts, redirects are disabled, and HTTP error response bodies are not retained. Retry network failures, 408, 425, 429 and 5xx with bounded exponential backoff; other non-2xx responses are terminal. Attempts are bounded including crashed attempts. Delivery is at-least-once: consumers deduplicate on Webhook-Id. Never promise exactly-once external effects.

Secrets come from configuration, never hard-coded production defaults. Distinct ingress, destination, and admin secrets must each be at least 32 characters. HTTPS is required for the configured destination except an explicit local-development HTTP opt-in. Per-request destination URLs are unsupported. Admin bearer auth protects event history and JSON metrics. `/health` is liveness; `/ready` checks the database. No UI, paid services, real customer data, or automatic cloud deployment.

## Deliverables

Runnable API and worker, meaningful API/security/recovery/concurrency tests, synthetic loopback HTTP demo, exact dependency lock, PostgreSQL and SQLite CI, Docker Compose example, README architecture and operational limitations, interview notes and a hands-on exercise, truthful validation evidence. Docker is not installed locally; validate container execution in GitHub CI before claiming it works. Retention, tenant isolation, migrations, rate limits, and high-volume performance are explicitly future work.
