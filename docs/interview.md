# RelayQueue interview notes

Use these notes to explain decisions in the implementation. Be ready to point to the code and discuss tradeoffs rather than presenting the project as a production service.

## Questions and answers

### Why persist before returning success to the sender?

The ingress writes the event to SQL before returning `202`. This means an accepted event survives an API process restart. A duplicate ID with identical raw bytes returns the existing record; a duplicate ID with changed bytes is a `409` conflict. The raw bytes matter because the signature authenticates those exact bytes and the receiver gets the same payload.

### What exactly does the signature protect?

The HMAC-SHA256 input is `timestamp.event_id.` followed by the raw body. The API checks a five-minute timestamp window and compares signatures in constant time. Including the ID and timestamp binds those values to the payload and limits replay duration. Delivery uses the same protocol with a newly generated timestamp for each attempt.

### How does the worker avoid two workers delivering the same event concurrently?

It claims work with a conditional database update, records an attempt, and assigns a random lease token and expiry. Completion writes require the current token and an unexpired lease. If a worker stalls and its lease is reclaimed, its later completion cannot overwrite the newer state.

### Why describe delivery as at least once?

The receiver can accept a request and the worker can crash before recording success. After lease expiry the event may be sent again. The queue cannot atomically commit a database transaction together with an arbitrary receiver's side effect, so the receiver should deduplicate using `Webhook-Id`.

### Which failures are retried?

Network failures, HTTP 408, 425, 429, and 5xx responses use bounded exponential backoff, subject to the attempt limit. Other non-2xx statuses are terminal. Redirects are disabled so signed content cannot be forwarded to a different URL. The response body is not stored.

### What would need to change before a public deployment?

Add schema migrations, retention and deletion policy, rate limits, tenant isolation, and operational monitoring. Configure TLS and proxy limits, manage secrets outside checked-in files, and define backup and recovery procedures. Test workload-specific capacity before setting any throughput expectations.

### How would you test recovery behavior?

Use a controllable receiver and a database with an injectable clock. Test that a temporary response schedules a retry, a terminal response reaches `dead`, a lease expiry consumes an attempt, and a stale lease token cannot change the reclaimed event. The loopback demo provides an end-to-end synthetic scenario; automated tests cover focused cases.

## 30-minute exercise: authenticated dead-letter redrive

Add an admin-only operation that schedules one `dead` event for another delivery attempt without changing its ID or body. Define the API response and audit behavior first. Write acceptance tests for an unauthorized request, a missing event, rejection of non-dead events, successful redrive, and preservation of payload and event ID. Ensure the operation is transactional and that a redrive cannot bypass the configured attempt policy silently. Document the operator workflow and explain how repeat redrives appear in the attempt history.

## Resume drafts

Adapt these only after reviewing and exercising the project. Keep the wording accurate to the code and your own contribution; do not add impact or scale claims without evidence.

- Built a Python/FastAPI webhook relay with HMAC-authenticated ingress, SQL persistence, and duplicate protection; validated behavior with 72 automated tests and 90% statement coverage.
- Implemented durable retries, dead-letter handling, and lease-token fencing; verified recovery with real HTTP fault scenarios and passing PostgreSQL, Python 3.11/3.13, and container CI checks.

AI assistance contributed to this project. Be candid about which design, implementation, testing, and review work you personally performed, and be prepared to explain each component.
