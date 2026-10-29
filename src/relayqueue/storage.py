"""SQL-backed durable queue and attempt audit."""

from __future__ import annotations

import hashlib
import time
import uuid
from typing import Any

from sqlalchemy import (
    Column,
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    create_engine,
    event,
    func,
    select,
    update,
)
from sqlalchemy.exc import IntegrityError, OperationalError


class ConflictError(Exception):
    """An event id was reused for different bytes."""


metadata = MetaData()
events = Table(
    "relay_events",
    metadata,
    Column("id", String(128), primary_key=True),
    Column("topic", String(255), nullable=False),
    Column("payload", LargeBinary, nullable=False),
    Column("body_hash", String(64), nullable=False),
    Column("status", String(20), nullable=False),
    Column("attempt_count", Integer, nullable=False, default=0),
    Column("last_error", String(512)),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    Column("available_at", Float, nullable=False),
    Column("lease_token", String(36)),
    Column("lease_until", Float),
    Column("active_attempt_id", Integer),
)
attempts = Table(
    "relay_attempts",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "event_id", String(128), ForeignKey("relay_events.id", ondelete="CASCADE"), nullable=False
    ),
    Column("attempt_no", Integer, nullable=False),
    Column("started_at", Float, nullable=False),
    Column("finished_at", Float),
    Column("outcome", String(32), nullable=False),
    Column("status_code", Integer),
    Column("error", String(512)),
)


def _snapshot(row: Any, history: list[dict] | None = None) -> dict:
    return {
        "id": row["id"],
        "topic": row["topic"],
        "status": row["status"],
        "attempt_count": row["attempt_count"],
        "last_error": row["last_error"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "available_at": row["available_at"],
        "attempts": history or [],
    }


class Database:
    def __init__(self, url: str):
        options: dict[str, Any] = {"pool_pre_ping": True}
        if url.startswith("sqlite"):
            options["connect_args"] = {"check_same_thread": False, "timeout": 30}
            if ":memory:" in url:
                from sqlalchemy.pool import StaticPool

                options.update(poolclass=StaticPool)
        self.engine = create_engine(url, **options)
        if url.startswith("sqlite"):

            @event.listens_for(self.engine, "connect")
            def _sqlite_pragmas(connection, _record):
                cursor = connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.execute("PRAGMA busy_timeout=30000")
                cursor.close()

    def ensure_schema(self) -> None:
        metadata.create_all(self.engine)

    def ingest(self, event_id: str, topic: str, body: bytes, now: float) -> tuple[dict, bool]:
        digest = hashlib.sha256(body).hexdigest()
        values = dict(
            id=event_id,
            topic=topic,
            payload=body,
            body_hash=digest,
            status="pending",
            attempt_count=0,
            last_error=None,
            created_at=now,
            updated_at=now,
            available_at=now,
        )
        try:
            with self.engine.begin() as conn:
                conn.execute(events.insert().values(**values))
                row = conn.execute(select(events).where(events.c.id == event_id)).mappings().one()
            return self.get_event(event_id), True
        except IntegrityError:
            row = self._event_row(event_id)
            if row is None:
                raise
            if row["body_hash"] != digest:
                raise ConflictError(event_id) from None
            return self.get_event(event_id), False

    def _event_row(self, event_id: str):
        with self.engine.connect() as conn:
            return conn.execute(select(events).where(events.c.id == event_id)).mappings().first()

    def _history(self, event_id: str) -> list[dict]:
        with self.engine.connect() as conn:
            rows = (
                conn.execute(
                    select(attempts)
                    .where(attempts.c.event_id == event_id)
                    .order_by(attempts.c.attempt_no)
                )
                .mappings()
                .all()
            )
        return [dict(r) for r in rows]

    def get_event(self, event_id: str) -> dict | None:
        row = self._event_row(event_id)
        return None if row is None else _snapshot(row, self._history(event_id))

    def list_events(self, status: str | None = None, limit: int = 50) -> list[dict]:
        query = (
            select(events).order_by(events.c.created_at.desc(), events.c.id).limit(max(0, limit))
        )
        if status:
            query = query.where(events.c.status == status)
        with self.engine.connect() as conn:
            rows = conn.execute(query).mappings().all()
        return [_snapshot(row, self._history(row["id"])) for row in rows]

    def metrics(self) -> dict:
        statuses = ("pending", "inflight", "retry_wait", "succeeded", "dead")
        with self.engine.connect() as conn:
            counts = {
                status: conn.execute(
                    select(func.count()).select_from(events).where(events.c.status == status)
                ).scalar_one()
                for status in statuses
            }
            total = conn.execute(select(func.count()).select_from(attempts)).scalar_one()
        return {"by_status": counts, "total_attempts": total}

    def claim_one(
        self, *, now: float | None = None, lease_seconds: float = 30, max_attempts: int = 5
    ) -> dict | None:
        now = time.time() if now is None else now
        # Each transition is a conditional write; only the transaction that wins the
        # status/lease predicate gets to create the attempt and receive the payload.
        for _ in range(5):
            try:
                with self.engine.begin() as conn:
                    expired = (
                        conn.execute(
                            select(events)
                            .where(events.c.status == "inflight", events.c.lease_until <= now)
                            .order_by(events.c.lease_until)
                            .limit(1)
                        )
                        .mappings()
                        .first()
                    )
                    if expired is not None:
                        if expired["attempt_count"] >= max_attempts:
                            reclaimed = conn.execute(
                                update(events)
                                .where(
                                    events.c.id == expired["id"],
                                    events.c.status == "inflight",
                                    events.c.lease_token == expired["lease_token"],
                                    events.c.lease_until <= now,
                                )
                                .values(
                                    status="dead",
                                    last_error="LeaseExpired",
                                    updated_at=now,
                                    lease_token=None,
                                    lease_until=None,
                                    active_attempt_id=None,
                                )
                            )
                            if reclaimed.rowcount == 1:
                                conn.execute(
                                    update(attempts)
                                    .where(attempts.c.id == expired["active_attempt_id"])
                                    .values(
                                        finished_at=now, outcome="abandoned", error="LeaseExpired"
                                    )
                                )
                            continue
                        reclaimed = conn.execute(
                            update(events)
                            .where(
                                events.c.id == expired["id"],
                                events.c.status == "inflight",
                                events.c.lease_token == expired["lease_token"],
                                events.c.lease_until <= now,
                            )
                            .values(
                                status="retry_wait",
                                last_error="LeaseExpired",
                                updated_at=now,
                                available_at=now,
                                lease_token=None,
                                lease_until=None,
                                active_attempt_id=None,
                            )
                        )
                        if reclaimed.rowcount == 1:
                            conn.execute(
                                update(attempts)
                                .where(attempts.c.id == expired["active_attempt_id"])
                                .values(finished_at=now, outcome="abandoned", error="LeaseExpired")
                            )
                    conn.execute(
                        update(events)
                        .where(
                            events.c.status.in_(("pending", "retry_wait")),
                            events.c.attempt_count >= max_attempts,
                        )
                        .values(status="dead", last_error="AttemptLimitChanged", updated_at=now)
                    )
                    query = (
                        select(events)
                        .where(
                            events.c.status.in_(("pending", "retry_wait")),
                            events.c.available_at <= now,
                            events.c.attempt_count < max_attempts,
                        )
                        .order_by(events.c.available_at, events.c.created_at, events.c.id)
                        .limit(1)
                    )
                    candidate = conn.execute(query).mappings().first()
                    if candidate is None:
                        return None
                    token = str(uuid.uuid4())
                    attempt_no = candidate["attempt_count"] + 1
                    result = conn.execute(
                        update(events)
                        .where(
                            events.c.id == candidate["id"],
                            events.c.status == candidate["status"],
                            events.c.attempt_count == candidate["attempt_count"],
                        )
                        .values(
                            status="inflight",
                            attempt_count=attempt_no,
                            updated_at=now,
                            lease_token=token,
                            lease_until=now + lease_seconds,
                        )
                    )
                    if result.rowcount != 1:
                        continue
                    attempt_id = conn.execute(
                        attempts.insert().values(
                            event_id=candidate["id"],
                            attempt_no=attempt_no,
                            started_at=now,
                            outcome="inflight",
                        )
                    ).inserted_primary_key[0]
                    conn.execute(
                        update(events)
                        .where(events.c.id == candidate["id"], events.c.lease_token == token)
                        .values(active_attempt_id=attempt_id)
                    )
                    row = (
                        conn.execute(select(events).where(events.c.id == candidate["id"]))
                        .mappings()
                        .one()
                    )
                    return {**dict(row), "lease_token": token}
            except OperationalError as exc:
                if "locked" not in str(exc).lower():
                    raise
                time.sleep(0.01)
        return None

    def finish(
        self,
        event_id: str,
        lease_token: str,
        *,
        outcome: str,
        now: float,
        error: str | None = None,
        status_code: int | None = None,
        retry_at: float | None = None,
    ) -> bool:
        status = {
            "succeeded": "succeeded",
            "retryable_error": "retry_wait",
            "permanent_error": "dead",
        }[outcome]
        with self.engine.begin() as conn:
            row = (
                conn.execute(
                    select(events).where(
                        events.c.id == event_id,
                        events.c.status == "inflight",
                        events.c.lease_token == lease_token,
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                return False
            saved = conn.execute(
                update(events)
                .where(
                    events.c.id == event_id,
                    events.c.status == "inflight",
                    events.c.lease_token == lease_token,
                    events.c.lease_until > now,
                )
                .values(
                    status=status,
                    last_error=error,
                    updated_at=now,
                    available_at=retry_at if retry_at is not None else row["available_at"],
                    lease_token=None,
                    lease_until=None,
                    active_attempt_id=None,
                )
            )
            if saved.rowcount != 1:
                return False
            conn.execute(
                update(attempts)
                .where(attempts.c.id == row["active_attempt_id"])
                .values(finished_at=now, outcome=outcome, status_code=status_code, error=error)
            )
            return True

    def close(self) -> None:
        self.engine.dispose()
