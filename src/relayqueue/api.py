"""Signed ingress and authenticated operational views."""

import hmac
import json
import re
import time
from contextlib import asynccontextmanager
from typing import Annotated, Any, Literal

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from starlette.concurrency import run_in_threadpool

from relayqueue.config import Settings
from relayqueue.signing import sign
from relayqueue.storage import ConflictError, Database


class Envelope(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    topic: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    data: dict[str, Any]


def reject_constant(value):
    raise ValueError("Non-finite numbers are not valid JSON")


def create_app(settings: Settings | None = None, database: Database | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    db = database or Database(settings.database_url)

    @asynccontextmanager
    async def lifespan(app):
        await run_in_threadpool(db.ensure_schema)
        yield
        if database is None:
            await run_in_threadpool(db.close)

    app = FastAPI(
        title="RelayQueue",
        version="0.1.0",
        lifespan=lifespan,
        description="Signed webhook ingestion with durable, at-least-once delivery.",
    )
    app.state.database = db
    bearer = HTTPBearer(auto_error=False)

    def require_admin(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ):
        if credentials is None or not hmac.compare_digest(
            credentials.credentials.encode(), settings.admin_token.encode()
        ):
            raise HTTPException(
                401, "Valid admin bearer token required", headers={"WWW-Authenticate": "Bearer"}
            )

    @app.exception_handler(SQLAlchemyError)
    async def database_error(request, exc):
        # Database exception strings can contain URLs, SQL values, and credentials.
        return JSONResponse({"detail": "Database temporarily unavailable"}, status_code=503)

    @app.get("/health", tags=["operations"])
    def health():
        return {"status": "ok"}

    @app.get("/ready", tags=["operations"])
    def ready():
        with db.engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return {"status": "ready"}

    @app.post("/webhooks", tags=["ingress"], status_code=202)
    async def ingest(request: Request):
        if request.headers.get("content-type", "").split(";")[0].strip() != "application/json":
            raise HTTPException(415, "Content-Type must be application/json")
        event_id = request.headers.get("webhook-id", "")
        timestamp = request.headers.get("webhook-timestamp", "")
        signature = request.headers.get("webhook-signature", "")
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,128}", event_id):
            raise HTTPException(
                400, "Webhook-Id must be 1-128 letters, digits, dots, dashes or underscores"
            )
        if (
            not re.fullmatch(r"[0-9]{1,12}", timestamp)
            or abs(time.time() - int(timestamp)) > settings.signature_tolerance_seconds
            or not re.fullmatch(r"v1=[0-9a-f]{64}", signature)
        ):
            raise HTTPException(401, "Invalid or expired webhook signature")
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > settings.max_body_bytes:
                raise HTTPException(413, "Webhook body exceeds configured limit")
            body.extend(chunk)
        raw = bytes(body)
        if not hmac.compare_digest(
            signature, sign(settings.ingress_secret, event_id, timestamp, raw)
        ):
            raise HTTPException(401, "Invalid or expired webhook signature")
        try:
            parsed = json.loads(raw.decode("utf-8"), parse_constant=reject_constant)
            envelope = Envelope.model_validate(parsed)
        except (ValueError, ValidationError, RecursionError):
            raise HTTPException(
                422, "Expected JSON with a valid topic and object-valued data"
            ) from None
        try:
            snapshot, created = await run_in_threadpool(
                db.ingest, event_id, envelope.topic, raw, time.time()
            )
        except ConflictError:
            raise HTTPException(409, "Webhook-Id already exists with different content") from None
        return JSONResponse(
            {"id": snapshot["id"], "status": snapshot["status"], "created": created},
            status_code=202 if created else 200,
        )

    @app.get("/events", dependencies=[Depends(require_admin)], tags=["operations"])
    def events(
        status: Literal["pending", "inflight", "retry_wait", "succeeded", "dead"] | None = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
    ):
        return db.list_events(status=status, limit=limit)

    @app.get("/events/{event_id}", dependencies=[Depends(require_admin)], tags=["operations"])
    def event(event_id: str):
        snapshot = db.get_event(event_id)
        if snapshot is None:
            raise HTTPException(404, "Event not found")
        return snapshot

    @app.get("/metrics", dependencies=[Depends(require_admin)], tags=["operations"])
    def metrics():
        return db.metrics()

    return app
