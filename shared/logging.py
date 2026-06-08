from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, Request, Response


def configure_logging(service_name: str) -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    return logging.getLogger(service_name)


def log_event(logger: logging.Logger, service_name: str, message: str, **fields: Any) -> None:
    logger.info(json.dumps({"service": service_name, "message": message, **fields}))


SENSITIVE_KEYS = {"authorization", "password", "token", "access_token"}
MAX_LOG_BODY_CHARS = 2000


def _safe_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: "***" if key.lower() in SENSITIVE_KEYS else _safe_json(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_safe_json(item) for item in value]
    return value


def _decode_body(body: bytes, content_type: str | None) -> Any:
    if not body:
        return None
    text = body.decode("utf-8", errors="replace")
    if "application/json" in (content_type or ""):
        try:
            return _safe_json(json.loads(text))
        except json.JSONDecodeError:
            pass
    if len(text) > MAX_LOG_BODY_CHARS:
        return f"{text[:MAX_LOG_BODY_CHARS]}...<truncated>"
    return text


def _request_headers(request: Request) -> dict[str, str]:
    allowed = {"content-type", "user-agent", "x-store-id", "x-user-role", "x-user-id"}
    return {
        key: ("***" if key.lower() in SENSITIVE_KEYS else value)
        for key, value in request.headers.items()
        if key.lower() in allowed
    }


def install_api_logging(app: FastAPI, logger: logging.Logger, service_name: str) -> None:
    @app.middleware("http")
    async def api_logging_middleware(request: Request, call_next):
        request_id = request.headers.get("x-request-id", str(uuid4()))
        request_body = await request.body()

        async def receive() -> dict[str, Any]:
            return {"type": "http.request", "body": request_body, "more_body": False}

        logged_request = Request(request.scope, receive)
        started_at = time.perf_counter()
        try:
            response = await call_next(logged_request)
        except Exception as exc:
            duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
            log_event(
                logger,
                service_name,
                "api call failed",
                request_id=request_id,
                method=request.method,
                path=request.url.path,
                query=str(request.query_params),
                status_code=500,
                duration_ms=duration_ms,
                request_headers=_request_headers(request),
                request_body=_decode_body(request_body, request.headers.get("content-type")),
                error=str(exc),
            )
            raise

        response_chunks = [chunk async for chunk in response.body_iterator]
        response_body = b"".join(response_chunks)
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        log_event(
            logger,
            service_name,
            "api call",
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            query=str(request.query_params),
            status_code=response.status_code,
            duration_ms=duration_ms,
            request_headers=_request_headers(request),
            request_body=_decode_body(request_body, request.headers.get("content-type")),
            response_body=_decode_body(response_body, response.headers.get("content-type")),
        )
        return Response(
            content=response_body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type,
            background=response.background,
        )
