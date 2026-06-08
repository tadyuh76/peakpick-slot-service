from __future__ import annotations

import os
from typing import Any

from fastapi import Request


DEFAULT_STORE_ID = os.getenv("DEFAULT_STORE_ID", "store-ueh")


def store_id_from_request(request: Request, default: str = DEFAULT_STORE_ID) -> str:
    return (
        request.headers.get("x-store-id")
        or request.query_params.get("store_id")
        or default
    )


def store_id_from_event_payload(payload: dict[str, Any], default: str = DEFAULT_STORE_ID) -> str:
    value = payload.get("store_id")
    return str(value) if value else default
