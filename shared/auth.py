from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException


DEFAULT_AUTH_SECRET = "peakpick-demo-secret"
DEFAULT_TOKEN_TTL_SECONDS = 12 * 60 * 60
PASSWORD_SALT = "peakpick-demo-password"


@dataclass(frozen=True)
class AuthPrincipal:
    username: str
    role: str
    store_id: str
    display_name: str


def password_hash(password: str) -> str:
    return hashlib.sha256(f"{PASSWORD_SALT}:{password}".encode("utf-8")).hexdigest()


def _auth_secret() -> str:
    return os.getenv("PEAKPICK_AUTH_SECRET", DEFAULT_AUTH_SECRET)


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _b64decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(f"{data}{padding}".encode("utf-8"))


def create_access_token(payload: dict[str, Any], ttl_seconds: int = DEFAULT_TOKEN_TTL_SECONDS) -> str:
    now = int(time.time())
    claims = {**payload, "iat": now, "exp": now + ttl_seconds}
    encoded_payload = _b64encode(json.dumps(claims, separators=(",", ":")).encode("utf-8"))
    signature = hmac.new(_auth_secret().encode("utf-8"), encoded_payload.encode("utf-8"), hashlib.sha256).digest()
    return f"{encoded_payload}.{_b64encode(signature)}"


def decode_access_token(token: str) -> dict[str, Any]:
    try:
        encoded_payload, encoded_signature = token.split(".", 1)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail="Invalid auth token") from exc

    expected_signature = hmac.new(
        _auth_secret().encode("utf-8"),
        encoded_payload.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    try:
        provided_signature = _b64decode(encoded_signature)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail="Invalid auth token") from exc

    if not hmac.compare_digest(expected_signature, provided_signature):
        raise HTTPException(status_code=401, detail="Invalid auth token")

    claims = json.loads(_b64decode(encoded_payload))
    if int(claims.get("exp", 0)) < int(time.time()):
        raise HTTPException(status_code=401, detail="Auth token expired")
    return claims


def bearer_token(authorization: str | None) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(status_code=401, detail="Authorization must use Bearer token")
    return token


def principal_from_authorization(authorization: str | None) -> AuthPrincipal:
    claims = decode_access_token(bearer_token(authorization))
    return AuthPrincipal(
        username=str(claims["sub"]),
        role=str(claims["role"]),
        store_id=str(claims.get("store_id", "")),
        display_name=str(claims.get("display_name", claims["sub"])),
    )


def public_user(principal: AuthPrincipal) -> dict[str, str]:
    return {
        "username": principal.username,
        "role": principal.role,
        "store_id": principal.store_id,
        "display_name": principal.display_name,
    }
