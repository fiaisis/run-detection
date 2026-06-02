"""Valkey cache helpers."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from functools import cache
from typing import Any

from redis import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

DEFAULT_VALKEY_URL = "redis://valkey.valkey.svc.cluster.local:6379/0"


@dataclass(slots=True)
class _State:
    client: Redis | None = None
    disabled: bool = False


@cache
def _state() -> _State:
    return _State()


def _create_client() -> Redis:
    url = os.environ.get("VALKEY_URL", DEFAULT_VALKEY_URL)
    return Redis.from_url(
        url,
        decode_responses=True,
        socket_connect_timeout=0.5,
        socket_timeout=1,
        retry_on_timeout=False,
    )


def get_valkey_client() -> Redis | None:
    """Return a shared Valkey client, or None if Valkey is unavailable."""
    state = _state()
    if state.disabled:
        return None
    if state.client is None:
        try:
            state.client = _create_client()
        except (RedisError, ValueError) as exc:
            state.disabled = True
            logger.warning("Valkey cache disabled: %s", exc)
            return None
    return state.client


def _disable(exc: Exception) -> None:
    state = _state()
    state.disabled = True
    logger.warning("Valkey cache disabled: %s", exc)


def cache_get_json(key: str) -> Any | None:
    """Return a JSON value from Valkey, or None on miss/error."""
    client = get_valkey_client()
    if client is None:
        return None
    try:
        raw = client.get(key)
    except RedisError as exc:
        _disable(exc)
        return None
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if not isinstance(raw, str):
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def cache_set_json(key: str, value: Any, ttl: int) -> None:
    """Store a JSON value in Valkey when caching is enabled."""
    if ttl <= 0:
        return
    client = get_valkey_client()
    if client is None:
        return
    try:
        payload = json.dumps(value)
    except TypeError:
        return
    try:
        client.setex(key, ttl, payload)
    except RedisError as exc:
        _disable(exc)
