"""Valkey cache helpers."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from functools import cache
from typing import Any

from valkey import Valkey
from valkey.exceptions import ValkeyError

logger = logging.getLogger(__name__)

DEFAULT_VALKEY_URL = "redis://valkey.valkey.svc.cluster.local:6379/0"


@dataclass(slots=True)
class _ValkeyState:
    client: Valkey | None = None
    disabled: bool = False


@cache
def _valkey_state() -> _ValkeyState:
    return _ValkeyState()


def _create_client() -> Valkey:
    url = os.environ.get("VALKEY_URL", DEFAULT_VALKEY_URL)
    return Valkey.from_url(
        url,
        decode_responses=True,
        socket_connect_timeout=0.5,
        socket_timeout=1,
        retry_on_timeout=False,
    )


def get_valkey_client() -> Valkey | None:
    """
    Get or create a shared Valkey client.

    The client is created lazily. If Valkey is unavailable, future calls return
    None without repeatedly attempting a connection.
    """
    state = _valkey_state()
    if state.disabled:
        logger.warning("Valkey cache disabled: previous connection error")
        return None
    if state.client is None:
        try:
            state.client = _create_client()
        except (ValkeyError, ValueError) as exc:
            state.disabled = True
            logger.warning("Valkey cache disabled: %s", exc)
            return None
    return state.client


def _disable_cache(exc: Exception) -> None:
    state = _valkey_state()
    if not state.disabled:
        state.disabled = True
        logger.warning("Valkey cache disabled: %s", exc)


def cache_get_json(key: str) -> Any | None:
    """Retrieve and deserialize a JSON value from Valkey."""
    client = get_valkey_client()
    if client is None:
        return None
    try:
        raw = client.get(key)
    except ValkeyError as exc:
        _disable_cache(exc)
        logger.exception("Failed to retrieve JSON from Valkey cache", exc_info=exc)
        return None
    if raw is None:
        return None
    if isinstance(raw, bytes | bytearray):
        raw_text = raw.decode("utf-8")
    elif isinstance(raw, str):
        raw_text = raw
    else:
        return None
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        logger.warning("Failed to parse JSON from Valkey cache for key: %s", key)
        return None


def cache_set_json(key: str, value: Any, ttl_seconds: int) -> None:
    """Store a JSON-serializable value in Valkey with a time-to-live."""
    if ttl_seconds <= 0:
        return
    client = get_valkey_client()
    if client is None:
        return
    try:
        payload = json.dumps(value)
    except TypeError:
        return
    try:
        client.setex(key, ttl_seconds, payload)
    except ValkeyError as exc:
        _disable_cache(exc)
