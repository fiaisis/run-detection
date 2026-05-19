"""Valkey cache helpers."""

from __future__ import annotations

import contextlib
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
VALKEY_CACHE_ENABLED_ENV_VAR = "VALKEY_CACHE_ENABLED"
DISABLED_ENV_VALUES = {"0", "false", "no", "off"}


@dataclass(slots=True)
class _ValkeyState:
    client: Valkey | None = None
    disabled: bool = False


@cache
def _valkey_state() -> _ValkeyState:
    return _ValkeyState()


def _valkey_cache_enabled() -> bool:
    configured_value = os.environ.get(VALKEY_CACHE_ENABLED_ENV_VAR)
    if configured_value is None:
        return True
    return configured_value.strip().lower() not in DISABLED_ENV_VALUES


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

    The client is created lazily. Set VALKEY_CACHE_ENABLED=false to opt
    out of Valkey caching explicitly.
    """
    if not _valkey_cache_enabled():
        return None

    state = _valkey_state()
    if state.disabled:
        return None
    if state.client is None:
        try:
            state.client = _create_client()
        except (ValkeyError, ValueError) as exc:
            _disable_valkey_cache(exc)
            return None
    return state.client


def _disable_valkey_cache(exc: Exception) -> None:
    state = _valkey_state()
    state.disabled = True
    client = state.client
    state.client = None
    if client is not None:
        with contextlib.suppress(Exception):
            client.close()
    logger.warning("Valkey cache disabled: %s", exc)


def cache_get_json(key: str) -> Any | None:
    """Retrieve and deserialize a JSON value from Valkey."""
    client = get_valkey_client()
    if client is None:
        return None
    try:
        raw = client.get(key)
    except ValkeyError as exc:
        _disable_valkey_cache(exc)
        logger.warning("Failed to retrieve JSON from Valkey cache for key %s: %s", key, exc)
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
    except TypeError as exc:
        logger.warning("Failed to serialize JSON for Valkey cache key %s: %s", key, exc)
        return
    try:
        client.setex(key, ttl_seconds, payload)
    except ValkeyError as exc:
        _disable_valkey_cache(exc)
        logger.warning("Failed to store JSON in Valkey cache for key %s: %s", key, exc)
