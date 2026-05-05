"""Tests for Valkey cache helpers."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from valkey.exceptions import ValkeyError

from rundetection.cache import _valkey_state, cache_get_json, cache_set_json, get_valkey_client


@pytest.fixture(autouse=True)
def reset_valkey_state():
    """Clear the shared Valkey state around each test."""
    _valkey_state.cache_clear()
    yield
    _valkey_state.cache_clear()


def test_get_valkey_client_uses_configured_url():
    """The client is created from VALKEY_URL with short timeouts."""
    with (
        patch.dict(os.environ, {"VALKEY_URL": "redis://localhost:6379/0"}),
        patch("rundetection.cache.Valkey.from_url") as mock_from_url,
    ):
        assert get_valkey_client() == mock_from_url.return_value

    mock_from_url.assert_called_once_with(
        "redis://localhost:6379/0",
        decode_responses=True,
        socket_connect_timeout=0.5,
        socket_timeout=1,
        retry_on_timeout=False,
    )


def test_get_valkey_client_disables_cache_after_connection_error():
    """A connection error disables further Valkey client creation attempts."""
    with patch("rundetection.cache.Valkey.from_url", side_effect=ValkeyError("boom")) as mock_from_url:
        assert get_valkey_client() is None
        assert get_valkey_client() is None

    mock_from_url.assert_called_once()


def test_cache_get_json_returns_parsed_payload():
    """JSON cache values are deserialized before being returned."""
    mock_client = MagicMock()
    mock_client.get.return_value = '{"answer": 42}'

    with patch("rundetection.cache.get_valkey_client", return_value=mock_client):
        assert cache_get_json("key") == {"answer": 42}


def test_cache_get_json_disables_cache_on_valkey_error():
    """Valkey read errors disable the shared cache."""
    mock_client = MagicMock()
    mock_client.get.side_effect = ValkeyError("boom")

    with (
        patch("rundetection.cache.get_valkey_client", return_value=mock_client),
        patch("rundetection.cache._disable_cache") as mock_disable,
    ):
        assert cache_get_json("key") is None

    mock_disable.assert_called_once()


def test_cache_set_json_sets_payload_with_ttl():
    """JSON-serializable values are stored using the given TTL."""
    mock_client = MagicMock()

    with patch("rundetection.cache.get_valkey_client", return_value=mock_client):
        cache_set_json("key", {"answer": 42}, 60)

    mock_client.setex.assert_called_once_with("key", 60, '{"answer": 42}')


def test_cache_set_json_ignores_non_positive_ttl():
    """Non-positive TTLs skip cache writes before creating a client."""
    with patch("rundetection.cache.get_valkey_client") as mock_get_client:
        cache_set_json("key", {"answer": 42}, 0)

    mock_get_client.assert_not_called()
