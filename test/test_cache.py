"""Tests for Valkey cache helpers."""

from __future__ import annotations

import logging
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
        patch.dict(os.environ, {"VALKEY_URL": "redis://localhost:6379/0", "VALKEY_CACHE_ENABLED": "true"}),
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


def test_get_valkey_client_returns_none_when_cache_disabled():
    """VALKEY_CACHE_ENABLED=false opts out of Valkey client creation."""
    with (
        patch.dict(os.environ, {"VALKEY_CACHE_ENABLED": "false"}),
        patch("rundetection.cache.Valkey.from_url") as mock_from_url,
    ):
        assert get_valkey_client() is None

    mock_from_url.assert_not_called()


def test_get_valkey_client_disables_cache_after_connection_error(caplog: pytest.LogCaptureFixture):
    """A connection error disables further Valkey client creation attempts."""
    caplog.set_level(logging.WARNING)

    with patch("rundetection.cache.Valkey.from_url", side_effect=ValkeyError("boom")) as mock_from_url:
        assert get_valkey_client() is None
        assert get_valkey_client() is None

    mock_from_url.assert_called_once()
    assert "Valkey cache disabled: boom" in caplog.text


def test_cache_get_json_returns_parsed_payload():
    """JSON cache values are deserialized before being returned."""
    mock_client = MagicMock()
    mock_client.get.return_value = '{"answer": 42}'

    with patch("rundetection.cache.get_valkey_client", return_value=mock_client):
        assert cache_get_json("key") == {"answer": 42}


def test_cache_get_json_logs_closes_and_returns_none_on_valkey_error(caplog: pytest.LogCaptureFixture):
    """Valkey read errors are logged and treated as cache misses."""
    caplog.set_level(logging.WARNING)
    mock_client = MagicMock()
    mock_client.get.side_effect = ValkeyError("boom")

    with (
        patch.dict(os.environ, {"VALKEY_URL": "redis://localhost:6379/0", "VALKEY_CACHE_ENABLED": "true"}),
        patch("rundetection.cache.Valkey.from_url", return_value=mock_client),
    ):
        assert cache_get_json("key") is None

    assert _valkey_state().client is None
    assert _valkey_state().disabled is True
    mock_client.close.assert_called_once()
    assert "Failed to retrieve JSON from Valkey cache for key key: boom" in caplog.text


def test_cache_get_json_skips_client_when_cache_disabled():
    """Disabled Valkey caching returns no cached value without connecting."""
    with (
        patch.dict(os.environ, {"VALKEY_CACHE_ENABLED": "off"}),
        patch("rundetection.cache.Valkey.from_url") as mock_from_url,
    ):
        assert cache_get_json("key") is None

    mock_from_url.assert_not_called()


def test_cache_get_json_suppresses_client_close_errors_on_valkey_error():
    """Client cleanup failures should not escape the cache read path."""
    mock_client = MagicMock()
    mock_client.get.side_effect = ValkeyError("boom")
    mock_client.close.side_effect = RuntimeError("close failed")

    with (
        patch.dict(os.environ, {"VALKEY_URL": "redis://localhost:6379/0", "VALKEY_CACHE_ENABLED": "true"}),
        patch("rundetection.cache.Valkey.from_url", return_value=mock_client),
    ):
        assert cache_get_json("key") is None

    assert _valkey_state().client is None
    mock_client.close.assert_called_once()


def test_cache_set_json_sets_payload_with_ttl():
    """JSON-serializable values are stored using the given TTL."""
    mock_client = MagicMock()

    with patch("rundetection.cache.get_valkey_client", return_value=mock_client):
        cache_set_json("key", {"answer": 42}, 60)

    mock_client.setex.assert_called_once_with("key", 60, '{"answer": 42}')


def test_cache_set_json_logs_closes_and_returns_on_valkey_error(caplog: pytest.LogCaptureFixture):
    """Valkey write errors are logged with key context and ignored."""
    caplog.set_level(logging.WARNING)
    mock_client = MagicMock()
    mock_client.setex.side_effect = ValkeyError("boom")

    with (
        patch.dict(os.environ, {"VALKEY_URL": "redis://localhost:6379/0", "VALKEY_CACHE_ENABLED": "true"}),
        patch("rundetection.cache.Valkey.from_url", return_value=mock_client),
    ):
        cache_set_json("key", {"answer": 42}, 60)

    assert _valkey_state().client is None
    assert _valkey_state().disabled is True
    mock_client.close.assert_called_once()
    assert "Failed to store JSON in Valkey cache for key key: boom" in caplog.text


def test_cache_set_json_ignores_non_positive_ttl():
    """Non-positive TTLs skip cache writes before creating a client."""
    with patch("rundetection.cache.get_valkey_client") as mock_get_client:
        cache_set_json("key", {"answer": 42}, 0)

    mock_get_client.assert_not_called()
