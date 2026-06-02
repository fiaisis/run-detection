"""Tests for Valkey cache helpers."""

import json
import os
from unittest.mock import Mock, patch

from redis.exceptions import RedisError

from rundetection.cache import _create_client, _state, cache_get_json, cache_set_json, get_valkey_client

TTL = 30


def setup_function() -> None:
    """Clear cached Valkey state between tests."""
    _state.cache_clear()


def test_cache_get_json_returns_none_without_client():
    """No client should behave like a cache miss."""
    with patch("rundetection.cache.get_valkey_client", return_value=None):
        assert cache_get_json("key") is None


def test_cache_get_json_returns_parsed_payload():
    """JSON cache values should be decoded."""
    client = Mock()
    client.get.return_value = '{"answer": 42}'

    with patch("rundetection.cache.get_valkey_client", return_value=client):
        assert cache_get_json("key") == {"answer": 42}

    client.get.assert_called_once_with("key")


def test_cache_get_json_returns_none_for_bad_json():
    """Invalid JSON should behave like a cache miss."""
    client = Mock()
    client.get.return_value = "not-json"

    with patch("rundetection.cache.get_valkey_client", return_value=client):
        assert cache_get_json("key") is None


def test_cache_get_json_disables_on_redis_error():
    """Redis errors should disable cache access for the process."""
    client = Mock()
    client.get.side_effect = RedisError("boom")

    with patch("rundetection.cache.get_valkey_client", return_value=client):
        assert cache_get_json("key") is None

    assert _state().disabled is True


def test_cache_set_json_sets_payload_with_ttl():
    """JSON cache values should be stored with the requested TTL."""
    client = Mock()

    with patch("rundetection.cache.get_valkey_client", return_value=client):
        cache_set_json("key", {"answer": 42}, TTL)

    client.setex.assert_called_once()
    key, ttl, payload = client.setex.call_args[0]
    assert key == "key"
    assert ttl == TTL
    assert json.loads(payload) == {"answer": 42}


def test_cache_set_json_skips_non_positive_ttl():
    """Non-positive TTL values should skip cache writes."""
    with patch("rundetection.cache.get_valkey_client") as get_client:
        cache_set_json("key", {"answer": 42}, 0)

    get_client.assert_not_called()


def test_create_client_uses_valkey_url():
    """The Valkey URL should come from the environment when set."""
    with (
        patch.dict(os.environ, {"VALKEY_URL": "redis://localhost:6379/1"}),
        patch("rundetection.cache.Redis.from_url") as from_url,
    ):
        _create_client()

    from_url.assert_called_once()
    assert from_url.call_args[0][0] == "redis://localhost:6379/1"


def test_get_valkey_client_disables_on_create_error():
    """Client creation errors should disable further cache attempts."""
    with patch("rundetection.cache._create_client", side_effect=RedisError("boom")):
        assert get_valkey_client() is None

    assert _state().disabled is True
