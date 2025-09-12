"""Tests for the Heartbeat background task in rundetection.health."""

from __future__ import annotations

import contextlib
import logging
import time
from typing import TYPE_CHECKING, Any

import pytest

from rundetection.health import Heartbeat

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path


@pytest.fixture
def heartbeat(tmp_path: Path) -> Generator[Heartbeat, Any, None]:
    """Heartbeat fixture."""
    hb = Heartbeat()
    hb.path = tmp_path / "heartbeat"
    hb.interval = 0.05
    try:
        yield hb
    finally:
        # Ensure no background thread leaks if the test started it
        with contextlib.suppress(Exception):
            hb.stop()
            hb.path.unlink()


def wait_until(predicate, timeout: float = 1.0, interval: float = 0.01) -> bool:
    """Wait until predicate() returns True or timeout occurs."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            time.sleep(0.4)
            return True
        time.sleep(interval)
    return False


def test_thread_properties_before_start(heartbeat: Heartbeat) -> None:
    """Thread should be created as daemon and have the expected name."""
    assert heartbeat._thread.daemon is True
    assert heartbeat._thread.name == "heartbeat"
    assert not heartbeat._thread.is_alive()


def test_writes_file_and_updates_periodically(heartbeat: Heartbeat, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that the heartbeat writes a file periodically with the current time."""
    monkeypatch.setattr(time, "strftime", lambda fmt: "STAMP1")

    heartbeat.start()

    assert wait_until(lambda: heartbeat.path.exists())
    assert heartbeat.path.read_text(encoding="utf-8") == "STAMP1"

    monkeypatch.setattr(time, "strftime", lambda fmt: "STAMP2")
    assert wait_until(lambda: heartbeat.path.read_text(encoding="utf-8") == "STAMP2")


def test_stop_prevents_further_writes(heartbeat: Heartbeat, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that stopping the heartbeat prevents further writes to the heartbeat file."""
    monkeypatch.setattr(time, "strftime", lambda fmt: "BEFORE")
    heartbeat.start()
    assert wait_until(lambda: heartbeat.path.exists())
    assert heartbeat.path.read_text(encoding="utf-8") == "BEFORE"

    heartbeat.stop()
    monkeypatch.setattr(time, "strftime", lambda fmt: "AFTER")

    time.sleep(heartbeat.interval * 2)
    assert heartbeat.path.read_text(encoding="utf-8") == "BEFORE"


def test_exception_during_write_is_logged_and_does_not_crash(
    heartbeat: Heartbeat, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that an exception during write is logged and the thread continues running."""

    class Boom(Exception):  # noqa: N818
        pass

    def boom_write_text(*_args, **_kwargs):
        raise Boom("fs error")

    caplog.set_level(logging.DEBUG)
    monkeypatch.setattr(type(heartbeat.path), "write_text", boom_write_text, raising=True)

    heartbeat.start()

    time.sleep(heartbeat.interval * 3)

    assert any("Heartbeat write failed" in rec.message for rec in caplog.records)

    assert heartbeat._thread.is_alive() is True
