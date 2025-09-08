"""Health utilities for the service.

This module provides basic health functionality including a simple heartbeat
mechanism that periodically writes a timestamp to a file on disk. This can be
used by external systems (e.g. k8s liveness/readiness probes) to verify the
process is alive.
"""

import logging
import time
from pathlib import Path
from threading import Event, Thread

logger = logging.getLogger(__name__)


class Heartbeat:

    """Background task that periodically touches a heartbeat file.

    The heartbeat writes the current timestamp to ``/tmp/heartbeat`` at a fixed
    interval. It runs in a daemon thread and can be started and stopped by the
    hosting process.

    Attributes:
        path: Filesystem path of the heartbeat file.
        interval: Seconds to wait between writes.
    """

    def __init__(self):
        """Create a heartbeat with default path and interval."""
        self.path = Path("/tmp/heartbeat")  # noqa: S108
        self.interval = 5.0
        self._stop = Event()
        self._thread = Thread(target=self._run, daemon=True, name="heartbeat")

    def start(self) -> None:
        """Start the heartbeat thread.

        :return: None.
        """
        self._thread.start()

    def stop(self) -> None:
        """Signal the heartbeat to stop and wait a short time for it to exit.

        Uses a timeout to avoid blocking indefinitely if the thread cannot join.

        :return: None.
        """
        self._stop.set()
        self._thread.join(timeout=3.0)

    def _run(self) -> None:
        """Worker loop that writes the timestamp and waits between iterations."""
        while not self._stop.is_set():
            try:
                self.path.write_text(time.strftime("%Y-%m-%d %H:%M:%S"), encoding="utf-8")
            except Exception:
                # Keep heartbeat resilient; don't crash on transient FS issues.
                logger.debug("Heartbeat write failed", exc_info=True)
            # Use wait() so we can exit promptly
            self._stop.wait(self.interval)

    def _write_readiness_probe_file(self) -> None:
        """
        Write/update the readiness probe file with the current timestamp.

        :return: None.
        """
        with self.path.open("w", encoding="utf-8") as file:
            file.write(time.strftime("%Y-%m-%d %H:%M:%S"))
