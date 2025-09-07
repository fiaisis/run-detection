"""Module providing health functionality such as readiness and liveness checks and heartbeating."""

import logging
import time
from pathlib import Path
from threading import Event, Thread

logger = logging.getLogger(__name__)


class Heartbeat:
    def __init__(self):
        self.path = Path("/tmp/heartbeat")  # noqa: S108
        self.interval = 5.0
        self._stop = Event()
        self._thread = Thread(target=self._run, daemon=True, name="heartbeat")

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=3.0)

    def _run(self) -> None:
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
        Write the file with the timestamp for the readinessprobe
        :return: None.
        """
        with self.path.open("w", encoding="utf-8") as file:
            file.write(time.strftime("%Y-%m-%d %H:%M:%S"))
