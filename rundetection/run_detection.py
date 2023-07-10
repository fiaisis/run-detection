"""
Run detection module holds the RunDetector main class
"""
import logging
import re
import signal
import sys
import time
from pathlib import Path
from queue import SimpleQueue, Empty
from types import FrameType
from typing import Optional

from rundetection.ingest import ingest
from rundetection.notifications import Notifier, Notification
from rundetection.queue_listener import Message, QueueListener
from rundetection.specifications import InstrumentSpecification

file_handler = logging.FileHandler(filename="run-detection.log")
stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(
    handlers=[file_handler, stdout_handler],
    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


class RunDetector:
    """
    Run Detector class orchestrates the complete run detection process, from consuming messages from icat
    pre-queue, to notifying downstream
    """

    def __init__(self) -> None:
        self._message_queue: SimpleQueue[Message] = SimpleQueue()
        self._queue_listener: QueueListener = QueueListener(self._message_queue)
        self._notifier: Notifier = Notifier()
        signal.signal(signal.SIGTERM, self.shutdown)
        signal.signal(signal.SIGINT, self.shutdown)

    def restart_listener(self) -> None:
        """Stop the queue listener, wait 30 seconds, then restart the listener"""
        logger.info("Stopping the queue listener and waiting 30 seconds...")
        self._queue_listener.stop()
        time.sleep(30)
        logger.info("Starting queue listener")
        self._queue_listener.run()

    def shutdown_listener(self) -> None:
        """
        Stop the queue listener
        :return: None
        """
        logger.info("Stopping listener")
        self._queue_listener.stop()

    def shutdown(self, _: int, __: Optional[FrameType]) -> None:
        """
        Shutdown the queue listener, TODO shutdown the notifier. Automatically called when sigterm or sigint happens
        :param _: Thrown away
        :param __: Thrown away
        :return: None
        """
        logger.info("Shutting down run detection...")
        self.shutdown_listener()

    def run(self) -> None:
        """
        Starts the run detector
        """
        logger.info("Starting RunDetector")
        self._queue_listener.run()
        while True:
            try:
                self._process_message(self._message_queue.get(timeout=10))
                time.sleep(0.1)
            except Empty:
                if self._queue_listener.stopping:
                    logger.info("No messages processing, breaking main loop...")
                    break
                if not self._queue_listener.is_connected():
                    logger.warning("Queue listener failed silently")
                    self.restart_listener()

    @staticmethod
    def _map_path(path_str: str) -> Path:
        """
        The paths recieved from pre-icat queue are windows formatted and assume the archive is at \\isis. This maps
        them to the expected location of /archive
        :param path_str: The path string to map
        :return: The mapped path object
        """
        match = re.search(r"cycle_(\d{2})_(\d+)\\(NDX\w+)\\([a-zA-Z]+\d+\.nxs)", path_str)
        if match is None:
            raise ValueError(f"Path was not in expected format: {path_str}")
        year, cycle, ndx_name, filename = match.groups()

        # Creating the new path format
        converted_path = f"/archive/{ndx_name}/Instrument/data/cycle_{year}_{cycle}/{filename}"
        return Path(converted_path)

    def _process_message(self, message: Message) -> None:
        logger.info("Processing message: %s", message)
        try:
            data_path = self._map_path(message.value)
            job_request = ingest(data_path)
            specification = InstrumentSpecification(job_request.instrument)
            specification.verify(job_request)
            if job_request.will_reduce:
                logger.info("Specification met for job_request: %s", job_request)
                notification = Notification(job_request.to_json_string())
                self._notifier.notify(notification)
                for additional_request in job_request.additional_requests:
                    self._notifier.notify(Notification(additional_request.to_json_string()))

            else:
                logger.info("Specificaiton not met, skipping job_request: %s", job_request)
        # pylint: disable = broad-except
        except Exception:
            logger.exception("Problem processing message: %s", message.value)
        finally:
            message.processed = True
            self._queue_listener.acknowledge(message)


def main(archive_path: str = "/archive") -> None:
    """
    run-detection entrypoint.
    :arg archive_path: Added purely for testing purposes, but should also be potentially useful.
    :return: None
    """
    # Check that the archive can be accessed
    if Path(archive_path, "NDXALF").exists():
        logger.info("The archive has been mounted correctly, and can be accessed.")
    else:
        logger.error("The archive has not been mounted correctly, and cannot be accessed.")

    logger.info("Starting run detection")
    run_detector = RunDetector()
    run_detector.run()


if __name__ == "__main__":
    main()
