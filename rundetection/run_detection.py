"""
Run detection module holds the RunDetector main class
"""
import logging
import sys
import time
from pathlib import Path
from queue import SimpleQueue

from rundetection.notifications import Notifier, Notification
from rundetection.topic_listener import Message, TopicListener

file_handler = logging.FileHandler(filename="run-detection.log")
stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(handlers=[file_handler, stdout_handler],
                    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
                    level=logging.INFO)
logger = logging.getLogger(__name__)


class RunDetector:
    """
    Run Detector class orchestrates the complete run detection process, from consuming messages from icat
    pre-queue, to notifying downstream
    """

    def __init__(self) -> None:
        self._message_queue: SimpleQueue[Message] = SimpleQueue()
        self._queue_listener: TopicListener = TopicListener(self._message_queue)
        self._notifier: Notifier = Notifier()

    def run(self) -> None:
        """
        Starts the run detector
        """
        logging.info("Starting RunDetector")
        self._queue_listener.run()
        while True:
            self._process_message(self._message_queue.get())
            time.sleep(0.1)

    def _process_message(self, message: Message) -> None:
        logger.info("Processing message: %s", message)
        notification = Notification(message.value)
        self._notifier.notify(notification)
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
