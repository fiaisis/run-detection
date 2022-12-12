"""
Run detection module holds the RunDetector main class
"""
import logging
import sys
import time
from queue import SimpleQueue

from rundetection.notifications import Notifier, Notification
from rundetection.queue_listener import Message, QueueListener

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
        self._queue_listener: QueueListener = QueueListener(self._message_queue)
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


def main() -> None:
    """
    run-detection entrypoint.
    :return: None
    """
    logger.info("Starting run detection")
    run_detector = RunDetector()
    run_detector.run()


if __name__ == "__main__":
    main()
