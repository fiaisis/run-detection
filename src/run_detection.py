"""
Run detection module holds the RunDetector main class
"""
import logging
import time
from queue import SimpleQueue

from src.notifications import Notifier, Notification
from src.queue_listener import Message, QueueListener

logger = logging.getLogger(__name__)


class RunDetector:
    """
    Run Detector class orchestrates the the complete run detection process, from consuming messages from icat
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
        notification = Notification(message.value)
        self._notifier.notify(notification)
        message.processed = True
        self._queue_listener.acknowledge(message)
