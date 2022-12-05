"""
Run detection module holds the RunDetector main class
"""
import time
from multiprocessing import SimpleQueue

from src.queue_listener import Message


class RunDetector:
    """
    Run Detector class orchestrates the the complete run detection process, from consuming messages from icat
    pre-queue, to notifying downstream
    """

    # pylint: disable=(unsubscriptable-object)
    def __init__(self, message_queue: SimpleQueue[Message],
                 ack_queue: SimpleQueue[Message]) -> None:
        self.message_queue = message_queue
        self.ack_queue = ack_queue

    def run(self) -> None:
        """
        Starts the run detector
        """
        while True:
            self._process_message(self.message_queue.get())
            time.sleep(0.1)

    def _process_message(self, message: Message) -> None:
        print(message)
        message.processed = False
        self.ack_queue.put(message)
