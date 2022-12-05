"""
Run detection module holds the RunDetector main class
"""
import time
from queue import SimpleQueue

from src.queue_listener import Message, QueueListener


class RunDetector:
    """
    Run Detector class orchestrates the the complete run detection process, from consuming messages from icat
    pre-queue, to notifying downstream
    """

    def __init__(self) -> None:
        self._message_queue: SimpleQueue[Message] = SimpleQueue()
        self._ack_queue: SimpleQueue[Message] = SimpleQueue()
        self._queue_listener: QueueListener = QueueListener(self._message_queue, self._ack_queue)

    def run(self) -> None:
        """
        Starts the run detector
        """
        self._queue_listener.run()
        while True:
            self._process_message(self._message_queue.get())
            self._queue_listener.acknowledge(self._ack_queue.get())
            time.sleep(0.1)

    def _process_message(self, message: Message) -> None:
        print(message)
        message.processed = False
        self._ack_queue.put(message)
