"""
Main Module
"""

from multiprocessing import SimpleQueue, Process

from src.queue_listener import QueueListener
from src.run_detection import RunDetector


def main():
    """
    run-detection entrypoint.
    :return: None
    """
    message_queue = SimpleQueue()
    ack_queue = SimpleQueue()
    run_detector = RunDetector(message_queue, ack_queue)
    queue_listener = QueueListener(message_queue, ack_queue)
    Process(target=queue_listener.run).start()
    run_detector.run()


if __name__ == "__main__":
    main()
