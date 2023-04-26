"""
Run detection module holds the RunDetector main class
"""
import logging
import re
import sys
import time
from pathlib import Path
from queue import SimpleQueue

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

    def run(self) -> None:
        """
        Starts the run detector
        """
        logging.info("Starting RunDetector")
        self._queue_listener.run()
        while True:
            self._process_message(self._message_queue.get())
            time.sleep(0.1)

    @staticmethod
    def _map_path(path_str: str) -> Path:
        """
        The paths recieved from pre-icat queue are windows formatted and assume the archive is at \\isis. This maps
        them to the expected location of /archive
        :param path_str: The path string to map
        :return: The mapped path object
        """
        match = re.search(r"cycle_(\d{2})_(\d+)\\(NDX\w+)\\([a-zA-Z]+)(\d+)\.nxs", path_str)
        if match is None:
            raise ValueError(f"Path was not in expected format: {path_str}")
        year, cycle, ndx_name, instrument, run_number = match.groups()

        # Creating the new path format
        converted_path = f"/archive/{ndx_name}/Instrument/data/cycle_{year}_{cycle}/{instrument}{run_number}.nxs"
        return Path(converted_path)

    def _process_message(self, message: Message) -> None:
        logger.info("Processing message: %s", message)
        try:
            data_path = self._map_path(message.value)
            run = ingest(data_path)
            specification = InstrumentSpecification(run.instrument)
            specification.verify(run)
            if run.will_reduce:
                logger.info("Specification met for run: %s", run)
                notification = Notification(run.to_json_string())
                self._notifier.notify(notification)
                for additional_run in run.additional_runs:
                    self._notifier.notify(Notification(additional_run.to_json_string()))

            else:
                logger.info("Specificaiton not met, skipping run: %s", run)
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
