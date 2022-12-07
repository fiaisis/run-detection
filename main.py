"""
Main Module
"""
import logging

from src.run_detection import RunDetector

logging.basicConfig(filename="run-detection.log", format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
                    level=logging.INFO)
logger = logging.getLogger(__name__)


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
