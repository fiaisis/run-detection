"""
Main Module
"""

from src.run_detection import RunDetector


def main() -> None:
    """
    run-detection entrypoint.
    :return: None
    """
    run_detector = RunDetector()
    run_detector.run()


if __name__ == "__main__":
    main()
