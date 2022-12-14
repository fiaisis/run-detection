"""
Tests for run detection module's main function
"""
import unittest
from unittest import mock
from unittest.mock import Mock

from rundetection.run_detection import main


@mock.patch("rundetection.run_detection.RunDetector")
def test_main_uses_activemq_env_vars(run_detector: Mock) -> None:
    """
    Testing that run_detector.run receives the correct variables from the environment
    """
    main()

    run_detector.return_value.run.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
