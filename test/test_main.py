"""
Tests for run detection module's main function
"""
import unittest
from unittest import mock

from rundetection.run_detection import main


@mock.patch("rundetection.run_detection.RunDetector")
def test_main_uses_activemq_env_vars(run_detector) -> None:
    """
    Testing that run_detector.run receives the correct variables from the environment
    """
    main()

    run_detector.return_value.run.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
