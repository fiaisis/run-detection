"""
Tests for run detection module's main function
"""
import os
import unittest
from unittest import mock

from rundetection.run_detection import main


@mock.patch("rundetection.run_detection.RunDetector")
def test_main_uses_activemq_env_vars(run_detector) -> None:
    """
    Testing that run_detector.run receives the correct variables from the environment
    """
    amq_ip = "great_ip"
    amq_user = "great_username"
    amq_pass = "great_password"
    os.environ["ACTIVEMQ_IP"] = amq_ip
    os.environ["ACTIVEMQ_USER"] = amq_user
    os.environ["ACTIVEMQ_PASS"] = amq_pass

    main()

    run_detector.return_value.run.assert_called_once_with(amq_ip, amq_user, amq_pass)


@mock.patch("rundetection.run_detection.RunDetector")
def test_main_uses_useful_defaults_when_activemq_env_vars_not_set(run_detector) -> None:
    """
    Testing that run_detector.run uses useful defaults when env variables are not set
    """
    del os.environ["ACTIVEMQ_IP"]
    del os.environ["ACTIVEMQ_USER"]
    del os.environ["ACTIVEMQ_PASS"]

    main()

    run_detector.return_value.run.assert_called_once_with("localhost", "admin", "admin")


if __name__ == '__main__':
    unittest.main()
