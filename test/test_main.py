"""
Tests for run detection module's main function
"""
import os
import unittest
from unittest import mock

from rundetection.run_detection import main


class TestMain(unittest.TestCase):
    def setUp(self):
        self.amq_ip = "great_ip"
        self.amq_user = "great_username"
        self.amq_pass = "great_password"

        os.environ["ACTIVEMQ_IP"] = self.amq_ip
        os.environ["ACTIVEMQ_USER"] = self.amq_user
        os.environ["ACTIVEMQ_PASS"] = self.amq_pass

    @mock.patch("rundetection.run_detection.RunDetector")
    def test_main_uses_activemq_env_vars(self, run_detector) -> None:
        """
        Testing that run_detector.run receives the correct variables from the environment
        """
        main()

        run_detector.return_value.run.assert_called_once_with(self.amq_ip, self.amq_user, self.amq_pass)


if __name__ == '__main__':
    unittest.main()
