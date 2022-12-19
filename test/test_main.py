"""
Tests for run detection module's main function
"""
import random
import string
import tempfile
import unittest
import os
from unittest import mock
from unittest.mock import Mock

from rundetection.run_detection import main


class MainTest(unittest.TestCase):
    @mock.patch("rundetection.run_detection.RunDetector")
    def test_main_finds_archive_if_present(self, _: Mock) -> None:
        """
        Testing that it checks for the archive being present by checking for /archive/ndxalf
        """
        expected_path = "/archive/ndxalf"
        with self.assertLogs('rundetection.run_detection', level='INFO') as cm:
            if not os.path.exists(expected_path):
                # If archive does not exist
                os.makedirs(expected_path)
                main()
                os.removedirs(expected_path)
            else:
                # If archive exists and is mounted on the system
                main()
        self.assertEqual(cm.output, ['INFO:rundetection.run_detection:The archive has been mounted correctly, and can be accessed.',
                                     'INFO:rundetection.run_detection:Starting run detection'])

    @mock.patch("rundetection.run_detection.RunDetector")
    def test_main_outputs_error_if_archive_not_present(self, _: Mock) -> None:
        """
        Testing that it checks for the archive being present by checking for /archive/ndxalf
        """
        with self.assertLogs('rundetection.run_detection', level='ERROR') as info_logs:
            if os.path.exists("/archive/ndxalf"):
                # If archive does exist
                letters = string.ascii_letters
                result_str = ''.join(random.choice(letters) for _ in range(20))
                main(f"/tmp/{result_str}")
            else:
                # If archive does not exist
                main()
        self.assertEqual(info_logs.output, ['ERROR:rundetection.run_detection:The archive has not been mounted correctly, and cannot '
                                            'be accessed.'])

    @mock.patch("rundetection.run_detection.RunDetector")
    def test_main_uses_activemq_env_vars(self, run_detector: Mock) -> None:
        """
        Testing that run_detector.run receives the correct variables from the environment
        """
        main()

        run_detector.return_value.run.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
