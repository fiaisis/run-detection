"""
Tests for run detection module's main function
"""
import random
import string
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock
from unittest.mock import Mock

from rundetection.run_detection import main


class MainTest(unittest.TestCase):
    """
    The main test class
    """
    @staticmethod
    def generate_string():
        """
        Generate a fake directory name that is 20 characters long from upper and lower case characters
        """
        return ''.join(random.choice(string.ascii_letters) for _ in range(20))

    @mock.patch("rundetection.run_detection.RunDetector")
    def test_main_finds_archive_if_present(self, _: Mock) -> None:
        """
        Testing that it checks for the archive being present by checking for /archive/ndxalf
        """
        with self.assertLogs('rundetection.run_detection', level='INFO') as info_logs:
            if not Path("/archive/ndxalf").exists():
                # If archive does not exist
                with TemporaryDirectory() as temp_dir:
                    Path(temp_dir, "NDXALF").mkdir()
                    main(temp_dir)

            else:
                # If archive exists and is mounted on the system
                main()
        self.assertEqual(info_logs.output,
                         ['INFO:rundetection.run_detection:The archive has been mounted correctly, and can be '
                          'accessed.',
                          'INFO:rundetection.run_detection:Starting run detection'])

    @mock.patch("rundetection.run_detection.RunDetector")
    def test_main_outputs_error_if_archive_not_present(self, _: Mock) -> None:
        """
        Testing that it checks for the archive not being present by checking for /archive/ndxalf
        """
        result_str = self.generate_string()
        expected_path = f"/tmp/{result_str}"
        with self.assertLogs('rundetection.run_detection', level='ERROR') as error_logs:
            if Path("/archive/ndxalf").exists():
                # If archive does exist, use a fake directory
                main(expected_path)
            else:
                # If archive does not exist
                main()
        self.assertEqual(error_logs.output,
                         ['ERROR:rundetection.run_detection:The archive has not been mounted correctly, and cannot '
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
