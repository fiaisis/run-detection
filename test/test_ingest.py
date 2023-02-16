"""
Ingest and metadata tests
"""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Tuple, List
from unittest.mock import Mock, patch

import pytest
from _pytest.logging import LogCaptureFixture

from rundetection.ingest import ingest, DetectedRun, get_sibling_nexus_files, get_sibling_runs

TEST_FILE_METADATA_PAIRS: List[Tuple[str, DetectedRun]] = [
    (
        "e2e_data/1510111/ENGINX00241391.nxs",
        DetectedRun(
            run_number=241391,
            instrument="ENGINX",
            experiment_title="CeO2 4 x 4 x 15",
            experiment_number="1510111",
            filepath=Path("test/test_data/e2e_data/1510111/ENGINX00241391.nxs"),
        ),
    ),
    (
        "e2e_data/1600007/IMAT00004217.nxs",
        DetectedRun(
            run_number=4217,
            instrument="IMAT",
            experiment_title="Check DAE and end of run working after move",
            experiment_number="1600007",
            filepath=Path("test/test_data/e2e_data/1600007/IMAT00004217.nxs"),
        ),
    ),
    (
        "e2e_data/1920302/ALF82301.nxs",
        DetectedRun(
            run_number=82301,
            instrument="ALF",
            experiment_title="YbCl3 rot=0",
            experiment_number="1920302",
            filepath=Path("test/test_data/e2e_data/1920302/ALF82301.nxs"),
        ),
    ),
]

# Allows test to be run via pycharm play button or from project root
TEST_DATA_PATH = Path("test_data") if Path("test_data").exists() else Path("test", "test_data")


def test_ingest() -> None:
    """
    Test the metadata is built from test nexus files
    :return: None
    """
    for pair in TEST_FILE_METADATA_PAIRS:
        nexus_file = TEST_DATA_PATH / pair[0]
        assert (ingest(nexus_file)) == pair[1]


def test_to_json_string() -> None:
    """
    Test valid json string can be built from metadata
    :return: None
    """
    nexus_metadata = DetectedRun(
        run_number=12345,
        instrument="LARMOR",
        experiment_number="54321",
        experiment_title="my experiment",
        filepath=Path("e2e_data/1920302/ALF82301.nxs"),
    )
    assert (
        nexus_metadata.to_json_string() == '{"run_number": 12345, "instrument": "LARMOR", "experiment_title": '
        '"my experiment", "experiment_number": "54321", "filepath": '
        '"e2e_data/1920302/ALF82301.nxs", "will_reduce": true, '
        '"additional_values": {}}'
    )


def test_logging_and_exception_when_nexus_file_does_not_exit(caplog: LogCaptureFixture):
    """
    Test correct logging and exception reraised when nexus file is missing
    :param caplog: LogCaptureFixture
    :return: None
    """
    with pytest.raises(FileNotFoundError):
        ingest(Path("e2e_data/foo/bar.nxs"))

    assert "Nexus file could not be found: e2e_data/foo/bar.nxs" in caplog.text


def test_get_sibling_nexus_files():
    """
    Test that nexus files from within the same directory are returned
    :return: None
    """
    with TemporaryDirectory() as temp_dir:
        Path(temp_dir, "1.nxs").touch()
        Path(temp_dir, "2.nxs").touch()
        Path(temp_dir, "1.log").touch()
        sibling_files = get_sibling_nexus_files(Path(temp_dir, "1.nxs"))
        assert sibling_files == [Path(temp_dir, "2.nxs")]


@patch("rundetection.ingest.ingest")
def test_get_sibling_runs(mock_ingest: Mock):
    """
    Tests that a list of detected runs are returned when ingesting sibling nexus files
    :param mock_ingest: Mock ingest
    :return: None
    """
    run = DetectedRun(1, "inst", "title", "num", Path("path"))
    mock_ingest.return_value = run
    with TemporaryDirectory() as temp_dir:
        Path(temp_dir, "1.nxs").touch()
        Path(temp_dir, "2.nxs").touch()
        assert get_sibling_runs(Path(temp_dir, "1.nxs")) == [run]


if __name__ == "__main__":
    unittest.main()
