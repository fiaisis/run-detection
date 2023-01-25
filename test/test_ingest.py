"""
Ingest and metadata tests
"""
import unittest
from pathlib import Path
from typing import Tuple, List

import pytest
from _pytest.logging import LogCaptureFixture

from rundetection.ingest import NexusMetadata, ingest

TEST_FILE_METADATA_PAIRS: List[Tuple[str, NexusMetadata]] = [
    (
        "e2e_data/1510111/ENGINX00241391.nxs",
        NexusMetadata(
            run_number=241391, instrument="ENGINX", experiment_title="CeO2 4 x 4 x 15", experiment_number="1510111"
        ),
    ),
    (
        "e2e_data/1600007/IMAT00004217.nxs",
        NexusMetadata(
            run_number=4217,
            instrument="IMAT",
            experiment_title="Check DAE and end of run working after move",
            experiment_number="1600007",
        ),
    ),
    (
        "e2e_data/1920302/ALF82301.nxs",
        NexusMetadata(run_number=82301, instrument="ALF", experiment_title="YbCl3 rot=0", experiment_number="1920302"),
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
    nexus_metadata = NexusMetadata(
        run_number=12345, instrument="LARMOR", experiment_number="54321", experiment_title="my experiment"
    )
    assert (
            nexus_metadata.to_json_string() == '{"run_number": 12345, "instrument": "LARMOR", "experiment_title": '
                                               '"my experiment", "experiment_number": "54321"}'
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


if __name__ == "__main__":
    unittest.main()
