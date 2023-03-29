"""
Ingest and metadata tests
"""
# pylint: disable=redefined-outer-name
import logging
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

import pytest
from _pytest.logging import LogCaptureFixture

from rundetection.ingest import (
    ingest,
    DetectedRun,
    get_sibling_nexus_files,
    get_sibling_runs,
    skip_extract,
    get_extraction_function,
    get_run_title,
    mari_extract,
)

# Allows test to be run via pycharm play button or from project root
TEST_DATA_PATH = Path("test_data") if Path("test_data").exists() else Path("test", "test_data")


@pytest.fixture()
def detected_run():
    """Detected run fixture"""
    return DetectedRun(
        run_number=12345,
        instrument="instrument",
        experiment_title="experiment title",
        filepath=Path("./25581.nxs"),
        experiment_number="experiment number",
        raw_frames=0,
        good_frames=0,
        run_start="",
        run_end="",
        users="",
    )


@pytest.mark.parametrize(
    "pair",
    [
        (
            "e2e_data/1510111/ENGINX00241391.nxs",
            DetectedRun(
                run_number=241391,
                instrument="ENGINX",
                experiment_title="CeO2 4 x 4 x 15",
                experiment_number="1510111",
                filepath=Path(TEST_DATA_PATH, "e2e_data/1510111/ENGINX00241391.nxs"),
                run_start="2015-07-01T15:29:17",
                run_end="2015-07-01T15:53:16",
                raw_frames=23740,
                good_frames=18992,
                users="Liu,Andriotis,Smith,Hallam,Flewitt,Kabra",
            ),
        ),
        (
            "e2e_data/1600007/IMAT00004217.nxs",
            DetectedRun(
                run_number=4217,
                instrument="IMAT",
                experiment_title="Check DAE and end of run working after move",
                experiment_number="1600007",
                filepath=Path(TEST_DATA_PATH, "e2e_data/1600007/IMAT00004217.nxs"),
                run_start="2017-04-26T17:22:50",
                run_end="2017-04-26T17:22:57",
                raw_frames=1,
                good_frames=1,
                users="Salvato,Kockelmann,Aliotta,Minniti,Ponterio,Vasi,Ewings",
            ),
        ),
        (
            "e2e_data/1920302/ALF82301.nxs",
            DetectedRun(
                run_number=82301,
                instrument="ALF",
                experiment_title="YbCl3 rot=0",
                experiment_number="1920302",
                filepath=Path(TEST_DATA_PATH, "e2e_data/1920302/ALF82301.nxs"),
                run_start="2019-11-12T14:30:39",
                run_end="2019-11-12T14:34:20",
                raw_frames=2998,
                good_frames=2998,
                users="Zhao",
            ),
        ),
        (
            "e2e_data/25581/MAR25581.nxs",
            DetectedRun(
                run_number=25581,
                instrument="MARI",
                experiment_title="Whitebeam - vanadium - detector tests - vacuum bad - HT on not on all LAB",
                experiment_number="1820497",
                filepath=Path(TEST_DATA_PATH, "e2e_data/25581/MAR25581.nxs"),
                raw_frames=8067,
                good_frames=6452,
                run_start="2019-03-22T10:15:44",
                run_end="2019-03-22T10:18:26",
                users="Wood,Guidi,Benedek,Mansson,Juranyi,Nocerino,Forslund,Matsubara",
                additional_values={
                    "ei": "auto",
                    "monovan": 0,
                    "runno": 25581,
                    "sam_mass": 0.0,
                    "sam_rmm": 0.0,
                    "sum_runs": False,
                    "remove_bkg": True,
                },
            ),
        ),
    ],
    ids=["ENGINX00241391.nxs", "IMAT00004217.nxs", "ALF82301.nxs", "MARI25581.nxs"],
)
def test_ingest(pair) -> None:
    """
    Test the metadata is built from test nexus files
    :return: None
    """
    nexus_file = TEST_DATA_PATH / pair[0]
    assert (ingest(nexus_file)) == pair[1]


def test_ingest_raises_exception_non_nexus_file() -> None:
    """
    Test value error is raised when a non nexus file is given to be ingested
    :return: None
    """
    with pytest.raises(ValueError):
        ingest(Path("25581.log"))


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
        run_start="2015-07-01T15:29:17",
        run_end="2015-07-01T15:53:16",
        raw_frames=23740,
        good_frames=18992,
        users="Keiran",
    )
    assert (
        nexus_metadata.to_json_string() == '{"run_number": 12345, "instrument": "LARMOR", "experiment_title": '
        '"my experiment", "experiment_number": "54321", "filepath": '
        '"e2e_data/1920302/ALF82301.nxs", "run_start": "2015-07-01T15:29:17", '
        '"run_end": "2015-07-01T15:53:16", "raw_frames": 23740, "good_frames": 18992, '
        '"users": "Keiran", "additional_values": {}}'
    )


@patch("rundetection.ingest.ingest")
def test_get_sibling_runs(mock_ingest: Mock):
    """
    Tests that a list of detected runs are returned when ingesting sibling nexus files
    :param mock_ingest: Mock ingest
    :return: None
    """
    run = DetectedRun(1, "inst", "title", "num", Path("path"), "run_start", "run_end", 0, 0, "users")
    mock_ingest.return_value = run
    with TemporaryDirectory() as temp_dir:
        Path(temp_dir, "1.nxs").touch()
        Path(temp_dir, "2.nxs").touch()
        assert get_sibling_runs(Path(temp_dir, "1.nxs")) == [run]


def test_logging_and_exception_when_nexus_file_does_not_exit(caplog: LogCaptureFixture):
    """
    Test correct logging and exception reraised when nexus file is missing
    :param caplog: LogCaptureFixture
    :return: None
    """
    with pytest.raises(FileNotFoundError):
        ingest(Path("e2e_data/25581/bar.nxs"))

    assert "Nexus file could not be found: e2e_data/25581/bar.nxs" in caplog.text


@patch("rundetection.ingest.ingest")
def test_get_run_title(mock_ingest):
    """
    Test file ingested and title returned
    :param mock_ingest: Mock ingest function
    :return: None
    """
    mock_run = Mock()
    mock_run.experiment_title = "25581"
    mock_ingest.return_value = mock_run
    assert get_run_title(Path("/dir/file.nxs")) == "25581"
    mock_ingest.assert_called_once_with(Path("/dir/file.nxs"))


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


def test_skip_extract(caplog: LogCaptureFixture):
    """
    Test that run remains unchanged and log is made
    :param caplog: LogCaptureFixture
    Tests that a list of detected runs are returned when ingesting sibling nexus files
    :return: None
    """
    run = Mock()
    run.instrument = "instrument"
    run.run_number = 123
    with caplog.at_level(logging.INFO):
        run_ = skip_extract(run, object())
        assert "No additional extraction needed for run: instrument 123" in caplog.text
        assert run_ == run


def test_get_extraction_function():
    """
    Test correct function returned from factory function
    :return: None
    """
    skip_extract_func = get_extraction_function("LET")
    assert skip_extract_func.__name__ == "skip_extract"
    mari_extract_func = get_extraction_function("mari")
    assert mari_extract_func.__name__ == "mari_extract"


def test_mari_extract_single_ei(detected_run: DetectedRun):
    """
    Test mari extract with single ei value. we use a dict instead of a h5py group since they have identical API
    :param detected_run: Detected Run fixture
    :return: None
    """
    dataset = {"ei": [10.0], "sam_mass": [5.0], "sam_rmm": [100.0]}
    result = mari_extract(detected_run, dataset)

    assert result.additional_values["ei"] == 10.0
    assert result.additional_values["sam_mass"] == 5.0
    assert result.additional_values["sam_rmm"] == 100.0
    assert result.additional_values["monovan"] == 12345
    assert result.additional_values["remove_bkg"] is True


def test_mari_extract_multiple_ei(detected_run: DetectedRun):
    """
    Test mari extract with multiple ei values. we use a dict instead of a h5py group since they have identical API
    :param detected_run: Detected Run fixture
    :return: None
    """
    dataset = {"ei": [10.0, 20.0], "sam_mass": [5.0], "sam_rmm": [100.0]}
    result = mari_extract(detected_run, dataset)

    assert result.additional_values["ei"] == [10.0, 20.0]
    assert result.additional_values["sam_mass"] == 5.0
    assert result.additional_values["sam_rmm"] == 100.0
    assert result.additional_values["monovan"] == 12345
    assert result.additional_values["remove_bkg"] is True


def test_mari_extract_no_ei(detected_run: DetectedRun):
    """
    Test mari extract with no ei values. we use a dict instead of a h5py group since they have identical API
    :param detected_run: Detected Run fixture
    :return: None
    """
    dataset = {"sam_mass": [5.0], "sam_rmm": [100.0]}
    result = mari_extract(detected_run, dataset)

    assert result.additional_values["ei"] == "auto"
    assert result.additional_values["sam_mass"] == 5.0
    assert result.additional_values["sam_rmm"] == 100.0
    assert result.additional_values["monovan"] == 12345
    assert result.additional_values["remove_bkg"] is True


def test_mari_extract_no_sam_mass_or_sam_rmm(detected_run: DetectedRun):
    """
    Test mari extract with no sample values. we use a dict instead of a h5py group since they have identical API
    :param detected_run: Detected Run fixture
    :return: None
    """
    dataset = {"ei": [10.0]}
    result = mari_extract(detected_run, dataset)

    assert result.additional_values["ei"] == 10.0
    assert result.additional_values["sam_mass"] == 0.0
    assert result.additional_values["sam_rmm"] == 0.0
    assert result.additional_values["monovan"] == 0
    assert result.additional_values["remove_bkg"] is True


def test_mari_extract_remove_bkg_false(detected_run: DetectedRun):
    """
    Test mari extract with no background radiation correction we use a dict instead of a h5py group since they have
    identical API
    :param detected_run: Detected Run fixture
    :return: None
    """
    dataset = {"ei": [10.0], "sam_mass": [5.0], "sam_rmm": [100.0], "remove_bkg": [False]}
    result = mari_extract(detected_run, dataset)

    assert result.additional_values["ei"] == 10.0
    assert result.additional_values["sam_mass"] == 5.0
    assert result.additional_values["sam_rmm"] == 100.0
    assert result.additional_values["monovan"] == 12345
    assert result.additional_values["remove_bkg"] is False


def test_ingest_to_json_string_produces_no_decode_errors():
    """
    Test the full process from ingestion to json string. Specifically to check for decode errors
    :return: None
    """
    run = ingest(Path(TEST_DATA_PATH, "e2e_data/1510111/ENGINX00241391.nxs"))
    run.to_json_string()


if __name__ == "__main__":
    unittest.main()
