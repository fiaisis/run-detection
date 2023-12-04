"""Test cases for the extracts module."""
import logging
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from _pytest.logging import LogCaptureFixture

from rundetection.exceptions import IngestError, ReductionMetadataError
from rundetection.ingestion.extracts import (
    skip_extract,
    get_extraction_function,
    mari_extract,
    tosca_extract,
    get_cycle_string_from_path,
    osiris_extract,
)
from rundetection.job_requests import JobRequest


@pytest.fixture()
def job_request():
    """job_request fixture"""
    return JobRequest(
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


def test_skip_extract(caplog: LogCaptureFixture):
    """
    Test that run remains unchanged and log is made
    :param caplog: LogCaptureFixture
    Tests that a list of job requests are returned when ingesting sibling nexus files
    :return: None
    """
    job_request = Mock()
    job_request.instrument = "instrument"
    job_request.run_number = 123
    with caplog.at_level(logging.INFO):
        job_request_ = skip_extract(job_request, object())
        assert "No additional extraction needed for job_request: instrument 123" in caplog.text
        assert job_request_ == job_request


def test_get_extraction_function():
    """
    Test correct function returned from factory function
    :return: None
    """
    skip_extract_func = get_extraction_function("LET")
    assert skip_extract_func.__name__ == "skip_extract"
    mari_extract_func = get_extraction_function("mari")
    assert mari_extract_func.__name__ == "mari_extract"
    tosca_extract_func = get_extraction_function("tosca")
    assert tosca_extract_func.__name__ == "tosca_extract"


def test_mari_extract_single_ei(job_request):
    """
    Test mari extract with single ei value. we use a dict instead of a h5py group since they have identical API
    :param job_request: job request fixture
    :return: None
    """
    dataset = {"ei": [10.0], "sam_mass": [5.0], "sam_rmm": [100.0]}
    result = mari_extract(job_request, dataset)

    assert result.additional_values["ei"] == 10.0
    assert result.additional_values["sam_mass"] == 5.0
    assert result.additional_values["sam_rmm"] == 100.0
    assert result.additional_values["monovan"] == 12345
    assert result.additional_values["remove_bkg"] is False


def test_mari_extract_multiple_ei(job_request):
    """
    Test mari extract with multiple ei values. we use a dict instead of a h5py group since they have identical API
    :param job_request: job request fixture
    :return: None
    """
    dataset = {"ei": [10.0, 20.0], "sam_mass": [5.0], "sam_rmm": [100.0]}
    result = mari_extract(job_request, dataset)

    assert result.additional_values["ei"] == [10.0, 20.0]
    assert result.additional_values["sam_mass"] == 5.0
    assert result.additional_values["sam_rmm"] == 100.0
    assert result.additional_values["monovan"] == 12345
    assert result.additional_values["remove_bkg"] is False


def test_mari_extract_no_ei(job_request):
    """
    Test mari extract with no ei values. we use a dict instead of a h5py group since they have identical API
    :param job_request: job request fixture
    :return: None
    """
    dataset = {"sam_mass": [5.0], "sam_rmm": [100.0]}
    result = mari_extract(job_request, dataset)

    assert result.additional_values["ei"] == "'auto'"
    assert result.additional_values["sam_mass"] == 5.0
    assert result.additional_values["sam_rmm"] == 100.0
    assert result.additional_values["monovan"] == 12345
    assert result.additional_values["remove_bkg"] is False


def test_mari_extract_no_sam_mass_or_sam_rmm(job_request):
    """
    Test mari extract with no sample values. we use a dict instead of a h5py group since they have identical API
    :param job_request: job request fixture
    :return: None
    """
    dataset = {"ei": [10.0]}
    result = mari_extract(job_request, dataset)

    assert result.additional_values["ei"] == 10.0
    assert result.additional_values["sam_mass"] == 0.0
    assert result.additional_values["sam_rmm"] == 0.0
    assert result.additional_values["monovan"] == 0
    assert result.additional_values["remove_bkg"] is False


def test_mari_extract_remove_bkg_true(job_request):
    """
    Test mari extract with no background radiation correction we use a dict instead of a h5py group since they have
    identical API
    :param job_request: job request fixture
    :return: None
    """
    dataset = {"ei": [10.0], "sam_mass": [5.0], "sam_rmm": [100.0], "remove_bkg": [True]}
    result = mari_extract(job_request, dataset)

    assert result.additional_values["ei"] == 10.0
    assert result.additional_values["sam_mass"] == 5.0
    assert result.additional_values["sam_rmm"] == 100.0
    assert result.additional_values["monovan"] == 12345
    assert result.additional_values["remove_bkg"] is True


@patch("rundetection.ingestion.extracts.get_cycle_string_from_path", return_value="some string")
def test_tosca_extract(_: Mock, job_request):
    """Test Tosca Extract adds_cycle_string"""
    tosca_extract(job_request, None)
    assert job_request.additional_values["cycle_string"] == "some string"


def test_osiris_extract(job_request):
    """Test Osiris extract"""
    dataset = {
        "selog": {"freq6": {"value_log": {"value": (6,)}}, "freq10": {"value_log": {"value": (6,)}}},
        "instrument": {
            "dae": {
                "time_channels_1": {"time_of_flight": (10.0, 100.0)},
                "time_channels_2": {"time_of_flight": (12.1, 121.0)},
            }
        },
    }
    osiris_extract(job_request, dataset)
    assert job_request.additional_values["freq10"] == 6
    assert job_request.additional_values["freq6"] == 6
    assert job_request.additional_values["tcb_detector_min"] == 10.0
    assert job_request.additional_values["tcb_detector_max"] == 100.0
    assert job_request.additional_values["tcb_monitor_min"] == 12.1
    assert job_request.additional_values["tcb_monitor_max"] == 121.0


def test_osiris_extract_raises_on_bad_frequencies(job_request):
    """Test correct exception raised when freq6 and freq10 do not match"""
    dataset = {
        "selog": {"freq6": {"value_log": {"value": (6,)}}, "freq10": {"value_log": {"value": (11,)}}},
        "instrument": {
            "dae": {
                "time_channels_1": {"time_of_flight": (10.0, 100.0)},
                "time_channels_2": {"time_of_flight": (12.1, 121.0)},
            }
        },
    }
    with pytest.raises(ReductionMetadataError):
        osiris_extract(job_request, dataset)


def test_get_cycle_string_from_path_valid():
    """
    Test get cycle string returns correct string
    :return: None
    """
    path = Path("/some/path/to/cycle_2023_42/and/some/file")
    result = get_cycle_string_from_path(path)
    assert result == "cycle_2023_42"


def test_get_cycle_string_from_path_valid_alternative():
    """
    Test get cycle string returns correct string for short year
    :return: None
    """
    path = Path("/another/path/cycle_19_2/file")
    result = get_cycle_string_from_path(path)
    assert result == "cycle_19_2"


def test_get_cycle_string_from_path_invalid():
    """
    Test get cycle string raises when year missing
    :return: None
    """
    path = Path("/no/cycle/string/here")
    with pytest.raises(IngestError):
        get_cycle_string_from_path(path)
