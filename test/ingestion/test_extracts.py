"""Test cases for the extracts module."""

import logging
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from _pytest.logging import LogCaptureFixture

from rundetection.exceptions import IngestError, ReductionMetadataError
from rundetection.ingestion.extracts import (
    get_cycle_string_from_path,
    get_extraction_function,
    loq_extract,
    mari_extract,
    osiris_extract,
    sans2d_extract,
    sans_extract,
    skip_extract,
    tosca_extract,
    vesuvio_extract,
)
from rundetection.job_requests import JobRequest


@pytest.fixture
def job_request():
    """job_request fixture."""
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
    :return: None.
    """
    job_request = Mock()
    job_request.instrument = "instrument"
    job_request.run_number = 123
    with caplog.at_level(logging.INFO):
        job_request_ = skip_extract(job_request, object())
        assert "No additional extraction needed for job_request: instrument 123" in caplog.text
        assert job_request_ == job_request


@pytest.mark.parametrize(
    ("input_value", "expected_function_name"),
    [
        ("foo", "skip_extract"),
        ("mari", "mari_extract"),
        ("tosca", "tosca_extract"),
        ("osiris", "osiris_extract"),
        ("loq", "loq_extract"),
        ("sans2d", "sans2d_extract"),
        ("iris", "iris_extract"),
        ("vesuvio", "vesuvio_extract"),
    ],
)
def test_get_extraction_function(input_value, expected_function_name):
    """
    Test that the correct function is returned from the factory function
    :param input_value: The input value to the factory function
    :param expected_function_name: The expected name of the function returned
    :return: None.
    """
    extracted_func = get_extraction_function(input_value)
    assert extracted_func.__name__ == expected_function_name


def test_mari_extract_single_ei(job_request):
    """
    Test mari extract with single ei value. we use a dict instead of a h5py group since they have identical API
    :param job_request: job request fixture
    :return: None.
    """
    dataset = {"ei": [10.0], "sam_mass": [5.0], "sam_rmm": [100.0]}
    result = mari_extract(job_request, dataset)

    assert result.additional_values["ei"] == 10.0  # noqa: PLR2004
    assert result.additional_values["sam_mass"] == 5.0  # noqa: PLR2004
    assert result.additional_values["sam_rmm"] == 100.0  # noqa: PLR2004
    assert result.additional_values["monovan"] == 12345  # noqa: PLR2004
    assert result.additional_values["remove_bkg"] is False


def test_mari_extract_multiple_ei(job_request):
    """
    Test mari extract with multiple ei values. we use a dict instead of a h5py group since they have identical API
    :param job_request: job request fixture
    :return: None.
    """
    dataset = {"ei": [10.0, 20.0], "sam_mass": [5.0], "sam_rmm": [100.0]}
    result = mari_extract(job_request, dataset)

    assert result.additional_values["ei"] == [10.0, 20.0]
    assert result.additional_values["sam_mass"] == 5.0  # noqa: PLR2004
    assert result.additional_values["sam_rmm"] == 100.0  # noqa: PLR2004
    assert result.additional_values["monovan"] == 12345  # noqa: PLR2004
    assert result.additional_values["remove_bkg"] is False


def test_mari_extract_no_ei(job_request):
    """
    Test mari extract with no ei values. we use a dict instead of a h5py group since they have identical API
    :param job_request: job request fixture
    :return: None.
    """
    dataset = {"sam_mass": [5.0], "sam_rmm": [100.0]}
    result = mari_extract(job_request, dataset)

    assert result.additional_values["ei"] == "'auto'"
    assert result.additional_values["sam_mass"] == 5.0  # noqa: PLR2004
    assert result.additional_values["sam_rmm"] == 100.0  # noqa: PLR2004
    assert result.additional_values["monovan"] == 12345  # noqa: PLR2004
    assert result.additional_values["remove_bkg"] is False


def test_mari_extract_no_sam_mass_or_sam_rmm(job_request):
    """
    Test mari extract with no sample values. we use a dict instead of a h5py group since they have identical API
    :param job_request: job request fixture
    :return: None.
    """
    dataset = {"ei": [10.0]}
    result = mari_extract(job_request, dataset)

    assert result.additional_values["ei"] == 10.0  # noqa: PLR2004
    assert result.additional_values["sam_mass"] == 0.0
    assert result.additional_values["sam_rmm"] == 0.0
    assert result.additional_values["monovan"] == 0
    assert result.additional_values["remove_bkg"] is False


def test_mari_extract_remove_bkg_true(job_request):
    """
    Test mari extract with no background radiation correction we use a dict instead of a h5py group since they have
    identical API
    :param job_request: job request fixture
    :return: None.
    """
    dataset = {"ei": [10.0], "sam_mass": [5.0], "sam_rmm": [100.0], "remove_bkg": [True]}
    result = mari_extract(job_request, dataset)

    assert result.additional_values["ei"] == 10.0  # noqa: PLR2004
    assert result.additional_values["sam_mass"] == 5.0  # noqa: PLR2004
    assert result.additional_values["sam_rmm"] == 100.0  # noqa: PLR2004
    assert result.additional_values["monovan"] == 12345  # noqa: PLR2004
    assert result.additional_values["remove_bkg"] is True


def test_tosca_extract(job_request):
    """Test Tosca Extract adds_cycle_string."""
    with patch("rundetection.ingestion.extracts.get_cycle_string_from_path", return_value="some string"):
        tosca_extract(job_request, None)
        assert job_request.additional_values["cycle_string"] == "some string"


def test_osiris_extract(job_request):
    """Test Osiris extract."""
    dataset = {
        "selog": {
            "phase6": {"value": (1221.0,)},
            "phase10": {"value": (1221.0,)},
            "freq6": {"value_log": {"value": (6,)}},
            "freq10": {"value_log": {"value": (6,)}},
        },
        "instrument": {
            "dae": {
                "time_channels_1": {"time_of_flight": (10.0, 100.0)},
                "time_channels_2": {"time_of_flight": (12.1, 121.0)},
            }
        },
    }
    with patch("rundetection.ingestion.extracts.get_cycle_string_from_path", return_value="some string"):
        osiris_extract(job_request, dataset)
    assert job_request.additional_values["freq10"] == 6  # noqa: PLR2004
    assert job_request.additional_values["freq6"] == 6  # noqa: PLR2004
    assert job_request.additional_values["tcb_detector_min"] == 10.0  # noqa: PLR2004
    assert job_request.additional_values["tcb_detector_max"] == 100.0  # noqa: PLR2004
    assert job_request.additional_values["tcb_monitor_min"] == 12.1  # noqa: PLR2004
    assert job_request.additional_values["tcb_monitor_max"] == 121.0  # noqa: PLR2004
    assert job_request.additional_values["phase6"] == 1221.0  # noqa: PLR2004
    assert job_request.additional_values["phase10"] == 1221.0  # noqa: PLR2004
    assert job_request.additional_values["cycle_string"] == "some string"


def test_osiris_extract_non_matching_freqs_within_error_boundary(job_request):
    """Test Osiris extract."""
    dataset = {
        "selog": {
            "phase6": {"value": (1221.0,)},
            "phase10": {"value": (1221.0,)},
            "freq6": {"value_log": {"value": (6.0,)}},
            "freq10": {"value_log": {"value": (5.999,)}},
        },
        "instrument": {
            "dae": {
                "time_channels_1": {"time_of_flight": (10.0, 100.0)},
                "time_channels_2": {"time_of_flight": (12.1, 121.0)},
            }
        },
    }
    with patch("rundetection.ingestion.extracts.get_cycle_string_from_path", return_value="some string"):
        osiris_extract(job_request, dataset)
    assert job_request.additional_values["freq10"] == 6  # noqa: PLR2004
    assert job_request.additional_values["freq6"] == 6  # noqa: PLR2004
    assert job_request.additional_values["tcb_detector_min"] == 10.0  # noqa: PLR2004
    assert job_request.additional_values["tcb_detector_max"] == 100.0  # noqa: PLR2004
    assert job_request.additional_values["tcb_monitor_min"] == 12.1  # noqa: PLR2004
    assert job_request.additional_values["tcb_monitor_max"] == 121.0  # noqa: PLR2004
    assert job_request.additional_values["phase6"] == 1221.0  # noqa: PLR2004
    assert job_request.additional_values["phase10"] == 1221.0  # noqa: PLR2004
    assert job_request.additional_values["cycle_string"] == "some string"


def test_osiris_extract_raises_on_bad_frequencies(job_request):
    """Test correct exception raised when freq6 and freq10 do not match."""
    dataset = {
        "selog": {"freq6": {"value_log": {"value": (6,)}}, "freq10": {"value_log": {"value": (11,)}}},
        "instrument": {
            "dae": {
                "time_channels_1": {"time_of_flight": (10.0, 100.0)},
                "time_channels_2": {"time_of_flight": (12.1, 121.0)},
            }
        },
    }
    with (
        pytest.raises(ReductionMetadataError),
        patch("rundetection.ingestion.extracts.get_cycle_string_from_path", return_value="some string"),
    ):
        osiris_extract(job_request, dataset)


def test_sans_extract(job_request):
    """
    Test SANS extract function correctly processes sample data.

    :param job_request: job request fixture
    :return: None.
    """
    dataset = {
        "sample": {
            "thickness": [1.0],
            "shape": ["b'Disc'"],
            "height": [8.0],
            "width": [8.0],
        }
    }
    with patch("rundetection.ingestion.extracts.get_cycle_string_from_path", return_value="some string"):
        sans_extract(job_request, dataset)

    assert job_request.additional_values["cycle_string"] == "some string"
    assert job_request.additional_values["sample_thickness"] == 1.0
    assert job_request.additional_values["sample_geometry"] == "Disc"
    assert job_request.additional_values["sample_height"] == 8.0  # noqa: PLR2004
    assert job_request.additional_values["sample_width"] == 8.0  # noqa: PLR2004


def test_sans2d_instrumentation(job_request):
    """
    Test SANS2D extract function correctly processes instrumentation data.

    :param job_request: job request fixture
    :return: None.
    """
    dataset = {
        "sample": {
            "thickness": [1.0],
            "shape": ["b'Disc'"],
            "height": [8.0],
            "width": [8.0],
        },
        "selog": {
            "Rear_Det_Z": {"value": ["1"]},
            "Front_Det_Z": {"value": ["2"]},
            "G1": {"value": ["3"]},
            "G2": {"value": ["4"]},
            "G3": {"value": ["5"]},
            "G4": {"value": ["6"]},
            "G5": {"value": ["7"]},
            "S1": {"value": ["8"]},
            "S2": {"value": ["9"]},
            "S3": {"value": ["10"]},
            "S4": {"value": ["11"]},
            "S5": {"value": ["12"]},
            "S6": {"value": ["13"]},
            "Jaw_E": {"value": ["14"]},
            "Jaw_N": {"value": ["15"]},
            "Jaw_S": {"value": ["16"]},
            "Jaw_W": {"value": ["17"]},
        },
    }
    expected_instrumentation_dict = {
        "selog": {
            "Rear_Det_Z": "1",
            "Front_Det_Z": "2",
            "G1": "3",
            "G2": "4",
            "G3": "5",
            "G4": "6",
            "G5": "7",
            "S1": "8",
            "S2": "9",
            "S3": "10",
            "S4": "11",
            "S5": "12",
            "S6": "13",
            "Jaw_E": "14",
            "Jaw_N": "15",
            "Jaw_S": "16",
            "Jaw_W": "17",
        }
    }
    with patch("rundetection.ingestion.extracts.get_cycle_string_from_path", return_value="some string"):
        sans2d_extract(job_request, dataset)

    assert job_request.additional_values["instrument_direct_file_comparison"] == expected_instrumentation_dict
    assert job_request.additional_values["cycle_string"] == "some string"
    assert job_request.additional_values["sample_thickness"] == 1.0
    assert job_request.additional_values["sample_geometry"] == "Disc"
    assert job_request.additional_values["sample_height"] == 8.0  # noqa: PLR2004
    assert job_request.additional_values["sample_width"] == 8.0  # noqa: PLR2004


def test_loq_instrumentation(job_request):
    """
    Test LOQ extract function correctly processes instrumentation data with aperture.

    :param job_request: job request fixture
    :return: None.
    """
    dataset = {
        "sample": {
            "thickness": [1.0],
            "shape": ["b'Disc'"],
            "height": [8.0],
            "width": [8.0],
        },
        "selog": {
            "Aperture_2": {"value": ["MEDIUM"]},
        },
    }
    expected_instrumentation_dict = {"selog": {"Aperture_2": "MEDIUM"}}
    with patch("rundetection.ingestion.extracts.get_cycle_string_from_path", return_value="some string"):
        loq_extract(job_request, dataset)

    assert job_request.additional_values["instrument_direct_file_comparison"] == expected_instrumentation_dict
    assert job_request.additional_values["cycle_string"] == "some string"
    assert job_request.additional_values["sample_thickness"] == 1.0
    assert job_request.additional_values["sample_geometry"] == "Disc"
    assert job_request.additional_values["sample_height"] == 8.0  # noqa: PLR2004
    assert job_request.additional_values["sample_width"] == 8.0  # noqa: PLR2004


def test_loq_instrumentation_no_aperturs(job_request):
    """
    Test LOQ extract function correctly processes instrumentation data without aperture.

    :param job_request: job request fixture
    :return: None.
    """
    dataset = {
        "sample": {
            "thickness": [1.0],
            "shape": ["b'Disc'"],
            "height": [8.0],
            "width": [8.0],
        },
        "selog": {},
    }
    with patch("rundetection.ingestion.extracts.get_cycle_string_from_path", return_value="some string"):
        loq_extract(job_request, dataset)

    assert job_request.additional_values["instrument_direct_file_comparison"] == {}


def test_get_cycle_string_from_path_valid():
    """
    Test get cycle string returns correct string
    :return: None.
    """
    path = Path("/some/path/to/cycle_2023_42/and/some/file")
    result = get_cycle_string_from_path(path)
    assert result == "cycle_2023_42"


def test_get_cycle_string_from_path_valid_alternative():
    """
    Test get cycle string returns correct string for short year
    :return: None.
    """
    path = Path("/another/path/cycle_19_2/file")
    result = get_cycle_string_from_path(path)
    assert result == "cycle_19_2"


def test_get_cycle_string_from_path_invalid():
    """
    Test get cycle string raises when year missing
    :return: None.
    """
    path = Path("/no/cycle/string/here")
    with pytest.raises(IngestError):
        get_cycle_string_from_path(path)


def test_vesuvio_extract_adds_runno(job_request):
    """Tests that the extract adds runno to Vesuvio jobs."""
    result = vesuvio_extract(job_request, None)

    assert result.additional_values["runno"] == 12345  # noqa: PLR2004
