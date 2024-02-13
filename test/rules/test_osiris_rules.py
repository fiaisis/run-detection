"""
Tests for osiris rules
"""
from pathlib import Path

import pytest

from rundetection.ingestion.ingest import JobRequest
from rundetection.rules.osiris_rules import (
    OsirisPanadiumRule,
    OsirisReductionModeRule,
    OsirisStitchRule,
    is_x_within_5_percent_of_y,
)


# pylint: disable = redefined-outer-name
@pytest.fixture
def job_request():
    """
    job request fixture
    :return: job request
    """
    return JobRequest(
        run_number=100,
        filepath=Path("/archive/100/OSIRIS100.nxs"),
        experiment_title="Test experiment",
        additional_values={},
        additional_requests=[],
        raw_frames=0,
        good_frames=0,
        users="",
        run_start="",
        run_end="",
        instrument="osiris",
        experiment_number="",
    )


def test_is_x_within_5percent_of_y():
    """
    Test is_x_within_5_percent_of_y for multiple cases
    """
    assert is_x_within_5_percent_of_y(95, 100) is True
    assert is_x_within_5_percent_of_y(105, 100) is True
    assert is_x_within_5_percent_of_y(100, 100) is True
    assert is_x_within_5_percent_of_y(47.5, 50) is True
    assert is_x_within_5_percent_of_y(52.5, 50) is True

    assert is_x_within_5_percent_of_y(94.9, 100) is False
    assert is_x_within_5_percent_of_y(105.1, 100) is False
    assert is_x_within_5_percent_of_y(47.49, 50) is False
    assert is_x_within_5_percent_of_y(52.51, 50) is False

    assert is_x_within_5_percent_of_y(-95, -100) is True
    assert is_x_within_5_percent_of_y(-105, -100) is True
    assert is_x_within_5_percent_of_y(-94.9, -100) is False
    assert is_x_within_5_percent_of_y(-105.1, -100) is False

    assert is_x_within_5_percent_of_y(95, -100) is False
    assert is_x_within_5_percent_of_y(-105, 100) is False

    assert is_x_within_5_percent_of_y(0, 0) is True
    assert is_x_within_5_percent_of_y(0, 1) is False
    assert is_x_within_5_percent_of_y(1, 0) is False


def test_osiris_panadium_rule(job_request):
    """
    Test that the panadium number is set via the specification
    :param job_request: job request fixture
    :return: None
    """
    rule = OsirisPanadiumRule(12345)
    rule.verify(job_request)

    assert job_request.additional_values["panadium"] == 12345


def test_osiris_reduction_mode_freq_50_and_16(job_request):
    """
    Test defaults to spec for freq of 50 and 16
    """
    job_request.additional_values["freq10"] = 50
    rule = OsirisReductionModeRule(True)
    rule.verify(job_request)
    assert job_request.additional_values["mode"] == "spectroscopy"
    job_request.additional_values["freq10"] = 16
    del job_request.additional_values["mode"]
    rule.verify(job_request)
    assert job_request.additional_values["mode"] == "spectroscopy"


def test_osiris_stitch_rule(job_request):
    """
    Test runs are summed and additional request made
    :param job_request:
    :return:
    """
    assert False, "Not implemented"


def test_osiris_stitch_rule_will_do_nothing_if_diffraction_mode(job_request):
    """
    Test that sum runs will not be enabled for a diffraction run
    """
    job_request.additional_values["mode"] = "diffraction"
    rule = OsirisStitchRule(True)
    rule.verify(job_request)
    assert job_request.additional_values["sum_runs"] is False
