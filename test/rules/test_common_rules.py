"""Unit tests for common rules."""

import os
import unittest
from pathlib import Path

import pytest

from rundetection.ingestion.ingest import JobRequest
from rundetection.rules.common_rules import (
    EnabledRule,
    get_journal_from_file_based_on_run_file_archive_path,
    is_y_within_5_percent_of_x,
)
from rundetection.rules.sans_rules import SansPhiLimits, SansSliceWavs


@pytest.fixture
def job_request():
    """
    job_request Fixture
    :return: JobRequest.
    """
    return JobRequest(1, "larmor", "1", "1", Path("/archive/larmor/1/1.nxs"), "start time", "end time", 1, 1, "users")


def test_enabled_rule_when_enabled(job_request) -> None:
    """
    Test verify method will return true when value is true
    :param job_request: JobRequest fixture
    :return: None.
    """
    rule = EnabledRule(True)
    rule.verify(job_request)
    assert job_request.will_reduce


def test_enabled_rule_when_not_enabled(job_request) -> None:
    """
    Test verify method will return false when value is false
    :param job_request: JobRequest fixture
    :return: None.
    """
    rule = EnabledRule(False)
    rule.verify(job_request)
    assert job_request.will_reduce is False


@pytest.mark.parametrize(
    ("y", "x", "expected"),
    [
        (100, 95, True),
        (100, 105, True),
        (100, 100, True),
        (50, 47.5, True),
        (50, 52.5, True),
        (100, 94.9, False),
        (100, 105.1, False),
        (50, 47.49, False),
        (50, 52.51, False),
        (-100, -95, True),
        (-100, -105, True),
        (-100, -94.9, False),
        (-100, -105.1, False),
        (-100, 95, False),
        (100, -105, False),
        (0, 0, True),
        (0, 1, False),
        (1, 0, False),
    ],
)
def test_is_y_within_5_percent_of_x(x, y, expected):
    """Simple test cases for is_y_within_5_percent_of_x."""
    assert is_y_within_5_percent_of_x(x, y) is expected


def test_sans_slice_wavs_rule_when_not_enabled(job_request) -> None:
    """
    Test verify method will return expected value
    :param job_request: JobRequest fixture
    :return: None.
    """
    rule = SansSliceWavs("[1.0, 2.0, 3.0, 4.0]")
    rule.verify(job_request)
    assert job_request.additional_values["slice_wavs"] == "[1.0, 2.0, 3.0, 4.0]"


def test_sans_phi_limit_rule_when_not_enabled(job_request) -> None:
    """
    Test verify method will return expected value
    :param job_request: JobRequest fixture
    :return: None.
    """
    rule = SansPhiLimits("[(1.0, 2.0), (3.0, 4.0)]")
    rule.verify(job_request)
    assert job_request.additional_values["phi_limits"] == "[(1.0, 2.0), (3.0, 4.0)]"


def test_get_journal_from_file_based_on_run_file_archive_path(job_request) -> None:
    """
    Test that we can get the journal path from the nexus file path.
    :param job_request: a job request object
    Returns: None

    """
    #assert os.getcwd() == 0
    job_request.filepath = Path("/home/runner/work/run-detection/test/test_data/e2e_data/NDXMAR/Instrument/data/cycle_22_04/MAR25581.nxs")
    first_line = '<?xml version="1.0" encoding="UTF-8"?>'

    assert first_line in get_journal_from_file_based_on_run_file_archive_path(job_request)


if __name__ == "__main__":
    unittest.main()
