"""
Unit tests for common rules
"""

import unittest
from pathlib import Path
from unittest import mock

import pytest

from rundetection.ingestion.ingest import JobRequest
from rundetection.rules.common_rules import CheckIfScatterSANS, EnabledRule, is_y_within_5_percent_of_x


@pytest.fixture()
def job_request():
    """
    job_request Fixture
    :return: JobRequest
    """
    return JobRequest(1, "larmor", "1", "1", Path("/archive/larmor/1/1.nxs"), "start time", "end time", 1, 1, "users")


def test_enabled_rule_when_enabled(job_request) -> None:
    """
    Test verify method will return true when value is true
    :param job_request: JobRequest fixture
    :return: None
    """
    rule = EnabledRule(True)
    rule.verify(job_request)
    assert job_request.will_reduce


def test_enabled_rule_when_not_enabled(job_request) -> None:
    """
    Test verify method will return false when value is false
    :param job_request: JobRequest fixture
    :return: None
    """
    rule = EnabledRule(False)
    rule.verify(job_request)
    assert job_request.will_reduce is False


@pytest.mark.parametrize("end_of_title", ["_TRANS", "_SANS", "COOL", "_sans/trans"])
def test_checkifscattersans_verify_raises_for_no_sans_trans(end_of_title) -> None:
    job_request = mock.MagicMock()
    job_request.experiment_title = "{fancy chemical}" + end_of_title
    CheckIfScatterSANS(True).verify(job_request)

    assert job_request.will_reduce is False


@pytest.mark.parametrize("to_raise", ["direct", "DIRECT", "empty", "EMPTY"])
def test_checkifscattersans_verify_raises_for_direct_or_empty_in_title(to_raise) -> None:
    job_request = mock.MagicMock()
    job_request.experiment_title = "{fancy chemical " + to_raise + "}_SANS/TRANS"
    CheckIfScatterSANS(True).verify(job_request)

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
    """Simple test cases for is_y_within_5_percent_of_x"""
    assert is_y_within_5_percent_of_x(x, y) is expected


if __name__ == "__main__":
    unittest.main()
