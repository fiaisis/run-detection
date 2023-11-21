"""
Tests for osiris rules
"""
from pathlib import Path

import pytest

from rundetection.ingest import JobRequest
from rundetection.rules.osiris_rules import OsirisPanadiumRule


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


def test_osiris_panadium_rule(job_request):
    """
    Test that the panadium number is set via the specification
    :param job_request: job request fixture
    :return: None
    """
    rule = OsirisPanadiumRule(12345)
    rule.verify(job_request)

    assert job_request.additional_values["panadium"] == 12345


def test_osiris_stitch_rule(job_request):
    """
    Test runs are summed and additional request made
    :param job_request:
    :return:
    """
    assert False, "Not implemented"
