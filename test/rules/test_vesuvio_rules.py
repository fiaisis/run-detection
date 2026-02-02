"""Test for vesuvio rules."""

import os
from pathlib import Path

import pytest

from rundetection.ingestion.ingest import JobRequest
from rundetection.rules.vesuvio_rules import VesuvioDiffIPFileRule, VesuvioEmptyRunsRule, VesuvioIPFileRule


@pytest.fixture(autouse=True)
def _working_directory_fix():
    # Set dir to repo root for purposes of the test.
    current_working_directory = Path.cwd()
    if current_working_directory.name == "rules":
        os.chdir(current_working_directory / ".." / "..")


@pytest.fixture
def job_request():
    """
    Return a Job request fixture
    :return: job request.
    """
    return JobRequest(
        run_number=100,
        filepath=Path("/archive/100/VESUVIO100.nxs"),
        experiment_title="Test experiment",
        additional_values={},
        additional_requests=[],
        raw_frames=3,
        good_frames=0,
        users="",
        run_start="",
        run_end="",
        instrument="vesuvio",
        experiment_number="",
    )


def test_vesuvio_empty_runs_rule(job_request):
    """
    Test empty runs are set and attached to additional values
    :param job_request: job request fixture
    :return: none.
    """
    rule = VesuvioEmptyRunsRule("123-132")
    rule.verify(job_request)

    assert job_request.additional_values["empty_runs"] == "123-132"


def test_vesuvio_ip_file_rule(job_request):
    """
    Test that the IP file is set via the specification
    :param job_request: JobRequest fixture
    :return: None.
    """
    rule = VesuvioIPFileRule("IP0001.par")
    rule.verify(job_request)

    assert job_request.additional_values["ip_file"] == "IP0001.par"


def test_vesuvio_diff_ip_file_rule(job_request):
    """
    Test that the diffraction IP file is set via the specification
    :param job_request: JobRequest fixture
    :return: None.
    """
    rule = VesuvioDiffIPFileRule("IP0001.par")
    rule.verify(job_request)

    assert job_request.additional_values["diff_ip_file"] == "IP0001.par"
