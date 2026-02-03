"""Test for vesuvio rules."""

import os
import re
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from rundetection.ingestion.ingest import JobRequest
from rundetection.rules.vesuvio_rules import (
    VesuvioDiffIPFileRule,
    VesuvioEmptyRunsRule,
    VesuvioIPFileRule,
    VesuvioSumRunsRule,
)


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


def test_vesuvio_sum_runs_rule(job_request):
    """Test that multiple runs with the same title are correctly grouped."""
    temp_dir = tempfile.mkdtemp()
    try:
        # Create mockup files
        # 55958: Test experiment (matching job_request)
        # 55957: Test experiment
        # 55956: Test experiment
        # 55955: Other title

        runs = {55958: "Test experiment", 55957: "Test experiment", 55956: "Test experiment", 55955: "Other title"}

        for run_num in runs:
            Path(temp_dir, f"VESUVIO{run_num:08d}.nxs").touch()

        def mock_get_run_title(path):
            match = re.search(r"VESUVIO(\d+)\.nxs", path.name)
            if match:
                run_num = int(match.group(1))
                return runs.get(run_num, "Unknown")
            return "Unknown"

        rule = VesuvioEmptyRunsRule("123-132")
        rule.verify(job_request)
        rule = VesuvioIPFileRule("IP0001.par")
        rule.verify(job_request)
        rule = VesuvioDiffIPFileRule("IP0001.par")
        rule.verify(job_request)
        rule = VesuvioSumRunsRule(True)

        # update job_request for this test
        job_request.run_number = 55958
        job_request.filepath = Path(temp_dir, "VESUVIO00055958.nxs")
        job_request.experiment_title = "Test experiment"

        with patch("rundetection.rules.vesuvio_rules.get_run_title", side_effect=mock_get_run_title):
            rule.verify(job_request)

            assert len(job_request.additional_requests) == 1
            additional_request = job_request.additional_requests[0]
            assert additional_request.additional_values["runno"] == [55956, 55957, 55958]
            assert additional_request.additional_values["sum_runs"] is True
            assert additional_request.additional_values["ip_file"] == "IP0001.par"
            assert additional_request.additional_values["empty_runs"] == "123-132"
            assert additional_request.additional_values["diff_ip_file"] == "IP0001.par"

    finally:
        shutil.rmtree(temp_dir)


def test_vesuvio_sum_runs_rule_disabled(job_request):
    """Test that no grouping occurs when the rule is disabled."""
    rule = VesuvioSumRunsRule(False)
    rule.verify(job_request)
    assert len(job_request.additional_requests) == 0
