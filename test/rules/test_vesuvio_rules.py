"""Test for vesuvio rules."""

import os
import re
import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from rundetection.ingestion.ingest import JobRequest
from rundetection.rules.vesuvio_rules import (
    VesuvioDiffIPFileRule,
    VesuvioEmptyRunsRule,
    VesuvioIPFileRule,
    VesuvioSumRunsRule,
    get_file_from_request,
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


# ---------------------------------------------------------------------------
# get_file_from_request tests
# ---------------------------------------------------------------------------


def test_get_file_from_request_success_on_first_attempt(tmp_path):
    """
    When the HTTP response is OK on the first attempt the content should be
    written to the given path and no exception should be raised.
    """
    output_file = tmp_path / "output.txt"

    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.text = "file content"

    with patch("rundetection.rules.vesuvio_rules.requests.get", return_value=mock_response) as mock_get, patch(
        "rundetection.rules.vesuvio_rules.time.sleep"
    ) as mock_sleep:
        get_file_from_request("http://example.com/file", str(output_file))

    mock_get.assert_called_once_with("http://example.com/file", timeout=10)
    mock_sleep.assert_not_called()
    assert output_file.read_text() == "file content"


def test_get_file_from_request_success_after_retries(tmp_path):
    """
    When the first two responses are not OK but the third succeeds, the file
    should still be written and sleep should have been called twice with the
    correct exponentially increasing wait times (15 s then 45 s).
    """
    output_file = tmp_path / "output.txt"

    fail_response = MagicMock()
    fail_response.ok = False

    ok_response = MagicMock()
    ok_response.ok = True
    ok_response.text = "success content"

    with patch(
        "rundetection.rules.vesuvio_rules.requests.get",
        side_effect=[fail_response, fail_response, ok_response],
    ) as mock_get, patch("rundetection.rules.vesuvio_rules.time.sleep") as mock_sleep:
        get_file_from_request("http://example.com/file", str(output_file))

    expected_call_count = 3
    assert mock_get.call_count == expected_call_count
    assert mock_sleep.call_args_list == [call(15), call(45)]
    assert output_file.read_text() == "success content"


def test_get_file_from_request_raises_after_all_attempts_fail(tmp_path):
    """
    When all three attempts return a non-OK response a RuntimeError should be
    raised and the output file should not be created.
    """
    output_file = tmp_path / "output.txt"

    fail_response = MagicMock()
    fail_response.ok = False

    with patch(
        "rundetection.rules.vesuvio_rules.requests.get",
        return_value=fail_response,
    ) as mock_get, patch("rundetection.rules.vesuvio_rules.time.sleep"), pytest.raises(
        RuntimeError, match="Reduction not possible with missing resource"
    ):
        get_file_from_request("http://example.com/file", str(output_file))
    
    expected_call_count = 3
    assert mock_get.call_count == expected_call_count
    assert not output_file.exists()


def test_get_file_from_request_sleep_not_called_on_immediate_success(tmp_path):
    """
    Confirm that time.sleep is never called when the very first request
    succeeds, i.e. no unnecessary delay is introduced.
    """
    output_file = tmp_path / "output.txt"

    ok_response = MagicMock()
    ok_response.ok = True
    ok_response.text = ""

    with patch("rundetection.rules.vesuvio_rules.requests.get", return_value=ok_response), patch(
        "rundetection.rules.vesuvio_rules.time.sleep"
    ) as mock_sleep:
        get_file_from_request("http://example.com/file", str(output_file))

    mock_sleep.assert_not_called()
