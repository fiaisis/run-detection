"""unit tests for tosca rules."""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from rundetection.ingestion.ingest import JobRequest
from rundetection.rules.common_rules import MolSpecStitchRule


@pytest.fixture
def job_request():
    """Return a job_request fixture."""
    return JobRequest(
        run_number=12345,
        instrument="instrument",
        experiment_title="experiment title run 1",
        filepath=Path("./25581.nxs"),
        experiment_number="experiment number",
        raw_frames=2,
        good_frames=0,
        run_start="",
        run_end="",
        users="",
    )


@pytest.mark.parametrize(
    ("title_one", "title_two", "expected"),
    [
        ("some long run run-1", "some long run run 2", True),
        ("some other run run-1", "different run completely", False),
        ("Olive oil sample 90pcEVOO8/10pcSTD1 run 2", "Olive oil sample 90pcEVOO8/10pcSTD1 run 1", True),
        ("Peanut oil sample 90pcEVOO8/10pcSTD1 run 2", "Olive oil sample 90pcEVOO8/10pcSTD1 run 2", False),
        ("FOX-7+DMSO-d6", "FOX-7+DMSO-d6 run 2", True),
    ],
)
def test_is_title_similar(title_one, title_two, expected):
    """
    Test similar titles will be correctly identified
    :return: None.
    """
    assert MolSpecStitchRule._is_title_similar(title_one, title_two) is expected


@patch("rundetection.rules.common_rules.MolSpecStitchRule._get_runs_to_stitch")
def test_stitch_rule_does_nothing_if_disabled(mock_get_runs: Mock, job_request):
    """Test verify returns instantly."""
    rule = MolSpecStitchRule(value=False)
    rule.verify(job_request)
    mock_get_runs.assert_not_called()


def test_verify_should_stitch(job_request):
    """
    Test the case where the previous run should stitch, but not the one prior
    :param job_request: job request fixture
    :return: None.
    """
    with (
        patch(
            "rundetection.rules.common_rules.get_run_title",
            side_effect=[job_request.experiment_title, "experiment title run 2", "different experiment"],
        ),
        patch("rundetection.rules.common_rules.Path.exists", return_value=True),
    ):
        rule = MolSpecStitchRule(True)
        rule.verify(job_request)

    assert job_request.additional_requests[0].additional_values["input_runs"] == [12345, 12344]
