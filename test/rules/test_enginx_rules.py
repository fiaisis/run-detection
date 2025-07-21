"""Test for enginx rules."""

import pytest

from rundetection.ingestion.ingest import JobRequest
from rundetection.rules.enginx_rules import EnginxVanadiumRunRule, EnginxFocusRunsRule, EnginxCeriaRunRule


@pytest.fixture
def job_request():
    """
    Job request fixture
    :return: job request.
    """
    return JobRequest(
        run_number=100,
        filepath="/archive/100/ENGINX100.nxs",
        experiment_title="Test experiment",
        additional_values={},
        additional_requests=[],
        raw_frames=3,
        good_frames=0,
        users="",
        run_start="",
        run_end="",
        instrument="enginx",
        experiment_number="",
    )


def test_enginx_vanadium_run_rule(job_request):
    """
    Test that the vanadium run number is set via the specification
    :param job_request: JobRequest fixture
    :return: None.
    """
    rule = EnginxVanadiumRunRule(12345)
    rule.verify(job_request)

    assert job_request.additional_values["vanadium_run"] == 12345


def test_enginx_focus_runs_rule(job_request):
    """
    Test that the focus runs are set via the specification
    :param job_request: JobRequest fixture
    :return: None.
    """
    focus_runs = [23456, 23457, 23458]
    rule = EnginxFocusRunsRule(focus_runs)
    rule.verify(job_request)

    assert job_request.additional_values["focus_runs"] == focus_runs


def test_enginx_ceria_run_rule(job_request):
    """
    Test that the ceria run number is set via the specification
    :param job_request: JobRequest fixture
    :return: None.
    """
    rule = EnginxCeriaRunRule(34567)
    rule.verify(job_request)

    assert job_request.additional_values["ceria_run"] == 34567