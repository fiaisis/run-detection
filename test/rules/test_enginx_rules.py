"""Test for enginx rules."""

import pytest

from rundetection.exceptions import RuleViolationError
from rundetection.ingestion.ingest import JobRequest
from rundetection.rules.enginx_rules import (
    EnginxCeriaCycleRule,
    EnginxCeriaRunRule,
    EnginxGroupRule,
    EnginxVanadiumRunRule,
)


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

    assert job_request.additional_values["vanadium_run"] == 12345  # noqa: PLR2004


def test_enginx_ceria_run_rule(job_request):
    """
    Test that the ceria run number is set via the specification
    :param job_request: JobRequest fixture
    :return: None.
    """
    rule = EnginxCeriaRunRule(34567)
    rule.verify(job_request)

    assert job_request.additional_values["ceria_run"] == 34567  # noqa: PLR2004


def test_enginx_group_rule_valid_values(job_request):
    """Test that valid group values are accepted and set in additional_values"""
    valid_groups = ["both", "north", "south", "cropped", "custom", "texture20", "texture30"]
    for group in valid_groups:
        jr = job_request
        rule = EnginxGroupRule(group)
        rule.verify(jr)
        assert jr.additional_values["group"] == group


def test_enginx_group_rule_invalid_value_raises(job_request):
    """Test that an invalid group raises an exception"""
    with pytest.raises(RuleViolationError):
        EnginxGroupRule("invalid_group").verify(job_request)


def test_enginx_ceria_cycle_rule(job_request):
    """Test that the ceria cycle string is set via the specification"""
    rule = EnginxCeriaCycleRule("cycle_20_01")
    rule.verify(job_request)
    assert job_request.additional_values["ceria_cycle"] == "cycle_20_01"
