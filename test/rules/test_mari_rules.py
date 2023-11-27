"""
Test for mari rules
"""
import json
# pylint:disable = redefined-outer-name, protected-access
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from rundetection.ingestion.ingest import JobRequest
from rundetection.rules.mari_rules import MariStitchRule, MariMaskFileRule, MariWBVANRule


def get_specification_value(key: str) -> Any:
    """
    Given a key, return the specification value
    :param key: The key for the rule
    :return: The rule value
    """
    with open("rundetection/specifications/mari_specification.json", "r", encoding="utf-8") as fle:
        spec = json.load(fle)
        return spec[key]


@pytest.fixture
def job_request():
    """
    job request fixture
    :return: job request
    """
    return JobRequest(
        run_number=100,
        filepath=Path("/archive/100/MARI100.nxs"),
        experiment_title="Test experiment",
        additional_values={},
        additional_requests=[],
        raw_frames=0,
        good_frames=0,
        users="",
        run_start="",
        run_end="",
        instrument="mari",
        experiment_number="",
    )


@pytest.fixture
def mari_stitch_rule_true():
    """
    stitch rule fixture for true
    :return: MariStitchRule
    """
    return MariStitchRule(value=True)


@pytest.fixture
def mari_stitch_rule_false():
    """
    Stitch rule fixture for false
    :return: MariStitchRule
    """
    return MariStitchRule(value=False)


def test_verify_with_stitch_rule_false(mari_stitch_rule_false, job_request):
    """
    Test not added when none to stitch
    :param mari_stitch_rule_false: rule fixture
    :param job_request: run fixture
    :return: none
    """
    mari_stitch_rule_false.verify(job_request)
    assert not job_request.additional_requests


@patch("rundetection.ingest.get_run_title", return_value="Test experiment")
@patch("pathlib.Path.exists", return_value=False)
def test_verify_with_single_run(_, __, mari_stitch_rule_true, job_request):
    """
    Test not added for single run
    :param _: unused mock path
    :param __: unused mock get title
    :param mari_stitch_rule_true: stitch rule fixture
    :param job_request: job request fixture
    :return: none
    """
    mari_stitch_rule_true.verify(job_request)
    assert not job_request.additional_requests


@patch("rundetection.rules.mari_rules.MariStitchRule._get_runs_to_stitch", return_value=[1, 2, 3])
def test_verify_multiple_runs(_, mari_stitch_rule_true, job_request):
    """
    Test additional requests are included with other rules applied
    :param _: additional run fixture
    :param mari_stitch_rule_true: rule fixture
    :param job_request: job request fixture
    :return: None
    """
    mari_stitch_rule_true.verify(job_request)

    assert len(job_request.additional_requests) == 1
    assert job_request.additional_requests[0].additional_values["mask_file_link"] == get_specification_value(
        "marimaskfile"
    )
    assert job_request.additional_requests[0].additional_values["wbvan"] == get_specification_value("mariwbvan")


def test_mari_mask_rule(job_request):
    """
    Test given link is attached to additional values
    :param job_request: job request fixture
    :return: none
    """
    rule = MariMaskFileRule("some link")
    rule.verify(job_request)

    assert job_request.additional_values["mask_file_link"] == "some link"


def test_mari_wbvan_rule(job_request):
    """
    Test that the wbvan number is set via the specification
    :param job_request: JobRequest fixture
    :return: None
    """
    rule = MariWBVANRule(1234567)
    rule.verify(job_request)

    assert job_request.additional_values["wbvan"] == 1234567
