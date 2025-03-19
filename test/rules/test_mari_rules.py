"""
Test for mari rules
"""

import os
from pathlib import Path
from unittest.mock import call, patch

import pytest

from rundetection.ingestion.ingest import JobRequest
from rundetection.rules.mari_rules import MariMaskFileRule, MariStitchRule, MariWBVANRule


@pytest.fixture(autouse=True)
def _working_directory_fix():
    # Set dir to repo root for purposes of the test.
    current_working_directory = Path.cwd()
    if current_working_directory.name == "rules":
        os.chdir(current_working_directory / ".." / "..")


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
        additional_values={"cycle_string": "cycle_24_5"},
        additional_requests=[],
        raw_frames=3,
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


@pytest.mark.usefixtures("_working_directory_fix")
def test_verify_with_stitch_rule_false(mari_stitch_rule_false, job_request):
    """
    Test not added when none to stitch
    :param mari_stitch_rule_false: rule fixture
    :param job_request: run fixture
    :return: none
    """
    mari_stitch_rule_false.verify(job_request)
    assert not job_request.additional_requests


@pytest.mark.usefixtures("_working_directory_fix")
def test_verify_with_single_run(mari_stitch_rule_true, job_request):
    """
    Test not added for single run
    :param mari_stitch_rule_true: stitch rule fixture
    :param job_request: job request fixture
    :return: none
    """
    with (
        patch("rundetection.ingestion.ingest.get_run_title", return_value="Test experiment"),
        patch("pathlib.Path.exists", return_value=False),
    ):
        mari_stitch_rule_true.verify(job_request)
    assert not job_request.additional_requests


@pytest.mark.usefixtures("_working_directory_fix")
def test_verify_multiple_runs(mari_stitch_rule_true, job_request):
    """
    Test additional requests are included with other rules applied
    :param mari_stitch_rule_true: rule fixture
    :param job_request: job request fixture
    :return: None
    """
    rule = MariMaskFileRule("some link")
    rule.verify(job_request)
    with (patch("rundetection.rules.common_rules.requests") as requests_mock,
          patch("rundetection.rules.mari_rules.xmltodict") as xmltodict_mock,
          patch("rundetection.rules.mari_rules.MariStitchRule._get_runs_to_stitch", return_value=[1, 2, 3])):
        rule = MariWBVANRule(1234567)
        rule.verify(job_request)
        xmltodict_mock.parse.return_value = {
            "NXroot": {
                "NXentry": ""
            }
        }
        mari_stitch_rule_true.verify(job_request)

    assert len(job_request.additional_requests) == 1
    assert job_request.additional_requests[0].additional_values["mask_file_link"] == "some link"
    assert job_request.additional_requests[0].additional_values["wbvan"] == 1234567  # noqa: PLR2004
    assert call(requests_mock.get().text) in xmltodict_mock.parse.call_args_list


def test_mari_mask_rule(job_request):
    """
    Test given link is attached to additional values
    :param job_request: job request fixture
    :return: none
    """
    rule = MariMaskFileRule("some link")
    rule.verify(job_request)

    assert job_request.additional_values["mask_file_link"] == "some link"


def test_mari_wbvan_rule_run_from_this_cycle(job_request):
    with (patch("rundetection.rules.common_rules.requests") as requests_mock,
          patch("rundetection.rules.mari_rules.xmltodict") as xmltodict_mock):
        rule = MariWBVANRule(1234567)
        xmltodict_mock.parse.return_value = {
            "NXroot": {
                "NXentry": ""
            }
        }
        rule.verify(job_request)

    assert job_request.additional_values["wbvan"] == 1234567  # noqa: PLR2004
    assert call(requests_mock.get().text) in xmltodict_mock.parse.call_args_list


def test_mari_wbvan_rule_run_from_old_cycle_new_van_unfindable(job_request):
    with (patch("rundetection.rules.common_rules.requests") as requests_mock,
          patch("rundetection.rules.mari_rules.xmltodict") as xmltodict_mock):
        rule = MariWBVANRule(1234567)
        xmltodict_mock.parse.return_value = {
            "NXroot": {
                "NXentry": [
                    {
                        "run_number": {
                            "#text": "1234567"
                        },
                        "title": {
                            "#text": '"white van" - Ei=30meV 50Hz Gd chopper'
                        }
                    }
                ]
            }
        }
        rule.verify(job_request)

    assert job_request.additional_values["wbvan"] == 1234567  # noqa: PLR2004
    assert call(requests_mock.get().text) in xmltodict_mock.parse.call_args_list


def test_mari_wbvan_rule_run_from_old_cycle_van_found(job_request):
    with (patch("rundetection.rules.common_rules.requests") as requests_mock,
          patch("rundetection.rules.mari_rules.xmltodict") as xmltodict_mock):
        rule = MariWBVANRule(1234)
        xmltodict_mock.parse.return_value = {
            "NXroot": {
                "NXentry": [
                    {
                        "run_number": {
                            "#text": "1234567"
                        },
                        "title": {
                            "#text": '"white van" - Ei=30meV 50Hz Gd chopper'
                        }
                    }
                ]
            }
        }
        rule.verify(job_request)

    assert job_request.additional_values["wbvan"] == 1234567  # noqa: PLR2004
    assert call(requests_mock.get().text) in xmltodict_mock.parse.call_args_list

