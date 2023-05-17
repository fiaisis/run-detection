"""
Test for mari rules
"""
# pylint:disable = redefined-outer-name, protected-access
from pathlib import Path
from unittest.mock import patch

import pytest

from rundetection.ingest import DetectedRun
from rundetection.rules.mari_rules import MariStitchRule, MariMaskFileRule, MariWBVANRule


@pytest.fixture
def detected_run():
    """
    Detected run fixture
    :return: Detected run
    """
    return DetectedRun(
        run_number=100,
        filepath=Path("/archive/100/MARI100.nxs"),
        experiment_title="Test experiment",
        additional_values={},
        additional_runs=[],
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


def test_get_previous_run_path():
    """
    Test previous path is obtained
    :return: None
    """
    run_path = Path("/archive/MARI100.nxs")
    run_number = 100

    mari_stitch_rule = MariStitchRule(value=True)

    assert mari_stitch_rule._get_previous_run_path(run_number, run_path) == Path("/archive/MARI99.nxs")

    run_path = Path("/archive/MARI100.nxs")
    assert mari_stitch_rule._get_previous_run_path(run_number, run_path) == Path("/archive/MARI99.nxs")


def test_verify_with_stitch_rule_false(mari_stitch_rule_false, detected_run):
    """
    Test not added when none to stitch
    :param mari_stitch_rule_false: rule fixture
    :param detected_run: run fixture
    :return: none
    """
    mari_stitch_rule_false.verify(detected_run)
    assert not detected_run.additional_runs


@patch("rundetection.ingest.get_run_title", return_value="Test experiment")
@patch("pathlib.Path.exists", return_value=False)
def test_verify_with_single_run(_, __, mari_stitch_rule_true, detected_run):
    """
    Test not added for single run
    :param _: unused mock path
    :param __: unused mock get title
    :param mari_stitch_rule_true: stitch rule fixture
    :param detected_run: detected run fixture
    :return: none
    """
    mari_stitch_rule_true.verify(detected_run)
    assert not detected_run.additional_runs


def test_mari_mask_rule(detected_run):
    """
    Test given link is attached to additional values
    :param detected_run: detected run fixture
    :return: none
    """
    rule = MariMaskFileRule("some link")
    rule.verify(detected_run)

    assert detected_run.additional_values["mask_file_link"] == "some link"


def test_mari_wbvan_rule(detected_run):
    """
    Test that the wbvan number is set via the specification
    :param detected_run: DetectedRun fixture
    :return: None
    """
    rule = MariWBVANRule(1234567)
    rule.verify(detected_run)

    assert detected_run.additional_values["wbvan"] == 1234567
