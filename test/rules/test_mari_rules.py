from pathlib import Path
from unittest.mock import patch

import pytest

from rundetection.ingestion.detected_run import DetectedRun
from rundetection.rules.mari_rules import MariStitchRule, MariMaskFileRule


@pytest.fixture
def detected_run():
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
    return MariStitchRule(value=True)


@pytest.fixture
def mari_stitch_rule_false():
    return MariStitchRule(value=False)


def test_get_previous_run_path():
    run_path = Path("/archive/MARI100.nxs")
    run_number = 100

    mari_stitch_rule = MariStitchRule(value=True)

    assert mari_stitch_rule._get_previous_run_path(run_number, run_path) == Path("/archive/MARI99.nxs")

    run_path = Path("/archive/MARI100.nxs")
    assert mari_stitch_rule._get_previous_run_path(run_number, run_path) == Path("/archive/MARI99.nxs")


def test_verify_with_stitch_rule_false(mari_stitch_rule_false, detected_run):
    mari_stitch_rule_false.verify(detected_run)
    assert not detected_run.additional_runs


@patch("rundetection.ingest.get_run_title", return_value="Test experiment")
@patch("pathlib.Path.exists", return_value=False)
def test_verify_with_single_run(_, __, mari_stitch_rule_true, detected_run):
    mari_stitch_rule_true.verify(detected_run)
    assert not detected_run.additional_runs


def test_mari_mask_rule(detected_run):
    rule = MariMaskFileRule("some link")
    rule.verify(detected_run)

    assert detected_run.additional_values["mask_file_link"] == "some link"
