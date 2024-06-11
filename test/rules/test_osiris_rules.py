"""
Tests for osiris rules
"""

from pathlib import Path
from unittest.mock import patch

import pytest

from rundetection.exceptions import RuleViolationError
from rundetection.ingestion.ingest import JobRequest
from rundetection.rules.osiris_rules import (
    OsirisAnalyserRule,
    OsirisPanadiumRule,
    OsirisReductionModeRule,
    OsirisStitchRule,
    is_y_within_5_percent_of_x,
)


@pytest.fixture()
def job_request():
    """
    job request fixture
    :return: job request
    """
    return JobRequest(
        run_number=100,
        filepath=Path("/archive/100/OSIRIS100.nxs"),
        experiment_title="Test experiment",
        additional_values={},
        additional_requests=[],
        raw_frames=0,
        good_frames=0,
        users="",
        run_start="",
        run_end="",
        instrument="osiris",
        experiment_number="",
    )


@pytest.mark.parametrize(
    ("y", "x", "expected"),
    [
        (100, 95, True),
        (100, 105, True),
        (100, 100, True),
        (50, 47.5, True),
        (50, 52.5, True),
        (100, 94.9, False),
        (100, 105.1, False),
        (50, 47.49, False),
        (50, 52.51, False),
        (-100, -95, True),
        (-100, -105, True),
        (-100, -94.9, False),
        (-100, -105.1, False),
        (-100, 95, False),
        (100, -105, False),
        (0, 0, True),
        (0, 1, False),
        (1, 0, False),
    ],
)
def test_is_y_within_5_percent_of_x(x, y, expected):
    """Simple test cases for is_y_within_5_percent_of_x"""
    assert is_y_within_5_percent_of_x(x, y) is expected


def test_osiris_panadium_rule(job_request):
    """
    Test that the panadium number is set via the specification
    :param job_request: job request fixture
    :return: None
    """
    rule = OsirisPanadiumRule(12345)
    rule.verify(job_request)

    assert job_request.additional_values["panadium"] == 12345  # noqa: PLR2004


@pytest.fixture()
def osiris_mode_rule():
    """Reduction mode rule fixture"""
    return OsirisReductionModeRule(True)


@pytest.fixture()
def analyser_rule():
    """Analyser rule fixture"""
    return OsirisAnalyserRule(True)


def test_spectroscopy_mode(osiris_mode_rule, job_request):
    """Test spec values result in spec"""
    job_request.additional_values.update(
        {
            "phase10": 14250,
            "phase6": 8573,
            "freq10": 25,
            "tcb_detector_min": 40200,
            "tcb_detector_max": 80200,
        }
    )
    osiris_mode_rule.verify(job_request)
    assert job_request.additional_values["mode"] == "spectroscopy"


def test_diffraction_mode(osiris_mode_rule, job_request):
    """Test diffraction values result in diffraction"""
    job_request.additional_values.update(
        {
            "phase10": 17715,
            "phase6": 10407,
            "freq10": 25,
            "tcb_detector_min": 0,
            "tcb_detector_max": 0,
        }
    )
    osiris_mode_rule.verify(job_request)
    assert job_request.additional_values["mode"] == "diffraction"


def test_phase_conflict_resolves_with_tcb_values_to_diff(osiris_mode_rule, job_request):
    """Test phase conflict resolving with tcb values to diffraction mode"""
    job_request.additional_values.update(
        {
            "phase10": 0,
            "phase6": 0,
            "freq10": 25,
            "tcb_detector_min": 0,
            "tcb_detector_max": 0,
        }
    )
    with (
        patch("rundetection.rules.osiris_rules.OsirisReductionModeRule._is_spec_phase", return_value=True),
        patch("rundetection.rules.osiris_rules.OsirisReductionModeRule._is_diff_phase", return_value=True),
    ):
        osiris_mode_rule.verify(job_request)
    assert job_request.additional_values["mode"] == "diffraction"


def test_phase_conflict_resolves_with_tcb_values_to_spec(osiris_mode_rule, job_request):
    """Test phase conflict resolves with tcb values"""
    job_request.additional_values.update(
        {
            "phase10": 0,
            "phase6": 0,
            "freq10": 25,
            "tcb_detector_min": 40200,
            "tcb_detector_max": 80200,
        }
    )
    with (
        patch("rundetection.rules.osiris_rules.OsirisReductionModeRule._is_spec_phase", return_value=True),
        patch("rundetection.rules.osiris_rules.OsirisReductionModeRule._is_diff_phase", return_value=True),
    ):
        osiris_mode_rule.verify(job_request)
    assert job_request.additional_values["mode"] == "spectroscopy"


def test_rule_violation_error(osiris_mode_rule, job_request):
    """Test rule violation raised when values match neither spec or diff"""
    job_request.additional_values.update(
        {
            "phase10": 0,
            "phase6": 0,
            "freq10": 25,
            "tcb_detector_min": 0,
            "tcb_detector_max": 0,
        }
    )
    with pytest.raises(RuleViolationError):
        osiris_mode_rule.verify(job_request)


def test_non_25_freq_defaults_to_spectroscopy(osiris_mode_rule, job_request):
    "Test non 25 frequency will result in spectroscopy"
    job_request.additional_values.update(
        {
            "phase10": 0,
            "phase6": 0,
            "freq10": 20,
            "tcb_detector_min": 0,
            "tcb_detector_max": 0,
        }
    )
    osiris_mode_rule.verify(job_request)
    assert job_request.additional_values["mode"] == "spectroscopy"


def test_osiris_stitch_rule_will_do_nothing_if_diffraction_mode(job_request):
    """
    Test that sum runs will not be enabled for a diffraction run
    """
    job_request.additional_values["mode"] = "diffraction"
    rule = OsirisStitchRule(True)
    rule.verify(job_request)
    assert job_request.additional_values["sum_runs"] is False


@pytest.mark.parametrize(
    ("high_limit", "low_limit", "actual_high", "actual_low", "expected_analyser"),
    [
        (51500, 71500, 45900, 65900, 2),
        (22500, 42500, 19000.03, 39000, 4),
    ],
)
def test_determine_analyser_valid_values(
    analyser_rule, high_limit, low_limit, actual_high, actual_low, expected_analyser
):
    """Test correct analysers are returned with valid TCB"""
    assert (
        analyser_rule._determine_analyser_from_tcb_values(high_limit, low_limit, actual_high, actual_low)
        == expected_analyser
    )


def test_determine_analyser_raises_when_invalid_values(analyser_rule):
    """Test raises when invalid tcb given"""
    with pytest.raises(RuleViolationError):
        analyser_rule._determine_analyser_from_tcb_values(10000, 20000, 10000, 20000)


def test_analyser_rule_verify_freq_less_than_50(job_request, analyser_rule):
    """Test correct analyser returned"""
    job_request.additional_values["mode"] = "spectroscopy"
    job_request.additional_values["freq10"] = 49
    analyser_rule.verify(job_request)
    assert job_request.additional_values["analyser"] == 2  # noqa: PLR2004


def test_verify_freq_greater_than_50_valid_tcb(job_request, analyser_rule):
    """Test correct analyser returned"""
    job_request.additional_values = {
        "freq10": 55,
        "tcb_detector_min": 51500,
        "tcb_detector_max": 71500,
        "tcb_monitor_min": 45900,
        "tcb_monitor_max": 65900,
        "mode": "spectroscopy",
    }
    analyser_rule.verify(job_request)
    assert job_request.additional_values["analyser"] == 2  # noqa: PLR2004


def test_analyser_verify_raises_when_freq_greater_than_50_invalid_tcb(job_request, analyser_rule):
    """Test"""
    job_request.additional_values = {
        "freq10": 55,
        "tcb_detector_min": 10000,
        "tcb_detector_max": 20000,
        "tcb_monitor_min": 10000,
        "tcb_monitor_max": 20000,
        "mode": "spectroscopy",
    }
    with pytest.raises(RuleViolationError):
        analyser_rule.verify(job_request)


def test_analyser_rule_returns_when_off():
    """Test rule always returns when rule is off"""
    rule = OsirisAnalyserRule(False)
    rule.verify(object())


def test_stitch_rule_returns_when_off():
    """Test rule always returns when rule is off"""
    rule = OsirisStitchRule(False)
    rule.verify(object())


def test_mode_rule_returns_when_on():
    """Test rule always returns when rule is off"""
    rule = OsirisReductionModeRule(False)
    rule.verify(object())


def test_analyser_rule_returns_for_diffraction(job_request):
    """
    Test analyser immediate returns when diffraction
    """
    rule = OsirisAnalyserRule(True)
    job_request.additional_values["mode"] = "diffraction"
    rule.verify(job_request)


def test_verify_should_stitch(job_request: JobRequest) -> None:
    """
    Test verify stitches run
    """
    with (
        patch(
            "rundetection.rules.osiris_rules.get_run_title",
            side_effect=[job_request.experiment_title, "Test experiment  run 2", "different random title"],
        ),
        patch("rundetection.rules.osiris_rules.Path.exists", return_value=True),
    ):
        rule = OsirisStitchRule(True)
        rule.verify(job_request)

    assert job_request.additional_requests[0].additional_values["input_runs"] == [100, 99]
