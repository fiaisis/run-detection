"""Tests for osiris rules."""

from pathlib import Path
from unittest import mock
from unittest.mock import call, patch

import pytest

from rundetection.exceptions import RuleViolationError
from rundetection.ingestion.ingest import JobRequest
from rundetection.rules.common_rules import MolSpecStitchRule
from rundetection.rules.osiris_rules import (
    OsirisDefaultGraphiteAnalyser,
    OsirisDefaultSpectroscopy,
    OsirisReductionModeRule,
    OsirisReflectionCalibrationRule,
)
from rundetection.rules.rule import Rule


@pytest.fixture
def job_request():
    """
    Job request fixture
    :return: job request.
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


@pytest.fixture
def reflection_rule():
    """Analyser rule fixture."""
    return OsirisReflectionCalibrationRule({"002": "00148587", "004": "00148587"})


@pytest.fixture
def osiris_mode_rule():
    """Reduction mode rule fixture."""
    return OsirisReductionModeRule(True)


def test_spectroscopy_mode(osiris_mode_rule, job_request):
    """Test spec values result in spec."""
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
    """Test diffraction values result in diffraction."""
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
    """Test phase conflict resolving with tcb values to diffraction mode."""
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
    """Test phase conflict resolves with tcb values."""
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
    """Test rule violation raised when values match neither spec or diff."""
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
    """Test non 25 frequency will result in spectroscopy."""
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
    """Test that sum runs will not be enabled for a diffraction run."""
    job_request.additional_values["mode"] = "diffraction"
    rule = MolSpecStitchRule(True)
    rule.verify(job_request)
    assert job_request.additional_values["sum_runs"] is False


@pytest.mark.parametrize(
    ("high_limit", "low_limit", "actual_high", "actual_low", "expected_analyser"),
    [
        (51500, 71500, 45900, 65900, "002"),
        (22500, 42500, 19000.03, 39000, "004"),
    ],
)
def test_determine_analyser_valid_values(
    reflection_rule, high_limit, low_limit, actual_high, actual_low, expected_analyser
):
    """Test correct analysers are returned with valid TCB."""
    assert (
        reflection_rule._determine_reflection_from_tcb_values(high_limit, low_limit, actual_high, actual_low)
        == expected_analyser
    )


def test_determine_analyser_raises_when_invalid_values(reflection_rule):
    """Test raises when invalid tcb given."""
    with pytest.raises(RuleViolationError):
        reflection_rule._determine_reflection_from_tcb_values(10000, 20000, 10000, 20000)


def test_analyser_rule_verify_freq_less_than_50(job_request, reflection_rule):
    """Test correct analyser returned."""
    job_request.additional_values["mode"] = "spectroscopy"
    job_request.additional_values["freq10"] = 49
    reflection_rule.verify(job_request)
    assert job_request.additional_values["reflection"] == "002"
    assert job_request.additional_values["calibration_run_numbers"] == "00148587"


def test_analyser_rule_verify_freq_less_than_50_but_close(job_request, reflection_rule):
    """Test correct analyser returned."""
    job_request.additional_values["mode"] = "spectroscopy"
    job_request.additional_values["freq10"] = 49.981
    job_request.additional_values["tcb_detector_min"] = 20500.0
    job_request.additional_values["tcb_detector_max"] = 40500.0
    job_request.additional_values["tcb_monitor_min"] = 16700.0
    job_request.additional_values["tcb_monitor_max"] = 36700.0
    reflection_rule.verify(job_request)
    assert job_request.additional_values["reflection"] == "004"
    assert job_request.additional_values["calibration_run_numbers"] == "00148587"


def test_verify_freq_greater_than_50_valid_tcb(job_request, reflection_rule):
    """Test correct analyser returned."""
    job_request.additional_values = {
        "freq10": 55,
        "tcb_detector_min": 51500,
        "tcb_detector_max": 71500,
        "tcb_monitor_min": 45900,
        "tcb_monitor_max": 65900,
    }
    reflection_rule.verify(job_request)
    assert job_request.additional_values["reflection"] == "002"
    assert job_request.additional_values["calibration_run_numbers"] == "00148587"


def test_analyser_verify_raises_when_freq_greater_than_50_invalid_tcb(job_request, reflection_rule):
    """Test."""
    job_request.additional_values = {
        "freq10": 55,
        "tcb_detector_min": 10000,
        "tcb_detector_max": 20000,
        "tcb_monitor_min": 10000,
        "tcb_monitor_max": 20000,
        "mode": "spectroscopy",
    }
    with pytest.raises(RuleViolationError):
        reflection_rule.verify(job_request)


def test_verify_should_stitch(job_request: JobRequest) -> None:
    """Test verify stitches run."""
    with (
        patch(
            "rundetection.rules.common_rules.get_run_title",
            side_effect=[job_request.experiment_title, "Test experiment  run 2", "different random title"],
        ),
        patch("rundetection.rules.common_rules.Path.exists", return_value=True),
    ):
        rule = MolSpecStitchRule(True)
        rule.verify(job_request)

    assert job_request.additional_requests[0].additional_values["input_runs"] == [100, 99]


@pytest.mark.parametrize(
    ("values_to_be_set", "set_values", "default_rule"),
    [
        (["spectroscopy_reduction", "diffraction_reduction"], ["true", "false"], OsirisDefaultSpectroscopy),
        (["analyser"], ["graphite"], OsirisDefaultGraphiteAnalyser),
    ],
)
def test_default_rules_true(values_to_be_set: list[str], set_values: list[str], default_rule: type[Rule]) -> None:
    """Test defaults OsirisDefaultSpectroscopy, OsirisDefaultGraphiteAnalyser, etc..."""
    default_rule = default_rule(True)
    job_request = mock.MagicMock()
    default_rule.verify(job_request)
    for value_to_be_set, set_value in zip(values_to_be_set, set_values, strict=False):
        assert call(value_to_be_set, set_value) in job_request.additional_values.__setitem__.mock_calls


@pytest.mark.parametrize(
    ("values_to_be_set", "set_values", "default_rule"),
    [
        (["spectroscopy_reduction", "diffraction_reduction"], ["true", "false"], OsirisDefaultSpectroscopy),
        (["analyser"], ["graphite"], OsirisDefaultGraphiteAnalyser),
    ],
)
def test_default_rules_false(values_to_be_set: list[str], set_values: list[str], default_rule: type[Rule]) -> None:
    """Test defaults OsirisDefaultSpectroscopy, OsirisDefaultGraphiteAnalyser, etc..."""
    default_rule = default_rule(False)
    job_request = mock.MagicMock()
    default_rule.verify(job_request)
    for value_to_be_set, set_value in zip(values_to_be_set, set_values, strict=False):
        assert call(value_to_be_set, set_value) not in job_request.additional_values.__setitem__.mock_calls
