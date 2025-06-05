"""Tests for iris rules."""

from pathlib import Path

import pytest

from rundetection.job_requests import JobRequest
from rundetection.rules.iris_rules import IrisCalibrationRule, IrisReductionRule


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
        instrument="iris",
        experiment_number="",
    )


@pytest.fixture
def reflection_rule():
    """Analyser rule fixture."""
    return IrisCalibrationRule({"002": "105275", "004": "105276"})


@pytest.fixture
def iris_mode_rule():
    """Reduction mode rule fixture."""
    return IrisReductionRule(True)


@pytest.mark.parametrize(
    ("first_tuple", "second_tuple", "expected_result"),
    [
        ((100, 100), (100, 100), True),  # Both equal
        ((100, 95), (100, 100), True),  # second near 5%
        ((95, 100), (100, 100), True),  # first near 5%
        ((95, 105), (100, 100), True),  # Both near 5%
        ((5, 10), (100, 100), False),
    ],
)
def test_iris_reduction_rule_tuple_match(first_tuple, second_tuple, expected_result, iris_mode_rule):
    """
    Test the tuple matching functionality of the IrisReductionRule.

    :param first_tuple: First tuple to compare
    :param second_tuple: Second tuple to compare
    :param expected_result: Expected result of the comparison
    :param iris_mode_rule: Fixture for the IrisReductionRule
    :return: None.
    """
    result = iris_mode_rule._tuple_match(first_tuple, second_tuple)
    assert result == expected_result


@pytest.mark.parametrize(("freq10"), [25, 49.4, 16.6])
def test_iris_reduction_rule_verify_freq10_below_50(freq10, iris_mode_rule, job_request):
    """
    Test that IrisReductionRule correctly sets reflection and analyser when freq10 is below 50.

    :param freq10: Test frequency value below 50
    :param iris_mode_rule: Fixture for the IrisReductionRule
    :param job_request: Job request fixture
    :return: None.
    """
    job_request.additional_values["freq10"] = freq10

    iris_mode_rule.verify(job_request)

    assert job_request.additional_values["reflection"] == "002"
    assert job_request.additional_values["analyser"] == "graphite"


# Test for mica and all 3 reflections
# Test for graphite and all 2 reflections
@pytest.mark.parametrize(
    ("phases", "freq10", "tcb_1", "tcb_2", "results"),
    [
        ((8967, 14413), 50, (56000.0, 76000.0), (52200.0, 72200.0), {"analyser": "graphite", "reflection": "002"}),
        ((3653, 5959), 50, (24000.0, 44000.0), (22700.0, 42700.0), {"analyser": "graphite", "reflection": "004"}),
    ],
)
def test_iris_reduction_rule_verify(phases, freq10, tcb_1, tcb_2, results, job_request, iris_mode_rule):
    """
    Test that IrisReductionRule correctly identifies analyser and reflection based on input parameters.

    :param phases: Tuple of phase6 and phase10 values
    :param freq10: Frequency value
    :param tcb_1: Tuple of detector time channel boundaries
    :param tcb_2: Tuple of monitor time channel boundaries
    :param results: Expected results dictionary with analyser and reflection
    :param job_request: Job request fixture
    :param iris_mode_rule: Fixture for the IrisReductionRule
    :return: None.
    """
    job_request.additional_values["phase6"], job_request.additional_values["phase10"] = phases
    job_request.additional_values["tcb_detector_min"], job_request.additional_values["tcb_detector_max"] = tcb_1
    job_request.additional_values["tcb_monitor_min"], job_request.additional_values["tcb_monitor_max"] = tcb_2
    job_request.additional_values["freq10"] = freq10

    iris_mode_rule.verify(job_request)

    assert job_request.additional_values["analyser"] == results["analyser"]
    assert job_request.additional_values["reflection"] == results["reflection"]


@pytest.mark.parametrize(
    ("reflection", "analyser", "output"),
    [
        ("002", "graphite", "105275"),
        ("004", "graphite", "105276"),
    ],
)
def test_iris_calibration_rule_verify(reflection, analyser, output, reflection_rule, job_request):
    """
    Test that IrisCalibrationRule correctly sets calibration run numbers based on reflection and analyser.

    :param reflection: Reflection value to test
    :param analyser: Analyser value to test
    :param output: Expected calibration run number
    :param reflection_rule: Fixture for the IrisCalibrationRule
    :param job_request: Job request fixture
    :return: None.
    """
    job_request.additional_values["calibration_run_numbers"] = None
    job_request.additional_values["reflection"] = reflection
    job_request.additional_values["analyser"] = analyser

    reflection_rule.verify(job_request)

    assert output == job_request.additional_values["calibration_run_numbers"]
