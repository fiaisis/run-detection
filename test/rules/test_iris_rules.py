from pathlib import Path

import pytest

from rundetection.job_requests import JobRequest
from rundetection.rules.iris_rules import IrisCalibrationRule, IrisReductionRule


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


@pytest.fixture()
def reflection_rule():
    """Analyser rule fixture"""
    return IrisCalibrationRule({"graphite": {"002": "00105275", "004": "00105276"},
                                "mica": {"002": "00105277", "004": "00105278", "006": "00105279"}})


@pytest.fixture()
def iris_mode_rule():
    """Reduction mode rule fixture"""
    return IrisReductionRule(True)


@pytest.mark.parametrize(("first_tuple", "second_tuple", "expected_result"),[
    ((100, 100), (100, 100), True),  # Both equal
    ((100, 95), (100, 100), True),  # second near 5%
    ((95, 100), (100, 100), True),  # first near 5%
    ((95, 105), (100, 100), True),  # Both near 5%
    ((5, 10), (100, 100), False),
])
def test_iris_reduction_rule_tuple_match(first_tuple, second_tuple, expected_result, iris_mode_rule):
    result =iris_mode_rule._tuple_match(first_tuple, second_tuple)
    assert result == expected_result


@pytest.mark.parametrize(("freq10"), [25, 49.4, 16.6])
def test_iris_reduction_rule_verify_freq10_below_50(freq10, iris_mode_rule, job_request):
    job_request.additional_values["freq10"] = freq10

    iris_mode_rule.verify(job_request)

    assert job_request.additional_values["reflection"] == "002"
    assert job_request.additional_values["spectroscopy_reduction"] == "true"
    assert job_request.additional_values["diffraction_reduction"] == "false"
    assert job_request.additional_values["analyser"] == "graphite"


# Test for mica and all 3 reflections
# Test for graphite and all 2 reflections
@pytest.mark.parametrize(("phases", "freq10", "tcb_1", "tcb_2", "results"),[
    ((8967, 14413), 50, (56000.0, 76000.0), (52200.0, 72200.0), {"analyser": "graphite", "reflection": "002",
                                                                 "spectroscopy_reduction": "true",
                                                                 "diffraction_reduction": "false"}),
    ((3653, 5959), 50, (24000.0, 44000.0), (22700.0, 42700.0), {"analyser": "graphite", "reflection": "004",
                                                                "spectroscopy_reduction": "true",
                                                                "diffraction_reduction": "false"}),
    ((9726, 7430), 50, (181000.0, 201000.0), (52000.0, 72000.0), {"analyser": "mica", "reflection": "002",
                                                                  "spectroscopy_reduction": "true",
                                                                  "diffraction_reduction": "false"}),
    ((13949, 2339), 50, (86000.0, 106000.0), (86000.0, 106000.0), {"analyser": "mica", "reflection": "004",
                                                                   "spectroscopy_reduction": "true",
                                                                   "diffraction_reduction": "false"}),
    ((8969, 14413), 50, (56000.0, 76000.0), (52200.0, 72200.0), {"analyser": "mica", "reflection": "006",
                                                                 "spectroscopy_reduction": "true",
                                                                 "diffraction_reduction": "false"})
])
def test_iris_reduction_rule_verify(phases, freq10, tcb_1, tcb_2, results, job_request, iris_mode_rule):
    job_request.additional_values["phase6"], job_request.additional_values["phase10"] = phases
    job_request.additional_values["tcb_detector_min"], job_request.additional_values["tcb_detector_max"] = tcb_1
    job_request.additional_values["tcb_monitor_min"], job_request.additional_values["tcb_monitor_max"] = tcb_2
    job_request.additional_values["freq10"] = freq10

    iris_mode_rule.verify(job_request)

    assert job_request.additional_values["analyser"] == results["analyser"]
    assert job_request.additional_values["reflection"] == results["reflection"]
    assert job_request.additional_values["spectroscopy_reduction"] == results["spectroscopy_reduction"]
    assert job_request.additional_values["diffraction_reduction"] == results["diffraction_reduction"]


@pytest.mark.parametrize(("reflection", "analyser", "output"), [
    ("002", "graphite", "00105275"),
    ("004", "graphite", "00105276"),
    ("006", "mica", "00105279"),
    ("004", "mica", "00105278"),
    ("002", "mica", "00105277")
])
def test_iris_calibration_rule_verify(reflection, analyser, output, reflection_rule, job_request):
    job_request.additional_values["calibration_run_number"] = None
    job_request.additional_values["reflection"] = reflection
    job_request.additional_values["analyser"] = analyser

    reflection_rule.verify(job_request)

    assert output == job_request.additional_values["calibration_run_number"]
