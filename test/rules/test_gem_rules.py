"""Test for GEM rules."""

from pathlib import Path

import pytest

from rundetection.job_requests import JobRequest
from rundetection.rules.gem_rules import (
    GEMCalibrationMappingFileRule,
    GEMConfigFileRule,
    GEMDoAbsorbCorrectionsRule,
    GEMInputModeRule,
    GEMModeRule,
    GEMMultipleScatteringRule,
    GEMVanNormRule,
)


@pytest.fixture
def job_request():
    """
    Job request fixture
    :return: job request.
    """
    return JobRequest(
        run_number=100,
        filepath=Path("test/test_data/e2e_data/NDXGEM/Instrument/data/cycle_22_04/GEM00102137.nxs"),
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


def test_gem_mode_rule(job_request):
    """Test for GEMModeRule."""
    rule = GEMModeRule("PDF")
    rule.verify(job_request)
    assert job_request.additional_values["mode"] == "PDF"


def test_gem_input_mode_rule(job_request):
    """Test for GEMInputModeRule."""
    rule = GEMInputModeRule("Summed")
    rule.verify(job_request)
    assert job_request.additional_values["input_mode"] == "Summed"


def test_gem_calibration_mapping_file_rule(job_request):
    """Test for GEMCalibrationMappingFileRule."""
    rule = GEMCalibrationMappingFileRule("Gem_Mapping_25_3.yaml")
    rule.verify(job_request)
    assert job_request.additional_values["cal_mapping_file"] == "Gem_Mapping_25_3.yaml"


def test_gem_config_file_rule(job_request):
    """Test for GEMConfigFileRule."""
    rule = GEMConfigFileRule("Gem_config_25_3.yaml")
    rule.verify(job_request)
    assert job_request.additional_values["config_file"] == "Gem_config_25_3.yaml"


def test_gem_van_norm_rule(job_request):
    """Test for GEMVanNormRule."""
    rule = GEMVanNormRule(True)
    rule.verify(job_request)
    assert job_request.additional_values["van_norm"] is True


def test_gem_do_absorb_corrections_rule(job_request):
    """Test for GEMDoAbsorbCorrectionsRule."""
    rule = GEMDoAbsorbCorrectionsRule(True)
    rule.verify(job_request)
    assert job_request.additional_values["do_absorb_corrections"] is True


def test_gem_multiple_scattering_rule(job_request):
    """Test for GEMMultipleScatteringRule."""
    rule = GEMMultipleScatteringRule(True)
    rule.verify(job_request)
    assert job_request.additional_values["multiple_scattering"] is True
