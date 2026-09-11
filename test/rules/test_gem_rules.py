"""Test for GEM rules."""

from pathlib import Path

import pytest

from rundetection.job_requests import JobRequest
from rundetection.rules.gem_rules import (
    GEMCalibrationMappingFileRule,
    GEMCycleRule,
    GEMDoAbsorbCorrectionsRule,
    GEMInputModeRule,
    GEMModeRule,
    GEMMultipleScatteringRule,
    GEMOffsetFileRule,
    GEMPDFEmptyNumberRule,
    GEMPDFVanNumberRule,
    GEMReitveldEmptyNumberRule,
    GEMReitveldVanNumberRule,
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
        additional_values={
            "cycle_string": "cycle_22_04",
            "reitveld_van_number": 100,
            "reitveld_empty_number": 200,
            "pdf_van_number": 300,
            "pdf_empty_number": 400,
        },
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


def test_gem_offset_file_rule(job_request):
    """Test for GEMOffsetFileRule."""
    rule = GEMOffsetFileRule("offsets_2023_cycle231.cal")
    rule.verify(job_request)
    assert job_request.additional_values["offset_file"] == "offsets_2023_cycle231.cal"


def test_gem_cycle_rule(job_request):
    """Test for GEMCycleRule."""
    rule = GEMCycleRule("cycle_22_04")
    rule.verify(job_request)
    assert job_request.additional_values["cycle_string"] == "cycle_22_04"


def test_gem_reitveld_van_number_rule(job_request):
    """Test for GEMReitveldVanNumberRule."""
    rule = GEMReitveldVanNumberRule(100)
    rule.verify(job_request)
    expected_value = 100
    assert job_request.additional_values["reitveld_van_number"] == expected_value


def test_gem_reitveld_empty_number_rule(job_request):
    """Test for GEMReitveldEmptyNumberRule."""
    rule = GEMReitveldEmptyNumberRule(200)
    rule.verify(job_request)
    expected_value = 200
    assert job_request.additional_values["reitveld_empty_number"] == expected_value


def test_gem_pdf_van_number_rule(job_request):
    """Test for GEMPDFVanNumberRule."""
    rule = GEMPDFVanNumberRule(300)
    rule.verify(job_request)
    expected_value = 300
    assert job_request.additional_values["pdf_van_number"] == expected_value


def test_gem_pdf_empty_number_rule(job_request):
    """Test for GEMPDFEmptyNumberRule."""
    rule = GEMPDFEmptyNumberRule(400)
    rule.verify(job_request)
    expected_value = 400
    assert job_request.additional_values["pdf_empty_number"] == expected_value
