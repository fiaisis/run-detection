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
    GEMRietveldPDFVanEmptyRule,
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
            "rietveld_van_number": 100,
            "rietveld_empty_number": 200,
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


def test_gem_rietveld_pdf_van_empty_rule(job_request):
    """Test for GEMRietveldPDFVanEmptyRule."""
    rule = GEMRietveldPDFVanEmptyRule(
        {
            "rietveld": {"vanadium_run_numbers": 100, "empty_run_numbers": 200},
            "pdf": {"vanadium_run_numbers": 300, "empty_run_numbers": 400},
        }
    )
    expected_values = {
        "rietveld_van_number": 100,
        "rietveld_empty_number": 200,
        "pdf_van_number": 300,
        "pdf_empty_number": 400,
    }
    rule.verify(job_request)
    assert job_request.additional_values["rietveld_van_number"] == expected_values["rietveld_van_number"]
    assert job_request.additional_values["rietveld_empty_number"] == expected_values["rietveld_empty_number"]
    assert job_request.additional_values["pdf_van_number"] == expected_values
    assert job_request.additional_values["pdf_empty_number"] == expected_values["pdf_empty_number"]
