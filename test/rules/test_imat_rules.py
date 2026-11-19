"""Tests for IMAT's rules"""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from rundetection.exceptions import RuleViolationError
from rundetection.job_requests import JobRequest
from rundetection.rules.imat_rules import (
    NGEM_TOF_KEYS,
    IMATFindImagesRule,
    IMATNgemToFRule,
    find_correct_tomo_dir,
)

EXPECTED_ADDITIONAL_VALUES_LEN = 4
RUN_NUMBER = 100
NGEM_TOF_SPEC = {"start_tof": 20000, "end_tof": 85000, "bin_width": 10, "L": 56.4}


@pytest.fixture
def job_request():
    """
    Job request fixture
    :return: job request.
    """
    return JobRequest(
        run_number=100,
        filepath=Path("/imat/IMAT100.nxs"),
        experiment_title="Test experiment",
        additional_values={},
        additional_requests=[],
        raw_frames=0,
        good_frames=0,
        users="",
        run_start="",
        run_end="",
        instrument="IMAT",
        experiment_number="12345",
    )


def test_imat_find_images_success(job_request):
    """Test imat rules can find images successfully"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Setup
        os.environ["IMAT_DIR"] = tmpdirname
        exp_dir = Path(tmpdirname).joinpath("RB12345")
        exp_dir.mkdir(parents=True, exist_ok=True)
        # Create required structure: a file with run number and a Tomo dir
        data_dir = exp_dir.joinpath("data")
        data_dir.mkdir()
        data_dir.joinpath("run100.csv").touch()
        tomo_dir = exp_dir.joinpath("Tomo")
        tomo_dir.mkdir()

        # Test
        rule = IMATFindImagesRule(True)
        rule.verify(job_request)

        # Assertions
        assert len(job_request.additional_values) == EXPECTED_ADDITIONAL_VALUES_LEN
        assert job_request.additional_values["images_dir"] == str(tomo_dir)
        assert job_request.additional_values["runno"] == RUN_NUMBER
        assert job_request.additional_values["recon"] == "true"
        assert job_request.additional_values["ngem"] == "false"


def test_imat_find_images_tomo_first(job_request):
    """Test imat rules when Tomo dir is found before the run file"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Setup
        os.environ["IMAT_DIR"] = tmpdirname
        exp_dir = Path(tmpdirname).joinpath("RB12345")
        exp_dir.mkdir(parents=True, exist_ok=True)

        # We want to influence iterdir order if possible, but it's OS dependent.
        # We just ensure both exist.
        tomo_dir = exp_dir.joinpath("Tomo")
        tomo_dir.mkdir()

        data_dir = exp_dir.joinpath("data")
        data_dir.mkdir()
        data_dir.joinpath("z_run100.csv").touch()

        # Test
        rule = IMATFindImagesRule(True)
        rule.verify(job_request)

        # Assertions
        assert job_request.additional_values["images_dir"] == str(tomo_dir)


def test_imat_find_images_file_first(job_request):
    """Test imat rules when run file is found before the Tomo dir"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Setup
        os.environ["IMAT_DIR"] = tmpdirname
        exp_dir = Path(tmpdirname).joinpath("RB12345")
        exp_dir.mkdir(parents=True, exist_ok=True)

        data_dir = exp_dir.joinpath("data")
        data_dir.mkdir()
        data_dir.joinpath("0_run100.csv").touch()
        tomo_dir = exp_dir.joinpath("Tomo")
        tomo_dir.mkdir()

        # Test
        rule = IMATFindImagesRule(True)
        rule.verify(job_request)

        # Assertions
        assert job_request.additional_values["images_dir"] == str(tomo_dir)


def test_imat_find_images_missing_tomo(job_request):
    """Test imat rules fail when Tomo directory is missing"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Setup
        os.environ["IMAT_DIR"] = tmpdirname
        os.environ["IMAT_NGEM_DIR"] = tmpdirname  # Also set this to avoid loading nexus
        exp_dir = Path(tmpdirname).joinpath("RB12345")
        exp_dir.mkdir(parents=True, exist_ok=True)
        data_dir = exp_dir.joinpath("data")
        data_dir.mkdir()
        data_dir.joinpath("run100.csv").touch()

        # Test
        rule = IMATFindImagesRule(True)
        with patch("rundetection.rules.imat_rules.load_h5py_dataset") as mock_load:
            mock_load.return_value.get.return_value = [b"26_1"]
            with pytest.raises(RuleViolationError):
                rule.verify(job_request)


def test_imat_find_images_missing_run_file(job_request):
    """Test imat rules fail when run number file is missing"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Setup
        os.environ["IMAT_DIR"] = tmpdirname
        os.environ["IMAT_NGEM_DIR"] = tmpdirname
        exp_dir = Path(tmpdirname).joinpath("RB12345")
        exp_dir.mkdir(parents=True, exist_ok=True)
        exp_dir.joinpath("Tomo").mkdir()

        # Test
        rule = IMATFindImagesRule(True)
        with patch("rundetection.rules.imat_rules.load_h5py_dataset") as mock_load:
            mock_load.return_value.get.return_value = [b"26_1"]
            with pytest.raises(RuleViolationError):
                rule.verify(job_request)


def test_imat_find_images_failure(job_request):
    """Test imat rules act's correctly when it fails to find experiment dir"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Setup
        os.environ["IMAT_DIR"] = tmpdirname
        os.environ["IMAT_NGEM_DIR"] = tmpdirname

        # Test
        rule = IMATFindImagesRule(True)
        with patch("rundetection.rules.imat_rules.load_h5py_dataset") as mock_load:
            mock_load.return_value.get.return_value = [b"26_1"]
            with pytest.raises(RuleViolationError):
                rule.verify(job_request)

        # Assertions
        assert len(job_request.additional_values) == 0


def test_imat_find_images_ngem_success(job_request):
    """Test imat rules can find nGEM images successfully when Tomo is missing"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Setup
        os.environ["IMAT_DIR"] = tmpdirname
        os.environ["IMAT_NGEM_DIR"] = tmpdirname

        # Cycle information
        cycle_year = "26"
        cycle_num = "1"
        cycle_str = f"{cycle_year}_{cycle_num}"

        # Create nGEM directory structure
        ngem_data_dir = Path(tmpdirname) / "DATA" / f"IMAT_20{cycle_year}_0{cycle_num}"
        possible_path = ngem_data_dir / f"IMAT{job_request.run_number:08d}"
        possible_path.mkdir(parents=True, exist_ok=True)

        # Test
        rule = IMATFindImagesRule(True)
        with patch("rundetection.rules.imat_rules.load_h5py_dataset") as mock_load:
            mock_load.return_value.get.return_value = [cycle_str.encode("utf-8")]
            rule.verify(job_request)

        # Assertions
        assert job_request.additional_values["recon"] == "false"
        assert job_request.additional_values["ngem"] == "true"
        assert job_request.additional_values["ngem_path"] == str(possible_path)
        expected_output_path = str(Path(tmpdirname) / "DATA" / f"IMAT_{cycle_year}_{cycle_num}_nxs")
        assert job_request.additional_values["ngem_output_path"] == expected_output_path
        assert job_request.additional_values["runno"] == RUN_NUMBER


def test_imat_find_images_nested_success(job_request):
    """Test imat rules can find images in a nested directory"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Setup
        os.environ["IMAT_DIR"] = tmpdirname
        exp_dir = Path(tmpdirname).joinpath("RB12345")
        nested_dir = exp_dir.joinpath("nested")
        nested_dir.mkdir(parents=True, exist_ok=True)

        # Create required structure in nested dir
        nested_dir.joinpath("run100.csv").touch()
        tomo_dir = nested_dir.joinpath("Tomo")
        tomo_dir.mkdir()

        # Test
        rule = IMATFindImagesRule(True)
        rule.verify(job_request)

        # Assertions
        assert job_request.additional_values["images_dir"] == str(tomo_dir)
        assert job_request.additional_values["runno"] == RUN_NUMBER


def test_find_correct_tomo_dir_relative_path(monkeypatch):
    """Test find_correct_tomo_dir handles relative paths correctly"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        monkeypatch.chdir(tmpdirname)
        rel_path = Path("RB12345")
        rel_path.mkdir()
        (rel_path / "run100.csv").touch()
        tomo_dir = rel_path / "Tomo"
        tomo_dir.mkdir()

        result = find_correct_tomo_dir(rel_path, "100")
        assert result is not None
        assert result == tomo_dir
        assert result.exists()


def test_imat_ngem_tof_rule_adds_spec_values(job_request):
    """Test the nGEM reduction parameters of the specification are passed on to the job request"""
    IMATNgemToFRule(dict(NGEM_TOF_SPEC)).verify(job_request)
    assert job_request.additional_values == NGEM_TOF_SPEC


def test_imat_ngem_tof_rule_applies_to_reconstruction_runs(job_request):
    """Test the values are published for any IMAT run, the script only reads them in its nGEM branch"""
    job_request.additional_values["recon"] = "true"
    IMATNgemToFRule(dict(NGEM_TOF_SPEC)).verify(job_request)
    assert job_request.additional_values["start_tof"] == 20000  # noqa: PLR2004
    assert job_request.additional_values["recon"] == "true"


def test_imat_ngem_tof_rule_accepts_numbers_saved_as_strings(job_request):
    """Test the value editor saving specification numbers as strings still gives the script plain numbers"""
    IMATNgemToFRule({"start_tof": "25000", "end_tof": "90000.0", "bin_width": 20, "L": "55.25"}).verify(job_request)
    assert job_request.additional_values == {"start_tof": 25000, "end_tof": 90000, "bin_width": 20, "L": 55.25}


@pytest.mark.parametrize("key", NGEM_TOF_KEYS)
def test_imat_ngem_tof_rule_requires_every_value(key, job_request):
    """Test a specification missing one of the reduction parameters does not reduce the run"""
    spec = dict(NGEM_TOF_SPEC)
    del spec[key]
    with pytest.raises(RuleViolationError, match=key):
        IMATNgemToFRule(spec).verify(job_request)


@pytest.mark.parametrize("value", ["abc", None, True, [10], "inf", float("nan")])
def test_imat_ngem_tof_rule_rejects_values_that_are_not_numbers(value, job_request):
    """Test a reduction parameter which is not a finite number does not reduce the run"""
    spec = dict(NGEM_TOF_SPEC, start_tof=value)
    with pytest.raises(RuleViolationError, match="start_tof"):
        IMATNgemToFRule(spec).verify(job_request)
