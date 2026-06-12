"""Tests for IMAT's rules"""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from rundetection.exceptions import RuleViolationError
from rundetection.job_requests import JobRequest
from rundetection.rules.imat_rules import IMATFindImagesRule

EXPECTED_ADDITIONAL_VALUES_LEN = 4
RUN_NUMBER = 100


@pytest.fixture
def job_request():
    """
    Job request fixture
    :return: job request.
    """
    return JobRequest(
        run_number=100,
        filepath=Path("/imat/MERLIN100.nxs"),
        experiment_title="Test experiment",
        additional_values={},
        additional_requests=[],
        raw_frames=0,
        good_frames=0,
        users="",
        run_start="",
        run_end="",
        instrument="MERLIN",
        experiment_number="12345",
    )


def test_delete_me(job_request):
    """Test imat rules can find images successfully"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Setup
        os.environ["IMAT_DIR"] = tmpdirname
        run_number = 36779
        job_request.run_number = run_number
        job_request.experiment_number = "2322026"
        # Create required structure: a file with run number and a Tomo dir
        exp_dir = Path(tmpdirname).joinpath("RB2322026")
        exp_dir.mkdir(parents=True, exist_ok=True)
        exp_dir.joinpath("run36779.csv").touch()
        tomo_dir = exp_dir.joinpath("Tomo")
        tomo_dir.mkdir()

        # Test
        rule = IMATFindImagesRule(True)
        rule.verify(job_request)

        # Assertions
        assert len(job_request.additional_values) == EXPECTED_ADDITIONAL_VALUES_LEN
        assert job_request.additional_values["images_dir"] == str(tomo_dir)
        assert job_request.additional_values["runno"] == run_number


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
        os.environ["NGEM_DIR"] = tmpdirname  # Also set this to avoid loading nexus
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
        os.environ["NGEM_DIR"] = tmpdirname
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
        os.environ["NGEM_DIR"] = tmpdirname

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
        os.environ["NGEM_DIR"] = tmpdirname

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
        expected_output_path = str(ngem_data_dir) + "_nxs/RUN"
        assert job_request.additional_values["ngem_output_path"] == expected_output_path


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
