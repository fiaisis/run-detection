"""Tests for IMAT's rules"""

import os
import tempfile
from pathlib import Path

import pytest

from rundetection.exceptions import RuleViolationError
from rundetection.job_requests import JobRequest
from rundetection.rules.imat_rules import IMATFindImagesRule


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


def test_imat_find_images_success(job_request):
    """Test imat rules can find images successfully"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Setup
        os.environ["IMAT_DIR"] = tmpdirname
        exp_dir = Path(tmpdirname).joinpath("RB12345")
        exp_dir.mkdir(parents=True, exist_ok=True)
        # Create required structure: a file with run number and a Tomo dir
        exp_dir.joinpath("run100.csv").touch()
        tomo_dir = exp_dir.joinpath("Tomo")
        tomo_dir.mkdir()

        # Test
        rule = IMATFindImagesRule(True)
        rule.verify(job_request)

        # Assertions
        assert len(job_request.additional_values) == 2
        assert job_request.additional_values["images_dir"] == str(tomo_dir)
        assert job_request.additional_values["runno"] == 100


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
        exp_dir.joinpath("z_run100.csv").touch()

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

        exp_dir.joinpath("0_run100.csv").touch()
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
        exp_dir = Path(tmpdirname).joinpath("RB12345")
        exp_dir.mkdir(parents=True, exist_ok=True)
        exp_dir.joinpath("run100.csv").touch()

        # Test
        rule = IMATFindImagesRule(True)
        with pytest.raises(RuleViolationError):
            rule.verify(job_request)


def test_imat_find_images_missing_run_file(job_request):
    """Test imat rules fail when run number file is missing"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Setup
        os.environ["IMAT_DIR"] = tmpdirname
        exp_dir = Path(tmpdirname).joinpath("RB12345")
        exp_dir.mkdir(parents=True, exist_ok=True)
        exp_dir.joinpath("Tomo").mkdir()

        # Test
        rule = IMATFindImagesRule(True)
        with pytest.raises(RuleViolationError):
            rule.verify(job_request)


def test_imat_find_images_failure(job_request):
    """Test imat rules act's correctly when it fails to find experiment dir"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Setup
        os.environ["IMAT_DIR"] = tmpdirname

        # Test
        rule = IMATFindImagesRule(True)
        with pytest.raises(RuleViolationError):
            rule.verify(job_request)

        # Assertions
        assert len(job_request.additional_values) == 0
