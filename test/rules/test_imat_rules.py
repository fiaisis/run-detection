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
        Path(tmpdirname).joinpath("RB12345").mkdir(parents=True, exist_ok=True)

        # Test
        rule = IMATFindImagesRule(True)
        rule.verify(job_request)

        # Assertions
        assert len(job_request.additional_values) == 1
        assert job_request.additional_values["images_dir"] == tmpdirname + "/RB12345"


def test_imat_find_images_failure(job_request):
    """Test imat rules act's correctly when it fails to find"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Setup
        os.environ["IMAT_DIR"] = tmpdirname

        # Test
        rule = IMATFindImagesRule(True)
        with pytest.raises(RuleViolationError):
            rule.verify(job_request)

        # Assertions
        assert len(job_request.additional_values) == 0
