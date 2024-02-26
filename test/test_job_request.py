"""
Tests for JobRequest class
"""
from pathlib import Path

from rundetection.job_requests import JobRequest


def test_to_json_string() -> None:
    """
    Test valid json string can be built from metadata
    :return: None
    """
    job_request = JobRequest(
        run_number=12345,
        instrument="LARMOR",
        experiment_number="54321",
        experiment_title="my experiment",
        filepath=Path("e2e_data/1920302/ALF82301.nxs"),
        run_start="2015-07-01T15:29:17",
        run_end="2015-07-01T15:53:16",
        raw_frames=23740,
        good_frames=18992,
        users="Keiran",
    )
    assert (
        job_request.to_json_string() == '{"run_number": 12345, "instrument": "LARMOR", "experiment_title": '
        '"my experiment", "experiment_number": "54321", "filepath": '
        '"e2e_data/1920302/ALF82301.nxs", "run_start": "2015-07-01T15:29:17", '
        '"run_end": "2015-07-01T15:53:16", "raw_frames": 23740, "good_frames": 18992, '
        '"users": "Keiran", "additional_values": {}}'
    )
