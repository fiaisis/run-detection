"""Tests for inter specific rules."""

import unittest
from pathlib import Path
from unittest.mock import patch

from rundetection.ingestion.ingest import JobRequest
from rundetection.rules.inter_rules import InterStitchRule


@patch("rundetection.rules.inter_rules.get_sibling_runs")
def test_verify(mock_get_siblings):
    """
    Tests that additional files from the same run are added to the additional values, while ignoring unrelated
    :param mock_get_siblings: mocked function
    :return: (None).
    """
    job_request = JobRequest(
        1,
        "inst",
        "D2O/air h-DODAB ML Proteolip Thu post 300mM NaCl  th=2.3",
        "sd",
        Path("/archive/foo"),
        "start time",
        "end time",
        1,
        1,
        "users",
    )
    related_job_request = JobRequest(
        1,
        "inst",
        "D2O/air h-DODAB ML Proteolip Thu post 300mM NaCl  th=2.4",
        "sd",
        Path("/archive/foo/related.nxs"),
        "start time",
        "end time",
        1,
        1,
        "users",
    )
    unrelated_job_request = JobRequest(
        1, "inst", "ost 300mM NaCl  th=2.3", "sd", Path("/archive/foo"), "start time", "end time", 1, 1, "users"
    )
    mock_get_siblings.return_value = [related_job_request, unrelated_job_request]
    rule = InterStitchRule(True)

    rule.verify(job_request)
    assert str(related_job_request.filepath) in job_request.additional_values["additional_files"]
    assert str(unrelated_job_request.filepath) not in job_request.additional_values["additional_files"]


if __name__ == "__main__":
    unittest.main()
