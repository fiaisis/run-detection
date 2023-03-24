"""
Tests for inter specific rules
"""
import unittest
from pathlib import Path
from unittest.mock import patch

from rundetection.ingest import DetectedRun
from rundetection.rules.inter_rules import InterStitchRule


@patch("rundetection.rules.inter_rules.get_sibling_runs")
def test_verify(mock_get_siblings):
    """
    Tests that additional files from the same run are added to the additional values, while ignoring unrelated
    :param mock_get_siblings: mocked function
    :return: (None)
    """
    run = DetectedRun(
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
    related_run = DetectedRun(
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
    unrelated_run = DetectedRun(
        1, "inst", "ost 300mM NaCl  th=2.3", "sd", Path("/archive/foo"), "start time", "end time", 1, 1, "users"
    )
    mock_get_siblings.return_value = [related_run, unrelated_run]
    rule = InterStitchRule(True)

    rule.verify(run)
    assert str(related_run.filepath) in run.additional_values["additional_files"]
    assert str(unrelated_run.filepath) not in run.additional_values["additional_files"]


if __name__ == "__main__":
    unittest.main()
