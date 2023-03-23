"""
Unit tests for common rles
"""
# pylint: disable=redefined-outer-name
import unittest
from pathlib import Path

import pytest

from rundetection.ingest import DetectedRun
from rundetection.rules.common_rules import EnabledRule


@pytest.fixture
def run():
    """
    DetectedRun Fixture
    :return: DetectedRun
    """
    return DetectedRun(1, "larmor", "1", "1", Path("/archive/larmor/1/1.nxs"), "start time", "end time", 1, 1, "users")


def test_enabled_rule_when_enabled(run) -> None:
    """
    Test verify method will return true when value is true
    :param run: DetectedRun fixture
    :return: None
    """
    rule = EnabledRule(True)
    rule.verify(run)
    assert run.will_reduce


def test_enabled_rule_when_not_enabled(run: DetectedRun) -> None:
    """
    Test verify method will return false when value is false
    :param run: DetectedRun fixture
    :return: None
    """
    rule = EnabledRule(False)
    rule.verify(run)
    assert run.will_reduce is False


if __name__ == "__main__":
    unittest.main()
