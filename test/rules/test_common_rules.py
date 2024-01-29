"""
Unit tests for common rules
"""

# pylint: disable=redefined-outer-name
import unittest
from pathlib import Path

import pytest

from rundetection.ingest import JobRequest
from rundetection.rules.common_rules import EnabledRule


@pytest.fixture
def job_request():
    """
    job_request Fixture
    :return: JobRequest
    """
    return JobRequest(1, "larmor", "1", "1", Path("/archive/larmor/1/1.nxs"), "start time", "end time", 1, 1, "users")


def test_enabled_rule_when_enabled(job_request) -> None:
    """
    Test verify method will return true when value is true
    :param job_request: JobRequest fixture
    :return: None
    """
    rule = EnabledRule(True)
    rule.verify(job_request)
    assert job_request.will_reduce


def test_enabled_rule_when_not_enabled(job_request) -> None:
    """
    Test verify method will return false when value is false
    :param job_request: JobRequest fixture
    :return: None
    """
    rule = EnabledRule(False)
    rule.verify(job_request)
    assert job_request.will_reduce is False


if __name__ == "__main__":
    unittest.main()
