"""
Specification unit test module
"""
# pylint: disable=protected-access, redefined-outer-name
from unittest.mock import Mock, patch

import pytest
from _pytest.logging import LogCaptureFixture

from rundetection.ingest import DetectedRun
from rundetection.rules.common_rules import EnabledRule
from rundetection.specifications import InstrumentSpecification


@pytest.fixture
def run():
    """
    DetectedRun fixture
    :return: The detected run fixture
    """
    return DetectedRun(1, "larmor", "1", "1", "/archive/larmor/1/1,nxs")


@pytest.fixture
@patch("rundetection.specifications.InstrumentSpecification._load_rules")
def specification(_) -> InstrumentSpecification:
    """
    InstrumentSpecification Fixture
    :return: InstrumentSpecification
    """
    return InstrumentSpecification("foo")


def test_run_will_be_reduced_when_all_rules_are_ment(specification: InstrumentSpecification, run: DetectedRun) -> None:
    """
    Test that a run.will_reduce remains set to true when its relevant specification is met
    :param specification: specification fixture
    :param run: DetectedRun fixture
    :return: None
    """
    mock_rule = Mock()
    specification._rules = [mock_rule, mock_rule, mock_rule]
    specification.verify(run)
    assert run.will_reduce


def test_run_will_not_be_reduced_when_a_rule_is_not_met(specification, run) -> None:
    """
    Test that a run.will_reduce will be marked as false when its relvant specification is not met.
    :param specification: specification fixture
    :param run: DetectedRun fixture
    :return: None
    """
    mock_rule = Mock()
    mock_rule.verify.side_effect = lambda r: setattr(r, "will_reduce", False)
    specification._rules = [mock_rule]
    specification.verify(run)
    assert run.will_reduce is False


def test_specification_will_stop_checking_rules_on_first_failure(specification, run) -> None:
    """
    Tests that no further rules will be checked as soon as one fails
    :param specification: specification fixutre
    :param run: DetectedRun fixture
    :return: None
    """
    first_rule = Mock()
    first_rule.verify.side_effect = lambda r: setattr(r, "will_reduce", False)
    second_rule = Mock()
    specification._rules = [first_rule, second_rule]
    specification.verify(run)
    first_rule.verify.assert_called_once_with(run)
    second_rule.verify.assert_not_called()
    assert run.will_reduce is False


def test_run_will_not_be_reduced_for_a_no_rule_specification(specification, run: DetectedRun) -> None:
    """
    Test that run.will_reduce will be set to false when there are no rules in the relevant specification
    :param specification: Specification fixture
    :param run: DetectedRun fixture
    :return: None
    """
    specification.verify(run)
    assert run.will_reduce is False


def test_specification_rule_loading() -> None:
    """
    Test that the correct spec for each instrument is loaded. Currently specs can only have 1 rule, enabled is true
    or false
    :param run: Run Fixture
    :return: None
    """
    mari_specification = InstrumentSpecification("mari")
    chronus_specification = InstrumentSpecification("chronus")

    assert isinstance(mari_specification._rules[0], EnabledRule)
    assert mari_specification._rules[0]._value

    assert isinstance(chronus_specification._rules[0], EnabledRule)
    assert chronus_specification._rules[0]._value is False


def test_specification_file_missing(caplog: LogCaptureFixture):
    """
    Test logging and exception raised when specification file is missing
    :param caplog:
    :return:
    """
    with pytest.raises(FileNotFoundError):
        InstrumentSpecification("foo")

    assert "No specification for file: foo" in caplog.text
