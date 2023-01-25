"""
Specification unit test module
"""
# pylint: disable=protected-access, redefined-outer-name
import unittest
from unittest.mock import Mock, patch

import pytest
from _pytest.logging import LogCaptureFixture

from rundetection.ingest import NexusMetadata
from rundetection.specifications import InstrumentSpecification, rule_factory, EnabledRule, MissingRuleError


def test_rule_factory_returns_correct_rule() -> None:
    """
    Test that the rule factory will return the correct rule
    :return: None
    """
    rule = rule_factory("enabled", True)
    assert isinstance(rule, EnabledRule)
    assert rule._value is True


def test_raises_exception_for_missing_rule_class() -> None:
    """
    Test exception raised when non-existent rule name is given
    :return: None
    """
    with pytest.raises(MissingRuleError):
        rule_factory("foo", "bar")


def test_raises_exception_for_incorrect_rule_value_type() -> None:
    """
    Test exception raised when incorrect value type
    :return: None
    """
    with pytest.raises(ValueError):
        rule_factory("enabled", "string")
    with pytest.raises(ValueError):
        rule_factory("enabled", 1)


@pytest.fixture
@patch("rundetection.specifications.InstrumentSpecification._load_rules")
def specification(_) -> InstrumentSpecification:
    """
    InstrumentSpecification Fixture
    :return: InstrumentSpecification
    """
    return InstrumentSpecification("foo")


def test_specification_verify_passes_for_all_rules_pass(specification: InstrumentSpecification) -> None:
    """
    Test specification verify returns true when all rules pass
    :param specification: specification fixture
    :return: None
    """
    mock_rule = Mock()
    specification._rules = [mock_rule, mock_rule, mock_rule]
    mock_rule.verify.return_value = True

    metadata = NexusMetadata(1, "larmor", "1", "1")

    assert specification.verify(metadata) is True


def test_specification_verify_fails_when_a_rule_fails(specification) -> None:
    """
    Test Specification verify returns False when a rule fails
    :param specification: specification fixture
    :return: None
    """
    pass_rule, fail_rule = Mock(), Mock()
    pass_rule.verify.return_value = True
    fail_rule.verify.return_value = False
    specification._rules = [pass_rule, fail_rule]

    metadata = NexusMetadata(1, "larmor", "1", "1")

    assert specification.verify(metadata) is False


def test_specification_verify_fails_with_only_failing_rules(specification) -> None:
    """
    Test specification verify returns false with only failing rules
    :param specification:
    :return: None
    """
    rule = Mock()
    rule.verify.return_value = False
    specification._rules = [rule, rule]

    metadata = NexusMetadata(1, "larmor", "1", "1")

    assert specification.verify(metadata) is False


def test_specification_verify_fails_with_no_rules(specification) -> None:
    """
    Specification verify fails if a specification has no rules
    :param specification: Specification fixture
    :return: None
    """
    metadata = NexusMetadata(1, "larmor", "1", "1")
    assert specification.verify(metadata) is False


def test_specification_loads_correct_rule() -> None:
    """
    Test that the correct spec for each instrument is loaded. Currently specs can only have 1 rule, enabled is true
    or false
    :return: None
    """
    mari_specification = InstrumentSpecification("mari")
    chronus_specification = InstrumentSpecification("chronus")

    assert mari_specification.verify(NexusMetadata(1, "1", "1", "1"))
    assert chronus_specification.verify(NexusMetadata(1, "1", "1", "1")) is False


def test_enabled_rule_when_enabled() -> None:
    """
    Test verify method will return true when value is true
    :return: None
    """
    rule = EnabledRule(True)
    assert rule.verify(NexusMetadata(1, "1", "1", "1")) is True


def test_enabled_rule_when_not_enabled() -> None:
    """
    Test verify method will return false when value is false
    :return:
    """
    rule = EnabledRule(False)
    assert rule.verify(NexusMetadata(1, "1", "1", "1")) is False


def test_specification_file_missing(caplog: LogCaptureFixture):
    """
    Test logging and exception raised when specification file is missing
    :param caplog:
    :return:
    """
    with pytest.raises(FileNotFoundError):
        InstrumentSpecification("foo")

    assert "No specification for file: foo" in caplog.text


if __name__ == "__main__":
    unittest.main()
