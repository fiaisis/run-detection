"""
Rule factory unit tests
"""
# pylint: disable=protected-access
import unittest

import pytest

from rundetection.rules.common_rules import EnabledRule
from rundetection.rules.factory import rule_factory
from rundetection.rules.rule import MissingRuleError


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


if __name__ == "__main__":
    unittest.main()
