"""
Rule factory unit tests
"""

# pylint: disable=protected-access
import unittest
from typing import Type, Any
from unittest.mock import Mock, patch

import pytest

from rundetection.rules.common_rules import EnabledRule
from rundetection.rules.factory import rule_factory
from rundetection.rules.inter_rules import InterStitchRule
from rundetection.rules.mari_rules import MariStitchRule, MariMaskFileRule, MariWBVANRule
from rundetection.rules.rule import MissingRuleError, Rule
from rundetection.rules.tosca_rules import ToscaStitchRule


def assert_correct_rule(name: str, value: Any, rule_type: Type[Rule]):
    """
    Assert when given name and value is passed to factory, the given Rule Type is returned
    :param name: the rule name
    :param value: the rule value
    :param rule_type: the rule type
    :return: None
    """
    rule = rule_factory(name, value)
    assert isinstance(rule, rule_type)
    assert rule._value is value


@patch("rundetection.rules.mari_rules.MariStitchRule._load_mari_spec")
def test_rule_factory_returns_correct_rule(_: Mock) -> None:
    """
    Test that the rule factory will return the correct rule
    :return: None
    """
    assert_correct_rule("enabled", True, EnabledRule)
    assert_correct_rule("interstitch", True, InterStitchRule)
    assert_correct_rule("toscastitch", True, ToscaStitchRule)
    assert_correct_rule("maristitch", True, MariStitchRule)
    assert_correct_rule("marimaskfile", "foo", MariMaskFileRule)
    assert_correct_rule("mariwbvan", 12345, MariWBVANRule)


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
    with pytest.raises(ValueError):
        rule_factory("maristitch", 4)
    with pytest.raises(ValueError):
        rule_factory("marimaskfile", 5)
    with pytest.raises(ValueError):
        rule_factory("mariwbvan", 3.3)


if __name__ == "__main__":
    unittest.main()
