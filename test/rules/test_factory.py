"""Rule factory unit tests."""

import unittest
from typing import Any

import pytest

from rundetection.rules.common_rules import (
    EnabledRule,
    MolSpecStitchRule,
)
from rundetection.rules.enginx_rules import (
    EnginxCeriaPathRule,
    EnginxGroupRule,
    EnginxVanadiumPathRule,
)
from rundetection.rules.factory import rule_factory
from rundetection.rules.inter_rules import InterStitchRule
from rundetection.rules.iris_rules import IrisCalibrationRule, IrisReductionRule
from rundetection.rules.mari_rules import MariGitShaRule, MariMaskFileRule, MariStitchRule, MariWBVANRule
from rundetection.rules.osiris_rules import (
    OsirisDefaultGraniteAnalyser,
    OsirisDefaultSpectroscopy,
    OsirisReductionModeRule,
    OsirisReflectionCalibrationRule,
)
from rundetection.rules.rule import MissingRuleError, Rule
from rundetection.rules.sans_rules import (
    SansCanFiles,
    SansPhiLimits,
    SansScatterTransFiles,
    SansSliceWavs,
    SansUserFile,
)
from rundetection.rules.vesuvio_rules import VesuvioEmptyRunsRule, VesuvioIPFileRule


def assert_correct_rule(name: str, value: Any, rule_type: type[Rule]):
    """
    Assert when given name and value is passed to factory, the given Rule Type is returned
    :param name: the rule name
    :param value: the rule value
    :param rule_type: the rule type
    :return: None.
    """
    rule = rule_factory(name, value)
    assert isinstance(rule, rule_type)
    assert rule._value is value


@pytest.mark.parametrize(
    ("rule_key", "rule_value", "expected_rule"),
    [
        ("enabled", True, EnabledRule),
        ("interstitch", True, InterStitchRule),
        ("molspecstitch", True, MolSpecStitchRule),
        ("maristitch", True, MariStitchRule),
        ("marimaskfile", "foo", MariMaskFileRule),
        ("mariwbvan", 12345, MariWBVANRule),
        ("git_sha", "abc1234567", MariGitShaRule),
        ("osiriscalibfilesandreflection", {"002": "00148587", "004": "00148587"}, OsirisReflectionCalibrationRule),
        ("osirisdefaultspectroscopy", True, OsirisDefaultSpectroscopy),
        ("osirisdefaultgraniteanalyser", True, OsirisDefaultGraniteAnalyser),
        ("osirisreductionmode", True, OsirisReductionModeRule),
        ("sansscattertransfiles", True, SansScatterTransFiles),
        ("sansuserfile", "loquserfile.toml", SansUserFile),
        ("sanscanfiles", True, SansCanFiles),
        ("sansphilimits", "[(1.0, 2.0), (3.0, 4.0)]", SansPhiLimits),
        ("sansslicewavs", "[2.7, 3.7, 4.7, 5.7, 6.7, 8.7, 10.5]", SansSliceWavs),
        ("irisreduction", True, IrisReductionRule),
        ("iriscalibration", {"002": "00148587", "004": "00148587"}, IrisCalibrationRule),
        ("vesuvioipfilerule", "ip00001.par", VesuvioIPFileRule),
        ("vesuviovemptyrunsrule", "123-321", VesuvioEmptyRunsRule),
        ("enginxvanadiumrun", 12345, EnginxVanadiumPathRule),
        ("enginxceriarun", 34567, EnginxCeriaPathRule),
        ("enginxgroup", "north", EnginxGroupRule),
    ],
)
def test_rule_factory_returns_correct_rule(rule_key, rule_value, expected_rule):
    """
    Test that the rule factory will return the correct rule
    :param rule_key: The key to identify the rule
    :param rule_value: The value associated with the rule
    :param expected_rule: The expected rule class
    :return: None.
    """
    assert_correct_rule(rule_key, rule_value, expected_rule)


def test_mariwbvan_rule_factory_returns_correct_rule_int_and_str():
    """
    Test to ensure that the rule factory returns the correct Rule for MariWBVAN when using either a str or int to create
    the rule from the specification.
    """
    rule = rule_factory("mariwbvan", 12345)
    assert isinstance(rule, MariWBVANRule)
    assert rule._value == 12345  # noqa: PLR2004
    rule = rule_factory("mariwbvan", "12345")
    assert isinstance(rule, MariWBVANRule)
    assert rule._value == 12345  # noqa: PLR2004


def test_enginx_rules_factory_returns_correct_rule_int_and_str():
    """
    Test to ensure that the rule factory returns the correct Rule for Enginx rules when using either a str or int to
    create the rule from the specification.
    """
    # Test EnginxVanadiumPathRule
    rule = rule_factory("enginxvanadiumrun", 12345)
    assert isinstance(rule, EnginxVanadiumPathRule)
    assert rule._value == 12345  # noqa: PLR2004
    rule = rule_factory("enginxvanadiumrun", "12345")
    assert isinstance(rule, EnginxVanadiumPathRule)
    assert rule._value == "12345"  # value remains str for Enginx rules

    # Test EnginxCeriaPathRule
    rule = rule_factory("enginxceriarun", 34567)
    assert isinstance(rule, EnginxCeriaPathRule)
    assert rule._value == 34567  # noqa: PLR2004
    rule = rule_factory("enginxceriarun", "34567")
    assert isinstance(rule, EnginxCeriaPathRule)
    assert rule._value == "34567"


def test_raises_exception_for_missing_rule_class() -> None:
    """
    Test exception raised when non-existent rule name is given
    :return: None.
    """
    with pytest.raises(MissingRuleError):
        rule_factory("foo", "bar")


def test_raises_exception_for_incorrect_rule_value_type() -> None:
    """
    Test exception raised when incorrect value type
    :return: None.
    """
    with pytest.raises(ValueError):  # noqa: PT011
        rule_factory("enabled", "string")
    with pytest.raises(ValueError):  # noqa: PT011
        rule_factory("enabled", 1)
    with pytest.raises(ValueError):  # noqa: PT011
        rule_factory("maristitch", 4)
    with pytest.raises(ValueError):  # noqa: PT011
        rule_factory("marimaskfile", 5)
    with pytest.raises(ValueError):  # noqa: PT011
        rule_factory("mariwbvan", 3.3)
    with pytest.raises(ValueError):  # noqa: PT011
        rule_factory("git_sha", 4)
    with pytest.raises(ValueError):  # noqa: PT011
        rule_factory("enginxvanadiumrun", 3.3)
    with pytest.raises(ValueError):  # noqa: PT011
        rule_factory("enginxceriarun", 3.3)
    with pytest.raises(ValueError):  # noqa: PT011
        rule_factory("enginxgroup", 123)


if __name__ == "__main__":
    unittest.main()
