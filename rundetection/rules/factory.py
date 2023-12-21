"""
Module containing the factory function for each rule
"""
from typing import Any

from rundetection.rules.common_rules import EnabledRule
from rundetection.rules.inter_rules import InterStitchRule
from rundetection.rules.mari_rules import MariStitchRule, MariMaskFileRule, MariWBVANRule
from rundetection.rules.osiris_rules import OsirisPanadiumRule, OsirisStitchRule, OsirisAnalyserRule, \
    OsirisReductionModeRule
from rundetection.rules.rule import MissingRuleError, T, Rule
from rundetection.rules.tosca_rules import ToscaStitchRule


# pylint: disable = too-many-returns
def rule_factory(key_: str, value: T) -> Rule[Any]:
    """
    Given the rule key, and rule value, return the rule implementation
    :param key_: The key of the rule
    :param value: The value of the rule
    :return: The Rule implementation
    """
    match key_.lower():
        case "enabled":
            if isinstance(value, bool):
                return EnabledRule(value)
        case "interstitch":
            if isinstance(value, bool):
                return InterStitchRule(value)
        case "toscastitch":
            if isinstance(value, bool):
                return ToscaStitchRule(value)
        case "maristitch":
            if isinstance(value, bool):
                return MariStitchRule(value)
        case "marimaskfile":
            if isinstance(value, str):
                return MariMaskFileRule(value)
        case "mariwbvan":
            if isinstance(value, int):
                return MariWBVANRule(value)
        case "osirispanadium":
            if isinstance(value, int):
                return OsirisPanadiumRule(value)
        case "osirisstitch":
            if isinstance(value, bool):
                return OsirisStitchRule(value)
        case "osirisanalyser":
            if isinstance(value, bool):
                return OsirisAnalyserRule(value)
        case "osirisreductionmode":
            if isinstance(value, bool):
                return OsirisReductionModeRule(value)
        case _:
            raise MissingRuleError(f"Implementation of Rule: {key_} does not exist.")

    raise ValueError(f"Bad value: {value} in rule: {key_}")
