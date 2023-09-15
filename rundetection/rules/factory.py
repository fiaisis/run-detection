"""
Module containing the factory function for each rule
"""
from typing import Any

from rundetection.rules.common_rules import EnabledRule
from rundetection.rules.inter_rules import InterStitchRule
from rundetection.rules.mari_rules import MariStitchRule, MariMaskFileRule, MariWBVANRule
from rundetection.rules.rule import MissingRuleError, T, Rule
from rundetection.rules.tosca_rules import ToscaStitchRule


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

            raise ValueError(f"Bad value: {value} in rule: {key_}")
        case "interstitch":
            if isinstance(value, bool):
                return InterStitchRule(value)
            raise ValueError(f"Bad value: {value} in rule {key_}")
        case "toscastitch":
            if isinstance(value, bool):
                return ToscaStitchRule(value)
            raise ValueError(f"Bad value: {value} in rule {key_}")
        case "maristitch":
            if isinstance(value, bool):
                return MariStitchRule(value)
            raise ValueError(f"Bad value: {value} in rule {key_}")
        case "marimaskfile":
            if isinstance(value, str):
                return MariMaskFileRule(value)
            raise ValueError(f"Bad value: {value} in rule {key_}")
        case "mariwbvan":
            if isinstance(value, int):
                return MariWBVANRule(value)
            raise ValueError(f"Bad value: {value} in rule {key_}")
        case _:
            raise MissingRuleError(f"Implementation of Rule: {key_} does not exist.")
