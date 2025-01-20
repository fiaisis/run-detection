"""
Module containing the factory function for each rule
"""

from typing import Any

from rundetection.rules.common_rules import CheckIfScatterSANS, EnabledRule, MolSpecStitchRule, SansSliceWavs
from rundetection.rules.inter_rules import InterStitchRule
from rundetection.rules.iris_rules import IrisCalibrationRule, IrisReductionRule
from rundetection.rules.loq_rules import LoqFindFiles, LoqUserFile
from rundetection.rules.mari_rules import MariMaskFileRule, MariStitchRule, MariWBVANRule
from rundetection.rules.osiris_rules import (
    OsirisDefaultGraniteAnalyser,
    OsirisDefaultSpectroscopy,
    OsirisReductionModeRule,
    OsirisReflectionCalibrationRule,
)
from rundetection.rules.rule import MissingRuleError, Rule, T


def rule_factory(key_: str, value: T) -> Rule[Any]:  # noqa: C901, PLR0911, PLR0912
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
        case "molspecstitch":
            if isinstance(value, bool):
                return MolSpecStitchRule(value)
        case "maristitch":
            if isinstance(value, bool):
                return MariStitchRule(value)
        case "marimaskfile":
            if isinstance(value, str):
                return MariMaskFileRule(value)
        case "mariwbvan":
            if isinstance(value, int):
                return MariWBVANRule(value)
        case "osiriscalibfilesandreflection":
            if isinstance(value, dict):
                return OsirisReflectionCalibrationRule(value)
        case "osirisdefaultspectroscopy":
            if isinstance(value, bool):
                return OsirisDefaultSpectroscopy(value)
        case "osirisdefaultgraniteanalyser":
            if isinstance(value, bool):
                return OsirisDefaultGraniteAnalyser(value)
        case "osirisreductionmode":
            if isinstance(value, bool):
                return OsirisReductionModeRule(value)
        case "checkifscattersans":
            if isinstance(value, bool):
                return CheckIfScatterSANS(value)
        case "loqfindfiles":
            if isinstance(value, bool):
                return LoqFindFiles(value)
        case "loquserfile":
            if isinstance(value, str):
                return LoqUserFile(value)
        case "sansslicewavs":
            if isinstance(value, str):
                return SansSliceWavs(value)
        case "irisreduction":
            if isinstance(value, bool):
                return IrisReductionRule(value)
        case "iriscalibration":
            if isinstance(value, dict):
                return IrisCalibrationRule(value)
        case _:
            raise MissingRuleError(f"Implementation of Rule: {key_} does not exist.")

    raise ValueError(f"Bad value: {value} in rule: {key_}")
