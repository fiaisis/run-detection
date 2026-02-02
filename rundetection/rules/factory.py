"""Module containing the factory function for each rule."""

from typing import Any

from rundetection.rules.common_rules import (
    EnabledRule,
    MolSpecStitchRule,
)
from rundetection.rules.enginx_rules import (
    EnginxCeriaPathRule,
    EnginxGroupRule,
    EnginxVanadiumPathRule,
)
from rundetection.rules.imat_rules import IMATFindImagesRule
from rundetection.rules.inter_rules import InterStitchRule
from rundetection.rules.iris_rules import IrisCalibrationRule, IrisReductionRule
from rundetection.rules.mari_rules import MariGitShaRule, MariMaskFileRule, MariStitchRule, MariWBVANRule
from rundetection.rules.osiris_rules import (
    OsirisDefaultGraphiteAnalyser,
    OsirisDefaultSpectroscopy,
    OsirisReductionModeRule,
    OsirisReflectionCalibrationRule,
)
from rundetection.rules.rule import MissingRuleError, Rule, T
from rundetection.rules.sans_rules import (
    SansCanFiles,
    SansPhiLimits,
    SansScatterTransFiles,
    SansSliceWavs,
    SansUserFile,
)
from rundetection.rules.vesuvio_rules import (
    VesuvioDiffIPFileRule,
    VesuvioEmptyRunsRule,
    VesuvioIPFileRule,
    VesuvioSumRunsRule,
)


def rule_factory(key_: str, value: T) -> Rule[Any]:  # noqa: C901, PLR0911, PLR0912, PLR0915
    """
    Return the rule implementation for the given rule key and value.

    :param key_: The key of the rule
    :param value: The value of the rule
    :return: The Rule implementation.
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
            if isinstance(value, int | str):
                return MariWBVANRule(int(value))
        case "git_sha":
            if isinstance(value, str):
                return MariGitShaRule(value)
        case "osiriscalibfilesandreflection":
            if isinstance(value, dict):
                return OsirisReflectionCalibrationRule(value)
        case "osirisdefaultspectroscopy":
            if isinstance(value, bool):
                return OsirisDefaultSpectroscopy(value)
        case "osirisdefaultgraphiteanalyser":
            if isinstance(value, bool):
                return OsirisDefaultGraphiteAnalyser(value)
        case "osirisreductionmode":
            if isinstance(value, bool):
                return OsirisReductionModeRule(value)
        case "sansscattertransfiles":
            if isinstance(value, bool):
                return SansScatterTransFiles(value)
        case "sansuserfile":
            if isinstance(value, str):
                return SansUserFile(value)
        case "sanscanfiles":
            if isinstance(value, bool):
                return SansCanFiles(value)
        case "sansphilimits":
            if isinstance(value, str):
                return SansPhiLimits(value)
        case "sansslicewavs":
            if isinstance(value, str):
                return SansSliceWavs(value)
        case "irisreduction":
            if isinstance(value, bool):
                return IrisReductionRule(value)
        case "iriscalibration":
            if isinstance(value, dict):
                return IrisCalibrationRule(value)
        case "vesuviovemptyrunsrule":
            if isinstance(value, str):
                return VesuvioEmptyRunsRule(value)
        case "vesuvioipfilerule":
            if isinstance(value, str):
                return VesuvioIPFileRule(value)
        case "vesuviodiffipfilerule":
            if isinstance(value, str):
                return VesuvioDiffIPFileRule(value)
        case "vesuviosumruns":
            if isinstance(value, bool):
                return VesuvioSumRunsRule(value)
        case "enginxvanadiumrun":
            if isinstance(value, int | str):
                return EnginxVanadiumPathRule(value)
        case "enginxceriarun":
            if isinstance(value, int | str):
                return EnginxCeriaPathRule(value)
        case "enginxgroup":
            if isinstance(value, str):
                return EnginxGroupRule(value)
        case "imatfindimages":
            if isinstance(value, bool):
                return IMATFindImagesRule(value)
        case _:
            raise MissingRuleError(f"Implementation of Rule: {key_} does not exist.")

    raise ValueError(f"Bad value: {value} in rule: {key_}")
