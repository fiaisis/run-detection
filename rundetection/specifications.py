"""
Contains the InstrumentSpecification class, the abstract Rule Class and Rule Implementations
"""

import json
from abc import ABC, abstractmethod
from typing import Any, List, TypeVar, Generic

from rundetection.ingest import NexusMetadata

T_co = TypeVar("T_co", str, bool, int, float, None, List[str], covariant=True)


class Rule(Generic[T_co], ABC):
    """
    Abstract Rule, implement to define a rule that must be followed to allow a reduction to be run on a nexus file
    """

    def __init__(self, value: T_co):
        self._value: T_co = value

    @abstractmethod
    def verify(self, metadata: NexusMetadata) -> bool:
        """
        Given a NexusMetadata determine if the rule is met for the file.
        :param metadata: The metadata to check
        :return: true if rule is met, else false
        """


class MissingRuleError(Exception):
    """
    When a Rule concretion is searched for but does not exist
    """


def rule_factory(key_: str, value: T_co) -> Rule[T_co]:
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

        case _:
            raise MissingRuleError(f"Implementation of Rule: {key_} does not exist.")


class InstrumentSpecification:
    """
    The instrument specification loads the rules from the relevant specification json file
    and allows verification of the rules given some nexus metadata
    """

    def __init__(self, instrument: str) -> None:
        self._instrument = instrument
        self._rules: List[Rule[Any]] = []
        self._load_rules()

    def _load_rules(self) -> None:
        with open(
            f"specifications/{self._instrument}_specification.json",
            "r",
            encoding="utf-8",
        ) as spec_file:
            spec: dict[str, Any] = json.load(spec_file)
            self._rules = [rule_factory(key, value) for key, value in spec.items()]

    def verify(self, metadata: NexusMetadata) -> bool:
        """
        Verify that every rule for the NexusMetadata is met, and that the specification contains at least one rule.
        If the specification is empty verify will return false
        :param metadata:
        :return: whether the specification is met
        """
        for rule in self._rules:
            if not rule.verify(metadata):
                return False
        return len(self._rules) != 0


class EnabledRule(Rule[bool]):
    """
    Rule concretion for the enabled setting in specifications.
    """

    def verify(self, metadata: NexusMetadata) -> bool:
        return self._value
