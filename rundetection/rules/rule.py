"""
Module containing the abstract base Rule class and MissingRuleError
"""
from abc import ABC, abstractmethod
from typing import Generic, TypeVar, List

from rundetection.ingest import DetectedRun

T_co = TypeVar("T_co", str, bool, int, float, None, List[str], covariant=True)


class Rule(Generic[T_co], ABC):
    """
    Abstract Rule, implement to define a rule that must be followed to allow a reduction to be run on a nexus file
    """

    def __init__(self, value: T_co):
        self._value: T_co = value

    @abstractmethod
    def verify(self, run: DetectedRun) -> None:
        """
        Given a DetectedRun determine if the rule is met for the file.
        :param run: The Detected to check
        :return: true if rule is met, else false
        """


class MissingRuleError(Exception):
    """
    When a Rule concretion is searched for but does not exist
    """
