"""Module containing the abstract base Rule class and MissingRuleError."""

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from rundetection.job_requests import JobRequest

T = TypeVar("T")


class Rule(Generic[T], ABC):

    """
    Abstract Rule, implement to define a rule.

    A rule must be followed to allow a reduction to be run on a nexus file.
    """

    def __eq__(self, other: Any) -> bool:
        """
        Compare two Rule instances for equality.

        :param other: The object to compare with
        :return: True if the objects are equal, False otherwise
        """
        return isinstance(other, type(self)) and self._value == other._value

    def __init__(self, value: T):
        """
        Initialize a new Rule instance.

        :param value: The value to associate with this rule
        """
        self._value: T = value
        self.should_be_last = False
        self.should_be_first = False

    def __hash__(self) -> int:
        """Hash the rule based on the value"""
        return hash(self._value)

    @abstractmethod
    def verify(self, job_request: JobRequest) -> None:
        """
        Given a JobRequest determine if the rule is met for the file.
        :param job_request: The job request to check
        :return: true if rule is met, else false.
        """


class MissingRuleError(Exception):

    """When a Rule concretion is searched for but does not exist."""
