"""
Module containing the abstract base Rule class and MissingRuleError
"""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from rundetection.job_requests import JobRequest

T = TypeVar("T")


class Rule(Generic[T], ABC):
    """
    Abstract Rule, implement to define a rule that must be followed to allow a reduction to be run on a nexus file
    """

    def __init__(self, value: T):
        self._value: T = value

    @abstractmethod
    def verify(self, job_request: JobRequest) -> None:
        """
        Given a JobRequest determine if the rule is met for the file.
        :param job_request: The job request to check
        :return: true if rule is met, else false
        """


class MissingRuleError(Exception):
    """
    When a Rule concretion is searched for but does not exist
    """
