"""
Contains the InstrumentSpecification class, the abstract Rule Class and Rule Implementations
"""

import json
import logging
import typing
from pathlib import Path

from rundetection.exceptions import RuleViolationError
from rundetection.job_requests import JobRequest
from rundetection.rules.factory import rule_factory

if typing.TYPE_CHECKING:
    from typing import Any

    from rundetection.rules.rule import Rule

logger = logging.getLogger(__name__)


class InstrumentSpecification:
    """
    The instrument specification loads the rules from the relevant specification json file
    and allows verification of the rules given some nexus metadata
    """

    def __init__(self, instrument: str) -> None:
        logger.info("Loading instrument specification for: %s", instrument)
        self._instrument = instrument
        self._rules: list[Rule[Any]] = []
        self._load_rules()

    def _load_rules(self) -> None:
        try:
            path = Path(f"rundetection/specifications/{self._instrument.lower()}_specification.json")
            with path.open(encoding="utf-8") as spec_file:
                spec: dict[str, Any] = json.load(spec_file)
                self._rules = [rule_factory(key, value) for key, value in spec.items()]
        except FileNotFoundError:
            logger.error("No specification for file: %s", self._instrument)
            raise

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify that every rule for the JobRequest is met, and that the specification contains at least one rule.
        If the specification is empty verify will return false
        :param job_request: A JobRequest
        :return: whether the specification is met
        """
        if len(self._rules) == 0:
            job_request.will_reduce = False
        for rule in self._rules:
            logger.info("verifying rule: %s", rule)
            try:
                rule.verify(job_request)
            except RuleViolationError:
                job_request.will_reduce = False

            if job_request.will_reduce is False:
                logger.info("Rule %s not met for run %s", rule, job_request)
                break  # Stop processing as soon as one rule is not met.
