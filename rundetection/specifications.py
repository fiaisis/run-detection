"""Contains the InstrumentSpecification class, the abstract Rule Class and Rule Implementations."""

import datetime
import logging
import os
import typing

import requests

from rundetection.exceptions import RuleViolationError
from rundetection.job_requests import JobRequest
from rundetection.rules.factory import rule_factory

if typing.TYPE_CHECKING:
    from typing import Any

    from rundetection.rules.rule import Rule

logger = logging.getLogger(__name__)

FIA_API_URL = os.getenv("FIA_API_URL", "http://localhost:8000")
SPEC_REQUEST_TIMEOUT_MINS = 10


class InstrumentSpecification:
    """
    The instrument specification loads the rules from the FIA API
    and allows verification of the rules given some nexus metadata.
    """

    def __init__(self, instrument: str) -> None:
        """
        Initialize the InstrumentSpecification with the given instrument.

        :param str instrument: The instrument name to load rules for.
        """
        self._instrument = instrument
        self._rules: list[Rule[Any]] = []
        self.loaded_time: datetime.datetime | None = None
        self._load_rules_from_api()

    def _load_rules_from_api(self) -> None:
        logger.info("Requesting specification from API for %s", self._instrument)
        fia_api_api_key = os.environ["FIA_API_API_KEY"]
        headers: dict[str, Any] = {"Authorization": f"Bearer {fia_api_api_key}", "accept": "application/json"}
        response = requests.get(
            url=f"{FIA_API_URL}/instrument/{self._instrument.upper()}/specification", headers=headers, timeout=1
        )
        response.raise_for_status()
        spec: dict[str, Any] = response.json()
        logger.info("Response from API for spec is: \n%s", spec)
        self._rules = [rule_factory(key, value) for key, value in spec.items()]
        self._order_rules()
        self.loaded_time = datetime.datetime.now(tz=datetime.UTC)
        logger.info("Loaded instrument specification for: %s at: %s", self._instrument, self.loaded_time)

    def _order_rules(self) -> None:
        """
        Ensure rules are ordered correctly.

        Some rules need to be at the end of the list, notably those with stitch in the name.
        """
        for rule in self._rules:
            # We need to ensure rules that do a stitch, or any that added extra jobs, need to come last.
            if rule.should_be_last:
                self._rules.remove(rule)
                self._rules.append(rule)
            if rule.should_be_first:
                self._rules.remove(rule)
                self._rules.insert(0, rule)

    def _rule_old(self) -> bool:
        return self.loaded_time is None or datetime.timedelta(minutes=SPEC_REQUEST_TIMEOUT_MINS) < (
            datetime.datetime.now(tz=datetime.UTC) - self.loaded_time
        )

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify that every rule for the JobRequest is met, and that the specification contains at least one rule.
        If the specification is empty verify will return false
        :param job_request: A JobRequest
        :return: whether the specification is met.
        """
        if self._rule_old():
            logger.info(
                "Rule for instrument %s is older than %s minutes, reloading rule from API",
                self._instrument,
                SPEC_REQUEST_TIMEOUT_MINS,
            )
            self._load_rules_from_api()
        if len(self._rules) == 0:
            job_request.will_reduce = False
        for rule in self._rules:
            logger.info("verifying rule: %s", rule)
            try:
                rule.verify(job_request)
            except RuleViolationError:
                job_request.will_reduce = False

            if not job_request.will_reduce:
                logger.info("Rule %s not met for run %s", rule, job_request)
                break  # Stop processing as soon as one rule is not met.
