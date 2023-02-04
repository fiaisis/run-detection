"""
Contains the InstrumentSpecification class, the abstract Rule Class and Rule Implementations
"""

import json
import logging
from typing import Any, List

from rundetection.ingest import DetectedRun
from rundetection.rules.factory import rule_factory
from rundetection.rules.rule import Rule

logger = logging.getLogger(__name__)


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
        try:
            with open(
                    f"rundetection/specifications/{self._instrument.lower()}_specification.json",
                    "r",
                    encoding="utf-8",
            ) as spec_file:
                spec: dict[str, Any] = json.load(spec_file)
                self._rules = [rule_factory(key, value) for key, value in spec.items()]
        except FileNotFoundError:
            logger.error("No specification for file: %s", self._instrument)
            raise

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
