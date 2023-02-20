"""
Module containing rule implementations for instrument shared rules
"""
from rundetection.ingest import DetectedRun
from rundetection.rules.rule import Rule


class EnabledRule(Rule[bool]):
    """
    Rule for the enabled setting in specifications. If enabled is True, the run will be reduced, if not,
    it will be skipped
    """

    def verify(self, run: DetectedRun) -> None:
        run.will_reduce = self._value
