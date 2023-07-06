"""
Module containing rule implementations for instrument shared rules
"""
from rundetection.ingest import JobRequest
from rundetection.rules.rule import Rule


class EnabledRule(Rule[bool]):
    """
    Rule for the enabled setting in specifications. If enabled is True, the run will be reduced, if not,
    it will be skipped
    """

    def verify(self, job_request: JobRequest) -> None:
        job_request.will_reduce = self._value
