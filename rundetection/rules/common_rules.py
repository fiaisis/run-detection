"""
Module containing rule implementations for instrument shared rules
"""
import logging

from rundetection.job_requests import JobRequest
from rundetection.rules.rule import Rule

logger = logging.getLogger(__name__)


class EnabledRule(Rule[bool]):
    """
    Rule for the enabled setting in specifications. If enabled is True, the run will be reduced, if not,
    it will be skipped
    """

    def verify(self, job_request: JobRequest) -> None:
        job_request.will_reduce = self._value


class NotAScatterFileError(Exception):
    pass


class CheckIfScatterSANS(Rule[bool]):
    def verify(self, job_request: JobRequest) -> None:
        if "_SANS/TRANS" not in job_request.experiment_title:
            job_request.will_reduce = False
            logger.error("Not a scatter run. Does not have _SANS/TRANS in the experiment title.")
        # If it has empty or direct in the title assume it is a direct run file instead of a normal scatter.
        if ("empty" in job_request.experiment_title or "EMPTY" in job_request.experiment_title or
                "direct" in job_request.experiment_title or "DIRECT" in job_request.experiment_title):
            job_request.will_reduce = False
            logger.error("If it is a scatter, contains empty or direct in the title and is assumed to be a scatter "
                         "for an empty can run.")
