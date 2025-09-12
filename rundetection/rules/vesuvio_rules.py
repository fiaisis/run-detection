"""Vesuvio Rules."""

import logging

from rundetection.job_requests import JobRequest
from rundetection.rules.rule import Rule

logger = logging.getLogger(__name__)


class VesuvioEmptyRunsRule(Rule[str]):
    """Adds the empty runs numbers to JobRequest."""

    def verify(self, job_request: JobRequest) -> None:
        """
        Add empty runs numbers to the job request's additional values.

        :param job_request: The job request to update with empty runs.
        """
        job_request.additional_values["empty_runs"] = self._value


class VesuvioIPFileRule(Rule[str]):
    """Adds the ip_file to JobRequest."""

    def verify(self, job_request: JobRequest) -> None:
        """
        Add the IP file to the job request's additional values.

        :param job_request: The job request to update with the IP file.
        """
        job_request.additional_values["ip_file"] = self._value
