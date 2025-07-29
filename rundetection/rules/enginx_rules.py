"""Enginx Rules."""

from rundetection.job_requests import JobRequest
from rundetection.rules.rule import Rule


class EnginxVanadiumRunRule(Rule[int | str]):

    """Insert the vanadium run number into the JobRequest"""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request.

        Adds the vanadium run number to the additional values.

        :param job_request: The job request to verify.
        :return: None.
        """
        job_request.additional_values["vanadium_run"] = self._value


class EnginxCeriaRunRule(Rule[int | str]):

    """
    Insert the ceria run number into the JobRequest

    This value is used for calibration.
    """

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request.

        Adds the ceria run number to the additional values.

        :param job_request: The job request to verify.
        :return: None.
        """
        job_request.additional_values["ceria_run"] = self._value
