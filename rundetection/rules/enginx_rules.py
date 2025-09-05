"""Enginx Rules."""

from rundetection.exceptions import RuleViolationError
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


class EnginxGroupRule(Rule[str]):
    """Insert the group type into the JobRequest"""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request.

        Adds the group type to the additional values after validating it against a list of valid group types.

        :param job_request: The job request to verify.
        :raises RuleViolationError: If the group type is not in the list of valid groups.
        :return: None.
        """
        group = self._value

        valid_groups = ["both", "north", "south", "cropped", "custom", "texture20", "texture30"]

        if group.lower() not in valid_groups:
            raise RuleViolationError(f"Invalid group type: {group} for EnginxGroupRule")

        job_request.additional_values["group"] = group


class EnginxCeriaCycleRule(Rule[str]):
    """
    Insert the ceria cyle string into the JobRequest

    This value is used for calibration.
    """

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request.

        Adds the ceria run number to the additional values.

        :param job_request: The job request to verify.
        :return: None.
        """
        job_request.additional_values["ceria_cycle"] = self._value
