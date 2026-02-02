"""Vesuvio Rules."""

import logging
from copy import deepcopy
from pathlib import Path

from rundetection.ingestion.ingest import get_run_title
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


class VesuvioDiffIPFileRule(Rule[str]):
    """Adds the diff_ip_file to JobRequest."""

    def verify(self, job_request: JobRequest) -> None:
        """
        Add the diffraction IP file to the job request's additional values.

        :param job_request: The job request to update with the diffraction IP file.
        """
        job_request.additional_values["diff_ip_file"] = self._value

class VesuvioSumRunsRule(Rule[bool]):
    """Groups multiple VESUVIO runs with the same title for summation."""

    def __init__(self, value: bool) -> None:
        """
        Initialize the VesuvioSumRunsRule.

        :param value: The value of the rule.
        :return: None.
        """
        super().__init__(value)
        self.should_be_last = True

    @staticmethod
    def _is_title_similar(title: str, other_title: str) -> bool:
        """
        Compare one run title to another to check for similarity.

        :param title: The first run title.
        :param other_title: The second run title.
        :return: (bool) True if similar False otherwise.
        """
        logger.info("Comparing titles %s and %s", title, other_title)
        return title == other_title

    def _get_runs_to_stitch(self, run_path: Path, run_number: int, run_title: str) -> list[int]:
        """
        Get runs in order to stitch them together.

        :param run_path: The path to the runs.
        :param run_number: The run number for starting run.
        :param run_title: The title of the first run.
        """
        run_numbers = []
        while run_path.exists():
            if not self._is_title_similar(get_run_title(run_path), run_title):
                break
            run_numbers.append(run_number)
            run_number -= 1
            # Vesuvio files in archive usually follow VESUVIO000XXXXX.nxs (8 digits total)
            next_run_filename = f"VESUVIO{run_number:08d}.nxs"
            run_path = Path(run_path.parent, next_run_filename)
        return sorted(run_numbers)

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request.

        :param job_request: The job request to verify.
        """
        if not self._value:
            return

        logger.info("Checking stitch conditions for VESUVIO run %s", job_request.filepath)

        run_numbers = self._get_runs_to_stitch(
            job_request.filepath, job_request.run_number, job_request.experiment_title
        )

        if len(run_numbers) > 1:
            additional_request = deepcopy(job_request)
            # Use the range/list of runs for runno
            additional_request.additional_values["runno"] = run_numbers
            additional_request.additional_values["sum_runs"] = True

            # Ensure other vesuvio rules values are preserved
            if "ip_file" in job_request.additional_values:
                additional_request.additional_values["ip_file"] = job_request.additional_values["ip_file"]
            if "empty_runs" in job_request.additional_values:
                additional_request.additional_values["empty_runs"] = job_request.additional_values["empty_runs"]
            if "diff_ip_file" in job_request.additional_values:
                additional_request.additional_values["diff_ip_file"] = job_request.additional_values["diff_ip_file"]

            job_request.additional_requests.append(additional_request)
