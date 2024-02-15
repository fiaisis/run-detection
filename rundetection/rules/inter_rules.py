"""
Module for inter specific rules
"""

from rundetection.ingestion.ingest import get_sibling_runs
from rundetection.job_requests import JobRequest
from rundetection.rules.rule import Rule


class InterStitchRule(Rule[bool]):
    """
    Rule for collecting each related inter run and including them into the additional values
    """

    @staticmethod
    def _get_run_group(job_request: JobRequest) -> str:
        """
        Given an inter job request, return the experiment string preceeding th=
        :param job_request: The job request
        :return: The run group string
        """
        index = job_request.experiment_title.rfind("th=")
        return job_request.experiment_title[0:index]

    def verify(self, job_request: JobRequest) -> None:
        sibling_runs = get_sibling_runs(job_request.filepath)
        additional_files = []
        run_group = self._get_run_group(job_request)
        for run_ in sibling_runs:
            if self._get_run_group(run_) == run_group:
                additional_files.append(str(run_.filepath))
        job_request.additional_values["additional_files"] = additional_files
