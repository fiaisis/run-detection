"""Module for inter specific rules."""

from rundetection.ingestion.ingest import get_sibling_runs
from rundetection.job_requests import JobRequest
from rundetection.rules.rule import Rule


class InterStitchRule(Rule[bool]):

    """Rule for collecting each related inter run and including them into the additional values."""

    def __init__(self, value: bool) -> None:
        """
        Initialize the InterStitchRule.

        :param value: The value of the rule.
        :return: None.
        """
        super().__init__(value)
        self.should_be_last = True

    @staticmethod
    def _get_run_group(job_request: JobRequest) -> str:
        """
        Return the experiment string preceding the 'th=' marker in the experiment title.

        :param job_request: The job request.
        :return: The run group string.
        """
        index = job_request.experiment_title.rfind("th=")
        return job_request.experiment_title[0:index]

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request.

        Collects related inter runs and adds them to the additional values.

        :param job_request: The job request to verify.
        :return: None.
        """
        sibling_runs = get_sibling_runs(job_request.filepath)
        run_group = self._get_run_group(job_request)
        additional_files = [str(run_.filepath) for run_ in sibling_runs if self._get_run_group(run_) == run_group]
        job_request.additional_values["additional_files"] = additional_files
