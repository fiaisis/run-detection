"""
Module for inter specific rules
"""

from rundetection.ingest import DetectedRun, get_sibling_runs
from rundetection.rules.rule import Rule


class InterStitchRule(Rule[bool]):
    """
    Rule for collecting each related inter run and including them into the additional values
    """

    @staticmethod
    def _get_run_group(run: DetectedRun) -> str:
        """
        Given a detected inter run, return the experiment string preceeding th=
        :param run: The detected run
        :return: The run group string
        """
        index = run.experiment_title.rfind("th=")
        return run.experiment_title[0:index]

    def verify(self, run: DetectedRun) -> None:
        sibling_runs = get_sibling_runs(run.filepath)
        additional_files = []
        run_group = self._get_run_group(run)
        for run_ in sibling_runs:
            if self._get_run_group(run_) == run_group:
                additional_files.append(str(run_.filepath))
        run.additional_values["additional_files"] = additional_files
