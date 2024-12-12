"""
Mari Rules
"""

import logging
from copy import deepcopy
from pathlib import Path

from rundetection.ingestion.ingest import get_run_title
from rundetection.job_requests import JobRequest
from rundetection.rules.rule import Rule

logger = logging.getLogger(__name__)


class MariStitchRule(Rule[bool]):
    """
    The MariStitchRule is the rule that applies, dependent on the other rules running first. This runs last.
    """

    def __init__(self, value: bool) -> None:
        super().__init__(value)
        self.should_be_last = True

    @staticmethod
    def _get_runs_to_stitch(run_path: Path, run_number: int, run_title: str) -> list[int]:
        run_numbers = []
        while run_path.exists():
            if get_run_title(run_path) != run_title:
                break
            run_numbers.append(run_number)
            run_number -= 1
            run_path = Path(run_path.parent, f"MAR{run_number}.nxs")
        return run_numbers

    def verify(self, job_request: JobRequest) -> None:
        if not self._value:  # if the stitch rule is set to false, skip
            return

        run_numbers = self._get_runs_to_stitch(
            job_request.filepath, job_request.run_number, job_request.experiment_title
        )
        if len(run_numbers) > 1:
            additional_request = deepcopy(job_request)
            additional_request.additional_values["runno"] = run_numbers
            additional_request.additional_values["sum_runs"] = True
            # We must reapply the common mari rules manually here, if we apply the whole spec automatically it will
            # produce an infinite loop
            additional_request.additional_values["mask_file_link"] = job_request.additional_values["mask_file_link"]
            additional_request.additional_values["wbvan"] = job_request.additional_values["wbvan"]
            job_request.additional_requests.append(additional_request)


class MariMaskFileRule(Rule[str]):
    """
    Adds the permalink of the maskfile to the additional outputs
    """

    def verify(self, job_request: JobRequest) -> None:
        job_request.additional_values["mask_file_link"] = self._value


class MariWBVANRule(Rule[int]):
    """
    Inserts the cycles wbvan number into the script. This value is manually calculated by the MARI instrument scientist
    once per cycle.
    """

    def verify(self, job_request: JobRequest) -> None:
        job_request.additional_values["wbvan"] = self._value
