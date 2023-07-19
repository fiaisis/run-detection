"""
Mari Rules
"""
import logging
from copy import deepcopy
from pathlib import Path
from typing import List

from rundetection.ingest import JobRequest, get_run_title
from rundetection.rules.rule import Rule

logger = logging.getLogger(__name__)


class MariStitchRule(Rule[bool]):
    """
    The MariStitchRule is the rule that applies
    """

    @staticmethod
    def _get_previous_run_path(run_number: int, run_path: Path) -> Path:
        return Path(run_path.parent, f"MAR{run_number - 1}.nxs")

    def _get_runs_to_stitch(self, run_path: Path, run_number: int, run_title: str) -> List[int]:
        run_numbers = []
        while run_path.exists():
            if get_run_title(run_path) != run_title:
                break
            run_numbers.append(run_number)
            run_number -= 1
            run_path = self._get_previous_run_path(run_number, run_path)
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
