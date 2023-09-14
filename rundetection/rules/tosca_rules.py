"""Rules for TOSCA"""
import logging
from copy import deepcopy
from pathlib import Path
from typing import List

from rundetection.ingest import JobRequest, get_run_title
from rundetection.rules.rule import Rule

logger = logging.getLogger(__name__)


class ToscaStitchRule(Rule[bool]):
    """
    Rule for stitching TOSCA Runs
    """

    @staticmethod
    def _is_title_similar(title: str, other_title: str) -> bool:
        return title.replace(" ", "")[:-4] == other_title.replace(" ", "")[:-4]

    def _get_runs_to_stitch(self, run_path: Path, run_number: int, run_title: str) -> List[int]:
        run_numbers = []
        while run_path.exists():
            if not self._is_title_similar(get_run_title(run_path), run_title):
                logger.info("titles not similar")
                break
            run_numbers.append(run_number)
            run_number -= 1
            run_path = Path(run_path.parent, f"TSC{run_number}.nxs")
        return run_numbers

    def verify(self, job_request: JobRequest) -> None:
        if not self._value:  # if the stitch rule is set to false, skip
            return

        job_request.additional_values["input_runs"] = [job_request.run_number]
        run_numbers = self._get_runs_to_stitch(
            job_request.filepath, job_request.run_number, job_request.experiment_title
        )

        if len(run_numbers) > 1:
            additional_request = deepcopy(job_request)
            additional_request.additional_values["input_runs"] = run_numbers
            job_request.additional_requests.append(additional_request)
