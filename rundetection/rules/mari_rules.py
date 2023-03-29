"""
Mari Rules
"""
from copy import deepcopy
from pathlib import Path
from typing import List

from rundetection.ingest import DetectedRun, get_run_title
from rundetection.rules.rule import Rule


class MariStitchRule(Rule[bool]):
    """
    The MariStitchRule is the rule that applies
    """

    @staticmethod
    def _get_previous_run_path(run_number: int, run_path: Path) -> Path:
        return Path(run_path.parent, f"MARI{run_number - 1}.nxs")

    def _get_runs_to_stitch(self, run_path: Path, run_number: int, run_title: str) -> List[int]:
        run_numbers = []
        while run_path.exists():
            if get_run_title(run_path) != run_title:
                break
            run_numbers.append(run_number)
            run_number -= 1
            run_path = self._get_previous_run_path(run_number, run_path)
        return run_numbers

    def verify(self, run: DetectedRun) -> None:

        if not self._value:  # if the stitch rule is set to false, skip
            return

        run_numbers = self._get_runs_to_stitch(run.filepath, run.run_number, run.experiment_title)
        if len(run_numbers) > 1:
            additional_run = deepcopy(run)
            additional_run.additional_values["runno"] = run_numbers
            additional_run.additional_values["sum_runs"] = True
            run.additional_runs.append(additional_run)
