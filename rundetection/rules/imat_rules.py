"""Rules for Iris."""

from __future__ import annotations

import logging
import os
import typing
from pathlib import Path

from rundetection.exceptions import RuleViolationError
from rundetection.rules.rule import Rule

if typing.TYPE_CHECKING:
    from rundetection.job_requests import JobRequest

logger = logging.getLogger(__name__)


def find_correct_tomo_dir(path: Path, run_number: str) -> Path | None:
    """
    Check a directory for the IMAT image structure.

    This looks for a file containing the run number and a directory named 'Tomo'.

    :param path: The path to the experiment directory.
    :param run_number: The run number to check for.
    :return: The path to the Tomo directory if found, otherwise None.
    """
    for root, _, files in path.walk(top_down=False):
        for file in files:
            if run_number in file:
                # There are 2 known scenarios we are at the same level as Tomo, or in a directory one level lower.
                possible_tomo = root.parent / "Tomo"
                if root.parent.name == "Tomo":
                    return root.parent
                elif possible_tomo.exists():
                    return possible_tomo
    return None


class IMATFindImagesRule(Rule[bool]):
    """Finds the IMAT image files"""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request. Find the IMAT image files and prep for the script.

        :param job_request: The job request to verify.
        :return: None.
        """
        # Assume that the imat directory is loaded.
        imat_root_dir = os.environ.get("IMAT_DIR", "/imat")

        exp_dir_path = Path(imat_root_dir) / f"RB{job_request.experiment_number}"
        imat_dir_path: Path | None = None

        if exp_dir_path.exists() and exp_dir_path.is_dir():
            imat_dir_path = find_correct_tomo_dir(exp_dir_path, str(job_request.run_number))

        if imat_dir_path is not None and imat_dir_path.exists():
            job_request.additional_values["images_dir"] = str(imat_dir_path)
            job_request.additional_values["runno"] = job_request.run_number
        else:
            logger.error("Images dir could not be found for experiment number: %s", job_request.experiment_number)
            raise RuleViolationError(
                "Images dir could not be found for experiment number: %s", job_request.experiment_number
            )
