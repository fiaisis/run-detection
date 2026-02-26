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


def check_file(path: Path, run_number: str) -> bool:
    """
    Check if a file matches the run number.

    :param path: The path to the file.
    :param run_number: The run number to check for.
    :return: True if it is a file and the run number is in the name.
    """
    return path.is_file() and run_number in path.name


def check_dir(path: Path, run_number: str) -> Path | None:
    """
    Check a directory for the IMAT image structure.

    This looks for a file containing the run number and a directory named 'Tomo'.

    :param path: The path to the experiment directory.
    :param run_number: The run number to check for.
    :return: The path to the Tomo directory if found, otherwise None.
    """
    tomo = None
    file_found = False
    for child in path.iterdir():
        is_file = check_file(child, run_number)
        if is_file:
            # File found
            if tomo is not None:
                # Tomo is already known
                return tomo
            if not file_found and tomo is None:
                # Wait until we find the Tomo folder
                file_found = True
        if child.is_dir() and child.name == "Tomo":
            # Found a potential Tomo dir
            tomo = path / child
            if file_found:
                # Tomo and file found, now return
                return tomo
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

        imat_dir_path = Path(imat_root_dir) / f"RB{job_request.experiment_number}"

        if imat_dir_path.exists() and imat_dir_path.is_dir():
            # Find file with run number in it, often ending with .csv, then search for a dir in the same directory as it
            # called Tomo.
            imat_dir_path = check_dir(imat_dir_path, str(job_request.run_number))
        else:
            imat_dir_path = None

        if imat_dir_path is not None and imat_dir_path.exists():
            job_request.additional_values["images_dir"] = str(imat_dir_path)
            job_request.additional_values["runno"] = job_request.run_number
        else:
            logger.error("Images dir could not be found for experiment number: %s", job_request.experiment_number)
            raise RuleViolationError(
                "Images dir could not be found for experiment number: %s", job_request.experiment_number
            )
