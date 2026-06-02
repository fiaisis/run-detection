"""Rules for Iris."""

from __future__ import annotations

import logging
import os
import typing
from pathlib import Path

from rundetection.ingestion.ingest import load_h5py_dataset
from rundetection.rules.rule import Rule

if typing.TYPE_CHECKING:
    from rundetection.job_requests import JobRequest

logger = logging.getLogger(__name__)


def check_file_is_correct_for_run_number(path: Path, run_number: str) -> bool:
    """
    Check if a file matches the run number.

    :param path: The path to the file.
    :param run_number: The run number to check for.
    :return: True if it is a file and the run number is in the name.
    """
    return path.is_file() and run_number in path.name


def find_correct_tomo_dir(path: Path, run_number: str) -> Path | None:
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
        is_file = check_file_is_correct_for_run_number(child, run_number)
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


class INESFindNGEMorReduce(Rule[bool]):
    """Finds the INES image files"""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request. Find the nGEM image files or determine if it is a reduction run and
        prep for the script.

        :param job_request: The job request to verify.
        :return: None.
        """
        # Given this is INES we should check if the nGEM data exists for this run, else we should reduce this run.
        ngem_dir = os.environ.get("INES_NGEM_DIR", "/ngem/nGEM-INES")

        # Grab the cycle from the .nxs file from the path /raw_data_1/run_cycle value in the form 26_1
        cycle_str = load_h5py_dataset(job_request.filepath).get("run_cycle")[0].decode("utf-8")
        cycle_year, cycle_num = cycle_str.split("_")

        # Check if the correct dir exists in the nGEM dir for INES.
        exp_dir_path = Path(ngem_dir) / "DATA" / f"INES_20{cycle_year}_0{cycle_num}"
        if exp_dir_path.exists():
            possible_path = exp_dir_path / f"INES{job_request.run_number}"
            if possible_path.exists() and possible_path.is_dir():
                # We found it
                job_request.additional_values["reduce"] = "false"
                job_request.additional_values["ngem"] = "true"
                job_request.additional_values["ngem_path"] = str(possible_path)
                output_path = Path(str(exp_dir_path) + "_nxs") / "RUN"
                job_request.additional_values["ngem_output_path"] = str(output_path)
        else:
            # It's not an ngem run, so we reduce it.
            job_request.additional_values["reduce"] = "true"
            job_request.additional_values["ngem"] = "false"
