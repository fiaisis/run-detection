"""Rules for Iris."""

from __future__ import annotations

import logging
import os
import typing
from pathlib import Path

from rundetection.exceptions import RuleViolationError
from rundetection.ingestion.ingest import load_h5py_dataset
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
            job_request.additional_values["recon"] = "true"
            job_request.additional_values["ngem"] = "false"
            job_request.additional_values["images_dir"] = str(imat_dir_path)
            job_request.additional_values["runno"] = job_request.run_number
        else:
            # Given there is no Images, let's check for an nGEM run.
            # INES is temporary here and should be adjustable via env vars. Current technical limitation forces IMAT
            # data here.
            ngem_dir = os.environ.get("IMAT_NGEM_DIR", "/ngem/nGEM-INES")

            # Grab the cycle from the .nxs file from the path /raw_data_1/run_cycle value in the form 26_1
            cycle_str = load_h5py_dataset(job_request.filepath).get("run_cycle")[0].decode("utf-8")
            cycle_year, cycle_num = cycle_str.split("_")

            # Check if the correct dir exists in the nGEM dir for IMAT.
            exp_dir_path = Path(ngem_dir) / "DATA" / f"IMAT_20{cycle_year}_0{cycle_num}"
            if exp_dir_path.exists():
                possible_path = exp_dir_path / f"IMAT{job_request.run_number:08d}"
                if possible_path.exists() and possible_path.is_dir():
                    # We found it
                    job_request.additional_values["recon"] = "false"
                    job_request.additional_values["ngem"] = "true"
                    job_request.additional_values["ngem_path"] = str(possible_path)
                    output_path = Path(str(exp_dir_path) + "_nxs") / "RUN"
                    job_request.additional_values["ngem_output_path"] = str(output_path)

        if "ngem" not in job_request.additional_values and "recon" not in job_request.additional_values:
            # We did not find either an IMAT or nGEM detector run.
            logger.error(
                "Images dir and nGEM run could not be found for experiment number: %s", job_request.experiment_number
            )
            raise RuleViolationError(
                "Images dir and nGEM run could not be found for experiment number: %s", job_request.experiment_number
            )
