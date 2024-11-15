"""
Module containing rule implementations for instrument shared rules
"""

from __future__ import annotations

import logging
from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING

from rundetection.ingestion.ingest import get_run_title
from rundetection.rules.rule import Rule

if TYPE_CHECKING:
    from rundetection.job_requests import JobRequest

logger = logging.getLogger(__name__)


class EnabledRule(Rule[bool]):
    """
    Rule for the enabled setting in specifications. If enabled is True, the run will be reduced, if not,
    it will be skipped
    """

    def verify(self, job_request: JobRequest) -> None:
        job_request.will_reduce = self._value


class NotAScatterFileError(Exception):
    pass


class CheckIfScatterSANS(Rule[bool]):
    def verify(self, job_request: JobRequest) -> None:
        if "_SANS/TRANS" not in job_request.experiment_title:
            job_request.will_reduce = False
            logger.error("Not a scatter run. Does not have _SANS/TRANS in the experiment title.")
        # If it has empty or direct in the title assume it is a direct run file instead of a normal scatter.
        if (
            "empty" in job_request.experiment_title
            or "EMPTY" in job_request.experiment_title
            or "direct" in job_request.experiment_title
            or "DIRECT" in job_request.experiment_title
        ):
            job_request.will_reduce = False
            logger.error(
                "If it is a scatter, contains empty or direct in the title and is assumed to be a scatter "
                "for an empty can run."
            )


class MolSpecStitchRule(Rule[bool]):
    """
    Enables Tosca, Osiris, and Iris Run stitching
    """

    @staticmethod
    def _is_title_similar(title: str, other_title: str) -> bool:
        """
        Compare one run title to another to check for similarity
        :param title:the first run title
        :param other_title:the second run title
        :return: (bool) True if similar False otherwise
        """
        logger.info("Comparing titles %s and %s", title, other_title)
        if title == other_title:
            return True
        if title[:-5] == other_title[:-5]:
            return True
        if title[0:7] == other_title[0:7] and ("run" in other_title or "run" in title):
            return True
        logger.info("Titles not similar, continuing")
        return False

    def _get_runs_to_stitch(self, run_path: Path, run_number: int, run_title: str, instrument: str) -> list[int]:
        run_numbers = []
        while run_path.exists():
            logger.info("run path exists %s", run_path)
            if not self._is_title_similar(get_run_title(run_path), run_title):
                logger.info("titles not similar")
                break
            logger.info("titles are similar appending run number %s", run_number)
            run_numbers.append(run_number)
            run_number -= 1
            if instrument.upper() == "TOSCA":
                next_run_number = f"TSC{run_number}.nxs"
            else:
                next_run_number = f"{instrument}{run_number:08d}.nxs"
            run_path = Path(run_path.parent, next_run_number)
        logger.info("Run path %s does not exist", run_path)
        logger.info("Returning run numbers %s", run_numbers)
        return run_numbers

    def verify(self, job_request: JobRequest) -> None:
        if not self._value:  # if the stitch rule is set to false, skip
            return

        logger.info("Checking stitch conditions for %s run %s", job_request.instrument, job_request.filepath)
        if job_request.instrument.upper() == "OSIRIS":
            try:
                if job_request.additional_values["mode"] == "diffraction":
                    job_request.additional_values["sum_runs"] = False
                    logger.info("Diffraction run cannot be summed. Continuing")
                    return
            except KeyError:
                pass
        job_request.additional_values["input_runs"] = [job_request.run_number]
        run_numbers = self._get_runs_to_stitch(
            job_request.filepath, job_request.run_number - 1, job_request.experiment_title, job_request.instrument.upper()
        )

        if len(run_numbers) > 1:
            additional_request = deepcopy(job_request)
            additional_request.additional_values["input_runs"] = run_numbers
            job_request.additional_requests.append(additional_request)


def is_y_within_5_percent_of_x(x: int | float, y: int | float) -> bool:
    """
    Given 2 numbers, x and y, return True if y is within 5% of x
    :param x: x number
    :param y: y number
    :return: True if y is within 5% of x
    """

    return (y * 0.95 <= x <= y * 1.05) if y >= 0 else (y * 0.95 >= x >= y * 1.05)
