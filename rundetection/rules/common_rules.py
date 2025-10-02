"""Module containing rule implementations for instrument shared rules."""

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
    Rule for the enabled setting in specifications.

    If enabled is True, the run will be reduced, if not,
    it will be skipped.
    """

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request.

        :param job_request: The job request to verify.
        :return: None.
        """
        job_request.will_reduce = self._value


class MolSpecStitchRule(Rule[bool]):
    """Enable Tosca, Osiris, and Iris Run stitching."""

    def __init__(self, value: bool) -> None:
        """
        Initialize the MolSpecStitchRule.

        :param value: The value of the rule.
        :return: None.
        """
        super().__init__(value)
        self.should_be_last = True

    @staticmethod
    def _is_title_similar(title: str, other_title: str) -> bool:
        """
        Compare one run title to another to check for similarity.

        :param title: The first run title.
        :param other_title: The second run title.
        :return: (bool) True if similar False otherwise.
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
        """
        Get runs in order to stitch them together.

        :param run_path: The path to the runs.
        :param run_number: The run number for starting run.
        :param run_title: The title of the first run.
        :param instrument: Instrument that experiment runs occurred on.
        """
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
                next_run_number = f"{instrument.upper()}{run_number:08d}.nxs"
            run_path = Path(run_path.parent, next_run_number)
        logger.info("Returning run numbers %s", run_numbers)
        return run_numbers

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request.

        :param job_request: The job request to verify.
        :return: None.
        """
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
            job_request.filepath,
            job_request.run_number,
            job_request.experiment_title,
            job_request.instrument.upper(),
        )

        if len(run_numbers) > 1:
            additional_request = deepcopy(job_request)
            additional_request.additional_values["input_runs"] = run_numbers
            job_request.additional_requests.append(additional_request)


def is_y_within_5_percent_of_x(x: int | float, y: int | float) -> bool:
    """
    Check if y is within 5% of x.

    :param x: x number
    :param y: y number
    :return: True if y is within 5% of x.
    """
    return (y * 0.95 <= x <= y * 1.05) if y >= 0 else (y * 0.95 >= x >= y * 1.05)


def get_journal_from_file_based_on_run_file_archive_path(jobrequest: JobRequest):
    """
    Get the jounral raw text, in order to parse run information.

    :param archive_path: Path object to the jounral of run information.
    """
    cycle_year, cycle_num = str(jobrequest.filepath.parent.name).split("_")[-2:]
    # Go from /archive/NDXMARI/Instrument/data/cycle_25_1/MAR012345.nxs to /archive/NDXMARI/Instrument, allowing us to
    # then navigate to the appropriate journal at /archive/NDXMARI/Instrument/logs/journal/journal_25_1.xml
    root_path = jobrequest.filepath.parent.parent.parent
    journal_path = root_path / Path(f"logs/journal/journal_{cycle_year}_{cycle_num}.xml")
    return journal_path.read_text()
