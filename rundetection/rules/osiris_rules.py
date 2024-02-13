"""
Rules for Osiris
"""
from __future__ import annotations

import logging
from copy import deepcopy
from pathlib import Path
from typing import List

from rundetection.ingestion.ingest import JobRequest, get_run_title
from rundetection.rules.rule import Rule

logger = logging.getLogger(__name__)


def is_x_within_5_percent_of_y(x: int | float, y: int | float):
    """
    Given 2 numbers, x and y, return True if x is within 5% of y
    :param x: x number
    :param y: y number
    :return: True if x is within 5% of y
    """

    return (y * 0.95 <= x <= y * 1.05) if y >= 0 else (y * 0.95 >= x >= y * 1.05)


class OsirisReductionModeRule(Rule[bool]):
    """
    Determines the type of reduction to produce (spectroscopy or diffraction)
    """

    def verify(self, job_request: JobRequest) -> None:
        if job_request.additional_values["freq10"] == 25:
            job_request.additional_values["mode"] = "diffraction"
            # Diffraction runs cannot be summed, check for sum_runs and remove them if included
            job_request.additional_values["sum_runs"] = False
            job_request.additional_requests = []
            return

        job_request.additional_values["mode"] = "spectroscopy"
        # Create an additional diffraction run? Though this should maybe be called a reflection diffspec run?

        additional_run = deepcopy(job_request)
        additional_run.additional_values["mode"] = "diffraction"
        job_request.additional_requests.append(additional_run)

        # if job_request.additional_values["freq10"] == 50 or job_request.additional_values["freq10"] == 16:
        #     job_request.additional_values["mode"] = "spectroscopy"
        #     return

        # # handle diff
        # job_request.additional_values["mode"] = "diffraction"
        #
        # job_request.additional_values["sum_runs"] = False
        # job_request.additional_requests = []


class OsirisAnalyserRule(Rule[bool]):
    """
    Determines the analyser
    """

    # This map is based on the Appendix 1 - Quasi / inelastic settings pdf. It is reduced as the values for
    # frequency < 50 are removed as they default to analyser 2
    REDUCED_ANALYSER_TIME_CHANNEL_MAP = {
        (51500.0, 71500.0, 45900.0, 65900.0): 2,
        (45500.0, 65500.0, 40400.0, 60400.0): 2,
        (58700.0, 78700.0, 52000.0, 72000.0): 2,
        (40500.0, 60500.0, 35300.0, 55300.0): 2,
        (48500.0, 68500.0, 43600.0, 63600.0): 2,
        (22500.0, 42500.0, 19000.03, 39000.0): 4,
        (20500.0, 40500.0, 16700.0, 36700.0): 4,
    }

    @staticmethod
    def _is_x_within_5_percent_of_y(x: int | float, y: int | float):
        return y * 0.95 <= x <= y * 1.05

    def _determine_analyser_from_tcb_values(self, tcb_detector_min, tcb_detector_max, tcb_monitor_min, tcb_monitor_max):
        for key in self.REDUCED_ANALYSER_TIME_CHANNEL_MAP.keys():
            if (
                self._is_x_within_5_percent_of_y(tcb_detector_min, key[0])
                and self._is_x_within_5_percent_of_y(tcb_detector_max, key[1])
                and self._is_x_within_5_percent_of_y(tcb_monitor_min, key[2])
                and self._is_x_within_5_percent_of_y(tcb_monitor_max, key[3])
            ):
                return self.REDUCED_ANALYSER_TIME_CHANNEL_MAP[key]
        raise Exception("Oh dear")

    def verify(self, job_request: JobRequest) -> None:
        # We already know freq10 and 6 are the same from extraction. If it's less than 50 we assume its analyser 2
        if job_request.additional_values["freq10"] < 50:
            job_request.additional_values["analyser"] = 2
            return
        job_request.additional_values["analyser"] = self._determine_analyser_from_tcb_values(
            tcb_detector_min=job_request.additional_values["tcb_detector_min"],
            tcb_detector_max=job_request.additional_values["tcb_detector_max"],
            tcb_monitor_min=job_request.additional_values["tcb_monitor_min"],
            tcb_monitor_max=job_request.additional_values["tcb_monitor_max"],
        )


class OsirisPanadiumRule(Rule[int]):
    """
    Inserts the cycles panadium number into the request. This value is manually calcuated once per cycle
    """

    def verify(self, job_request: JobRequest) -> None:
        job_request.additional_values["panadium"] = self._value


class OsirisStitchRule(Rule[bool]):
    """
    Enables Osiris Run stitching
    """

    @staticmethod
    def _is_title_similar(title: str, other_title: str) -> bool:
        """
        Compare one run title to another to check for similarity
        :param title:the first run title
        :param other_title:the second run title
        :return: (bool) True if similar False otherwise
        """
        if title[:-5] == other_title[:-5]:
            return True
        if title[0:7] == other_title[0:7] and ("run" in other_title or "run" in title):
            return True
        return False

    def _get_runs_to_stitch(self, run_path: Path, run_number: int, run_title: str) -> List[int]:
        run_numbers = []
        while run_path.exists():
            if not self._is_title_similar(get_run_title(run_path), run_title):
                logger.info("titles not similar")
                break
            run_numbers.append(run_number)
            run_number -= 1
            run_path = Path(run_path.parent, f"OSIRIS{run_number}.nxs")
        return run_numbers

    def verify(self, job_request: JobRequest) -> None:
        if not self._value:  # if the stitch rule is set to false, skip
            return

        try:
            if job_request.additional_values["mode"] == "diffraction":
                job_request.additional_values["sum_runs"] = False
                return
        except KeyError:
            pass

        # stitch
        job_request.additional_values["input_runs"] = [job_request.run_number]
        run_numbers = self._get_runs_to_stitch(
            job_request.filepath, job_request.run_number, job_request.experiment_title
        )

        if len(run_numbers) > 1:
            additional_request = deepcopy(job_request)
            additional_request.additional_values["input_runs"] = run_numbers
            job_request.additional_requests.append(additional_request)
