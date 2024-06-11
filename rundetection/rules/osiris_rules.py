"""
Rules for Osiris
"""

from __future__ import annotations

import logging
import typing
from copy import deepcopy
from pathlib import Path

from rundetection.exceptions import RuleViolationError
from rundetection.ingestion.ingest import get_run_title
from rundetection.rules.rule import Rule

if typing.TYPE_CHECKING:
    from typing import ClassVar, Literal

    from rundetection.job_requests import JobRequest

logger = logging.getLogger(__name__)


def is_y_within_5_percent_of_x(x: int | float, y: int | float) -> bool:
    """
    Given 2 numbers, x and y, return True if y is within 5% of x
    :param x: x number
    :param y: y number
    :return: True if y is within 5% of x
    """

    return (y * 0.95 <= x <= y * 1.05) if y >= 0 else (y * 0.95 >= x >= y * 1.05)


class OsirisReductionModeRule(Rule[bool]):
    """
    Determines the type of reduction to produce (spectroscopy or diffraction)
    """

    # The spec phase tuples are (<phase6>, <phase10>) for the next 2 arrays Based on the PDF available here:
    # https://www.isis.stfc.ac.uk/Pages/osiris-user-guide.pdf.
    SPECTROSCOPY_PHASES: tuple[tuple[int, int]] = (
        (8573, 14250),
        (6052, 11250),
        (7500, 12500),
        (9738, 16166),
        (8964, 15211),
        (1500, 2805),
        (6569, 10861),
        (8207, 13502),
        (3717, 5675),
        (3217, 4904),
    )

    DIFFRACTION_PHASES: tuple[tuple[int, int]] = (
        (1011, 1566),
        (4599, 7715),
        (7590, 12859),
        (10407, 17715),
        (13015, 22800),
        (16100, 27973),
        (19480, 33251),
        (22571, 38130),
        (26062, 3609),
        (28953, 8228),
        (32144, 13367),
    )

    def _is_spec_phase(self, phase10: float, phase6: float) -> bool:
        for phases in self.SPECTROSCOPY_PHASES:
            # if the runs phase 6 and phase 10 are within 5% of the valid spec phases, return True
            if is_y_within_5_percent_of_x(phase6, phases[0]) and is_y_within_5_percent_of_x(phase10, phases[1]):
                return True
        return False

    def _is_diff_phase(self, phase10: float, phase6: float) -> bool:
        for phases in self.DIFFRACTION_PHASES:
            # if the runs phase 6 and phase 10 are within 5% of the valid spec phases, return True
            if is_y_within_5_percent_of_x(phase6, phases[0]) and is_y_within_5_percent_of_x(phase10, phases[1]):
                return True
        return False

    # pylint: disable=too-many-arguments
    def _determine_mode(
        self, phase10: float, phase6: float, freq: int, detector_tcb_min: float, detector_tcb_max: float
    ) -> Literal["diffraction"] | Literal["spectroscopy"]:
        if freq != 25:  # noqa: PLR2004
            return "spectroscopy"
        is_diff_phases = self._is_diff_phase(phase10, phase6)
        is_spec_phases = self._is_spec_phase(phase10, phase6)
        if is_diff_phases and is_spec_phases:
            # The phases match both a diffraction run and a spectroscopy run, we now check the detector
            # time channel boundaries to determine if is a spectroscopy run. The values are based on the PDF
            if (
                is_y_within_5_percent_of_x(detector_tcb_min, 40200)
                and is_y_within_5_percent_of_x(detector_tcb_max, 80200)
            ) or (
                is_y_within_5_percent_of_x(detector_tcb_min, 57300)
                and is_y_within_5_percent_of_x(detector_tcb_max, 97300)
            ):
                return "spectroscopy"
            return "diffraction"

        if not is_diff_phases and not is_spec_phases:
            raise RuleViolationError("Phases match neither diffraction nor spectroscopic.")

        return "diffraction" if is_diff_phases else "spectroscopy"

    # pylint: enable=too-many-arguments
    def verify(self, job_request: JobRequest) -> None:
        if not self._value:
            return
        mode = self._determine_mode(
            job_request.additional_values["phase10"],
            job_request.additional_values["phase6"],
            job_request.additional_values["freq10"],
            job_request.additional_values["tcb_detector_min"],
            job_request.additional_values["tcb_detector_max"],
        )

        if mode == "diffraction":
            # Diffraction runs cannot be summed, check for sum_runs and remove them if included
            job_request.additional_values["sum_runs"] = False
            job_request.additional_requests = []

        job_request.additional_values["mode"] = mode


class OsirisAnalyserRule(Rule[bool]):
    """
    Determines the analyser
    """

    # This map is based on the Appendix 1 - Quasi / inelastic settings pdf. It is reduced as the values for
    # frequency < 50 are removed as they default to analyser 2
    # available here https://www.isis.stfc.ac.uk/Pages/osiris-user-guide.pdf
    REDUCED_ANALYSER_TIME_CHANNEL_MAP: ClassVar[dict[tuple[int, int, int, int], int]] = {
        (51500.0, 71500.0, 45900.0, 65900.0): 2,
        (45500.0, 65500.0, 40400.0, 60400.0): 2,
        (58700.0, 78700.0, 52000.0, 72000.0): 2,
        (40500.0, 60500.0, 35300.0, 55300.0): 2,
        (48500.0, 68500.0, 43600.0, 63600.0): 2,
        (22500.0, 42500.0, 19000.03, 39000.0): 4,  # The .03 is NOT a typo
        (20500.0, 40500.0, 16700.0, 36700.0): 4,
    }

    def _determine_analyser_from_tcb_values(
        self, tcb_detector_min: float, tcb_detector_max: float, tcb_monitor_min: float, tcb_monitor_max: float
    ) -> int:
        for bounds, analyser in self.REDUCED_ANALYSER_TIME_CHANNEL_MAP.items():
            if (
                is_y_within_5_percent_of_x(tcb_detector_min, bounds[0])
                and is_y_within_5_percent_of_x(tcb_detector_max, bounds[1])
                and is_y_within_5_percent_of_x(tcb_monitor_min, bounds[2])
                and is_y_within_5_percent_of_x(tcb_monitor_max, bounds[3])
            ):
                return analyser
        raise RuleViolationError("Analyser cannot be determined")

    def verify(self, job_request: JobRequest) -> None:
        if not self._value:
            return

        if job_request.additional_values["mode"] == "diffraction":
            return

        # We already know freq10 and 6 are the same from extraction. If it's less than 50 we assume its analyser 2
        if job_request.additional_values["freq10"] < 50:  # noqa: PLR2004
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
        logger.info("Comparing titles %s and %s", title, other_title)
        if title == other_title:
            return True
        if title[:-5] == other_title[:-5]:
            return True
        if title[0:7] == other_title[0:7] and ("run" in other_title or "run" in title):
            return True
        logger.info("Titles not similar, continuing")
        return False

    def _get_runs_to_stitch(self, run_path: Path, run_number: int, run_title: str) -> list[int]:
        run_numbers = []
        while run_path.exists():
            logger.info("run path exists %s", run_path)
            if not self._is_title_similar(get_run_title(run_path), run_title):
                logger.info("titles not similar")
                break
            logger.info("titles are similar appending run number %s", run_number)
            run_numbers.append(run_number)
            run_number -= 1
            run_path = Path(run_path.parent, f"OSIRIS{run_number:08d}.nxs")
        logger.info("Run path %s does not exist", run_path)
        logger.info("Returning run numbers %s", run_numbers)
        return run_numbers

    def verify(self, job_request: JobRequest) -> None:
        if not self._value:  # if the stitch rule is set to false, skip
            return

        logger.info("Checking stitch conditions for osiris run %s", job_request.filepath)
        try:
            if job_request.additional_values["mode"] == "diffraction":
                job_request.additional_values["sum_runs"] = False
                logger.info("Diffraction run cannot be summed. Continuing")
                return
        except KeyError:
            pass
        # pylint: disable = duplicate-code
        # stitch
        job_request.additional_values["input_runs"] = [job_request.run_number]
        run_numbers = self._get_runs_to_stitch(
            job_request.filepath, job_request.run_number, job_request.experiment_title
        )

        if len(run_numbers) > 1:
            additional_request = deepcopy(job_request)
            additional_request.additional_values["input_runs"] = run_numbers
            job_request.additional_requests.append(additional_request)
        # pylint: enable = duplicate-code


class OsirisCalibrationRule(Rule[str]):
    """
    Sets the calibration file path
    """

    def verify(self, job_request: JobRequest) -> None:
        job_request.additional_values["calibration_file_path"] = self._value
