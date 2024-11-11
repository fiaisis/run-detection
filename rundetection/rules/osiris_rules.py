"""
Rules for Osiris
"""

from __future__ import annotations

import logging
import typing

from rundetection.exceptions import RuleViolationError
from rundetection.rules.common_rules import is_y_within_5_percent_of_x
from rundetection.rules.rule import Rule

if typing.TYPE_CHECKING:
    from typing import ClassVar, Literal

    from rundetection.job_requests import JobRequest

logger = logging.getLogger(__name__)


class OsirisReductionModeRule(Rule[bool]):
    """
    Determines the type of reduction to produce (spectroscopy or diffraction)
    """

    # The spec phase tuples are (<phase6>, <phase10>) for the next 2 arrays Based on the PDF available here:
    # https://www.isis.stfc.ac.uk/Pages/osiris-user-guide.pdf.
    SPECTROSCOPY_PHASES: ClassVar[list[tuple[int, int]]] = [
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
    ]

    DIFFRACTION_PHASES: ClassVar[list[tuple[int, int]]] = [
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
    ]

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


class OsirisDefaultSpectroscopy(Rule[bool]):
    def verify(self, job_request: JobRequest) -> None:
        if self._value is True:
            job_request.additional_values["spectroscopy_reduction"] = "true"
            job_request.additional_values["diffraction_reduction"] = "false"


class OsirisDefaultGraniteAnalyser(Rule[bool]):
    def verify(self, job_request: JobRequest) -> None:
        if self._value is True:
            job_request.additional_values["analyser"] = "graphite"


class OsirisReflectionCalibrationRule(Rule[dict[str, str]]):
    """
    Determine the reflection and set calibration run number based on the reflection
    """

    # This map is based on the Appendix 1 - Quasi / inelastic settings pdf. It is reduced as the values for
    # frequency < 50 are removed as they default to reflection 002
    # available here https://www.isis.stfc.ac.uk/Pages/osiris-user-guide.pdf
    REDUCED_REFLECTION_TIME_CHANNEL_MAP: ClassVar[dict[tuple[float, float, float, float], str]] = {
        (51500.0, 71500.0, 45900.0, 65900.0): "002",
        (45500.0, 65500.0, 40400.0, 60400.0): "002",
        (58700.0, 78700.0, 52000.0, 72000.0): "002",
        (40500.0, 60500.0, 35300.0, 55300.0): "002",
        (48500.0, 68500.0, 43600.0, 63600.0): "002",
        (22500.0, 42500.0, 19000.03, 39000.0): "004",  # The .03 is NOT a typo
        (20500.0, 40500.0, 16700.0, 36700.0): "004",
    }

    def _determine_reflection_from_tcb_values(
        self, tcb_detector_min: float, tcb_detector_max: float, tcb_monitor_min: float, tcb_monitor_max: float
    ) -> str:
        for bounds, reflection in self.REDUCED_REFLECTION_TIME_CHANNEL_MAP.items():
            if (
                is_y_within_5_percent_of_x(tcb_detector_min, bounds[0])
                and is_y_within_5_percent_of_x(tcb_detector_max, bounds[1])
                and is_y_within_5_percent_of_x(tcb_monitor_min, bounds[2])
                and is_y_within_5_percent_of_x(tcb_monitor_max, bounds[3])
            ):
                return reflection
        raise RuleViolationError("Analyser cannot be determined")

    def _determine_reflection(self, job_request: JobRequest) -> str:
        # Magic 50 number determined by knowing that no frequency below 50 uses 004,
        # as per https://www.isis.stfc.ac.uk/Pages/osiris-user-guide.pdf
        if round(job_request.additional_values["freq10"]) < 50:  # noqa: PLR2004
            return "002"
        return self._determine_reflection_from_tcb_values(
            tcb_detector_min=job_request.additional_values["tcb_detector_min"],
            tcb_detector_max=job_request.additional_values["tcb_detector_max"],
            tcb_monitor_min=job_request.additional_values["tcb_monitor_min"],
            tcb_monitor_max=job_request.additional_values["tcb_monitor_max"],
        )

    def verify(self, job_request: JobRequest) -> None:
        if not self._value:
            return
        reflection = self._determine_reflection(job_request)

        job_request.additional_values["reflection"] = reflection
        job_request.additional_values["calibration_run_number"] = self._value[reflection]


