"""
Rules for Iris
"""

from __future__ import annotations

import logging
import typing

from rundetection.rules.common_rules import is_y_within_5_percent_of_x
from rundetection.rules.rule import Rule

if typing.TYPE_CHECKING:
    from rundetection.job_requests import JobRequest

logger = logging.getLogger(__name__)


# The spec data for this list of dicts based on the PDF available here:
# https://www.isis.stfc.ac.uk/Pages/The%20IRIS%20User%20Guide%204.pdf.
GRAPHITE_DATA = [
    {"phases": (8967, 14413), "reflection": "002", "tcb_1": (56000.0, 76000.0), "tcb_2": (52200.0, 72200.0)},
    {"phases": (7996, 12868), "reflection": "002", "tcb_1": (50000.0, 70000.0), "tcb_2": (46700.0, 66700.0)},
    {"phases": (7649, 12316), "reflection": "002", "tcb_1": (48000.0, 68000.0), "tcb_2": (44700.0, 64700.0)},
    {"phases": (7336, 11967), "reflection": "002", "tcb_1": (47000.0, 67000.0), "tcb_2": (43200.0, 63200.0)},
    {"phases": (5922, 9569), "reflection": "002", "tcb_1": (38000.0, 58000.0), "tcb_2": (35200.0, 55200.0)},
    {"phases": (7133, 11493), "reflection": "002", "tcb_1": (45000.0, 65000.0), "tcb_2": (41900.0, 61900.0)},
    {"phases": (1500, 2829), "reflection": "002", "tcb_1": (14000.0, 74000.0), "tcb_2": (16000.0, 76000.0)},
    {"phases": (2655, 5148), "reflection": "002", "tcb_1": (22000.0, 82000.0), "tcb_2": (21500.0, 81500.0)},
    {"phases": (7750, 12623), "reflection": "002", "tcb_1": (50000.0, 90000.0), "tcb_2": (46500.0, 86500.0)},
    {"phases": (5919, 9712), "reflection": "002", "tcb_1": (38500.0, 78500.0), "tcb_2": (36500.0, 76500.0)},
    {"phases": (4502, 7457), "reflection": "002", "tcb_1": (30000.0, 70000.0), "tcb_2": (28800.0, 68800.0)},
    {"phases": (3500, 5800), "reflection": "002", "tcb_1": (25000.0, 65000.0), "tcb_2": (23500.0, 63500.0)},
    {"phases": (3653, 5959), "reflection": "004", "tcb_1": (24000.0, 44000.0), "tcb_2": (22700.0, 42700.0)},
    {"phases": (2850, 4275), "reflection": "004", "tcb_1": (18000.0, 38000.0), "tcb_2": (17500.0, 37500.0)},
]


class IrisReductionRule(Rule[bool]):
    """
    Determines the type of reduction to produce (spectroscopy or diffraction)
    """

    @staticmethod
    def _tuple_match(x: tuple[int | float, int | float], y: tuple[int | float, int | float]) -> bool:
        return is_y_within_5_percent_of_x(x[0], y[0]) and is_y_within_5_percent_of_x(x[1], y[1])

    def verify(self, job_request: JobRequest) -> None:
        if not self._value:
            return
        if round(job_request.additional_values["freq10"]) < 50:  # noqa: PLR2004
            # Known "guaranteed" to be Graphite analyser and 002 reflection as frequency is not 50
            # (or close enough to 50 for it to not matter)
            job_request.additional_values["reflection"] = "002"
            job_request.additional_values["analyser"] = "graphite"
            return
        reflection = "002"
        phases = (job_request.additional_values["phase6"], job_request.additional_values["phase10"])
        tcb_1 = (job_request.additional_values["tcb_detector_min"], job_request.additional_values["tcb_detector_max"])
        tcb_2 = (job_request.additional_values["tcb_monitor_min"], job_request.additional_values["tcb_monitor_max"])
        for spec_type in GRAPHITE_DATA:
            if (
                self._tuple_match(phases, spec_type["phases"])  # type: ignore
                and self._tuple_match(tcb_1, spec_type["tcb_1"])  # type: ignore
                and self._tuple_match(tcb_2, spec_type["tcb_2"])  # type: ignore
            ):
                reflection = spec_type["reflection"]
                break
        job_request.additional_values["reflection"] = reflection
        job_request.additional_values["analyser"] = "graphite"


class IrisCalibrationRule(Rule[dict[str, str]]):
    """
    Set calibration run number based on the reflection, needs to be called after the IrisReductionRule so reflection and
    analyser are set in the job_request.additional_values
    """
    def __init__(self, value: dict[str, str]):
        super().__init__(value)
        self._should_be_last = True

    def verify(self, job_request: JobRequest) -> None:
        if not self._value:
            return
        reflection = job_request.additional_values["reflection"]
        job_request.additional_values["calibration_run_numbers"] = self._value[reflection]
