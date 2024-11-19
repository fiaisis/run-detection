"""
Rules for LOQ
"""

from __future__ import annotations

import logging
import typing

from rundetection.rules.common_rules import FileData, create_list_of_files, strip_excess_files
from rundetection.rules.rule import Rule

if typing.TYPE_CHECKING:
    from rundetection.job_requests import JobRequest

logger = logging.getLogger(__name__)


def _extract_run_number_from_filename(filename: str) -> str:
    # Assume filename looks like so: LOQ00100002.nxs, then strip.
    return filename.split(".")[0].lstrip("LOQ").lstrip("0")


def _is_sample_transmission_file(sans_file: FileData, sample_title: str) -> bool:
    return sample_title in sans_file.title and sans_file.type == "TRANS"


def _is_sample_direct_file(sans_file: FileData) -> bool:
    return ("direct" in sans_file.title.lower() or "empty" in sans_file.title.lower()) and sans_file.type == "TRANS"


def _is_can_scatter_file(sans_file: FileData, can_title: str) -> bool:
    return can_title == sans_file.title.split("_")[0] and sans_file.type == "SANS/TRANS"


def _is_can_transmission_file(sans_file: FileData, can_title: str) -> bool:
    return can_title in sans_file.title and sans_file.type == "TRANS"


def _find_trans_file(sans_files: list[FileData], sample_title: str) -> FileData | None:
    for sans_file in sans_files:
        if _is_sample_transmission_file(sans_file=sans_file, sample_title=sample_title):
            return sans_file
    return None


def _find_direct_file(sans_files: list[FileData]) -> FileData | None:
    reversed_files = reversed(sans_files)
    for sans_file in reversed_files:
        if _is_sample_direct_file(sans_file=sans_file):
            return sans_file
    return None


def _find_can_scatter_file(sans_files: list[FileData], can_title: str) -> FileData | None:
    for sans_file in sans_files:
        if _is_can_scatter_file(sans_file=sans_file, can_title=can_title):
            return sans_file
    return None


def _find_can_trans_file(sans_files: list[FileData], can_title: str) -> FileData | None:
    for sans_file in sans_files:
        if _is_can_transmission_file(sans_file=sans_file, can_title=can_title):
            return sans_file
    return None


class LoqFindFiles(Rule[bool]):
    def verify(self, job_request: JobRequest) -> None:
        # Expecting 3 values
        title_parts = job_request.experiment_title.split("_")
        if len(title_parts) != 3:  # noqa: PLR2004
            job_request.will_reduce = False
            logger.error(
                f"Less or more than 3 sections to the experiment_title, probably missing Can Scatter title: "
                f"{job_request.experiment_title}"
            )
            return
        sample_title, can_title, ___ = title_parts
        sans_files = create_list_of_files(job_request)
        if sans_files == []:
            job_request.will_reduce = False
            logger.error("No files found for this cycle excluding this run.")
            return
        sans_files = strip_excess_files(sans_files, scatter_run_number=job_request.run_number)

        job_request.additional_values["run_number"] = job_request.run_number

        trans_file = _find_trans_file(sans_files=sans_files, sample_title=sample_title)
        if trans_file is not None:
            job_request.additional_values["scatter_transmission"] = trans_file.run_number

        can_scatter = _find_can_scatter_file(sans_files=sans_files, can_title=can_title)
        if can_scatter is not None:
            job_request.additional_values["can_scatter"] = can_scatter.run_number

        can_trans = _find_can_trans_file(sans_files=sans_files, can_title=can_title)
        if can_trans is not None and can_scatter is not None:
            job_request.additional_values["can_transmission"] = can_trans.run_number

        direct_file = _find_direct_file(sans_files=sans_files)
        if direct_file is not None:
            if trans_file is not None:
                job_request.additional_values["scatter_direct"] = direct_file.run_number
            if can_scatter is not None and can_trans is not None:
                job_request.additional_values["can_direct"] = direct_file.run_number


class LoqUserFile(Rule[str]):
    def verify(self, job_request: JobRequest) -> None:
        job_request.additional_values["user_file"] = f"/extras/loq/{self._value}"
