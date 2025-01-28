from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

import requests
import xmltodict

from rundetection.rules.common_rules import logger
from rundetection.rules.rule import Rule

if TYPE_CHECKING:
    from rundetection.job_requests import JobRequest


@dataclass
class SansFileData:
    title: str
    type: str
    run_number: str


def _is_sample_transmission_file(sans_file: SansFileData, sample_title: str) -> bool:
    return sample_title in sans_file.title and sans_file.type == "TRANS"


def _is_sample_direct_file(sans_file: SansFileData) -> bool:
    return (
        "direct" in sans_file.title.lower()
        or "empty" in sans_file.title.lower()
        or "mt " in sans_file.title.lower()
        or " mt" in sans_file.title.lower()
        or sans_file.title.lower() == "{mt}"
    ) and sans_file.type == "TRANS"


def _is_can_scatter_file(sans_file: SansFileData, can_title: str) -> bool:
    title_contents = re.findall(r"{.*?}", sans_file.title)
    return len(title_contents) == 1 and can_title == title_contents[0] and sans_file.type in {"SANS/TRANS", "SANS"}


def _is_can_transmission_file(sans_file: SansFileData, can_title: str) -> bool:
    return can_title in sans_file.title and sans_file.type == "TRANS"


def _find_trans_file(sans_files: list[SansFileData], sample_title: str) -> SansFileData | None:
    for sans_file in sans_files:
        if _is_sample_transmission_file(sans_file=sans_file, sample_title=sample_title):
            return sans_file
    return None


def _find_direct_file(sans_files: list[SansFileData]) -> SansFileData | None:
    reversed_files = reversed(sans_files)
    for sans_file in reversed_files:
        if _is_sample_direct_file(sans_file=sans_file):
            return sans_file
    return None


def _find_can_scatter_file(sans_files: list[SansFileData], can_title: str) -> SansFileData | None:
    for sans_file in sans_files:
        if _is_can_scatter_file(sans_file=sans_file, can_title=can_title):
            return sans_file
    return None


def _find_can_trans_file(sans_files: list[SansFileData], can_title: str) -> SansFileData | None:
    for sans_file in sans_files:
        if _is_can_transmission_file(sans_file=sans_file, can_title=can_title):
            return sans_file
    return None


def grab_cycle_instrument_index(cycle: str, instrument: str) -> str:
    _, cycle_year, cycle_num = cycle.split("_")
    url = f"http://data.isis.rl.ac.uk/journals/ndx{instrument.lower()}/journal_{cycle_year}_{cycle_num}.xml"
    return requests.get(url, timeout=5).text


def create_list_of_files(job_request: JobRequest) -> list[SansFileData]:
    cycle = job_request.additional_values["cycle_string"]
    xml = grab_cycle_instrument_index(cycle=cycle, instrument=job_request.instrument)
    cycle_run_info = xmltodict.parse(xml)
    list_of_files = []
    for run_info in cycle_run_info["NXroot"]["NXentry"]:
        title = run_info["title"]["#text"]
        run_number = run_info["run_number"]["#text"]
        file_type = title.split("_")[-1]
        list_of_files.append(SansFileData(title=title, type=file_type, run_number=run_number))
    return list_of_files


def strip_excess_files(sans_files: list[SansFileData], scatter_run_number: int) -> list[SansFileData]:
    new_list_of_files: list[SansFileData] = []
    for sans_file in sans_files:
        if int(sans_file.run_number) >= scatter_run_number:
            return new_list_of_files
        new_list_of_files.append(sans_file)
    return new_list_of_files


def _set_transmission_file(job_request: JobRequest, sample_title: str, sans_files: list[SansFileData]) -> None:
    # If using M4 monitor then scatter is the transmission
    if not job_request.additional_values["included_trans_as_scatter"]:
        trans_file = _find_trans_file(sans_files=sans_files, sample_title=sample_title)
        trans_run_number = trans_file.run_number if trans_file is not None else None
        logger.info("%s trans found %s", job_request.instrument, trans_run_number)
    else:
        trans_run_number = str(job_request.run_number)
        logger.info("%s trans set as scatter %s", job_request.instrument, trans_run_number)
    if trans_run_number is not None:
        job_request.additional_values["scatter_transmission"] = trans_run_number


def _set_can_files(can_title: str | None, job_request: JobRequest, sans_files: list[SansFileData]) -> None:
    if can_title is not None:
        can_scatter = _find_can_scatter_file(sans_files=sans_files, can_title=can_title)
        logger.info("%s can scatter found %s", job_request.instrument, can_scatter)
        if can_scatter is not None:
            job_request.additional_values["can_scatter"] = can_scatter.run_number

        # If using M4 monitor then can scatter is the transmission
        if not job_request.additional_values["included_trans_as_scatter"]:
            can_trans = _find_can_trans_file(sans_files=sans_files, can_title=can_title)
            logger.info("%s can trans found %s", job_request.instrument, can_trans)
        else:
            can_trans = can_scatter
            logger.info("%s can trans set as scatter %s", job_request.instrument, can_scatter)
        if can_trans is not None and can_scatter is not None:
            job_request.additional_values["can_transmission"] = can_trans.run_number


def _set_direct_files(job_request: JobRequest, sans_files: list[SansFileData]) -> None:
    direct_file = _find_direct_file(sans_files=sans_files)
    logger.info("%s direct files found %s", job_request.instrument, direct_file)
    if direct_file is not None:
        if "scatter_transmission" in job_request.additional_values:
            job_request.additional_values["scatter_direct"] = direct_file.run_number
        if "can_scatter" in job_request.additional_values and "can_transmission" in job_request.additional_values:
            job_request.additional_values["can_direct"] = direct_file.run_number


class CheckIfScatterSANS(Rule[bool]):
    def __init__(self, value: bool):
        super().__init__(value)
        self.should_be_first = True

    def verify(self, job_request: JobRequest) -> None:
        if not job_request.experiment_title.endswith("_SANS/TRANS") and not job_request.experiment_title.endswith(
            "_SANS"
        ):
            job_request.will_reduce = False
            logger.info("Not a scatter run. Does not have _SANS or _SANS/TRANS at the end of the experiment title.")
            return
        # If it is a direct fix, sans or trans, it should fail, which is why hard coded TRANS as we want to check
        # part of the logic not all.
        if _is_sample_direct_file(
            SansFileData(title=job_request.experiment_title, type="TRANS", run_number=str(job_request.run_number))
        ):
            job_request.will_reduce = False
            logger.info("File is an empty cell or direct beam scatter run, and should not be processed")
            return
        if "{" not in job_request.experiment_title and "}" not in job_request.experiment_title:
            job_request.will_reduce = False
            logger.info(
                "Not a parsable scatter title, a scatter contains {} in format {x}_{y}_SANS/TRANS. or {x}_SANS/TRANS."
            )
            return


class SansSliceWavs(Rule[str]):
    """
    This rule enables users to set the SliceWavs for each script
    """

    def verify(self, job_request: JobRequest) -> None:
        job_request.additional_values["slice_wavs"] = self._value


class SansPhiLimits(Rule[str]):
    """
    This rule enables users to set the PhiLimits for each script
    """

    def verify(self, job_request: JobRequest) -> None:
        job_request.additional_values["phi_limits"] = self._value


class SansFindFiles(Rule[bool]):
    def __init__(self, value: bool):
        super().__init__(value)
        self._should_be_last = True

    def verify(self, job_request: JobRequest) -> None:
        title = job_request.experiment_title
        logger.info("%s title is %s", job_request.instrument, title)
        # Find all of the "titles" [0] is the scatter, [1] is the background
        title_parts = re.findall(r"{.*?}", title)
        sample_title = title_parts[0]
        logger.info("%s sample title is %s", job_request.instrument, sample_title)
        # If background was defined in the title set can title
        can_title = title_parts[1] if len(title_parts) > 1 else None
        logger.info("%s can title is %s from list %s", job_request.instrument, can_title, title_parts)

        # Get the file lists
        sans_files = create_list_of_files(job_request)
        if not sans_files:  # == None
            job_request.will_reduce = False
            logger.error("No files found for this cycle excluding this run.")
            return
        sans_files = strip_excess_files(sans_files, scatter_run_number=job_request.run_number)

        job_request.additional_values["run_number"] = job_request.run_number

        _set_transmission_file(job_request, sample_title, sans_files)
        _set_can_files(can_title, job_request, sans_files)
        _set_direct_files(job_request, sans_files)


class SansUserFile(Rule[str]):
    def verify(self, job_request: JobRequest) -> None:
        # If M4 in user file then the transmission and scatter files are the same.
        job_request.additional_values["included_trans_as_scatter"] = "_M4" in self._value
        job_request.additional_values["user_file"] = f"/extras/{job_request.instrument.lower()}/{self._value}"
