"""
Module containing rule implementations for instrument shared rules
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import requests
import xmltodict

from rundetection.job_requests import JobRequest
from rundetection.rules.rule import Rule

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


@dataclass
class FileData:
    title: str
    type: str
    run_number: str


def grab_cycle_instrument_index(cycle: str, instrument: str) -> str:
    _, cycle_year, cycle_num = cycle.split("_")
    url = f"http://data.isis.rl.ac.uk/journals/ndx{instrument.lower()}/journal_{cycle_year}_{cycle_num}.xml"
    return requests.get(url, timeout=5).text


def create_list_of_files(job_request: JobRequest) -> list[FileData]:
    cycle = job_request.additional_values["cycle_string"]
    xml = grab_cycle_instrument_index(cycle=cycle, instrument=job_request.instrument)
    cycle_run_info = xmltodict.parse(xml)
    list_of_files = []
    for run_info in cycle_run_info["NXroot"]["NXentry"]:
        title_contents = run_info["title"]["#text"].split("_")
        run_number = run_info["run_number"]["#text"]
        if len(title_contents) not in {2, 3}:
            continue
        file_type = title_contents[-1]
        list_of_files.append(FileData(title=run_info["title"]["#text"], type=file_type, run_number=run_number))
    return list_of_files


def strip_excess_files(run_files: list[FileData], scatter_run_number: int) -> list[FileData]:
    new_list_of_files: list[FileData] = []
    for run_file in run_files:
        if int(run_file.run_number) >= scatter_run_number:
            return new_list_of_files
        new_list_of_files.append(run_file)
    return new_list_of_files


def find_path_for_run_number(cycle_path: str, run_number: int, file_start: str) -> Path | None:
    # 10 is just a magic number, but we needed an unrealistic value for the maximum
    for padding in range(11):
        potential_path = Path(f"{cycle_path}/{file_start}{str(run_number).zfill(padding)}.nxs")
        if potential_path.exists():
            return potential_path
    return None
