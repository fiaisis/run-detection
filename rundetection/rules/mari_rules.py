"""
Mari Rules
"""

import logging
from copy import deepcopy
from pathlib import Path

import xmltodict

from rundetection.ingestion.ingest import get_run_title
from rundetection.job_requests import JobRequest
from rundetection.rules.common_rules import grab_cycle_instrument_index
from rundetection.rules.rule import Rule

logger = logging.getLogger(__name__)


class MariStitchRule(Rule[bool]):
    """
    The MariStitchRule is the rule that applies, dependent on the other rules running first. This runs last.
    """

    def __init__(self, value: bool) -> None:
        super().__init__(value)
        self.should_be_last = True

    @staticmethod
    def _get_runs_to_stitch(run_path: Path, run_number: int, run_title: str) -> list[int]:
        run_numbers = []
        while run_path.exists():
            if get_run_title(run_path) != run_title:
                break
            run_numbers.append(run_number)
            run_number -= 1
            run_path = Path(run_path.parent, f"MAR{run_number}.nxs")
        return run_numbers

    def verify(self, job_request: JobRequest) -> None:
        if not self._value:  # if the stitch rule is set to false, skip
            return

        run_numbers = self._get_runs_to_stitch(
            job_request.filepath, job_request.run_number, job_request.experiment_title
        )
        if len(run_numbers) > 1:
            additional_request = deepcopy(job_request)
            additional_request.additional_values["runno"] = run_numbers
            additional_request.additional_values["sum_runs"] = True
            # We must reapply the common mari rules manually here, if we apply the whole spec automatically it will
            # produce an infinite loop
            additional_request.additional_values["mask_file_link"] = job_request.additional_values["mask_file_link"]
            additional_request.additional_values["wbvan"] = job_request.additional_values["wbvan"]
            job_request.additional_requests.append(additional_request)


class MariMaskFileRule(Rule[str]):
    """
    Adds the permalink of the maskfile to the additional outputs
    """

    def verify(self, job_request: JobRequest) -> None:
        job_request.additional_values["mask_file_link"] = self._value


class MariWBVANRule(Rule[int]):
    """
    Inserts the cycles wbvan number into the script. This value is manually calculated by the MARI instrument scientist
    once per cycle.
    """

    def __init__(self, value: int):
        super().__init__(value)
        self.cycle_run_info = None

    def get_run_numbers_from_cycle(self, cycle_string, instrument):
        if self.cycle_run_info is None:
            cycle_xml = grab_cycle_instrument_index(cycle_string, instrument)
            self.cycle_run_info = xmltodict.parse(cycle_xml)
        return [run_info["run_number"]["#text"] for run_info in self.cycle_run_info["NXroot"]["NXentry"]]

    def get_run_numbers_and_titles(self, cycle_string, instrument):
        if self.cycle_run_info is None:
            cycle_xml = grab_cycle_instrument_index(cycle_string, instrument)
            self.cycle_run_info = xmltodict.parse(cycle_xml)
        return [
            (run_info["run_number"]["#text"], run_info["title"]["#text"])
            for run_info in self.cycle_run_info["NXroot"]["NXentry"]
        ]

    def find_wbvan(self, job_request: JobRequest) -> int | None:
        runs_this_cycle = self.get_run_numbers_and_titles(
            job_request.additional_values["cycle_string"], job_request.instrument
        )
        for run_number, title in reversed(runs_this_cycle):
            if ("van" in title.lower() and "30mev" in title.lower().replace(" ", "") and
                    "50hz" in title.lower().replace(" ", "")):
                return int(run_number)
        return None

    def file_in_cycle(self, run_number: str, job_request: JobRequest) -> bool:
        run_numbers = self.get_run_numbers_from_cycle(
            job_request.additional_values["cycle_string"], job_request.instrument
        )
        return run_number in run_numbers

    def verify(self, job_request: JobRequest) -> None:
        wbvan = self._value
        # If the run number is not from this cycle then we should try to find the most recent vanadium file from this
        # cycle.
        if not self.file_in_cycle(str(wbvan), job_request):
            wbvan = self.find_wbvan(job_request)
            if wbvan is None:
                # If wbvan cannot be found still, give up and use the defaulted value.
                wbvan = self._value

        job_request.additional_values["wbvan"] = wbvan
