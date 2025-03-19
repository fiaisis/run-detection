from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING

import xmltodict

from rundetection.ingestion.ingest import load_h5py_dataset
from rundetection.rules.common_rules import grab_cycle_instrument_index, logger
from rundetection.rules.rule import Rule

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from rundetection.job_requests import JobRequest

# Global List of data so it can be reused by multiple classes.
SANS_FILES: list[SansFileData] | None = None


@dataclass
class SansFileData:
    title: str
    type: str
    run_number: str

    def __hash__(self) -> int:
        return hash((self.title, self.type, self.run_number))

    @property
    def is_scatter(self) -> bool:
        return self.type.upper() in ("SANS/TRANS", "SANS")

    @property
    def is_trans(self) -> bool:
        return self.type.upper() == "TRANS"


class SansFileError(Exception):
    pass


def _create_sans_file_data(title: str, run_number: str) -> SansFileData:
    return SansFileData(title=title, type=title.split("_")[-1], run_number=run_number)


def _create_list_of_files(job_request: JobRequest) -> list[SansFileData]:
    cycle = job_request.additional_values["cycle_string"]
    xml = grab_cycle_instrument_index(cycle=cycle, instrument=job_request.instrument)
    cycle_run_info = xmltodict.parse(xml)
    return [
        _create_sans_file_data(title=run_info["title"]["#text"], run_number=run_info["run_number"]["#text"])
        for run_info in cycle_run_info["NXroot"]["NXentry"]
    ]


def _set_metadata_files(
    job_request: JobRequest, scatter_file: SansFileData, trans_file: SansFileData, direct_file: SansFileData
) -> None:
    # Used in later rules
    job_request.additional_values["scatter_title"] = scatter_file.title
    job_request.additional_values["scatter_direct_title"] = direct_file.title

    # Used in the script
    job_request.additional_values["scatter_number"] = scatter_file.run_number
    job_request.additional_values["scatter_direct_number"] = direct_file.run_number
    job_request.additional_values["scatter_transmission_number"] = trans_file.run_number


def _get_scatter_title(scatter: SansFileData | str | None) -> str:
    if scatter is None:
        return ""
    if isinstance(scatter, SansFileData):
        scatter = scatter.title
    if "}_{" in scatter:
        return scatter.split("}_{")[0].lstrip("{").rstrip("}")
    return scatter.split("}_")[0].lstrip("{").rstrip("}")


def _get_background_title(scatter_title: str) -> str | None:
    if "}_{" in scatter_title:
        title = scatter_title.split("}_{")[1]
        # Strip off the _SANS/TRANS or _SANS from the title
        title = title[: len(title) - 1 - len(title.split("_")[-1])]
        return title.lstrip("{").rstrip("}")
    return None


def _find_trans(job_request: JobRequest, scatter: SansFileData | None) -> SansFileData | None:
    if job_request.additional_values["included_trans_as_scatter"]:
        # Trans is the scatter file
        return scatter
    return _find_file_in_journal_by_title_and_type(title=_get_scatter_title(scatter), file_types={"TRANS"})


def _is_direct_file(job_request: JobRequest, file_path: Path | None) -> bool:
    if file_path is None:
        return False
    possible_file = load_h5py_dataset(file_path)
    instrumentation_dict = job_request.additional_values["instrument_direct_file_comparison"]
    # Following code can only handle a 2 deep dictionary with the one item in the top dict layer,
    # this could be refactored to be more extensible in future but unneeded at present as it is usually just selog
    top_layer_key = next(iter(instrumentation_dict.keys()))
    for key in instrumentation_dict[top_layer_key]:
        if instrumentation_dict[top_layer_key][key] != possible_file.get(top_layer_key).get(key).get("value")[0]:
            return False
    return True


def _grab_file_prefix(job_file_path: Path) -> str:
    # Reverse through the path stem until it finds the 0s then once past the 0s append to the file_prefix all
    # characters from the stem, then finally reverse the string before returning it.
    file_prefix = ""
    first_zero_found = False
    last_zero_found = False
    for char in reversed(job_file_path.stem):
        if last_zero_found:
            file_prefix += char
        elif char != "0" and first_zero_found and not last_zero_found:
            last_zero_found = True
            file_prefix += char
        elif char == "0":
            first_zero_found = True
    # Now reverse the string and return using slicing
    return file_prefix[::-1]


def _get_zeros(job_file_path: Path, run_number: str) -> str:
    num_zeros = len(job_file_path.stem) - len(run_number) - len(_grab_file_prefix(job_file_path))
    return num_zeros * "0"


def _generate_direct_file_path(job_file_path: Path, run_number: int) -> Path | None:
    zeros = _get_zeros(job_file_path, run_number=str(run_number))
    inst_file_prefix = _grab_file_prefix(job_file_path)
    initial_file_path = job_file_path.parent / f"{inst_file_prefix}{zeros}{run_number}.nxs"
    if initial_file_path.exists():
        return initial_file_path
    secondary_file_path = job_file_path.parent / f"{inst_file_prefix}{zeros[:-1]}{run_number}.nxs"
    if secondary_file_path.exists():
        return secondary_file_path
    return None


def _find_direct(job_request: JobRequest) -> SansFileData | None:
    direct_files = set()
    for empty_shorthand in ("direct", "empty", " mt", "mt ", "{mt}"):
        direct_files.update(
            _find_all_files_in_journal_on_condition(
                partial(
                    lambda file, empty_shorthand: empty_shorthand in file.title.lower() and file.type == "TRANS",
                    empty_shorthand=empty_shorthand,
                )
            )
        )
    # Remove duplicates and sort in order to get most recent direct file first
    direct_files_ordered = list(direct_files)
    direct_files_ordered.sort(key=lambda x: x.run_number, reverse=True)
    for file in direct_files_ordered:
        file_path = _generate_direct_file_path(job_request.filepath, job_request.run_number)
        if _is_direct_file(job_request=job_request, file_path=file_path):
            return file
    return None


def _refresh_local_journal(job_request: JobRequest) -> None:
    global SANS_FILES  # noqa: PLW0603
    logger.info("Refreshing local copy of %s journal", job_request.instrument)
    SANS_FILES = _create_list_of_files(job_request)


def _find_all_files_in_journal_on_condition(condition: Callable[[SansFileData], bool]) -> list[SansFileData]:
    global SANS_FILES  # noqa: PLW0603
    if SANS_FILES is None:
        SANS_FILES = []
    return [file for file in SANS_FILES if condition(file)]


def _find_file_in_journal_by_title_and_type(title: str, file_types: set[str]) -> SansFileData | None:
    found_files = _find_all_files_in_journal_on_condition(
        lambda file: "{" + title.lower() + "}" in file.title.lower() and file.type in file_types
    )
    return found_files[0] if len(found_files) > 0 else None


class SansUserFile(Rule[str]):
    def __init__(self, value: str):
        super().__init__(value)
        self.should_be_first = True

    def verify(self, job_request: JobRequest) -> None:
        # If M4 in user file then the transmission and scatter files are the same.
        job_request.additional_values["included_trans_as_scatter"] = "_M4" in self._value
        job_request.additional_values["user_file"] = f"/extras/{job_request.instrument.lower()}/{self._value}"


def _clean_up_job_request(job_request: JobRequest) -> None:
    # We need to drop this in case it has none-JSON serializable data in it. Can be trivially extended to further
    # cleanup the jobrequest.
    if "instrument_direct_file_comparison" in job_request.additional_values:
        job_request.additional_values.pop("instrument_direct_file_comparison")


class SansScatterTransFiles(Rule[bool]):
    @staticmethod
    def _verify_scatter(job_request: JobRequest, job_file: SansFileData) -> None:
        trans_file = _find_trans(job_request, job_file)
        logger.info("Found trans file %s", trans_file)
        direct_file = _find_direct(job_request)
        logger.info("Found direct file %s", direct_file)
        if trans_file is None or direct_file is None:
            job_request.will_reduce = False
            logger.error("File: %s, is a scatter but trans and/or direct files could not be found", job_file.title)
        else:
            # Is a scatter file, and found it's trans, and direct files.
            job_request.will_reduce = True
            _set_metadata_files(
                job_request=job_request, scatter_file=job_file, trans_file=trans_file, direct_file=direct_file
            )

    @staticmethod
    def _verify_trans(job_request: JobRequest, job_file: SansFileData) -> None:
        scatter_title = _get_scatter_title(job_file)
        scatter_file = _find_file_in_journal_by_title_and_type(title=scatter_title, file_types={"SANS/TRANS", "SANS"})
        logger.info("Found scatter file %s", scatter_file)
        direct_file = _find_direct(job_request)
        logger.info("Found direct file %s", direct_file)
        if scatter_file is None or direct_file is None:
            job_request.will_reduce = False
            logger.error("File: %s, is a trans but scatter and/or direct files could not be found", job_file.title)
        else:
            # Is a trans file and found its scatter, and direct files.
            job_request.will_reduce = True
            _set_metadata_files(
                job_request=job_request, scatter_file=scatter_file, trans_file=job_file, direct_file=direct_file
            )

    def verify(self, job_request: JobRequest) -> None:
        if not self._value:
            return
        _refresh_local_journal(job_request=job_request)
        job_file = _create_sans_file_data(title=job_request.experiment_title, run_number=str(job_request.run_number))
        if job_file.is_scatter:
            self._verify_scatter(job_request=job_request, job_file=job_file)
        elif job_file.is_trans:
            self._verify_trans(job_request=job_request, job_file=job_file)
        else:
            job_request.will_reduce = False
            logger.error("File: %s is neither TRANS nor a SANS scatter", job_file.title)
        _clean_up_job_request(job_request)


class SansCanFiles(Rule[bool]):
    def __init__(self, value: bool):
        super().__init__(value)
        # Ordered last for 2 reasons, 1, dependent on scatter_title and scatter_direct_title, 2, No need to refresh
        # journal again
        self.should_be_last = True

    def verify(self, job_request: JobRequest) -> None:
        if not self._value:
            return
        # Ensure the job_request was set up correctly by previous rules
        for required_key in ("scatter_title", "scatter_direct_number", "scatter_direct_title"):
            if required_key not in job_request.additional_values:
                logger.error(
                    "%s rule needs rules to be ran before it and does not have %s in the job_request additional values",
                    self.__class__,
                    required_key,
                )
                job_request.will_reduce = False
                return

        # Perform the checks for this rule
        scatter_title = job_request.additional_values["scatter_title"]
        background_title = _get_background_title(scatter_title)
        if background_title:
            background_scatter = _find_file_in_journal_by_title_and_type(
                title=background_title, file_types={"SANS", "SANS/TRANS"}
            )
            background_trans = _find_trans(scatter=background_scatter, job_request=job_request)
        else:
            # Go and find the scatter/sans file for the direct and use that instead
            background_scatter = _find_file_in_journal_by_title_and_type(
                title=_get_scatter_title(job_request.additional_values["scatter_direct_title"]),
                file_types={"SANS", "SANS/TRANS"},
            )
            background_trans = SansFileData(
                title=job_request.additional_values["scatter_direct_title"],
                type="TRANS",
                run_number=job_request.additional_values["scatter_direct_number"],
            )

        if background_trans is None or background_scatter is None:
            logger.error("Background trans or scatter is not found for title: %s", background_title)
            job_request.will_reduce = False
            return
        job_request.additional_values["can_transmission"] = background_trans.run_number
        job_request.additional_values["can_scatter"] = background_scatter.run_number
        job_request.additional_values["can_direct"] = job_request.additional_values["scatter_direct_number"]


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
