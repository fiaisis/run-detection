"""Ingest module holds the JobRequest class and the ingest function used to build JobRequests from nexus files."""
from __future__ import annotations

import dataclasses
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, List, Callable

from h5py import File  # type: ignore

logger = logging.getLogger(__name__)


# splitting this class would be worse than this disable
# pylint: disable = too-many-instance-attributes
@dataclass
class JobRequest:
    """
    JobRequest
    """

    run_number: int
    instrument: str
    experiment_title: str
    experiment_number: str
    filepath: Path
    run_start: str
    run_end: str
    raw_frames: int
    good_frames: int
    users: str
    will_reduce: bool = True
    additional_values: Dict[str, Any] = dataclasses.field(default_factory=dict)
    additional_requests: List[JobRequest] = dataclasses.field(default_factory=list)

    def to_json_string(self) -> str:
        """
        Returns the metadata as a json string.
        :return: The json string
        """
        dict_ = dataclasses.asdict(self)
        dict_["filepath"] = str(dict_["filepath"])
        del dict_["will_reduce"]
        del dict_["additional_requests"]
        return json.dumps(dict_)


def _check_if_nexus_file(path: Path) -> None:
    """
    Check if a given path is a nexus file, if not raise ValueError
    :param path: path to check
    :return: None
    """
    if path.suffix != ".nxs":
        raise ValueError(f"File: {path} is not a nexus file")


def _load_h5py_dataset(path: Path) -> Any:
    """
    Load the nexus file into a h5py dataset
    :param path: the path of the nexus file
    :return: (Any) The h5py dataset
    """
    try:
        logger.info("loading dataset for %s", path)
        file = File(path)
        key = list(file.keys())[0]
        return file[key]
    except FileNotFoundError:
        logger.error("Nexus file could not be found")
        raise


def ingest(path: Path) -> JobRequest:
    """
    Given the path of a nexus file, Create and return a JobRequest
    :param path: The path of the nexus file
    :return: The JobRequest built from the given nexus file
    """
    logger.info("Ingesting file: %s", path)
    _check_if_nexus_file(path)
    dataset = _load_h5py_dataset(path)
    job_request = _build_initial_job_request(dataset, path)
    logger.info("Extracting instrument specific metadata...")
    additional_extraction_function = get_extraction_function(job_request.instrument)
    job_request = additional_extraction_function(job_request, dataset)
    logger.info("Created JobRequest: %s", job_request)
    return job_request


def _build_initial_job_request(dataset: Any, path: Path) -> JobRequest:
    """
    Build the initial job request from the given h5py job request
    :param dataset: the dataset
    :return: the new jobrequest
    """
    logger.info("Extracting common metadata...")
    return JobRequest(
        run_number=int(dataset.get("run_number")[0]),  # cast to int as i32 is not json serializable
        instrument=dataset.get("beamline")[0].decode("utf-8"),
        experiment_title=dataset.get("title")[0].decode("utf-8"),
        run_start=dataset.get("start_time")[0].decode("utf-8"),
        run_end=dataset.get("end_time")[0].decode("utf-8"),
        raw_frames=int(dataset.get("raw_frames")[0]),
        good_frames=int(dataset.get("good_frames")[0]),
        users=dataset.get("user_1").get("name")[0].decode("utf-8"),
        experiment_number=dataset.get("experiment_identifier")[0].decode("utf-8"),
        filepath=path,
    )


def skip_extract(job_request: JobRequest, _: Any) -> JobRequest:
    """
    Skips the extraction of additional metadata for a given JobRequest instance and dataset, when the extraction of
    additional metadata is not required or not applicable for a specific instrument or dataset.

    :param job_request: JobRequest instance for which the additional metadata extraction should be skipped
    :param _: The dataset from which the additional metadata extraction is to be skipped
    :return: JobRequest instance without updating additional metadata

    """
    logger.info(
        "No additional extraction needed for job_request: %s %s", job_request.instrument, job_request.run_number
    )
    return job_request


def mari_extract(job_request: JobRequest, dataset: Any) -> JobRequest:
    """
    Extracts additional metadata specific to the MARI instrument from the given dataset and updates the JobRequest
    instance. If the metadata does not exist, the default values will be set instead.

    :param job_request: JobRequest instance for which to extract additional metadata
    :param dataset: The dataset from which to extract additional MARI-specific metadata. (The type is a h5py group)
    :return: JobRequest instance with updated additional metadata

    This function extracts MARI-specific metadata including incident energy (ei), sample mass (sam_mass),
    sample relative molecular mass (sam_rmm), monovanadium run number (monovan), and background removal flag
    (remove_bkg). The extracted metadata is stored in the additional_values attribute of the JobRequest instance.
    """

    ei = dataset.get("ei")
    if ei and len(ei) == 1:
        ei = float(ei[0])
    elif ei and len(ei) > 1:
        ei = [float(val) for val in ei]
    else:
        ei = "'auto'"

    if dataset.get("sam_mass") is not None:
        sam_mass = float(dataset.get("sam_mass")[0])
    else:
        sam_mass = 0.0
    if dataset.get("sam_rmm") is not None:
        sam_rmm = float(dataset.get("sam_rmm")[0])
    else:
        sam_rmm = 0.0

    if dataset.get("remove_bkg") is not None:
        remove_bkg = bool(dataset.get("remove_bkg")[0])
    else:
        remove_bkg = False

    job_request.additional_values["ei"] = ei
    job_request.additional_values["sam_mass"] = sam_mass
    job_request.additional_values["sam_rmm"] = sam_rmm
    job_request.additional_values["monovan"] = job_request.run_number if (sam_rmm != 0 and sam_mass != 0) else 0
    job_request.additional_values["remove_bkg"] = remove_bkg
    job_request.additional_values["sum_runs"] = False
    job_request.additional_values["runno"] = job_request.run_number

    return job_request


def get_extraction_function(instrument: str) -> Callable[[JobRequest, Any], JobRequest]:
    """
    Given an instrument name, return the additional metadata extraction function for the instrument
    :param instrument: str - instrument name
    :return: Callable[[JobRequest, Any], JobRequest]: The additional metadata extraction function for the instrument
    """
    match instrument.lower():
        case "mari":
            return mari_extract
        case _:
            return skip_extract


def get_sibling_nexus_files(nexus_path: Path) -> List[Path]:
    """
    Given the path of a nexus file, return a list of any other nexus files in the same directory
    :param nexus_path: The nexus file for which directory to search
    :return: List of sibling nexus files
    """
    return [Path(file) for file in nexus_path.parents[0].glob("*.nxs") if Path(file) != nexus_path]


def get_sibling_runs(nexus_path: Path) -> List[JobRequest]:
    """
    Given the path of a nexus file, return a list of ingested sibling nexus files in the same directory
    :param nexus_path: The nexus file for which directory to search
    :return: List of JobRequest Objects
    """
    return [ingest(file) for file in get_sibling_nexus_files(nexus_path)]


def get_run_title(nexus_path: Path) -> str:
    """
    Given the path of a nexus file, get the run title for that file
    :param nexus_path: Path - the nexus file path
    :return: str - The title of the files run
    """
    return ingest(nexus_path).experiment_title
