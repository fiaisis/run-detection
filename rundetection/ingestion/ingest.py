"""Ingest module holds the JobRequest class and the ingest function used to build JobRequests from nexus files."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from h5py import File  # type: ignore

from rundetection.ingestion.extracts import get_extraction_function
from rundetection.job_requests import JobRequest

logger = logging.getLogger(__name__)


def _check_if_nexus_file(path: Path) -> None:
    """
    Check if a given path is a nexus file, if not raise ValueError.

    :param path: path to check
    :return: None.
    """
    if path.suffix != ".nxs":
        raise ValueError(f"File: {path} is not a nexus file")


def load_h5py_dataset(path: Path) -> Any:
    """
    Load the nexus file into a h5py dataset.

    :param path: the path of the nexus file
    :return: (Any) The h5py dataset.
    """
    try:
        logger.info("loading dataset for %s", path)
        file = File(path)
        key = next(iter(file.keys()))  # same as: list(file.keys())[0] without the cast cost
        return file[key]
    except FileNotFoundError:
        logger.error("Nexus file could not be found: %s", path)
        raise


def ingest(path: Path) -> JobRequest:
    """
    Given the path of a nexus file, Create and return a JobRequest.

    :param path: The path of the nexus file
    :return: The JobRequest built from the given nexus file.
    """
    logger.info("Ingesting file: %s", path)
    _check_if_nexus_file(path)
    dataset = load_h5py_dataset(path)
    job_request = _build_initial_job_request(dataset, path)
    logger.info("Extracting instrument specific metadata...")
    additional_extraction_function = get_extraction_function(job_request.instrument)
    job_request = additional_extraction_function(job_request, dataset)
    logger.info("Created JobRequest: %s", job_request)
    return job_request


def _build_initial_job_request(dataset: Any, path: Path) -> JobRequest:
    """
    Build the initial job request from the given h5py job request.

    :param dataset: the dataset
    :return: the new jobrequest.
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


def get_sibling_nexus_files(nexus_path: Path) -> list[Path]:
    """
    Given the path of a nexus file, return a list of any other nexus files in the same directory.

    :param nexus_path: The nexus file for which directory to search
    :return: List of sibling nexus files.
    """
    return [Path(file) for file in nexus_path.parents[0].glob("*.nxs") if Path(file) != nexus_path]


def get_sibling_runs(nexus_path: Path) -> list[JobRequest]:
    """
    Given the path of a nexus file, return a list of ingested sibling nexus files in the same directory.

    :param nexus_path: The nexus file for which directory to search
    :return: List of JobRequest Objects.
    """
    return [ingest(file) for file in get_sibling_nexus_files(nexus_path)]


def get_run_title(nexus_path: Path) -> str:
    """
    Given the path of a nexus file, get the run title for that file.

    :param nexus_path: Path - the nexus file path
    :return: str - The title of the files run.
    """
    # Instead of using Ingest here and reusing code, we won't bother with loading too much of the file every time and
    # JUST load the title instead of everything.
    return ingest(nexus_path).experiment_title
