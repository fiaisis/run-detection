"""Ingest module holds the DetectedRun class and the ingest function used to build DetectedRuns from nexus files."""
from __future__ import annotations

import dataclasses
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, List, Callable

from h5py import File  # type: ignore

logger = logging.getLogger(__name__)


@dataclass
class DetectedRun:
    """
    DetectedRun
    """

    run_number: int
    instrument: str
    experiment_title: str
    experiment_number: str
    filepath: Path
    will_reduce: bool = True
    additional_values: Dict[str, Any] = dataclasses.field(default_factory=dict)
    additional_runs: List[DetectedRun] = dataclasses.field(default_factory=list)

    def to_json_string(self) -> str:
        """
        Returns the metadata as a json string.
        :return: The json string
        """
        dict_ = dataclasses.asdict(self)
        dict_["filepath"] = str(dict_["filepath"])
        del dict_["will_reduce"]
        del dict_["additional_runs"]
        return json.dumps(dict_)

    def split_runs(self) -> List[DetectedRun]:
        """
        Return a list of additional runs, when a rule produces additional runs.
        :return: List[DetectedRun]
        """
        self.additional_runs.append(self)
        return self.additional_runs


def ingest(path: Path) -> DetectedRun:
    """
    Given the path of a nexus file, Create and return a DetectedRun
    :param path: The path of the nexus file
    :return: The DetectedRun built from the given nexus file
    """
    logger.info("Ingesting file: %s", path)
    if path.suffix != ".nxs":
        raise ValueError(f"File: {path} is not a nexus file")
    try:
        file = File(path)
        key = list(file.keys())[0]
        dataset = file[key]
        detection_result = DetectedRun(
            run_number=int(dataset.get("run_number")[0]),  # cast to int as i32 is not json serializable
            instrument=dataset.get("beamline")[0].decode("utf-8"),
            experiment_title=dataset.get("title")[0].decode("utf-8"),
            experiment_number=dataset.get("experiment_identifier")[0].decode("utf-8"),
            filepath=path,
        )

        logger.info("extracted metadata: %s", detection_result)
        return detection_result
    except FileNotFoundError:
        logger.error("Nexus file could not be found: %s", path)
        raise


def get_sibling_nexus_files(nexus_path: Path) -> List[Path]:
    """
    Given the path of a nexus file, return a list of any other nexus files in the same directory
    :param nexus_path: The nexus file for which directory to search
    :return: List of sibling nexus files
    """
    return [Path(file) for file in nexus_path.parents[0].glob("*.nxs") if Path(file) != nexus_path]


def get_sibling_runs(nexus_path: Path) -> List[DetectedRun]:
    """
    Given the path of a nexus file, return a list of ingested sibling nexus files in the same directory
    :param nexus_path: The nexus file for which directory to search
    :return: List of DetectedRun Objects
    """
    return [ingest(file) for file in get_sibling_nexus_files(nexus_path)]


def get_run_title(nexus_path: Path) -> str:
    """
    Given the path of a nexus file, get the run title for that file
    :param nexus_path: Path - the nexus file path
    :return: str - The title of the files run
    """
    return ingest(nexus_path).experiment_title
