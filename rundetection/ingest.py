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


def skip_extract(run: DetectedRun, _: Any) -> DetectedRun:
    """
    Skips the extraction of additional metadata for a given DetectedRun instance and dataset, when the extraction of
    additional metadata is not required or not applicable for a specific instrument or dataset.

    :param run: DetectedRun instance for which the additional metadata extraction should be skipped
    :param _: The dataset from which the additional metadata extraction is to be skipped
    :return: DetectedRun instance without updating additional metadata

    """
    logger.info("No additional extraction needed for run: %s %s", run.instrument, run.run_number)
    return run


def mari_extract(run: DetectedRun, dataset: Any) -> DetectedRun:
    """
    Extracts additional metadata specific to the MARI instrument from the given dataset and updates the DetectedRun
    instance. If the metadata does not exist, the default values will be set instead.

    :param run: DetectedRun instance for which to extract additional metadata
    :param dataset: The dataset from which to extract additional MARI-specific metadata. (The type is a h5py group)
    :return: DetectedRun instance with updated additional metadata

    This function extracts MARI-specific metadata including incident energy (ei), sample mass (sam_mass),
    sample relative molecular mass (sam_rmm), monovanadium run number (monovan), and background removal flag
    (remove_bkg). The extracted metadata is stored in the additional_values attribute of the DetectedRun instance.
    """

    ei = dataset.get("ei")
    if ei and len(ei) == 1:
        ei = float(ei[0])
    elif ei and len(ei) > 1:
        ei = [float(val) for val in ei]
    else:
        ei = "auto"

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
        remove_bkg = True

    run.additional_values["ei"] = ei
    run.additional_values["sam_mass"] = sam_mass
    run.additional_values["sam_rmm"] = sam_rmm
    run.additional_values["monovan"] = run.run_number if (sam_rmm != 0 and sam_mass != 0) else 0
    run.additional_values["remove_bkg"] = remove_bkg
    run.additional_values["sum_runs"] = False
    run.additional_values["runno"] = run.run_number

    return run


def get_extraction_function(instrument: str) -> Callable[[DetectedRun, Any], DetectedRun]:
    """
    Given an instrument name, return the additional metadata extraction function for the instrument
    :param instrument: str - instrument name
    :return: Callable[[DetectedRun, Any], DetectedRun]: The additional metadata extraction function for the instrument
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
