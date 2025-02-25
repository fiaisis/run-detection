"""Module containing the additional extract functions."""

from __future__ import annotations

import logging
import re
import typing

from rundetection.exceptions import IngestError, ReductionMetadataError

if typing.TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path
    from typing import Any

    from rundetection.job_requests import JobRequest

logger = logging.getLogger(__name__)


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


def sans_extract(job_request: JobRequest, dataset: Any) -> JobRequest:
    """
    Get the sample details and the cycle strings
    :param job_request: The job request
    :param dataset: The nexus file dataset
    :return: The updated job request
    """
    job_request.additional_values["cycle_string"] = get_cycle_string_from_path(job_request.filepath)
    job_request.additional_values["sample_thickness"] = float(dataset.get("sample").get("thickness")[0])
    job_request.additional_values["sample_geometry"] = str(dataset.get("sample").get("shape")[0]).lstrip("b").strip("'")
    job_request.additional_values["sample_height"] = float(dataset.get("sample").get("height")[0])
    job_request.additional_values["sample_width"] = float(dataset.get("sample").get("width")[0])

    return job_request


def _generate_loq_direct_instrumentation(dataset: Any) -> dict[str, Any]:
    keys = dataset.get("selog").keys()
    for key in keys:
        if "Aperture_" in key:
            return {"selog": {key: dataset.get("selog").get(key).get("value")[0]}}
    return {}


def loq_extract(job_request: JobRequest, dataset: Any) -> JobRequest:
    """
    Extract the LOQ specific SANs data needed later, additional_values will have a new dictionary called
    "instrument_direct_file_comparison". The dictionary will be populated in the same way as the nexus file is
    structured, excluding the raw_data_1 initial data set, this allows comparison with the direct files.
    :param job_request: The job request
    :param dataset: The nexus file dataset
    :return: The updated job request
    """
    direct_instrumentation = _generate_loq_direct_instrumentation(dataset)
    job_request.additional_values["instrument_direct_file_comparison"] = direct_instrumentation
    return sans_extract(job_request, dataset)


def _generate_sans2d_direct_instrumentation(dataset: Any) -> dict[str, Any]:
    selog_dict = {
        "selog": {
            "Rear_Det_Z": dataset.get("selog").get("Rear_Det_Z").get("value")[0],
            "Front_Det_Z": dataset.get("selog").get("Front_Det_Z").get("value")[0],
        }
    }
    # G1-5
    for ii in range(1, 6):
        g_str = f"G{ii}"
        selog_dict["selog"][g_str] = dataset.get("selog").get(g_str).get("value")[0]
    # S1-6
    for ii in range(1, 7):
        s_str = f"S{ii}"
        selog_dict["selog"][s_str] = dataset.get("selog").get(s_str).get("value")[0]
    # Jaw_E, Jaw_N, Jaw_S, Jaw_W
    for ii in ("E", "N", "S", "W"):  # type: ignore
        jaw_str = f"Jaw_{ii}"
        selog_dict["selog"][jaw_str] = dataset.get("selog").get(jaw_str).get("value")[0]

    return selog_dict


def sans2d_extract(job_request: JobRequest, dataset: Any) -> JobRequest:
    """
    Extract the SANs2D specific SANs data needed later, additional_values will have a new dictionary called
    "instrument_direct_file_comparison". The dictionary will be populated in the same way as the nexus file is
    structured, excluding the raw_data_1 initial data set, this allows comparison with the direct files.
    :param job_request: The job request
    :param dataset: The nexus file dataset
    :return: The updated job request
    """
    job_request.additional_values["instrument_direct_file_comparison"] = generate_sans2d_direct_instrumentation(dataset)
    return sans_extract(job_request, dataset)


def tosca_extract(job_request: JobRequest, _: Any) -> JobRequest:
    """
    Add the cycle_string to the job request
    :param job_request: The job request
    :param _:
    :return: The updated job request
    """
    logger.info("Performing additional tosca extraction")
    job_request.additional_values["cycle_string"] = get_cycle_string_from_path(job_request.filepath)
    return job_request


def osiris_and_iris_extract(job_request: JobRequest, dataset: Any) -> JobRequest:
    """
    Get the frequencies, and time channels from the dataset
    :param job_request: The job request
    :param dataset: The nexus file dataset
    :return: The updated job request
    """
    job_request.additional_values["cycle_string"] = get_cycle_string_from_path(job_request.filepath)

    freq_6 = float(dataset.get("selog").get("freq6").get("value_log").get("value")[0])
    freq_10 = float(dataset.get("selog").get("freq10").get("value_log").get("value")[0])

    # Accounting for floating point errors
    max_value = max(freq_6, freq_10)
    difference = abs(freq_6 - freq_10)
    if difference > max_value * 0.01:
        raise ReductionMetadataError(
            "Frequency 6 and 10 are not within 1% of each other. Osiris reduction is not possible"
        )
    if freq_6 != freq_10:
        freq_6 = round(freq_6)
        freq_10 = round(freq_10)

    job_request.additional_values["freq6"] = freq_6
    job_request.additional_values["freq10"] = freq_10

    job_request.additional_values["phase6"] = float(dataset.get("selog").get("phase6").get("value")[0])
    job_request.additional_values["phase10"] = float(dataset.get("selog").get("phase10").get("value")[0])

    tcb_detector_min = float(min(dataset.get("instrument").get("dae").get("time_channels_1").get("time_of_flight")))
    tcb_detector_max = float(max(dataset.get("instrument").get("dae").get("time_channels_1").get("time_of_flight")))
    tcb_monitor_min = float(min(dataset.get("instrument").get("dae").get("time_channels_2").get("time_of_flight")))
    tcb_monitor_max = float(max(dataset.get("instrument").get("dae").get("time_channels_2").get("time_of_flight")))
    job_request.additional_values["tcb_detector_min"] = tcb_detector_min
    job_request.additional_values["tcb_detector_max"] = tcb_detector_max
    job_request.additional_values["tcb_monitor_min"] = tcb_monitor_min
    job_request.additional_values["tcb_monitor_max"] = tcb_monitor_max

    return job_request


def osiris_extract(job_request: JobRequest, dataset: Any) -> JobRequest:
    """
    Get the frequencies, and time channels from the dataset
    :param job_request: The job request
    :param dataset: The nexus file dataset
    :return: The updated job request
    """
    return osiris_and_iris_extract(job_request, dataset)


def iris_extract(job_request: JobRequest, dataset: Any) -> JobRequest:
    """
    Get the frequencies, and time channels from the dataset
    :param job_request: The job request
    :param dataset: The nexus file dataset
    :return: The updated job request
    """
    return osiris_and_iris_extract(job_request, dataset)


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

    sam_mass = float(dataset.get("sam_mass")[0]) if dataset.get("sam_mass") is not None else 0.0
    sam_rmm = float(dataset.get("sam_rmm")[0]) if dataset.get("sam_rmm") is not None else 0.0
    remove_bkg = bool(dataset.get("remove_bkg")[0]) if dataset.get("remove_bkg") is not None else False

    job_request.additional_values["ei"] = ei
    job_request.additional_values["sam_mass"] = sam_mass
    job_request.additional_values["sam_rmm"] = sam_rmm
    job_request.additional_values["monovan"] = job_request.run_number if (sam_rmm != 0 and sam_mass != 0) else 0
    job_request.additional_values["remove_bkg"] = remove_bkg
    job_request.additional_values["sum_runs"] = False
    job_request.additional_values["runno"] = job_request.run_number

    return job_request


def get_extraction_function(instrument: str) -> Callable[[JobRequest, Any], JobRequest]:  # noqa: PLR0911
    """
    Given an instrument name, return the additional metadata extraction function for the instrument
    :param instrument: str - instrument name
    :return: Callable[[JobRequest, Any], JobRequest]: The additional metadata extraction function for the instrument
    """
    match instrument.lower():
        case "mari":
            return mari_extract
        case "tosca":
            return tosca_extract
        case "osiris":
            return osiris_extract
        case "loq":
            return loq_extract
        case "sans2d":
            return sans2d_extract
        case "iris":
            return iris_extract
        case _:
            return skip_extract


def get_cycle_string_from_path(nexus_path: Path) -> str:
    """
    Given the path of a nexus file, get the cycle string for that nexus file.
    An example of a cycle string is cycle_19_2
    :param nexus_path: The path of the nexus file
    :return: The cycle string
    """
    pattern = r"cycle_(\d+)_(\d+)"
    match = re.search(pattern, str(nexus_path))
    if match:
        return match.group(0)
    raise IngestError(f"Unable to build a cycle string for nexus file: {nexus_path}")
