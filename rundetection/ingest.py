"""
Ingest module holds the NexusMetadata class and the ingest function used to build NexusMetadata from nexus files
"""
import dataclasses
import json
import logging
from dataclasses import dataclass
from pathlib import Path

from h5py import File  # type: ignore

logger = logging.getLogger(__name__)


@dataclass
class NexusMetadata:
    """
    Dataclass of metadata built from nexus files. Includes method to return object as json string
    """
    run_number: int
    instrument: str
    experiment_title: str
    experiment_number: str

    def to_json_string(self) -> str:
        """
        Returns the metadata as a json string
        :return: The json string
        """
        return json.dumps(dataclasses.asdict(self))


def ingest(path: Path) -> NexusMetadata:
    """
    Given the path of a nexus file, Create and return a NexusMetadata object
    :param path: The path of the nexus file
    :return: The NexusMetadata of the given nexus file
    """
    logger.info("Ingesting file: %s", path)
    file = File(path)
    key = list(file.keys())[0]
    dataset = file[key]
    metadata = NexusMetadata(dataset.get('run_number')[0],
                             dataset.get("beamline")[0].decode("utf-8"),
                             dataset.get("title")[0].decode("utf-8"),
                             dataset.get("experiment_identifier")[0].decode("utf-8"))

    logger.info("extracted metadata: %s", metadata)
    return metadata
