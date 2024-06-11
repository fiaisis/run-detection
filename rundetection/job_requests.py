"""Module containing the job request model"""

from __future__ import annotations

import dataclasses
import json
import typing

if typing.TYPE_CHECKING:
    from pathlib import Path
    from typing import Any


# splitting this class would be worse than this disable
# pylint: disable = too-many-instance-attributes
@dataclasses.dataclass
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
    additional_values: dict[str, Any] = dataclasses.field(default_factory=dict)
    additional_requests: list[JobRequest] = dataclasses.field(default_factory=list)

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
