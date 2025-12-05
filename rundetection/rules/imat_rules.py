"""Rules for Iris."""

from __future__ import annotations

import logging
import os
import typing

from pathlib import Path

from rundetection.rules.rule import Rule
from rundetection.exceptions import RuleViolationError

if typing.TYPE_CHECKING:
    from rundetection.job_requests import JobRequest

logger = logging.getLogger(__name__)


class IMATFindImagesRule(Rule[bool]):
    """Finds the IMAT image files"""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request. Find the IMAT image files and prep for the script.

        :param job_request: The job request to verify.
        :return: None.
        """
        # Assume that the imat directory is loaded.
        imat_root_dir = os.environ.get("IMAT_DIR", "/imat")

        imat_dir_path = Path(imat_root_dir) / f"RB{job_request.experiment_number}"
        if imat_dir_path.exists():
            job_request.additional_values["images_dir"] = str(imat_dir_path)
        else:
            logger.error("Images dir could not be found for experiment number: %s", job_request.experiment_number)
            raise RuleViolationError("Images dir could not be found for experiment number: %s",
                                     job_request.experiment_number)
