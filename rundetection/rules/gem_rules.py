"Rules for GEM."

from __future__ import annotations

import logging
import os
from pathlib import Path

from rundetection.job_requests import JobRequest
from rundetection.rules.rule import Rule

logger = logging.getLogger(__name__)

class GEMSiliconeRunRule(Rule[bool]):
    """Rule to identify if a GEM run is a silicone run based on the presence of a specific file."""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request. Checks for the presence of a specific file to determine if it's a silicone run.

        :param job_request: The job request to verify.
        :return: None.
        """
        if not self._value:  # if the rule is set to false, skip
            return

        # Assume that the gem directory is loaded.
        gem_root_dir = os.environ.get("GEM_DIR", "/gem")
        run_file_path = Path(gem_root_dir) / f"RB{job_request.experiment_number}" / f"GR{job_request.run_number}.nxs"

        if run_file_path.exists():
            job_request.additional_values["is_silicone_run"] = True
        else:
            job_request.additional_values["is_silicone_run"] = False


class GEMVanadiumRunRule(Rule[bool]):
    """Rule to identify if a GEM run is a vanadium run based on the presence of a specific file."""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request. Checks for the presence of a specific file to determine if it's a vanadium run.

        :param job_request: The job request to verify.
        :return: None.
        """
        if not self._value:  # if the rule is set to false, skip
            return

        # Assume that the gem directory is loaded.
        gem_root_dir = os.environ.get("GEM_DIR", "/gem")
        run_file_path = Path(gem_root_dir) / f"RB{job_request.experiment_number}" / f"GR{job_request.run_number}.nxs"

        if run_file_path.exists():
            job_request.additional_values["is_vanadium_run"] = True
        else:
            job_request.additional_values["is_vanadium_run"] = False


class GEMEmptyRunsRule(Rule[str]):
    """Adds the empty runs numbers to JobRequest."""

    def verify(self, job_request: JobRequest) -> None:
        """
        Add empty runs numbers to the job request's additional values.

        :param job_request: The job request to update with empty runs.
        """
        job_request.additional_values["empty_runs"] = self._value

