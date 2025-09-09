"""Enginx Rules."""

from __future__ import annotations

import contextlib
import logging
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

import xmltodict

from rundetection.exceptions import RuleViolationError
from rundetection.rules.rule import Rule

if TYPE_CHECKING:
    from rundetection.job_requests import JobRequest

logger = logging.getLogger(__name__)


class EnginxGroupRule(Rule[str]):
    """Insert the group type into the JobRequest"""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request.

        Adds the group type to the additional values after validating it against a list of valid group types.

        :param job_request: The job request to verify.
        :raises RuleViolationError: If the group type is not in the list of valid groups.
        :return: None.
        """
        group = self._value

        valid_groups = ["both", "north", "south", "cropped", "custom", "texture20", "texture30"]

        if group.lower() not in valid_groups:
            raise RuleViolationError(f"Invalid group type: {group} for EnginxGroupRule")

        job_request.additional_values["group"] = group


class EnginxBasePathRule(Rule[int | str]):
    """Base rule for resolving nexus file paths in the archive"""

    _ROOT = Path("/archive/NDXENGINX/Instrument/data")
    _DIR_GLOB = "cycle_"
    path_key: str = "x_path"  # example: "x_path", would be ceria_path, etc.

    def verify(self, job_request: JobRequest) -> None:
        run = self._coerce_run(self._value)

        found = self._find_path(run)
        if found is not None:
            job_request.additional_values[self.path_key] = str(found)
            logger.info(f"Found ceria run {run} at {found}")
        else:
            logger.warning(f"Ceria run {run} not found")

    @staticmethod
    def _coerce_run(value: int | str) -> str:
        """Return the trailing digits of value as a string (e.g. '1234')."""
        s = str(value).strip()
        m = re.search(r"(\d+)$", s)
        if not m:
            raise ValueError(f"Invalid ceria run number: {value!r}")
        return m.group(1)

    @classmethod
    def _find_path(cls, run: str) -> Path | None:
        """
        Find a file ending with '0*{run}.nxs' (case-insensitive), where the digit
        sequence is not preceded by another digit. Examples:
          ENGINX1234.nxs       ✓
          ENGINX0001234.nxs    ✓
          foo991234.nxs        ✗ (blocked by non-digit boundary)
        """
        # non-digit boundary, then any number of '0', then the run, then .nxs at end
        # Determine which cycle directory to search
        file_re = re.compile(rf"(?i)(?<!\d)0*{re.escape(run)}\.nxs$")

        try:
            cycle_str = build_enginx_run_number_cycle_map()[int(run)]
            cycle_dir = cls._ROOT / f"{cls._DIR_GLOB}{cycle_str}"
        except KeyError:
            cycle_dir = cls._latest_cycle_dir()

        # Scan that directory for a matching file
        try:
            with os.scandir(cycle_dir) as it:
                for entry in it:
                    if file_re.search(entry.name):
                        return cycle_dir / entry.name
        except (PermissionError, FileNotFoundError, OSError):
            pass
        logger.warning(f"Could not find file ending with '0*{run}.nxs' in {cycle_dir}")
        return None

    @classmethod
    def _latest_cycle_dir(cls: EnginxBasePathRule) -> Path | None:
        try:
            dirs = [p for p in cls._ROOT.glob("cycle_*_*") if p.is_dir()]
        except OSError:
            return None
        if not dirs:
            return None
        # reverse lexicographic order, pick first
        return max(dirs, key=lambda p: p.name)


class EnginxCeriaPathRule(EnginxBasePathRule):
    """Resolve and attach the CERIA calibration file path for an EnginX run."""

    path_key: str = "ceria_path"


class EnginxVanadiumPathRule(EnginxBasePathRule):
    """Resolve and attach the Vanadium calibration file path for an EnginX run."""

    path_key: str = "vanadium_path"


@lru_cache(maxsize=1)
def build_enginx_run_number_cycle_map() -> dict[int, str]:
    """
    Generate a mapping of run numbers to cycle strings based on the journal files.
    For example. mapping[242666] -> "15_1"
    :return: A dict[int, str] mapping run numbers to cycle strings.
    """
    logger.info("Building run number cycle map")
    mapping = {}

    def _walk_node(node, journal_file):
        if isinstance(node, dict):
            if "@name" in node:
                with contextlib.suppress(ValueError):
                    mapping[int(node["@name"][6:])] = journal_file[8:]
                return
            for v in node.values():
                _walk_node(v, journal_file)
        elif isinstance(node, list):
            for item in node:
                _walk_node(item, journal_file)

    logger.info("Reading journal files...")
    for path in Path("/archive/NDXENGINX/Instrument/logs/journal").glob("*.xml"):
        logger.info("Reading journal file: %s", path)
        with path.open() as journal:
            journal_dict = xmltodict.parse(journal.read())
            _walk_node(journal_dict, path.stem)
    logger.info("Mapping complete. %s", str(mapping))
    return mapping
