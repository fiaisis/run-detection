"""Enginx Rules."""

import contextlib
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from pathlib import Path
from threading import Event

import xmltodict

from rundetection.exceptions import RuleViolationError
from rundetection.job_requests import JobRequest
from rundetection.rules.rule import Rule

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
    _ROOT = Path("/archive/NDXENGINX/Instrument/data")
    _DIR_GLOB = "cycle_"
    _MAX_WORKERS = 10  # This seems fine, recommended 8-32 for slow SMB shares
    path_key: str = "x_path"  # example: "x_path", would be ceria_path, etc.

    def verify(self, job_request: JobRequest) -> None:
        run = self._coerce_run(self._value)  # e.g., "1234"
        job_request.additional_values["ceria_run"] = run

        found = self._find_path(run)
        if found is not None:
            job_request.additional_values[self.path_key] = str(found)
            logger.info(f"Found ceria run {run} at {found}")
        else:
            # Optionally hard-fail:
            # raise RuleViolationError(f"Ceria run {run} not found")
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
    def _find_path(cls, run: str) -> Path | None:  # noqa: C901
        """
        Find a file ending with '0*{run}.nxs' (case-insensitive), where the digit
        sequence is not preceded by another digit. Examples:
          ENGINX1234.nxs       ✓
          ENGINX0001234.nxs    ✓
          foo991234.nxs        ✗ (blocked by non-digit boundary)
        """
        # non-digit boundary, then any number of '0', then the run, then .nxs at end
        file_re = re.compile(rf"(?i)(?<!\d)0*{re.escape(run)}\.nxs$")
        cycle_str = build_enginx_run_number_cycle_map()[int(run)]

        try:
            cycle_dirs = [p for p in cls._ROOT.glob(f"{cls._DIR_GLOB}{cycle_str}") if p.is_dir()]
        except OSError:
            cycle_dirs = []
        if not cycle_dirs:
            return None

        stop = Event()

        def scan_one_dir(d: Path) -> Path | None:
            if stop.is_set():
                return None
            try:
                with os.scandir(d) as it:
                    for entry in it:
                        if stop.is_set():
                            return None
                        name = entry.name
                        if file_re.search(name):
                            return d / name
            except (PermissionError, FileNotFoundError, OSError):
                return None
            return None

        with ThreadPoolExecutor(max_workers=cls._MAX_WORKERS) as ex:
            futures = {ex.submit(scan_one_dir, d): d for d in cycle_dirs}
            for fut in as_completed(futures):
                match = fut.result()
                if match:
                    stop.set()
                    return match

        return None


class EnginxCeriaPathRule(EnginxBasePathRule):
    path_key: str = "ceria_path"


class EnginxVanadiumPathRule(EnginxBasePathRule):
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

    for path in Path("/archive/NDXENGINX/Instrument/logs/journal").glob("*.xml"):
        with path.open() as journal:
            journal_dict = xmltodict.parse(journal.read())
            _walk_node(journal_dict, path.stem)
    return mapping
