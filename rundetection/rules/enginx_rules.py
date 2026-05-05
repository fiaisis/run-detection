"""Enginx Rules."""

from __future__ import annotations

import contextlib
import logging
import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

import xmltodict

from rundetection.cache import cache_get_json, cache_set_json
from rundetection.exceptions import RuleViolationError
from rundetection.rules.rule import Rule

if TYPE_CHECKING:
    from rundetection.job_requests import JobRequest

logger = logging.getLogger(__name__)

ENGINX_RUN_NUMBER_CYCLE_MAP_CACHE_KEY = "run_detection:enginx:run_number_cycle_map"
ENGINX_RUN_NUMBER_CYCLE_MAP_CACHE_TTL_SECONDS = 60 * 60


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
        """
        Find the given ceria or vanadium file path and attach it to the job request.
        :param job_request: The job request to add to
        :return: None
        """
        run = self._coerce_run(self._value)

        found = self._find_path(run)
        if found is not None:
            job_request.additional_values[self.path_key] = str(found)
            logger.info(f"Found {self.path_key} run {run} at {found}")
        else:
            logger.warning(f"{self.path_key} run {run} not found")

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
        file_re = re.compile(rf"(?i)(?<!\d)0*{re.escape(run)}\.nxs$")
        cycle_dir: Path | None
        try:
            cycle_str = build_enginx_run_number_cycle_map()[int(run)]
            cycle_dir = cls._ROOT / f"{cls._DIR_GLOB}{cycle_str}"
        except KeyError:
            cycle_dir = cls._latest_cycle_dir()
        if cycle_dir is None:
            return None

        try:
            with os.scandir(cycle_dir) as it:  # scandir is faster
                for entry in it:
                    if file_re.search(entry.name):
                        return cycle_dir / entry.name
        except (PermissionError, FileNotFoundError, OSError):
            pass
        logger.warning(f"Could not find file ending with '0*{run}.nxs' in {cycle_dir}")
        return None

    @classmethod
    def _latest_cycle_dir(cls: type[Self]) -> Path | None:
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


def _enginx_journal_dir() -> Path:
    configured_path = os.environ.get("ENGINX_JOURNAL_DIR")
    if configured_path:
        return Path(configured_path)
    return Path(os.environ.get("ARCHIVE_ROOT", "/archive")) / "NDXENGINX" / "Instrument" / "logs" / "journal"


def _enginx_run_number_cycle_map_cache_ttl_seconds() -> int:
    configured_ttl = os.environ.get("ENGINX_RUN_NUMBER_CYCLE_MAP_CACHE_TTL_SECONDS")
    if configured_ttl is None:
        return ENGINX_RUN_NUMBER_CYCLE_MAP_CACHE_TTL_SECONDS
    try:
        return int(configured_ttl)
    except ValueError:
        logger.warning("Invalid ENGINX_RUN_NUMBER_CYCLE_MAP_CACHE_TTL_SECONDS value: %s", configured_ttl)
        return ENGINX_RUN_NUMBER_CYCLE_MAP_CACHE_TTL_SECONDS


def _coerce_run_number_cycle_map(value: Any) -> dict[int, str] | None:
    if not isinstance(value, dict):
        return None

    mapping: dict[int, str] = {}
    for run_number, cycle in value.items():
        if not isinstance(cycle, str):
            return None
        try:
            mapping[int(run_number)] = cycle
        except (TypeError, ValueError):
            return None
    return mapping


def _read_enginx_run_number_cycle_map(journal_dir: Path) -> dict[int, str]:
    logger.info("Building run number cycle map")
    mapping: dict[int, str] = {}

    def _walk_node(node: list[Any] | dict[str, Any], journal_file: str) -> None:
        if isinstance(node, dict):
            entry_name = node.get("@name")
            if isinstance(entry_name, str) and entry_name.upper().startswith("ENGINX"):
                with contextlib.suppress(ValueError):
                    mapping[int(entry_name[6:])] = journal_file.removeprefix("journal_")
                return
            for v in node.values():
                _walk_node(v, journal_file)
        elif isinstance(node, list):
            for item in node:
                _walk_node(item, journal_file)

    logger.info("Reading journal files from %s", journal_dir)
    for path in journal_dir.glob("*.xml"):
        logger.info("Reading journal file: %s", path)
        with path.open() as journal:
            journal_dict = xmltodict.parse(journal.read())
            _walk_node(journal_dict, path.stem)
    logger.info("Mapping complete")
    return mapping


def build_enginx_run_number_cycle_map() -> dict[int, str]:
    """
    Generate a mapping of run numbers to cycle strings based on the journal files.
    For example. mapping[242666] -> "15_1"
    :return: A dict[int, str] mapping run numbers to cycle strings.
    """
    ttl_seconds = _enginx_run_number_cycle_map_cache_ttl_seconds()
    if ttl_seconds > 0:
        cached_mapping = _coerce_run_number_cycle_map(cache_get_json(ENGINX_RUN_NUMBER_CYCLE_MAP_CACHE_KEY))
        if cached_mapping is not None:
            logger.info("Using cached EnginX run number cycle map")
            return cached_mapping

    mapping = _read_enginx_run_number_cycle_map(_enginx_journal_dir())
    if ttl_seconds > 0:
        cache_set_json(ENGINX_RUN_NUMBER_CYCLE_MAP_CACHE_KEY, mapping, ttl_seconds)
    return mapping
