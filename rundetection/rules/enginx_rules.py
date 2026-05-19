"""Enginx Rules."""

from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

import xmltodict

from rundetection.cache import cache_get_json, cache_set_json
from rundetection.exceptions import RuleViolationError
from rundetection.rules.rule import Rule

if TYPE_CHECKING:
    from rundetection.job_requests import JobRequest

logger = logging.getLogger(__name__)

ENGINX_CACHE_KEY = "run_detection:enginx:run_number_cycle_map"
ENGINX_CACHE_TTL_SECONDS = 60 * 60
ENGINX_PREFIX = "ENGINX"


@dataclass(slots=True)
class EnginxCacheEntry:
    """In-process EnginX journal cache entry."""

    items: tuple[tuple[int, str], ...]
    expires_at: float


ENGINX_CACHE: dict[Path, EnginxCacheEntry] = {}


class EnginxGroupRule(Rule[str]):
    """Insert the group type into the JobRequest."""

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
    """Base rule for resolving nexus file paths in the archive."""

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
        return ENGINX_CACHE_TTL_SECONDS
    try:
        return int(configured_ttl)
    except ValueError:
        logger.warning("Invalid ENGINX_RUN_NUMBER_CYCLE_MAP_CACHE_TTL_SECONDS value: %s", configured_ttl)
        return ENGINX_CACHE_TTL_SECONDS


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


def _add_enginx_run_to_cycle_map(mapping: dict[int, str], entry_name: Any, journal_file: str) -> bool:
    if not isinstance(entry_name, str):
        return False
    if not entry_name.upper().startswith(ENGINX_PREFIX):
        return False

    try:
        run_number = int(entry_name[len(ENGINX_PREFIX) :])
    except ValueError:
        return False
    mapping[run_number] = journal_file.removeprefix("journal_")
    return True


def _walk_enginx_journal_node(node: Any, journal_file: str, mapping: dict[int, str]) -> None:
    if isinstance(node, dict):
        if _add_enginx_run_to_cycle_map(mapping, node.get("@name"), journal_file):
            return
        for value in node.values():
            _walk_enginx_journal_node(value, journal_file, mapping)
    elif isinstance(node, list):
        for item in node:
            _walk_enginx_journal_node(item, journal_file, mapping)


def _enginx_journal_files(journal_dir: Path) -> list[Path] | None:
    logger.info("Reading journal files from %s", journal_dir)
    if not journal_dir.exists():
        logger.warning("EnginX journal directory does not exist: %s", journal_dir)
        return None
    if not journal_dir.is_dir():
        logger.warning("EnginX journal path is not a directory: %s", journal_dir)
        return None

    journal_files = list(journal_dir.glob("*.xml"))
    if not journal_files:
        logger.warning("No EnginX journal XML files found in %s", journal_dir)
        return None
    return journal_files


def _read_enginx_run_number_cycle_map(journal_dir: Path) -> dict[int, str]:
    logger.info("Building run number cycle map")
    mapping: dict[int, str] = {}

    journal_files = _enginx_journal_files(journal_dir)
    if journal_files is None:
        return mapping

    for path in journal_files:
        logger.info("Reading journal file: %s", path)
        with path.open() as journal:
            journal_dict = xmltodict.parse(journal.read())
            _walk_enginx_journal_node(journal_dict, path.stem, mapping)
    if not mapping:
        logger.warning("No EnginX runs found in journal XML files from %s", journal_dir)
    logger.info("Mapping complete")
    return mapping


def _get_cached_enginx_run_number_cycle_map(journal_dir: Path, now: float) -> dict[int, str] | None:
    cached = ENGINX_CACHE.get(journal_dir)
    if cached is not None and cached.expires_at > now:
        return dict(cached.items)
    if cached is not None:
        ENGINX_CACHE.pop(journal_dir, None)
    return None


def _cache_enginx_run_number_cycle_map(
    journal_dir: Path,
    mapping: dict[int, str],
    ttl_seconds: int,
    now: float,
) -> None:
    if mapping and ttl_seconds > 0:
        ENGINX_CACHE[journal_dir] = EnginxCacheEntry(
            items=tuple(mapping.items()),
            expires_at=now + ttl_seconds,
        )
    else:
        ENGINX_CACHE.pop(journal_dir, None)


def _clear_enginx_run_number_cycle_map_cache() -> None:
    """Clear the memoized EnginX journal mapping."""
    ENGINX_CACHE.clear()


def build_enginx_run_number_cycle_map() -> dict[int, str]:
    """
    Generate a mapping of run numbers to cycle strings based on the journal
    files.

    For example. mapping[242666] -> "15_1"

    :return: A dict[int, str] mapping run numbers to cycle strings.
    """
    ttl_seconds = _enginx_run_number_cycle_map_cache_ttl_seconds()
    journal_dir = _enginx_journal_dir()
    if ttl_seconds > 0:
        now = time.monotonic()
        in_process_mapping = _get_cached_enginx_run_number_cycle_map(journal_dir, now)
        if in_process_mapping:
            return in_process_mapping

        cached_mapping = _coerce_run_number_cycle_map(cache_get_json(ENGINX_CACHE_KEY))
        if cached_mapping:
            logger.info("Using cached EnginX run number cycle map")
            _cache_enginx_run_number_cycle_map(journal_dir, cached_mapping, ttl_seconds, now)
            return cached_mapping

    mapping = _read_enginx_run_number_cycle_map(journal_dir)
    if ttl_seconds > 0:
        _cache_enginx_run_number_cycle_map(journal_dir, mapping, ttl_seconds, now)
    if ttl_seconds > 0 and mapping:
        cache_set_json(ENGINX_CACHE_KEY, mapping, ttl_seconds)
    return mapping
