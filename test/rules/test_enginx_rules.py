"""Tests for Enginx rules (group and path resolution)."""

from pathlib import Path
from unittest.mock import patch

import pytest

from rundetection.exceptions import RuleViolationError
from rundetection.ingestion.ingest import JobRequest
from rundetection.rules.enginx_rules import (
    ENGINX_MAP_CACHE_KEY,
    ENGINX_MAP_CACHE_TTL,
    EnginxBasePathRule,
    EnginxCeriaPathRule,
    EnginxGroupRule,
    EnginxVanadiumPathRule,
    build_enginx_run_number_cycle_map,
)


@pytest.fixture
def job_request():
    """
    Job request fixture
    :return: job request.
    """
    return JobRequest(
        run_number=100,
        filepath=Path("/archive/100/ENGINX100.nxs"),
        experiment_title="Test experiment",
        additional_values={},
        additional_requests=[],
        raw_frames=3,
        good_frames=0,
        users="",
        run_start="",
        run_end="",
        instrument="enginx",
        experiment_number="",
    )


def test_enginx_group_rule_valid_values(job_request):
    """Test that valid group values are accepted and set in additional_values"""
    valid_groups = ["both", "north", "south", "cropped", "custom", "texture20", "texture30"]
    for group in valid_groups:
        jr = job_request
        rule = EnginxGroupRule(group)
        rule.verify(jr)
        assert jr.additional_values["group"] == group


def test_enginx_group_rule_invalid_value_raises(job_request):
    """Test that an invalid group raises an exception"""
    with pytest.raises(RuleViolationError):
        EnginxGroupRule("invalid_group").verify(job_request)


@pytest.mark.parametrize(
    ("run", "expected_file"),
    [
        (241391, "ENGINX00241391.nxs"),
        ("299080", "ENGINX00299080.nxs"),
    ],
)
@patch(
    "rundetection.rules.enginx_rules.build_enginx_run_number_cycle_map", return_value={241391: "20_1", 299080: "20_1"}
)
def test_enginx_ceria_path_rule_finds_file(mock_map, run, expected_file, job_request, monkeypatch):
    """Test that EnginxCeriaPathRule sets ceria_run and ceria_path when file exists."""
    # Point the root to test data
    monkeypatch.setattr(EnginxBasePathRule, "_ROOT", Path("test/test_data/e2e_data/NDXENGINX/Instrument/data"))

    rule = EnginxCeriaPathRule(run)
    rule.verify(job_request)

    # path should end with expected_file
    ceria_path = Path(job_request.additional_values["ceria_path"])  # type: ignore[index]
    assert ceria_path.name == expected_file
    assert ceria_path.parent.name == "cycle_20_1"


@patch("rundetection.rules.enginx_rules.build_enginx_run_number_cycle_map", return_value={241391: "20_1"})
def test_enginx_vanadium_path_rule_finds_file(mock_map, job_request, monkeypatch):
    """Test that EnginxVanadiumPathRule sets the vanadium path when the file exists."""
    monkeypatch.setattr(EnginxBasePathRule, "_ROOT", Path("test/test_data/e2e_data/NDXENGINX/Instrument/data"))
    rule = EnginxVanadiumPathRule(241391)
    rule.verify(job_request)
    path = Path(job_request.additional_values["vanadium_path"])  # type: ignore[index]
    assert path.name == "ENGINX00241391.nxs"


def test_enginx_path_rule_not_found(job_request, monkeypatch):
    """If run cannot be found, no exception and no path_key set, but ceria_run still set."""
    # Ensure latest cycle dir points to test data but run is missing
    monkeypatch.setattr(EnginxBasePathRule, "_ROOT", Path("test/test_data/e2e_data/NDXENGINX/Instrument/data"))

    # Empty mapping to force fallback search
    with patch("rundetection.rules.enginx_rules.build_enginx_run_number_cycle_map", return_value={}):
        rule = EnginxCeriaPathRule(123)  # run is not present in files
        rule.verify(job_request)

    assert "ceria_path" not in job_request.additional_values


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (123, "123"),
        ("abc123", "123"),
        ("ENGINX0000123", "0000123"),
    ],
)
def test_coerce_run_parsing(value, expected):
    """Test that run is parsed correctly."""
    assert EnginxBasePathRule._coerce_run(value) == expected


def test_coerce_run_invalid_raises():
    """Test that invalid run raises an exception."""
    with pytest.raises(ValueError):  # noqa: PT011
        EnginxBasePathRule._coerce_run("no-digits")


def test_build_enginx_run_number_cycle_map_uses_cache():
    """The EnginX run cycle map should be returned from Valkey when present."""
    build_enginx_run_number_cycle_map.cache_clear()

    with (
        patch("rundetection.rules.enginx_rules.cache_get_json", return_value={"241391": "20_1"}) as cache_get,
        patch("rundetection.rules.enginx_rules.cache_set_json") as cache_set,
        patch("rundetection.rules.enginx_rules.ENGINX_JOURNAL_DIR") as journal_dir,
    ):
        mapping = build_enginx_run_number_cycle_map()

    assert mapping == {241391: "20_1"}
    cache_get.assert_called_once_with(ENGINX_MAP_CACHE_KEY)
    cache_set.assert_not_called()
    journal_dir.glob.assert_not_called()

    build_enginx_run_number_cycle_map.cache_clear()


def test_build_enginx_run_number_cycle_map_sets_cache(monkeypatch):
    """The EnginX run cycle map should be cached after reading journals."""
    build_enginx_run_number_cycle_map.cache_clear()
    journal_dir = Path("test/test_data/e2e_data/NDXENGINX/Instrument/logs/journal")
    monkeypatch.setattr("rundetection.rules.enginx_rules.ENGINX_JOURNAL_DIR", journal_dir)

    with (
        patch("rundetection.rules.enginx_rules.cache_get_json", return_value=None),
        patch("rundetection.rules.enginx_rules.cache_set_json") as cache_set,
    ):
        mapping = build_enginx_run_number_cycle_map()

    assert mapping[241391] == "20_1"
    assert mapping[299080] == "20_1"
    cache_set.assert_called_once_with(ENGINX_MAP_CACHE_KEY, mapping, ENGINX_MAP_CACHE_TTL)

    build_enginx_run_number_cycle_map.cache_clear()


def test_build_enginx_run_number_cycle_map_does_not_cache_empty_map(monkeypatch):
    """An unavailable journal directory should not poison Valkey with an empty map."""
    build_enginx_run_number_cycle_map.cache_clear()
    empty_dir = Path("test/test_data/e2e_data/NDXENGINX/Instrument/logs/missing")
    monkeypatch.setattr("rundetection.rules.enginx_rules.ENGINX_JOURNAL_DIR", empty_dir)

    with (
        patch("rundetection.rules.enginx_rules.cache_get_json", return_value=None),
        patch("rundetection.rules.enginx_rules.cache_set_json") as cache_set,
    ):
        mapping = build_enginx_run_number_cycle_map()

    assert mapping == {}
    cache_set.assert_not_called()

    build_enginx_run_number_cycle_map.cache_clear()
