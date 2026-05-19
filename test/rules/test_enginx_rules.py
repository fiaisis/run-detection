"""Tests for Enginx rules (group and path resolution)."""

import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from rundetection.exceptions import RuleViolationError
from rundetection.ingestion.ingest import JobRequest
from rundetection.rules.enginx_rules import (
    ENGINX_CACHE_KEY,
    EnginxBasePathRule,
    EnginxCeriaPathRule,
    EnginxGroupRule,
    EnginxVanadiumPathRule,
    _clear_enginx_run_number_cycle_map_cache,
    _read_enginx_run_number_cycle_map,
    build_enginx_run_number_cycle_map,
)

REPEATED_BUILD_CALL_COUNT = 2


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


@pytest.fixture(autouse=True)
def reset_enginx_run_number_cycle_map_cache():
    """Clear the in-process EnginX journal mapping cache around each test."""
    _clear_enginx_run_number_cycle_map_cache()
    yield
    _clear_enginx_run_number_cycle_map_cache()


def test_enginx_group_rule_valid_values(job_request):
    """Test that valid group values are accepted and set in additional_values."""
    valid_groups = ["both", "north", "south", "cropped", "custom", "texture20", "texture30"]
    for group in valid_groups:
        jr = job_request
        rule = EnginxGroupRule(group)
        rule.verify(jr)
        assert jr.additional_values["group"] == group


def test_enginx_group_rule_invalid_value_raises(job_request):
    """Test that an invalid group raises an exception."""
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


@patch("rundetection.rules.enginx_rules.cache_set_json")
@patch("rundetection.rules.enginx_rules.cache_get_json", return_value={"241391": "20_1"})
def test_build_enginx_run_number_cycle_map_uses_cached_mapping(mock_cache_get, mock_cache_set):
    """
    Cached JSON mapping is returned with run numbers coerced back to
    ints.
    """
    assert build_enginx_run_number_cycle_map() == {241391: "20_1"}
    mock_cache_get.assert_called_once_with(ENGINX_CACHE_KEY)
    mock_cache_set.assert_not_called()


@patch("rundetection.rules.enginx_rules.cache_set_json")
@patch("rundetection.rules.enginx_rules.cache_get_json", return_value=None)
def test_build_enginx_run_number_cycle_map_reads_journals_and_populates_cache(
    mock_cache_get,
    mock_cache_set,
    monkeypatch,
):
    """A cache miss reads journal files and stores the parsed mapping."""
    monkeypatch.setenv(
        "ENGINX_JOURNAL_DIR",
        "test/test_data/e2e_data/NDXENGINX/Instrument/logs/journal",
    )
    monkeypatch.setenv("ENGINX_RUN_NUMBER_CYCLE_MAP_CACHE_TTL_SECONDS", "123")

    mapping = build_enginx_run_number_cycle_map()

    assert mapping[241391] == "20_1"
    assert mapping[299080] == "20_1"
    mock_cache_get.assert_called_once_with(ENGINX_CACHE_KEY)
    mock_cache_set.assert_called_once_with(ENGINX_CACHE_KEY, mapping, 123)


@patch("rundetection.rules.enginx_rules.cache_set_json")
@patch("rundetection.rules.enginx_rules.cache_get_json")
def test_build_enginx_run_number_cycle_map_skips_cache_when_ttl_disabled(
    mock_cache_get,
    mock_cache_set,
    monkeypatch,
):
    """A non-positive TTL disables the Valkey read/write path."""
    monkeypatch.setenv(
        "ENGINX_JOURNAL_DIR",
        "test/test_data/e2e_data/NDXENGINX/Instrument/logs/journal",
    )
    monkeypatch.setenv("ENGINX_RUN_NUMBER_CYCLE_MAP_CACHE_TTL_SECONDS", "0")

    mapping = build_enginx_run_number_cycle_map()

    assert mapping[241391] == "20_1"
    mock_cache_get.assert_not_called()
    mock_cache_set.assert_not_called()


def test_read_enginx_run_number_cycle_map_warns_when_journal_dir_missing(
    tmp_path,
    caplog: pytest.LogCaptureFixture,
):
    """
    Missing journal directories return an empty mapping with a clear
    warning.
    """
    caplog.set_level(logging.WARNING)
    journal_dir = tmp_path / "missing"

    assert _read_enginx_run_number_cycle_map(journal_dir) == {}

    assert f"EnginX journal directory does not exist: {journal_dir}" in caplog.text


def test_read_enginx_run_number_cycle_map_warns_when_journal_path_is_not_dir(
    tmp_path,
    caplog: pytest.LogCaptureFixture,
):
    """A configured journal path that is a file is reported clearly."""
    caplog.set_level(logging.WARNING)
    journal_dir = tmp_path / "journal.xml"
    journal_dir.write_text("")

    assert _read_enginx_run_number_cycle_map(journal_dir) == {}

    assert f"EnginX journal path is not a directory: {journal_dir}" in caplog.text


def test_read_enginx_run_number_cycle_map_warns_when_no_xml_files_found(
    tmp_path,
    caplog: pytest.LogCaptureFixture,
):
    """
    Empty journal directories return an empty mapping with a clear
    warning.
    """
    caplog.set_level(logging.WARNING)

    assert _read_enginx_run_number_cycle_map(tmp_path) == {}

    assert f"No EnginX journal XML files found in {tmp_path}" in caplog.text


@patch("rundetection.rules.enginx_rules.cache_set_json")
@patch("rundetection.rules.enginx_rules.cache_get_json")
def test_build_enginx_run_number_cycle_map_does_not_cache_empty_mapping(
    mock_cache_get,
    mock_cache_set,
    monkeypatch,
    tmp_path,
):
    """Empty journal mappings are not stored locally or in Valkey."""
    monkeypatch.setenv("ENGINX_JOURNAL_DIR", str(tmp_path))
    monkeypatch.setenv("ENGINX_RUN_NUMBER_CYCLE_MAP_CACHE_TTL_SECONDS", "123")
    mock_cache_get.return_value = None

    with patch("rundetection.rules.enginx_rules._read_enginx_run_number_cycle_map", return_value={}) as mock_read:
        assert build_enginx_run_number_cycle_map() == {}
        assert build_enginx_run_number_cycle_map() == {}

    assert mock_read.call_count == REPEATED_BUILD_CALL_COUNT
    assert mock_cache_get.call_count == REPEATED_BUILD_CALL_COUNT
    mock_cache_set.assert_not_called()


@patch("rundetection.rules.enginx_rules.cache_set_json")
@patch("rundetection.rules.enginx_rules.cache_get_json")
def test_build_enginx_run_number_cycle_map_reads_journals_when_ttl_disabled(
    mock_cache_get,
    mock_cache_set,
    monkeypatch,
):
    """When caching is disabled by TTL, journal XML is parsed per call."""
    monkeypatch.setenv(
        "ENGINX_JOURNAL_DIR",
        "test/test_data/e2e_data/NDXENGINX/Instrument/logs/journal",
    )
    monkeypatch.setenv("ENGINX_RUN_NUMBER_CYCLE_MAP_CACHE_TTL_SECONDS", "0")

    with patch(
        "rundetection.rules.enginx_rules._read_enginx_run_number_cycle_map",
        wraps=_read_enginx_run_number_cycle_map,
    ) as mock_read:
        first_mapping = build_enginx_run_number_cycle_map()
        first_mapping[241391] = "mutated"
        second_mapping = build_enginx_run_number_cycle_map()

    assert second_mapping[241391] == "20_1"
    assert mock_read.call_count == REPEATED_BUILD_CALL_COUNT
    mock_cache_get.assert_not_called()
    mock_cache_set.assert_not_called()


@patch("rundetection.rules.enginx_rules.cache_set_json")
@patch("rundetection.rules.enginx_rules.cache_get_json", return_value=None)
def test_build_enginx_run_number_cycle_map_memoizes_journal_reads_after_valkey_miss(
    mock_cache_get,
    mock_cache_set,
    monkeypatch,
):
    """Repeated Valkey misses reuse the in-process journal mapping."""
    monkeypatch.setenv(
        "ENGINX_JOURNAL_DIR",
        "test/test_data/e2e_data/NDXENGINX/Instrument/logs/journal",
    )
    monkeypatch.setenv("ENGINX_RUN_NUMBER_CYCLE_MAP_CACHE_TTL_SECONDS", "123")

    with patch(
        "rundetection.rules.enginx_rules._read_enginx_run_number_cycle_map",
        wraps=_read_enginx_run_number_cycle_map,
    ) as mock_read:
        assert build_enginx_run_number_cycle_map()[241391] == "20_1"
        assert build_enginx_run_number_cycle_map()[241391] == "20_1"

    mock_read.assert_called_once()
    assert mock_cache_get.call_count == REPEATED_BUILD_CALL_COUNT
    assert mock_cache_set.call_count == REPEATED_BUILD_CALL_COUNT


@patch("rundetection.rules.enginx_rules.cache_set_json")
@patch("rundetection.rules.enginx_rules.cache_get_json", return_value=None)
def test_build_enginx_run_number_cycle_map_refreshes_expired_local_cache(
    mock_cache_get,
    mock_cache_set,
    monkeypatch,
):
    """Expired in-process cache entries are reread from journal files."""
    monkeypatch.setenv("ENGINX_JOURNAL_DIR", "journal")
    monkeypatch.setenv("ENGINX_RUN_NUMBER_CYCLE_MAP_CACHE_TTL_SECONDS", "50")

    with (
        patch(
            "rundetection.rules.enginx_rules._read_enginx_run_number_cycle_map",
            side_effect=[{241391: "20_1"}, {241391: "21_1"}],
        ) as mock_read,
        patch("rundetection.rules.enginx_rules.time.monotonic", side_effect=[100.0, 151.0]),
    ):
        assert build_enginx_run_number_cycle_map()[241391] == "20_1"
        assert build_enginx_run_number_cycle_map()[241391] == "21_1"

    assert mock_read.call_count == REPEATED_BUILD_CALL_COUNT
    assert mock_cache_get.call_count == REPEATED_BUILD_CALL_COUNT
    assert mock_cache_set.call_count == REPEATED_BUILD_CALL_COUNT
