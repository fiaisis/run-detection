from pathlib import Path

import pytest

from rundetection.rules.loq_rules import (
    _extract_cycle_from_file_path,
    _extract_run_number_from_filename,
    _is_can_scatter_file,
    _is_sample_direct_file,
    _is_sample_transmission_file,
)


@pytest.mark.parametrize(
    ("filename", "result"),
    [("LOQ00100002.nxs", "100002"), ("LOQ123456789.nxs", "123456789"), ("LOQ.nxs", ""), ("LOQ00000.nxs", "")],
)
def test_extract_run_number_from_filename(filename, result):
    assert _extract_run_number_from_filename(filename) == result


@pytest.mark.parametrize(
    ("filename", "result"),
    [
        (Path("/archive/NDXLOQ/Instrument/data/cycle_24_2/LOQ00100002.nxs"), "cycle_24_2"),
        (Path("/archive/NDXLOQ/Instrument/data/cycle_21_4/LOQ.nxs"), "cycle_21_4"),
    ],
)
def test_extract_cycle_from_file_path(filename, result):
    assert _extract_cycle_from_file_path(filename) == result


@pytest.mark.parametrize(
    ("file_title", "sample_title", "result"),
    [
        ("{Banana}_SANS/TRANS", "Banana", False),
        ("{Banana}_{}_SANS/TRANS", "Banana", False),
        ("{Banana}_TRANS", "Banana", True),
        ("{Banana}_SANS", "Banana", False),
        ("{Banana}_TRANS", "Apple", False),
    ],
)
def test_is_sample_transmission_file(file_title, sample_title, result):
    assert _is_sample_transmission_file(file_title, sample_title) == result


@pytest.mark.parametrize(
    ("file_title", "result"),
    [
        ("{Banana}_SANS/TRANS", False),
        ("{Banana direct}_{}_TRANS", False),
        ("{Banana direct}_TRANS", True),
        ("{Banana}_SANS", False),
        ("{Banana direct}_SANS/TRANS", False),
    ],
)
def test_is_sample_direct_file(file_title, result):
    assert _is_sample_direct_file(file_title) == result


@pytest.mark.parametrize(
    ("file_title", "can_title", "result"),
    [
        ("{Banana}_SANS/TRANS", "{Banana}", True),
        ("{Banana}_SANS/TRANS", "{Apple}", False),
        ("{Banana}_{}_SANS/TRANS", "{Banana}", False),
        ("{Banana}_TRANS", "{Banana}", False),
        ("{Banana}_{}_TRANS", "{Banana}", False),
    ],
)
def test_is_can_scatter_file(file_title, can_title, result):
    assert _is_can_scatter_file(file_title, can_title) == result


def test_is_can_scatter_file_raises():
    with pytest.raises(ValueError, match="The can title contains an _ character rundetection cannot handle that."):
        _is_can_scatter_file("{Banana}_SANS", "CAN_SCATTER_TITLE")


# @pytest.mark.parametrize("")
def test_is_can_transmission_file():
    pass


def test_find_trans_file_success():
    pass


def test_find_trans_file_fail():
    pass


def test_create_list_of_files():
    pass


def test_find_direct_file():
    pass


def test_find_can_scatter_file():
    pass


def test_can_trans_files():
    pass


def test_loq_find_files_verify():
    pass


def test_loq_user_file():
    pass
