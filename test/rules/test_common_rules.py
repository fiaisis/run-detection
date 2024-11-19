"""
Unit tests for common rules
"""
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pytest

from rundetection.ingestion.ingest import JobRequest
from rundetection.rules.common_rules import (
    CheckIfScatterSANS,
    EnabledRule,
    FileData,
    find_path_for_run_number,
    grab_cycle_instrument_index,
    strip_excess_files,
)


@pytest.fixture
def job_request():
    """
    job_request Fixture
    :return: JobRequest
    """
    return JobRequest(1, "larmor", "1", "1", Path("/archive/larmor/1/1.nxs"), "start time", "end time", 1, 1, "users")


def test_enabled_rule_when_enabled(job_request) -> None:
    """
    Test verify method will return true when value is true
    :param job_request: JobRequest fixture
    :return: None
    """
    rule = EnabledRule(True)
    rule.verify(job_request)
    assert job_request.will_reduce


def test_enabled_rule_when_not_enabled(job_request) -> None:
    """
    Test verify method will return false when value is false
    :param job_request: JobRequest fixture
    :return: None
    """
    rule = EnabledRule(False)
    rule.verify(job_request)
    assert job_request.will_reduce is False


@pytest.mark.parametrize("end_of_title", ["_TRANS", "_SANS", "COOL", "_sans/trans"])
def test_checkifscattersans_verify_raises_for_no_sans_trans(end_of_title) -> None:
    job_request = mock.MagicMock()
    job_request.experiment_title = "{fancy chemical}" + end_of_title
    CheckIfScatterSANS(True).verify(job_request)

    assert job_request.will_reduce is False


@pytest.mark.parametrize("to_raise", ["direct", "DIRECT", "empty", "EMPTY"])
def test_checkifscattersans_verify_raises_for_direct_or_empty_in_title(to_raise) -> None:
    job_request = mock.MagicMock()
    job_request.experiment_title = "{fancy chemical " + to_raise + "}_SANS/TRANS"
    CheckIfScatterSANS(True).verify(job_request)

    assert job_request.will_reduce is False


if __name__ == "__main__":
    unittest.main()


def test_path_for_run_number_with_some_zeros():
    tempdir = tempfile.mkdtemp()
    path = f"{tempdir}/LOQ0012345.nxs"
    with Path(path).open("a"):
        assert find_path_for_run_number(tempdir, 12345) == Path(path)


def test_path_for_run_number_with_no_zeros():
    tempdir = tempfile.mkdtemp()
    path = f"{tempdir}/LOQ12345.nxs"
    with Path(path).open("a"):
        assert find_path_for_run_number(tempdir, 12345) == Path(path)


def test_path_for_run_number_too_many_zeros():
    tempdir = tempfile.mkdtemp()
    with Path(f"{tempdir}/LOQ00000000000012345.nxs").open("a"):
        assert find_path_for_run_number(tempdir, 12345) is None


def test_path_for_run_number_doesnt_exist():
    tempdir = tempfile.mkdtemp()
    assert find_path_for_run_number(tempdir, 12345) is None


def test_grab_cycle_instrument_index():
    with mock.patch("rundetection.rules.loq_rules.requests") as requests:
        cycle_index_text = grab_cycle_instrument_index("cycle_24_2")
        assert cycle_index_text == requests.get.return_value.text
        requests.get.assert_called_once_with("http://data.isis.rl.ac.uk/journals/ndxloq/journal_24_2.xml", timeout=5)


def test_strip_excess_files():
    files = [
        FileData(title="", type="", run_number="0"),
        FileData(title="", type="", run_number="1"),
        FileData(title="", type="", run_number="2"),
    ]
    new_list = strip_excess_files(files, 1)
    assert new_list == [FileData(title="", type="", run_number="0")]
