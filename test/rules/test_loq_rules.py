from pathlib import Path
from unittest import mock

import pytest

from rundetection.job_requests import JobRequest
from rundetection.rules.common_rules import FileData
from rundetection.rules.loq_rules import (
    LoqFindFiles,
    LoqUserFile,
    _extract_run_number_from_filename,
    _find_can_scatter_file,
    _find_can_trans_file,
    _find_direct_file,
    _find_trans_file,
    _is_can_scatter_file,
    _is_can_transmission_file,
    _is_sample_direct_file,
    _is_sample_transmission_file,
)

SANS_FILES = [
    FileData(title="{direct/empty beam}", type="TRANS", run_number="-1"),
    FileData(title="{Banana}", type="SANS/TRANS", run_number="0"),
    FileData(title="{Banana}", type="TRANS", run_number="1"),
    FileData(title="{Apple}", type="SANS/TRANS", run_number="2"),
    FileData(title="{Apple}", type="TRANS", run_number="3"),
    FileData(title="{direct beam}", type="TRANS", run_number="4"),
]


@pytest.mark.parametrize(
    ("filename", "result"),
    [("LOQ00100002.nxs", "100002"), ("LOQ123456789.nxs", "123456789"), ("LOQ.nxs", ""), ("LOQ00000.nxs", "")],
)
def test_extract_run_number_from_filename(filename, result):
    assert _extract_run_number_from_filename(filename) == result


@pytest.mark.parametrize(
    ("sans_file", "sample_title", "result"),
    [
        (FileData(title="{Banana}", type="SANS/TRANS", run_number="0"), "Banana", False),
        (FileData(title="{Banana}", type="TRANS", run_number="0"), "Banana", True),
        (FileData(title="{Banana}", type="SANS", run_number="0"), "Banana", False),
        (FileData(title="{Banana}", type="TRANS", run_number="0"), "Apple", False),
    ],
)
def test_is_sample_transmission_file(sans_file, sample_title, result):
    assert _is_sample_transmission_file(sans_file, sample_title) == result


@pytest.mark.parametrize(
    ("sans_file", "result"),
    [
        (FileData(title="{Banana}", type="TRANS", run_number="0"), False),
        (FileData(title="{Banana direct}", type="SANS/TRANS", run_number="0"), False),
        (FileData(title="{Banana direct}", type="TRANS", run_number="0"), True),
        (FileData(title="{Banana empty}", type="TRANS", run_number="0"), True),
        (FileData(title="{Banana direct}", type="SANS", run_number="0"), False),
    ],
)
def test_is_sample_direct_file(sans_file, result):
    assert _is_sample_direct_file(sans_file) == result


@pytest.mark.parametrize(
    ("sans_file", "can_title", "result"),
    [
        (FileData(title="{Banana}", type="SANS/TRANS", run_number="0"), "{Banana}", True),
        (FileData(title="{Banana}", type="SANS/TRANS", run_number="0"), "{Apple}", False),
        (FileData(title="{Banana}", type="TRANS", run_number="0"), "{Banana}", False),
        (FileData(title="{Banana}_{}", type="TRANS", run_number="0"), "{Banana}", False),
    ],
)
def test_is_can_scatter_file(sans_file, can_title, result):
    assert _is_can_scatter_file(sans_file, can_title) == result


@pytest.mark.parametrize(
    ("sans_file", "can_title", "result"),
    [
        (FileData(title="{Banana}", type="SANS/TRANS", run_number="0"), "{Banana}", False),
        (FileData(title="{Banana}", type="TRANS", run_number="0"), "{Apple}", False),
        (FileData(title="{Banana}", type="TRANS", run_number="0"), "{Banana}", True),
    ],
)
def test_is_can_transmission_file(sans_file, can_title, result):
    assert _is_can_transmission_file(sans_file, can_title) == result


@pytest.mark.parametrize(
    ("sans_files", "sample_title", "expected"),
    [(SANS_FILES, "{Apple}", SANS_FILES[4]), (SANS_FILES, "{Banana}", SANS_FILES[2])],
)
def test_find_trans_file_success(sans_files, sample_title, expected):
    assert _find_trans_file(sans_files, sample_title) == expected


def test_find_trans_file_fail():
    assert _find_trans_file(SANS_FILES, "{Lemmon}") is None


def test_find_direct_file():
    assert _find_direct_file(SANS_FILES) == SANS_FILES[-1]


def test_find_can_scatter_file():
    assert _find_can_scatter_file(SANS_FILES, "{Apple}") == SANS_FILES[3]


def test_can_trans_files():
    assert _find_can_trans_file(SANS_FILES, "{Apple}") == SANS_FILES[4]


def test_loq_find_files_verify_title_too_long():
    job_request = JobRequest(
        run_number=0,
        instrument="",
        experiment_title="too_long_problems_here",
        experiment_number="",
        filepath=Path(),
        run_start="",
        run_end="",
        raw_frames=0,
        good_frames=0,
        users="",
        will_reduce=True,
        additional_values={},
        additional_requests=[],
    )
    LoqFindFiles(value=True).verify(job_request)
    assert job_request.will_reduce is False


def test_loq_find_files_verify_title_too_short():
    job_request = JobRequest(
        run_number=0,
        instrument="",
        experiment_title="tooshortproblemshere",
        experiment_number="",
        filepath=Path(),
        run_start="",
        run_end="",
        raw_frames=0,
        good_frames=0,
        users="",
        will_reduce=True,
        additional_values={},
        additional_requests=[],
    )
    LoqFindFiles(value=True).verify(job_request)
    assert job_request.will_reduce is False


def test_loq_find_files_verify_no_files_left():
    job_request = JobRequest(
        run_number=0,
        instrument="",
        experiment_title="{}_{}_sans/trans",
        experiment_number="",
        filepath=Path(),
        run_start="",
        run_end="",
        raw_frames=0,
        good_frames=0,
        users="",
        will_reduce=True,
        additional_values={},
        additional_requests=[],
    )
    with mock.patch("rundetection.rules.loq_rules.create_list_of_files", return_value=[]):
        loq_find_files = LoqFindFiles(value=True)
        loq_find_files.verify(job_request)
    assert job_request.will_reduce is False


def test_loq_find_files_verify_some_files_found_but_none_valid():
    job_request = JobRequest(
        run_number=0,
        instrument="",
        experiment_title="{}_{}_sans/trans",
        experiment_number="",
        filepath=Path("/path/cycle_24_2/LOQ.nxs"),
        run_start="",
        run_end="",
        raw_frames=0,
        good_frames=0,
        users="",
        will_reduce=True,
        additional_values={},
        additional_requests=[],
    )
    with (
        mock.patch("rundetection.rules.loq_rules.create_list_of_files", return_value=[FileData("", "", "")]),
        mock.patch(
            "rundetection.rules.loq_rules.strip_excess_files",
            return_value=[FileData("", "", ""), FileData("", "", ""), FileData("", "", "")],
        ),
    ):
        loq_find_files = LoqFindFiles(value=True)
        loq_find_files.verify(job_request)
    assert job_request.will_reduce is True
    assert job_request.additional_values["run_number"] == 0


def test_loq_find_files_trans_file_found():
    job_request = JobRequest(
        run_number=5,
        instrument="",
        experiment_title="{scatter}_{background}_sans/trans",
        experiment_number="",
        filepath=Path("/path/cycle_24_2/LOQ.nxs"),
        run_start="",
        run_end="",
        raw_frames=0,
        good_frames=0,
        users="",
        will_reduce=True,
        additional_values={},
        additional_requests=[],
    )
    with (
        mock.patch("rundetection.rules.loq_rules.create_list_of_files", return_value=[FileData("", "", "")]),
        mock.patch(
            "rundetection.rules.loq_rules.strip_excess_files",
            return_value=[
                FileData(title="{scatter}", type="TRANS", run_number="1"),
                FileData(title="{background}", type="TRANS", run_number="2"),
                FileData(title="{direct}", type="SANS/TRANS", run_number="3"),
            ],
        ),
    ):
        loq_find_files = LoqFindFiles(value=True)
        loq_find_files.verify(job_request)
    assert job_request.will_reduce is True
    assert job_request.additional_values["run_number"] == 5  # noqa: PLR2004
    assert job_request.additional_values["scatter_transmission"] == "1"


def test_loq_find_files_can_transmission_file_found():
    job_request = JobRequest(
        run_number=5,
        instrument="",
        experiment_title="{scatter}_{background}_sans/trans",
        experiment_number="",
        filepath=Path("/path/cycle_24_2/LOQ.nxs"),
        run_start="",
        run_end="",
        raw_frames=0,
        good_frames=0,
        users="",
        will_reduce=True,
        additional_values={},
        additional_requests=[],
    )
    with (
        mock.patch("rundetection.rules.loq_rules.create_list_of_files", return_value=[FileData("", "", "")]),
        mock.patch(
            "rundetection.rules.loq_rules.strip_excess_files",
            return_value=[
                FileData(title="{scatter}", type="TRANS", run_number="1"),
                FileData(title="{background}", type="SANS/TRANS", run_number="2"),
                FileData(title="{background}", type="TRANS", run_number="3"),
                FileData(title="{direct}", type="SANS/TRANS", run_number="4"),
            ],
        ),
    ):
        loq_find_files = LoqFindFiles(value=True)
        loq_find_files.verify(job_request)
    assert job_request.will_reduce is True
    assert job_request.additional_values["run_number"] == 5  # noqa: PLR2004
    assert job_request.additional_values["can_transmission"] == "3"


def test_loq_find_files_direct_file_found():
    job_request = JobRequest(
        run_number=5,
        instrument="",
        experiment_title="{scatter}_{background}_sans/trans",
        experiment_number="",
        filepath=Path("/path/cycle_24_2/LOQ.nxs"),
        run_start="",
        run_end="",
        raw_frames=0,
        good_frames=0,
        users="",
        will_reduce=True,
        additional_values={},
        additional_requests=[],
    )
    with (
        mock.patch("rundetection.rules.loq_rules.create_list_of_files", return_value=[FileData("", "", "")]),
        mock.patch(
            "rundetection.rules.loq_rules.strip_excess_files",
            return_value=[
                FileData(title="{scatter}", type="TRANS", run_number="1"),
                FileData(title="{background}", type="SANS/TRANS", run_number="2"),
                FileData(title="{background}", type="TRANS", run_number="3"),
                FileData(title="{direct}", type="TRANS", run_number="4"),
            ],
        ),
    ):
        loq_find_files = LoqFindFiles(value=True)
        loq_find_files.verify(job_request)
    assert job_request.will_reduce is True
    assert job_request.additional_values["run_number"] == 5  # noqa: PLR2004
    assert job_request.additional_values["scatter_direct"] == "4"
    assert job_request.additional_values["can_direct"] == "4"


def test_loq_find_files_can_scatter_file_found():
    job_request = JobRequest(
        run_number=5,
        instrument="",
        experiment_title="{scatter}_{background}_sans/trans",
        experiment_number="",
        filepath=Path("/path/cycle_24_2/LOQ.nxs"),
        run_start="",
        run_end="",
        raw_frames=0,
        good_frames=0,
        users="",
        will_reduce=True,
        additional_values={},
        additional_requests=[],
    )
    with (
        mock.patch("rundetection.rules.loq_rules.create_list_of_files", return_value=[FileData("", "", "")]),
        mock.patch(
            "rundetection.rules.loq_rules.strip_excess_files",
            return_value=[
                FileData(title="{scatter}", type="TRANS", run_number="1"),
                FileData(title="{background}", type="SANS/TRANS", run_number="2"),
                FileData(title="{background}", type="TRANS", run_number="3"),
                FileData(title="{direct}", type="SANS/TRANS", run_number="4"),
            ],
        ),
    ):
        loq_find_files = LoqFindFiles(value=True)
        loq_find_files.verify(job_request)
    assert job_request.will_reduce is True
    assert job_request.additional_values["run_number"] == 5  # noqa: PLR2004
    assert job_request.additional_values["can_scatter"] == "2"


def test_loq_user_file():
    job_request = JobRequest(
        run_number=0,
        instrument="",
        experiment_title="",
        experiment_number="",
        filepath=Path(),
        run_start="",
        run_end="",
        raw_frames=0,
        good_frames=0,
        users="",
        will_reduce=True,
        additional_values={},
        additional_requests=[],
    )
    LoqUserFile(value="loq_user_file").verify(job_request)
    assert job_request.additional_values["user_file"] == "/extras/loq/loq_user_file"
    assert len(job_request.additional_values) == 1
