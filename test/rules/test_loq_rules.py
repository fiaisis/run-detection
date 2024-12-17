import tempfile
from pathlib import Path
from unittest import mock

import pytest

from rundetection.job_requests import JobRequest
from rundetection.rules.loq_rules import (
    LoqFindFiles,
    LoqUserFile,
    SansFileData,
    _extract_run_number_from_filename,
    _find_can_scatter_file,
    _find_can_trans_file,
    _find_direct_file,
    _find_trans_file,
    _is_can_scatter_file,
    _is_can_transmission_file,
    _is_sample_direct_file,
    _is_sample_transmission_file,
    find_path_for_run_number,
    grab_cycle_instrument_index,
    strip_excess_files,
)

SANS_FILES = [
    SansFileData(title="{direct/empty beam}", type="TRANS", run_number="-1"),
    SansFileData(title="{Banana}", type="SANS/TRANS", run_number="0"),
    SansFileData(title="{Banana}", type="TRANS", run_number="1"),
    SansFileData(title="{Apple}", type="SANS/TRANS", run_number="2"),
    SansFileData(title="{Apple}", type="TRANS", run_number="3"),
    SansFileData(title="{direct beam}", type="TRANS", run_number="4"),
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
        (SansFileData(title="{Banana}", type="SANS/TRANS", run_number="0"), "Banana", False),
        (SansFileData(title="{Banana}", type="TRANS", run_number="0"), "Banana", True),
        (SansFileData(title="{Banana}", type="SANS", run_number="0"), "Banana", False),
        (SansFileData(title="{Banana}", type="TRANS", run_number="0"), "Apple", False),
    ],
)
def test_is_sample_transmission_file(sans_file, sample_title, result):
    assert _is_sample_transmission_file(sans_file, sample_title) == result


@pytest.mark.parametrize(
    ("sans_file", "result"),
    [
        (SansFileData(title="{Banana}", type="TRANS", run_number="0"), False),
        (SansFileData(title="{Banana direct}", type="SANS/TRANS", run_number="0"), False),
        (SansFileData(title="{Banana direct}", type="TRANS", run_number="0"), True),
        (SansFileData(title="{Banana empty}", type="TRANS", run_number="0"), True),
        (SansFileData(title="{Banana direct}", type="SANS", run_number="0"), False),
    ],
)
def test_is_sample_direct_file(sans_file, result):
    assert _is_sample_direct_file(sans_file) == result


@pytest.mark.parametrize(
    ("sans_file", "can_title", "result"),
    [
        (SansFileData(title="{Banana}", type="SANS/TRANS", run_number="0"), "{Banana}", True),
        (SansFileData(title="{Banana}", type="SANS/TRANS", run_number="0"), "{Apple}", False),
        (SansFileData(title="{Banana}", type="TRANS", run_number="0"), "{Banana}", False),
        (SansFileData(title="{Banana}_{}", type="TRANS", run_number="0"), "{Banana}", False),
    ],
)
def test_is_can_scatter_file(sans_file, can_title, result):
    assert _is_can_scatter_file(sans_file, can_title) == result


@pytest.mark.parametrize(
    ("sans_file", "can_title", "result"),
    [
        (SansFileData(title="{Banana}", type="SANS/TRANS", run_number="0"), "{Banana}", False),
        (SansFileData(title="{Banana}", type="TRANS", run_number="0"), "{Apple}", False),
        (SansFileData(title="{Banana}", type="TRANS", run_number="0"), "{Banana}", True),
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
        SansFileData(title="", type="", run_number="0"),
        SansFileData(title="", type="", run_number="1"),
        SansFileData(title="", type="", run_number="2"),
    ]
    new_list = strip_excess_files(files, 1)
    assert new_list == [SansFileData(title="", type="", run_number="0")]


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
        additional_values={
            "included_trans_as_scatter": False
        },
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
        additional_values={
            "included_trans_as_scatter": False
        },
        additional_requests=[],
    )
    with (
        mock.patch("rundetection.rules.loq_rules.create_list_of_files", return_value=[SansFileData("", "", "")]),
        mock.patch(
            "rundetection.rules.loq_rules.strip_excess_files",
            return_value=[SansFileData("", "", ""), SansFileData("", "", ""), SansFileData("", "", "")],
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
        additional_values={
            "included_trans_as_scatter": False
        },
        additional_requests=[],
    )
    with (
        mock.patch("rundetection.rules.loq_rules.create_list_of_files", return_value=[SansFileData("", "", "")]),
        mock.patch(
            "rundetection.rules.loq_rules.strip_excess_files",
            return_value=[
                SansFileData(title="{scatter}", type="TRANS", run_number="1"),
                SansFileData(title="{background}", type="TRANS", run_number="2"),
                SansFileData(title="{direct}", type="SANS/TRANS", run_number="3"),
            ],
        ),
    ):
        loq_find_files = LoqFindFiles(value=True)
        loq_find_files.verify(job_request)
    assert job_request.will_reduce is True
    assert job_request.additional_values["run_number"] == 5  # noqa: PLR2004
    assert job_request.additional_values["scatter_transmission"] == "1"
    assert "can_scatter" not in job_request.additional_values


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
        additional_values={
            "included_trans_as_scatter": False
        },
        additional_requests=[],
    )
    with (
        mock.patch("rundetection.rules.loq_rules.create_list_of_files", return_value=[SansFileData("", "", "")]),
        mock.patch(
            "rundetection.rules.loq_rules.strip_excess_files",
            return_value=[
                SansFileData(title="{scatter}", type="TRANS", run_number="1"),
                SansFileData(title="{background}", type="SANS/TRANS", run_number="2"),
                SansFileData(title="{background}", type="TRANS", run_number="3"),
                SansFileData(title="{direct}", type="SANS/TRANS", run_number="4"),
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
        additional_values={
            "included_trans_as_scatter": False
        },
        additional_requests=[],
    )
    with (
        mock.patch("rundetection.rules.loq_rules.create_list_of_files", return_value=[SansFileData("", "", "")]),
        mock.patch(
            "rundetection.rules.loq_rules.strip_excess_files",
            return_value=[
                SansFileData(title="{scatter}", type="TRANS", run_number="1"),
                SansFileData(title="{background}", type="SANS/TRANS", run_number="2"),
                SansFileData(title="{background}", type="TRANS", run_number="3"),
                SansFileData(title="{direct}", type="TRANS", run_number="4"),
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
        additional_values={
            "included_trans_as_scatter": False
        },
        additional_requests=[],
    )
    with (
        mock.patch("rundetection.rules.loq_rules.create_list_of_files", return_value=[SansFileData("", "", "")]),
        mock.patch(
            "rundetection.rules.loq_rules.strip_excess_files",
            return_value=[
                SansFileData(title="{scatter}", type="TRANS", run_number="1"),
                SansFileData(title="{background}", type="SANS/TRANS", run_number="2"),
                SansFileData(title="{background}", type="TRANS", run_number="3"),
                SansFileData(title="{direct}", type="SANS/TRANS", run_number="4"),
            ],
        ),
    ):
        loq_find_files = LoqFindFiles(value=True)
        loq_find_files.verify(job_request)
    assert job_request.will_reduce is True
    assert job_request.additional_values["run_number"] == 5  # noqa: PLR2004
    assert job_request.additional_values["can_scatter"] == "2"


def test_loq_user_file_m3():
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
    LoqUserFile(value="loq_user_file_M3").verify(job_request)
    assert job_request.additional_values["user_file"] == "/extras/loq/loq_user_file_M3"
    assert not job_request.additional_values["included_trans_as_scatter"]
    assert len(job_request.additional_values) == 2  # noqa: PLR2004


def test_loq_user_file_m4():
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
    LoqUserFile(value="loq_user_file_M4").verify(job_request)
    assert job_request.additional_values["user_file"] == "/extras/loq/loq_user_file_M4"
    assert job_request.additional_values["included_trans_as_scatter"]
    assert len(job_request.additional_values) == 2  # noqa: PLR2004


def test_loq_verify_checks_m4():
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
        additional_values={
            "included_trans_as_scatter": True
        },
        additional_requests=[],
    )
    with (
        mock.patch("rundetection.rules.loq_rules.create_list_of_files", return_value=[SansFileData("", "", "")]),
        mock.patch(
            "rundetection.rules.loq_rules.strip_excess_files",
            return_value=[
                SansFileData(title="{scatter}", type="TRANS", run_number="1"),
                SansFileData(title="{background}", type="SANS/TRANS", run_number="2"),
                SansFileData(title="{background}", type="TRANS", run_number="3"),
                SansFileData(title="{direct}", type="SANS/TRANS", run_number="4"),
            ],
        ),
    ):
        loq_find_files = LoqFindFiles(value=True)
        loq_find_files.verify(job_request)
    assert job_request.will_reduce is True
    assert job_request.additional_values["run_number"] == 5  # noqa: PLR2004
    assert job_request.additional_values["scatter_transmission"] == 5  # noqa: PLR2004
    assert job_request.additional_values["can_scatter"] == "2"
    assert job_request.additional_values["can_transmission"] == "2"


