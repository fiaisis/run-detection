"""Tests for LOQ rules."""

from pathlib import Path
from unittest import mock

from rundetection.job_requests import JobRequest
from rundetection.rules.sans_rules import (
    SansCanFiles,
    SansFileData,
    SansPhiLimits,
    SansScatterTransFiles,
    SansSliceWavs,
    SansUserFile,
    _refresh_local_journal,
)


def test_loq_trans_file_last():
    """
    Test that LOQ transmission file is correctly identified when it's the last file.

    :return: None.
    """
    job_request = JobRequest(
        run_number=5,
        instrument="",
        experiment_title="{Banana}_TRANS",
        experiment_number="",
        filepath=Path(),
        run_start="",
        run_end="",
        raw_frames=0,
        good_frames=0,
        users="",
        will_reduce=True,
        additional_values={"included_trans_as_scatter": False},
        additional_requests=[],
    )
    with (
        mock.patch(
            "rundetection.rules.sans_rules._create_list_of_files",
            return_value=[
                SansFileData(title="{Banana}", type="SANS/TRANS", run_number="1"),
                SansFileData(title="{Apple}", type="SANS/TRANS", run_number="2"),
                SansFileData(title="{Apple}", type="TRANS", run_number="3"),
                SansFileData(title="{direct beam}", type="TRANS", run_number="4"),
            ],
        ),
        mock.patch("rundetection.rules.sans_rules._is_direct_file", return_value=True),
    ):
        loq_find_files = SansScatterTransFiles(value=True)
        loq_find_files.verify(job_request)
    assert job_request.will_reduce is True
    assert job_request.additional_values["scatter_number"] == "1"
    assert job_request.additional_values["scatter_direct_number"] == "4"
    assert job_request.additional_values["scatter_transmission_number"] == "5"


def test_loq_trans_file_last_not_found_scatter():
    """
    Test that reduction is skipped when scatter file is not found for LOQ transmission file.

    :return: None.
    """
    job_request = JobRequest(
        run_number=5,
        instrument="",
        experiment_title="{Banana}_TRANS",
        experiment_number="",
        filepath=Path(),
        run_start="",
        run_end="",
        raw_frames=0,
        good_frames=0,
        users="",
        will_reduce=True,
        additional_values={"included_trans_as_scatter": False},
        additional_requests=[],
    )
    with (
        mock.patch(
            "rundetection.rules.sans_rules._create_list_of_files",
            return_value=[
                SansFileData(title="{Apple}", type="SANS/TRANS", run_number="2"),
                SansFileData(title="{Apple}", type="TRANS", run_number="3"),
                SansFileData(title="{direct beam}", type="TRANS", run_number="4"),
            ],
        ),
        mock.patch("rundetection.rules.sans_rules._is_direct_file", return_value=True),
    ):
        loq_find_files = SansScatterTransFiles(value=True)
        loq_find_files.verify(job_request)
    assert job_request.will_reduce is False


def test_loq_find_files_verify_no_files_left():
    """
    Test that reduction is skipped when no files are found for LOQ.

    :return: None.
    """
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
        additional_values={"included_trans_as_scatter": False},
        additional_requests=[],
    )
    with mock.patch("rundetection.rules.sans_rules._create_list_of_files", return_value=[]):
        loq_find_files = SansScatterTransFiles(value=True)
        loq_find_files.verify(job_request)
    assert job_request.will_reduce is False


def test_loq_find_files_verify_some_files_found_but_none_valid():
    """
    Test that reduction is skipped when files are found for LOQ but none are valid.

    :return: None.
    """
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
        additional_values={"included_trans_as_scatter": False},
        additional_requests=[],
    )
    with mock.patch(
        "rundetection.rules.sans_rules._create_list_of_files",
        return_value=[SansFileData("", "", ""), SansFileData("", "", ""), SansFileData("", "", "")],
    ):
        loq_find_files = SansScatterTransFiles(value=True)
        loq_find_files.verify(job_request)
    assert job_request.will_reduce is False


def test_loq_find_files_trans_file_found():
    """
    Test that LOQ transmission file is correctly identified and processed.

    :return: None.
    """
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
        additional_values={"included_trans_as_scatter": False},
        additional_requests=[],
    )
    with (
        mock.patch(
            "rundetection.rules.sans_rules._create_list_of_files",
            return_value=[
                SansFileData(title="{scatter}", type="TRANS", run_number="1"),
                SansFileData(title="{background}", type="TRANS", run_number="2"),
                SansFileData(title="{background}", type="SANS/TRANS", run_number="3"),
                SansFileData(title="{direct}", type="TRANS", run_number="4"),
            ],
        ),
        mock.patch("rundetection.rules.sans_rules._is_direct_file", return_value=True),
    ):
        loq_find_files = SansScatterTransFiles(value=True)
        loq_find_files.verify(job_request)
    assert job_request.will_reduce is True
    assert job_request.additional_values["scatter_number"] == "5"
    assert job_request.additional_values["scatter_title"] == "{scatter}_{background}_sans/trans"
    assert job_request.additional_values["scatter_transmission_number"] == "1"
    assert "can_scatter" not in job_request.additional_values


def test_loq_find_files_can_transmission_file_found():
    """
    Test that LOQ can transmission file is correctly identified and processed.

    :return: None.
    """
    job_request = JobRequest(
        run_number=5,
        instrument="",
        experiment_title="{scatter}_{background}_sans/trans",
        experiment_number="",
        filepath=Path("/path/cycle_24_2/LOQ0001234.nxs"),
        run_start="",
        run_end="",
        raw_frames=0,
        good_frames=0,
        users="",
        will_reduce=True,
        additional_values={"included_trans_as_scatter": False, "scatter_title": "{scatter}_{background}_sans/trans"},
        additional_requests=[],
    )
    with (
        mock.patch(
            "rundetection.rules.sans_rules._create_list_of_files",
            return_value=[
                SansFileData(title="{scatter}", type="TRANS", run_number="1"),
                SansFileData(title="{background}", type="SANS/TRANS", run_number="2"),
                SansFileData(title="{background}", type="TRANS", run_number="3"),
                SansFileData(title="{direct}", type="SANS/TRANS", run_number="4"),
            ],
        ),
        mock.patch("rundetection.rules.sans_rules._is_direct_file", return_value=True),
    ):
        _refresh_local_journal(job_request=job_request)
        job_request.additional_values["scatter_title"] = "{scatter}_{background}_sans/trans"
        job_request.additional_values["scatter_direct_number"] = "4"
        job_request.additional_values["scatter_direct_title"] = "{direct}_trans"
        loq_find_files = SansCanFiles(value=True)
        loq_find_files.verify(job_request)
    assert job_request.will_reduce is True
    assert job_request.additional_values["can_scatter"] == "2"
    assert job_request.additional_values["can_transmission"] == "3"


def test_loq_find_files_no_background_use_direct_only():
    """
    Test that LOQ direct file is used when no background is available.

    :return: None.
    """
    job_request = JobRequest(
        run_number=5,
        instrument="",
        experiment_title="{scatter}_sans/trans",
        experiment_number="",
        filepath=Path("/path/cycle_24_2/LOQ0001234.nxs"),
        run_start="",
        run_end="",
        raw_frames=0,
        good_frames=0,
        users="",
        will_reduce=True,
        additional_values={"included_trans_as_scatter": False, "scatter_title": "{scatter}_{background}_sans/trans"},
        additional_requests=[],
    )
    with (
        mock.patch(
            "rundetection.rules.sans_rules._create_list_of_files",
            return_value=[
                SansFileData(title="{scatter}", type="TRANS", run_number="1"),
                SansFileData(title="{background}", type="SANS/TRANS", run_number="2"),
                SansFileData(title="{direct}", type="TRANS", run_number="3"),
                SansFileData(title="{direct}", type="SANS/TRANS", run_number="4"),
            ],
        ),
        mock.patch("rundetection.rules.sans_rules._is_direct_file", return_value=True),
    ):
        _refresh_local_journal(job_request=job_request)
        job_request.additional_values["scatter_title"] = "{scatter}_sans/trans"
        job_request.additional_values["scatter_direct_number"] = "3"
        job_request.additional_values["scatter_direct_title"] = "{direct}_trans"
        loq_find_files = SansCanFiles(value=True)
        loq_find_files.verify(job_request)
    assert job_request.will_reduce is True
    assert job_request.additional_values["can_scatter"] == "4"
    assert job_request.additional_values["can_transmission"] == "3"


def test_loq_find_files_direct_file_found():
    """
    Test that LOQ direct file is correctly identified and processed.

    :return: None.
    """
    job_request = JobRequest(
        run_number=5,
        instrument="",
        experiment_title="{scatter}_{background}_sans/trans",
        experiment_number="",
        filepath=Path("/path/cycle_24_2/LOQ0005.nxs"),
        run_start="",
        run_end="",
        raw_frames=0,
        good_frames=0,
        users="",
        will_reduce=True,
        additional_values={
            "included_trans_as_scatter": False,
            "scatter_title": "{scatter}_{background}_sans/trans",
            "instrument_direct_file_comparison": {"seloq": {"Jaw_N": 0.1}},
        },
        additional_requests=[],
    )
    with (
        mock.patch(
            "rundetection.rules.sans_rules._create_list_of_files",
            return_value=[
                SansFileData(title="{scatter}", type="TRANS", run_number="1"),
                SansFileData(title="{background}", type="SANS/TRANS", run_number="2"),
                SansFileData(title="{background}", type="TRANS", run_number="3"),
                SansFileData(title="{direct}", type="TRANS", run_number="4"),
            ],
        ),
        mock.patch(
            "rundetection.rules.sans_rules._generate_direct_file_path",
            return_value=Path("/path/cycle_24_2/LOQ0004.nxs"),
        ),
        mock.patch("rundetection.rules.sans_rules.load_h5py_dataset") as load_h5py_dataset,
    ):
        load_h5py_dataset.return_value = {"seloq": {"Jaw_N": {"value": [0.1]}}}
        _refresh_local_journal(job_request=job_request)
        loq_find_files = SansScatterTransFiles(value=True)
        loq_find_files.verify(job_request)
        loq_can_files = SansCanFiles(value=True)
        loq_can_files.verify(job_request)
    assert job_request.will_reduce is True
    assert job_request.additional_values["scatter_number"] == "5"
    assert job_request.additional_values["scatter_direct_number"] == "4"
    assert job_request.additional_values["can_direct"] == "4"


def test_loq_find_files_can_scatter_file_found():
    """
    Test that LOQ can scatter file is correctly identified and processed.

    :return: None.
    """
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
        additional_values={"included_trans_as_scatter": False},
        additional_requests=[],
    )
    with (
        mock.patch(
            "rundetection.rules.sans_rules._create_list_of_files",
            return_value=[
                SansFileData(title="{scatter}", type="TRANS", run_number="1"),
                SansFileData(title="{background}", type="TRANS", run_number="2"),
                SansFileData(title="{background}", type="SANS/TRANS", run_number="3"),
                SansFileData(title="{direct}", type="TRANS", run_number="4"),
            ],
        ),
        mock.patch("rundetection.rules.sans_rules._is_direct_file", return_value=True),
    ):
        loq_find_files = SansScatterTransFiles(value=True)
        loq_find_files.verify(job_request)
        loq_can_files = SansCanFiles(value=True)
        loq_can_files.verify(job_request)
    assert job_request.will_reduce is True
    assert job_request.additional_values["scatter_number"] == "5"
    assert job_request.additional_values["can_scatter"] == "3"


def test_loq_user_file_m3():
    """
    Test that LOQ user file M3 is correctly set and does not include transmission as scatter.

    :return: None.
    """
    job_request = JobRequest(
        run_number=0,
        instrument="LOQ",
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
    SansUserFile(value="loq_user_file_M3").verify(job_request)
    assert job_request.additional_values["user_file"] == "/extras/loq/loq_user_file_M3"
    assert not job_request.additional_values["included_trans_as_scatter"]
    assert len(job_request.additional_values) == 2  # noqa: PLR2004


def test_loq_user_file_m4():
    """
    Test that LOQ user file M4 is correctly set and includes transmission as scatter.

    :return: None.
    """
    job_request = JobRequest(
        run_number=0,
        instrument="LOQ",
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
    SansUserFile(value="loq_user_file_M4").verify(job_request)
    assert job_request.additional_values["user_file"] == "/extras/loq/loq_user_file_M4"
    assert job_request.additional_values["included_trans_as_scatter"]
    assert len(job_request.additional_values) == 2  # noqa: PLR2004


def test_loq_verify_checks_m4():
    """
    Test that LOQ M4 mode correctly processes transmission as scatter.

    :return: None.
    """
    job_request = JobRequest(
        run_number=5,
        instrument="LOQ",
        experiment_title="{scatter}_{background}_sans/trans",
        experiment_number="",
        filepath=Path("/path/cycle_24_4/LOQ.nxs"),
        run_start="",
        run_end="",
        raw_frames=0,
        good_frames=0,
        users="",
        will_reduce=True,
        additional_values={"included_trans_as_scatter": True, "cycle_string": "cycle_24_4"},
        additional_requests=[],
    )
    with (
        mock.patch(
            "rundetection.rules.sans_rules._create_list_of_files",
            return_value=[
                SansFileData(title="{scatter}", type="TRANS", run_number="1"),
                SansFileData(title="{background}", type="TRANS", run_number="2"),
                SansFileData(title="{background}", type="SANS/TRANS", run_number="3"),
                SansFileData(title="{direct}", type="TRANS", run_number="4"),
            ],
        ),
        mock.patch("rundetection.rules.sans_rules._is_direct_file", return_value=True),
    ):
        loq_find_files = SansScatterTransFiles(value=True)
        loq_find_files.verify(job_request)
        loq_can_files = SansCanFiles(value=True)
        loq_can_files.verify(job_request)
    assert job_request.will_reduce is True
    assert job_request.additional_values["scatter_number"] == "5"
    assert job_request.additional_values["scatter_transmission_number"] == "5"
    assert job_request.additional_values["can_scatter"] == "3"
    assert job_request.additional_values["can_transmission"] == "3"


def test_loq_slice_wavs():
    """
    Test that LOQ slice wavelengths are correctly set in the job request.

    :return: None.
    """
    job_request = JobRequest(
        run_number=5,
        instrument="LOQ",
        experiment_title="{scatter}_{background}_sans/trans",
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
    SansSliceWavs("[1.2, 2.3, 3.4]").verify(job_request)
    assert job_request.additional_values["slice_wavs"] == "[1.2, 2.3, 3.4]"
    assert len(job_request.additional_values) == 1


def test_loq_phi_limits():
    """
    Test that LOQ phi limits are correctly set in the job request.

    :return: None.
    """
    job_request = JobRequest(
        run_number=5,
        instrument="LOQ",
        experiment_title="{scatter}_{background}_sans/trans",
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
    SansPhiLimits("[(-30, 30), (60, 120)]").verify(job_request)
    assert job_request.additional_values["phi_limits"] == "[(-30, 30), (60, 120)]"
    assert len(job_request.additional_values) == 1
