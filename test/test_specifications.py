"""
Specification unit test module
"""

import datetime
import os
from pathlib import Path
from unittest import mock
from unittest.mock import patch

import pytest

from rundetection.ingestion.ingest import JobRequest
from rundetection.specifications import InstrumentSpecification


@pytest.fixture()
def job_request():
    """
    JobRequest fixture
    :return: The job request fixture
    """
    return JobRequest(1, "larmor", "1", "1", Path("/archive/larmor/1/1,nxs"), "start time", "end time", 1, 1, "user")


@pytest.fixture()
@patch("rundetection.specifications.InstrumentSpecification._load_rules_from_api")
def specification(_) -> InstrumentSpecification:
    """
    InstrumentSpecification Fixture
    :return: InstrumentSpecification
    """
    return InstrumentSpecification("foo")


@pytest.fixture()
def _working_directory_fix():
    # Set dir to repo root for purposes of the test.
    current_working_directory = Path.cwd()
    if current_working_directory.name == "test":
        os.chdir(current_working_directory / "..")


@mock.patch("rundetection.specifications.InstrumentSpecification._load_rules_from_api")
def test_instrument_specification_tries_to_loads_api_from_db(load_rules_from_api):
    InstrumentSpecification("mari")
    load_rules_from_api.assert_called_once_with()


def test_instrument_specification_load_rules_for_api():
    pass


@mock.patch("rundetection.specifications.requests")
def test_instrument_specification_load_rules_for_api_sets_loaded_time(requests, specification):
    headers: dict = {"Authorization": "Bearer shh", "accept": "application/json"}
    specification._load_rules_from_api()

    requests.get.assert_called_once_with(url="localhost:8080/instrument/foo/specification", headers=headers, timeout=1)
    assert specification.loaded_time is not None
    assert datetime.timedelta(minutes=1) < datetime.datetime.now(tz=datetime.UTC) - specification.loaded_time
