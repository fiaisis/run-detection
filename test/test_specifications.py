"""
Specification unit test module
"""

import datetime
import os
from pathlib import Path
from unittest import mock
from unittest.mock import patch

import pytest

from rundetection.exceptions import RuleViolationError
from rundetection.ingestion.ingest import JobRequest
from rundetection.rules.common_rules import MolSpecStitchRule
from rundetection.rules.iris_rules import IrisCalibrationRule, IrisReductionRule
from rundetection.rules.mari_rules import MariWBVANRule
from rundetection.rules.sans_rules import SansUserFile
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
    spec = InstrumentSpecification("foo")
    spec.loaded_time = datetime.datetime.now(tz=datetime.UTC)
    spec._rules = [mock.MagicMock(), mock.MagicMock(), mock.MagicMock()]
    return spec


@pytest.fixture(autouse=True)
def _set_api_key() -> None:
    os.environ["FIA_API_API_KEY"] = "shh"


@pytest.fixture()
def _working_directory_fix():
    # Set dir to repo root for purposes of the test.
    current_working_directory = Path.cwd()
    if current_working_directory.name == "test":
        os.chdir(current_working_directory / "..")


@mock.patch("rundetection.specifications.InstrumentSpecification._load_rules_from_api")
def test_instrument_specification_tries_to_load_api_from_db(load_rules_from_api):
    InstrumentSpecification("mari")
    load_rules_from_api.assert_called_once_with()


@mock.patch("rundetection.specifications.requests")
def test_instrument_specification_load_rules_for_api(requests, specification):
    headers: dict = {"Authorization": "Bearer shh", "accept": "application/json"}
    requests.get.return_value.json.return_value = {
        "molspecstitch": True,
        "mariwbvan": 100,
        "loquserfile": "user_file.toml",
    }

    specification._load_rules_from_api()

    requests.get.assert_called_once_with(
        url="http://localhost:8000/instrument/FOO/specification", headers=headers, timeout=1
    )
    assert specification._rules == [MariWBVANRule(100), SansUserFile("user_file.toml"), MolSpecStitchRule(True)]


@mock.patch("rundetection.specifications.requests")
def test_instrument_specification_load_rules_for_api_sets_loaded_time(requests, specification):
    headers: dict = {"Authorization": "Bearer shh", "accept": "application/json"}
    specification._load_rules_from_api()

    requests.get.assert_called_once_with(
        url="http://localhost:8000/instrument/FOO/specification", headers=headers, timeout=1
    )
    assert specification.loaded_time is not None
    assert datetime.timedelta(minutes=1) > datetime.datetime.now(tz=datetime.UTC) - specification.loaded_time


def test_rule_old(specification):
    specification.loaded_time = datetime.datetime.now(tz=datetime.UTC) - datetime.timedelta(minutes=11)
    assert specification._rule_old()

    specification.loaded_time = datetime.datetime.now(tz=datetime.UTC)
    assert not specification._rule_old()

    specification.loaded_time = None
    assert specification._rule_old()


def test_specification_rule_old(specification, job_request):
    specification._rule_old = mock.MagicMock(return_value=True)
    specification._load_rules_from_api = mock.MagicMock()

    specification.verify(job_request)

    specification._load_rules_from_api.assert_called_once_with()


def test_specification__rules_len_0(specification, job_request):
    specification._rules = []

    specification.verify(job_request)

    assert not job_request.will_reduce


def test_runs_verify_on_all_rules(specification, job_request):
    specification.verify(job_request)

    assert job_request.will_reduce
    for rule in specification._rules:
        rule.verify.assert_called_once_with(job_request)


def test_specification_verify_rule_violation_doesnt_verify_more(specification, job_request):
    def raise_exception(_):
        raise RuleViolationError()

    specification._rules[1].verify = mock.MagicMock(side_effect=raise_exception)

    specification.verify(job_request)

    assert not job_request.will_reduce
    specification._rules[0].verify.assert_called_once()
    specification._rules[1].verify.assert_called_once()
    specification._rules[2].verify.assert_not_called()


def test_specification_ensures_rule_order_respected(specification):
    specification._rules = [IrisReductionRule(True), MolSpecStitchRule(True), IrisCalibrationRule({})]

    specification._order_rules()

    assert specification._rules[0] == IrisReductionRule(True)
    assert specification._rules[1] == IrisCalibrationRule({})
    assert specification._rules[-1] == MolSpecStitchRule(True)
