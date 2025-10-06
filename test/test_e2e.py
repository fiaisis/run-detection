"""End-to-end tests."""

from __future__ import annotations

import json
import time
import typing
from pathlib import Path

import pytest
from pika import BlockingConnection

if typing.TYPE_CHECKING:
    from typing import Any

    from pika.adapters.blocking_connection import BlockingChannel


@pytest.fixture(autouse=True, scope="module")
def producer_channel() -> BlockingChannel:
    """Return a producer channel fixture."""
    connection = BlockingConnection()
    channel = connection.channel()
    channel.exchange_declare("watched-files", exchange_type="direct", durable=True)
    channel.queue_declare("watched-files", durable=True, arguments={"x-queue-type": "quorum"})
    channel.queue_bind("watched-files", "watched-files", routing_key="")
    return channel


@pytest.fixture(autouse=True, scope="module")
def consumer_channel() -> BlockingChannel:
    """Return a consumer channel fixture."""
    connection = BlockingConnection()
    channel = connection.channel()
    channel.exchange_declare("scheduled-jobs", exchange_type="direct", durable=True)
    channel.queue_declare("scheduled-jobs", durable=True, arguments={"x-queue-type": "quorum"})
    channel.queue_bind("scheduled-jobs", "scheduled-jobs", routing_key="")
    return channel


@pytest.fixture(scope="module")
def failed_watched_files_consumer_channel() -> BlockingChannel:
    """Return a consumer channel fixture."""
    connection = BlockingConnection()
    channel = connection.channel()
    channel.exchange_declare("failed-watched-files", exchange_type="direct", durable=True)
    channel.queue_declare("failed-watched-files", durable=True, arguments={"x-queue-type": "quorum"})
    channel.queue_bind("failed-watched-files", "failed-watched-files", routing_key="")
    return channel


@pytest.fixture(autouse=True)
def _purge_queues(producer_channel, consumer_channel):
    """Purge queues on setup and teardown."""
    yield
    producer_channel.queue_purge(queue="watched-files")
    consumer_channel.queue_purge(queue="scheduled-jobs")


def produce_message(message: str, channel: BlockingChannel) -> None:
    """
    Given a message and a channel, produce the message to the queue on that channel
    :param message: The message to produce
    :param channel: The channel to produce to
    :return: None.
    """
    channel.basic_publish("watched-files", "", body=message.encode())


def get_specification_from_file(instrument: str) -> Any:
    """
    Given an instrument, return the specification
    :param instrument: The instrument for which specification to get
    :return: The specification file contents.
    """
    # This is ran in 2 places the CI and locally. On the CI, it has a different working directory to the default
    # selected by IDEs for local testing this works with either, if this fails raising is fine.
    path = Path(f"test/test_data/specifications/{instrument.lower()}_specification.json")
    if not path.exists():
        path = Path(f"test_data/specifications/{instrument.lower()}_specification.json")
    with path.open(encoding="utf-8") as fle:
        return json.load(fle)


def get_specification_value(instrument: str, key: str) -> Any:
    """
    Given an instrument and key, return the specification value
    :param instrument: The instrument for which specification to check
    :param key: The key for the rule
    :return: The rule value.
    """
    spec = get_specification_from_file(instrument)
    return spec[key]


def consume_all_messages(consumer_channel: BlockingChannel, expected_message_count: int) -> list[dict[str, Any]]:
    """Consume all messages from the queue."""
    recieved_messages = []
    timeout = time.time() + 60
    for mf, _, body in consumer_channel.consume("scheduled-jobs", inactivity_timeout=1):
        if len(recieved_messages) == expected_message_count or time.time() > timeout:
            break
        if mf is None:
            continue

        consumer_channel.basic_ack(mf.delivery_tag)
        recieved_messages.append(json.loads(body.decode()))
    return recieved_messages


def assert_run_in_recieved(run: Any, recieved: list[Any]):
    """
    Assert the given run is in the recieved list of runs
    :param run:
    :param recieved:
    :return:
    """
    assert run in recieved, f"{run} not in {recieved}"


EXPECTED_MARI_WBVAN = get_specification_value("mari", "mariwbvan")
EXPECTED_MARI_MASK = get_specification_value("mari", "marimaskfile")
EXPECTED_MARI_GIT_SHA = get_specification_value("mari", "git_sha")
EXPECTED_OSIRIS_MASK = get_specification_value("osiris", "osiriscalibfilesandreflection")
EXPECTED_IRIS_MASK = get_specification_value("iris", "iriscalibration")
EXPECTED_ENGINX_VANADIUM = get_specification_value("enginx", "enginxvanadiumrun")
EXPECTED_ENGINX_CERIA = get_specification_value("enginx", "enginxceriarun")


@pytest.mark.parametrize(
    ("messages", "expected_requests", "expected_request_count"),
    [
        (
            [
                "/archive/NDXMAR/Instrument/data/cycle_22_04/MAR25581.nxs",
                "/archive/NDXMAR/Instrument/data/cycle_19_4/MAR27030.nxs",
                "/archive/NDXMAR/Instrument/data/cycle_19_4/MAR27031.nxs",
            ],
            [
                {
                    "run_number": 27031,
                    "instrument": "MARI",
                    "experiment_title": "(Bi1-xYx)2O3 ; x=0.20 - Ei=130meV 400Hz Gd single - jaws=45x45 - T=900K",
                    "experiment_number": "1920321",
                    "filepath": "/archive/NDXMAR/Instrument/data/cycle_19_4/MAR27031.nxs",
                    "run_start": "2020-02-24T06:56:13",
                    "run_end": "2020-02-24T07:51:56",
                    "raw_frames": 167036,
                    "good_frames": 133622,
                    "users": "Goel,Le,Mittal",
                    "additional_values": {
                        "cycle_string": "cycle_19_4",
                        "ei": "'auto'",
                        "sam_mass": 0.0,
                        "sam_rmm": 0.0,
                        "monovan": 0,
                        "remove_bkg": False,
                        "sum_runs": False,
                        "runno": 27031,
                        "mask_file_link": EXPECTED_MARI_MASK,
                        "wbvan": EXPECTED_MARI_WBVAN,
                        "git_sha": EXPECTED_MARI_GIT_SHA,
                    },
                },
                {
                    "run_number": 27030,
                    "instrument": "MARI",
                    "experiment_title": "(Bi1-xYx)2O3 ; x=0.20 - Ei=130meV 400Hz Gd single - jaws=45x45 - T=900K",
                    "experiment_number": "1920321",
                    "filepath": "/archive/NDXMAR/Instrument/data/cycle_19_4/MAR27030.nxs",
                    "run_start": "2020-02-24T06:00:15",
                    "run_end": "2020-02-24T06:55:53",
                    "raw_frames": 166838,
                    "good_frames": 133470,
                    "users": "Goel,Le,Mittal",
                    "additional_values": {
                        "cycle_string": "cycle_19_4",
                        "ei": "'auto'",
                        "sam_mass": 0.0,
                        "sam_rmm": 0.0,
                        "monovan": 0,
                        "remove_bkg": False,
                        "sum_runs": False,
                        "runno": 27030,
                        "mask_file_link": EXPECTED_MARI_MASK,
                        "wbvan": EXPECTED_MARI_WBVAN,
                        "git_sha": EXPECTED_MARI_GIT_SHA,
                    },
                },
                {
                    "run_number": 27031,
                    "instrument": "MARI",
                    "experiment_title": "(Bi1-xYx)2O3 ; x=0.20 - Ei=130meV 400Hz Gd single - jaws=45x45 - T=900K",
                    "experiment_number": "1920321",
                    "filepath": "/archive/NDXMAR/Instrument/data/cycle_19_4/MAR27031.nxs",
                    "run_start": "2020-02-24T06:56:13",
                    "run_end": "2020-02-24T07:51:56",
                    "raw_frames": 167036,
                    "good_frames": 133622,
                    "users": "Goel,Le,Mittal",
                    "additional_values": {
                        "cycle_string": "cycle_19_4",
                        "ei": "'auto'",
                        "sam_mass": 0.0,
                        "sam_rmm": 0.0,
                        "monovan": 0,
                        "remove_bkg": False,
                        "sum_runs": True,
                        "runno": [27031, 27030],
                        "mask_file_link": EXPECTED_MARI_MASK,
                        "wbvan": EXPECTED_MARI_WBVAN,
                        "git_sha": EXPECTED_MARI_GIT_SHA,
                    },
                },
                {
                    "run_number": 25581,
                    "instrument": "MARI",
                    "experiment_title": "Whitebeam - vanadium - detector tests - vacuum bad - HT on not on all LAB",
                    "experiment_number": "1820497",
                    "filepath": "/archive/NDXMAR/Instrument/data/cycle_22_04/MAR25581.nxs",
                    "run_start": "2019-03-22T10:15:44",
                    "run_end": "2019-03-22T10:18:26",
                    "raw_frames": 8067,
                    "good_frames": 6452,
                    "users": "Wood,Guidi,Benedek,Mansson,Juranyi,Nocerino,Forslund,Matsubara",
                    "additional_values": {
                        "cycle_string": "cycle_22_04",
                        "ei": "'auto'",
                        "sam_mass": 0.0,
                        "sam_rmm": 0.0,
                        "monovan": 0,
                        "remove_bkg": False,
                        "sum_runs": False,
                        "runno": 25581,
                        "mask_file_link": EXPECTED_MARI_MASK,
                        "wbvan": EXPECTED_MARI_WBVAN,
                        "git_sha": EXPECTED_MARI_GIT_SHA,
                    },
                },
            ],
            4,
        ),
        (
            [
                "/archive/NDXTOSCA/Instrument/data/cycle_19_4/TSC25234.nxs",
                "/archive/NDXTOSCA/Instrument/data/cycle_19_4/TSC25235.nxs",
                "/archive/NDXTOSCA/Instrument/data/cycle_19_4/TSC25236.nxs",
            ],
            [
                {
                    "run_number": 25236,
                    "instrument": "TOSCA",
                    "experiment_title": "Olive oil sample 90pcEVOO8/10pcSTD1 run 2",
                    "experiment_number": "2000118",
                    "filepath": "/archive/NDXTOSCA/Instrument/data/cycle_19_4/TSC25236.nxs",
                    "run_start": "2020-03-03T07:51:40",
                    "run_end": "2020-03-03T09:37:30",
                    "raw_frames": 63437,
                    "good_frames": 63430,
                    "users": "Senesi,Parker,Andreani,Preziosi",
                    "additional_values": {"cycle_string": "cycle_19_4", "input_runs": [25236]},
                },
                {
                    "run_number": 25236,
                    "instrument": "TOSCA",
                    "experiment_title": "Olive oil sample 90pcEVOO8/10pcSTD1 run 2",
                    "experiment_number": "2000118",
                    "filepath": "/archive/NDXTOSCA/Instrument/data/cycle_19_4/TSC25236.nxs",
                    "run_start": "2020-03-03T07:51:40",
                    "run_end": "2020-03-03T09:37:30",
                    "raw_frames": 63437,
                    "good_frames": 63430,
                    "users": "Senesi,Parker,Andreani,Preziosi",
                    "additional_values": {"cycle_string": "cycle_19_4", "input_runs": [25236, 25235, 25234]},
                },
                {
                    "run_number": 25235,
                    "instrument": "TOSCA",
                    "experiment_title": "Olive oil sample 90pcEVOO8/10pcSTD1 run 1",
                    "experiment_number": "2000118",
                    "filepath": "/archive/NDXTOSCA/Instrument/data/cycle_19_4/TSC25235.nxs",
                    "run_start": "2020-03-03T04:22:42",
                    "run_end": "2020-03-03T07:51:32",
                    "raw_frames": 124795,
                    "good_frames": 124780,
                    "users": "Senesi,Parker,Andreani,Preziosi",
                    "additional_values": {"cycle_string": "cycle_19_4", "input_runs": [25235]},
                },
                {
                    "run_number": 25235,
                    "instrument": "TOSCA",
                    "experiment_title": "Olive oil sample 90pcEVOO8/10pcSTD1 run 1",
                    "experiment_number": "2000118",
                    "filepath": "/archive/NDXTOSCA/Instrument/data/cycle_19_4/TSC25235.nxs",
                    "run_start": "2020-03-03T04:22:42",
                    "run_end": "2020-03-03T07:51:32",
                    "raw_frames": 124795,
                    "good_frames": 124780,
                    "users": "Senesi,Parker,Andreani,Preziosi",
                    "additional_values": {"cycle_string": "cycle_19_4", "input_runs": [25235, 25234]},
                },
                {
                    "run_number": 25234,
                    "instrument": "TOSCA",
                    "experiment_title": "Olive oil sample 90pcEVOO8/10pcSTD1 run 0",
                    "experiment_number": "2000118",
                    "filepath": "/archive/NDXTOSCA/Instrument/data/cycle_19_4/TSC25234.nxs",
                    "run_start": "2020-03-03T00:54:31",
                    "run_end": "2020-03-03T04:22:34",
                    "raw_frames": 124806,
                    "good_frames": 124801,
                    "users": "Senesi,Parker,Andreani,Preziosi",
                    "additional_values": {"cycle_string": "cycle_19_4", "input_runs": [25234]},
                },
            ],
            5,
        ),
        (
            [
                "/archive/NDXOSIRIS/Instrument/data/cycle_14_1/OSIRIS00108538.nxs",
                "/archive/NDXOSIRIS/Instrument/data/cycle_14_1/OSIRIS00108539.nxs",
                "/archive/NDXOSIRIS/Instrument/data/cycle_12_2/OSI98933.nxs",
            ],
            [
                {
                    "run_number": 108538,
                    "instrument": "OSIRIS",
                    "experiment_title": "H2O 002_off QENS Cyl",
                    "experiment_number": "1410511",
                    "filepath": "/archive/NDXOSIRIS/Instrument/data/cycle_14_1/OSIRIS00108538.nxs",
                    "run_start": "2014-05-18T02:50:47",
                    "run_end": "2014-05-18T04:55:39",
                    "raw_frames": 374740,
                    "good_frames": 299728,
                    "users": "Dicko Dr I C",
                    "additional_values": {
                        "cycle_string": "cycle_14_1",
                        "freq6": 25.0,
                        "freq10": 25.0,
                        "phase6": 7700.0,
                        "phase10": 12500.0,
                        "tcb_detector_min": 45500.0,
                        "tcb_detector_max": 65500.0,
                        "tcb_monitor_min": 40700.0,
                        "tcb_monitor_max": 60700.0,
                        "reflection": "002",
                        "calibration_run_numbers": EXPECTED_OSIRIS_MASK["002"],
                        "spectroscopy_reduction": "true",
                        "diffraction_reduction": "false",
                        "analyser": "graphite",
                        "input_runs": [108538],
                    },
                },
                {
                    "run_number": 108539,
                    "instrument": "OSIRIS",
                    "experiment_title": "H2O 002_off QENS Cyl",
                    "experiment_number": "1410511",
                    "filepath": "/archive/NDXOSIRIS/Instrument/data/cycle_14_1/OSIRIS00108539.nxs",
                    "run_start": "2014-05-18T04:55:46",
                    "run_end": "2014-05-18T07:01:38",
                    "raw_frames": 375754,
                    "good_frames": 299820,
                    "users": "Dicko Dr I C",
                    "additional_values": {
                        "cycle_string": "cycle_14_1",
                        "freq6": 50.0,
                        "freq10": 50.0,
                        "phase6": 7700.0,
                        "phase10": 12500.0,
                        "tcb_detector_min": 45500.0,
                        "tcb_detector_max": 65500.0,
                        "tcb_monitor_min": 40700.0,
                        "tcb_monitor_max": 60700.0,
                        "reflection": "002",
                        "calibration_run_numbers": EXPECTED_OSIRIS_MASK["002"],
                        "spectroscopy_reduction": "true",
                        "diffraction_reduction": "false",
                        "analyser": "graphite",
                        "input_runs": [108539],
                    },
                },
                {
                    "run_number": 108539,
                    "instrument": "OSIRIS",
                    "experiment_title": "H2O 002_off QENS Cyl",
                    "experiment_number": "1410511",
                    "filepath": "/archive/NDXOSIRIS/Instrument/data/cycle_14_1/OSIRIS00108539.nxs",
                    "run_start": "2014-05-18T04:55:46",
                    "run_end": "2014-05-18T07:01:38",
                    "raw_frames": 375754,
                    "good_frames": 299820,
                    "users": "Dicko Dr I C",
                    "additional_values": {
                        "cycle_string": "cycle_14_1",
                        "freq6": 50.0,
                        "freq10": 50.0,
                        "phase6": 7700.0,
                        "phase10": 12500.0,
                        "tcb_detector_min": 45500.0,
                        "tcb_detector_max": 65500.0,
                        "tcb_monitor_min": 40700.0,
                        "tcb_monitor_max": 60700.0,
                        "reflection": "002",
                        "calibration_run_numbers": EXPECTED_OSIRIS_MASK["002"],
                        "spectroscopy_reduction": "true",
                        "diffraction_reduction": "false",
                        "analyser": "graphite",
                        "input_runs": [108539, 108538],
                    },
                },
                {
                    "run_number": 98933,
                    "instrument": "OSIRIS",
                    "experiment_title": "d1 12/2 Van Rod",
                    "experiment_number": "12345",
                    "filepath": "/archive/NDXOSIRIS/Instrument/data/cycle_12_2/OSI98933.nxs",
                    "run_start": "2012-07-11T16:23:24",
                    "run_end": "2012-07-11T16:46:50",
                    "raw_frames": 35169,
                    "good_frames": 28136,
                    "users": " ",
                    "additional_values": {
                        "cycle_string": "cycle_12_2",
                        "freq6": 25.0,
                        "freq10": 25.0,
                        "phase6": 1014.0,
                        "phase10": 1569.0,
                        "tcb_detector_min": 11700.0,
                        "tcb_detector_max": 51700.0,
                        "tcb_monitor_min": 11700.0,
                        "tcb_monitor_max": 51700.0,
                        "reflection": "002",
                        "calibration_run_numbers": EXPECTED_OSIRIS_MASK["002"],
                        "spectroscopy_reduction": "true",
                        "diffraction_reduction": "false",
                        "analyser": "graphite",
                        "input_runs": [98933],
                    },
                },
            ],
            4,
        ),
        (
            ["/archive/NDXIRIS/Instrument/data/cycle_24_3/IRIS00103226.nxs"],
            [
                {
                    "run_number": 103226,
                    "instrument": "IRIS",
                    "experiment_title": "Quiet Counts 24/2",
                    "experiment_number": "123456",
                    "filepath": "/archive/NDXIRIS/Instrument/data/cycle_24_3/IRIS00103226.nxs",
                    "run_start": "2024-09-10T11:04:11",
                    "run_end": "2024-09-10T17:04:18",
                    "raw_frames": 1080339,
                    "good_frames": 1080340,
                    "users": "team",
                    "additional_values": {
                        "cycle_string": "cycle_24_3",
                        "freq6": 50.0,
                        "freq10": 50.0,
                        "phase6": 8967.0,
                        "phase10": 14413.0,
                        "tcb_detector_min": 56000.0,
                        "tcb_detector_max": 76000.0,
                        "tcb_monitor_min": 52200.0,
                        "tcb_monitor_max": 72200.0,
                        "reflection": "002",
                        "calibration_run_numbers": EXPECTED_IRIS_MASK["002"],
                        "analyser": "graphite",
                        "input_runs": [103226],
                    },
                },
                {
                    "run_number": 103226,
                    "instrument": "IRIS",
                    "experiment_title": "Quiet Counts 24/2",
                    "experiment_number": "123456",
                    "filepath": "/archive/NDXIRIS/Instrument/data/cycle_24_3/IRIS00103226.nxs",
                    "run_start": "2024-09-10T11:04:11",
                    "run_end": "2024-09-10T17:04:18",
                    "raw_frames": 1080339,
                    "good_frames": 1080340,
                    "users": "team",
                    "additional_values": {
                        "cycle_string": "cycle_24_3",
                        "freq6": 50.0,
                        "freq10": 50.0,
                        "phase6": 8967.0,
                        "phase10": 14413.0,
                        "tcb_detector_min": 56000.0,
                        "tcb_detector_max": 76000.0,
                        "tcb_monitor_min": 52200.0,
                        "tcb_monitor_max": 72200.0,
                        "reflection": "002",
                        "calibration_run_numbers": EXPECTED_IRIS_MASK["002"],
                        "analyser": "graphite",
                        "input_runs": [103226, 103225],
                    },
                },
            ],
            2,
        ),
        (["/archive/NDXIMAT/Instrument/data/cycle_18_03/IMAT00004217.nxs"], [], 0),
        (
            ["/archive/NDXENGINX/Instrument/data/cycle_20_1/ENGINX00299080.nxs"],
            [
                {
                    "run_number": 299080,
                    "instrument": "ENGINX",
                    "experiment_title": "KangWang Al composite d0 sample B1 Axial B2 Rad Hoop 4x4x4",
                    "experiment_number": "1810794",
                    "filepath": "/archive/NDXENGINX/Instrument/data/cycle_20_1/ENGINX00299080.nxs",
                    "run_start": "2018-11-14T15:50:19",
                    "run_end": "2018-11-14T16:54:23",
                    "raw_frames": 95760,
                    "good_frames": 76485,
                    "users": "Garcia,Lee",
                    "additional_values": {
                        "vanadium_path": "/archive/NDXENGINX/Instrument/data/cycle_20_1/ENGINX00241391.nxs",
                        "ceria_path": "/archive/NDXENGINX/Instrument/data/cycle_20_1/ENGINX00241391.nxs",
                        "focus_path": "/archive/NDXENGINX/Instrument/data/cycle_20_1/ENGINX00299080.nxs",
                        "group": "BOTH",
                    },
                },
            ],
            1,
        ),
    ],
)
def test_e2e(producer_channel, consumer_channel, messages, expected_requests, expected_request_count):
    """
    Test expected messages are consumed from the scheduled jobs queue
    When the given messages are sent to the watched-files queue.
    """
    for message in messages:
        produce_message(message, producer_channel)
    if len(expected_requests) > 0:
        recieved_runs = consume_all_messages(consumer_channel, expected_request_count)
        for request in expected_requests:
            assert_run_in_recieved(request, recieved_runs)
    else:
        assert not consume_all_messages(consumer_channel, expected_request_count)


def test_non_existent_file_results_in_failed_queue(producer_channel, failed_watched_files_consumer_channel):
    """Test that a non-existent file results in a failed message being sent to the failed queue."""
    produce_message("/archive/some/file/that/doesnt/exist.nxs", producer_channel)

    recieved_messages = []
    timeout = time.time() + 30
    for mf, _, body in failed_watched_files_consumer_channel.consume("failed-watched-files", inactivity_timeout=1):
        if time.time() > timeout or len(recieved_messages) > 1:
            break
        if mf is None:
            continue

        failed_watched_files_consumer_channel.basic_ack(mf.delivery_tag)
        recieved_messages.append(body.decode())
    assert "/archive/some/file/that/doesnt/exist.nxs" in recieved_messages
    assert len(recieved_messages) == 1
