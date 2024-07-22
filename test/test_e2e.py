"""
End-to-end tests
"""

from __future__ import annotations

import json
import typing
from pathlib import Path

import pytest
from pika import BlockingConnection

if typing.TYPE_CHECKING:
    from typing import Any

    from pika.adapters.blocking_connection import BlockingChannel


@pytest.fixture(autouse=True, scope="module")
def producer_channel() -> BlockingChannel:
    """Producer channel fixture"""
    connection = BlockingConnection()
    channel = connection.channel()
    channel.exchange_declare("watched-files", exchange_type="direct", durable=True)
    channel.queue_declare("watched-files", durable=True, arguments={"x-queue-type": "quorum"})
    channel.queue_bind("watched-files", "watched-files", routing_key="")
    return channel


@pytest.fixture(autouse=True, scope="module")
def consumer_channel() -> BlockingChannel:
    """Consumer channel fixture"""
    connection = BlockingConnection()
    channel = connection.channel()
    channel.exchange_declare("scheduled-jobs", exchange_type="direct", durable=True)
    channel.queue_declare("scheduled-jobs", durable=True, arguments={"x-queue-type": "quorum"})
    channel.queue_bind("scheduled-jobs", "scheduled-jobs", routing_key="")
    return channel


@pytest.fixture(autouse=True)
def _purge_queues(producer_channel, consumer_channel):
    """Purge queues on setup and teardown"""
    yield
    producer_channel.queue_purge(queue="watched-files")
    consumer_channel.queue_purge(queue="scheduled-jobs")


def produce_message(message: str, channel: BlockingChannel) -> None:
    """
    Given a message and a channel, produce the message to the queue on that channel
    :param message: The message to produce
    :param channel: The channel to produce to
    :return: None
    """
    channel.basic_publish("watched-files", "", body=message.encode())


def get_specification_value(instrument: str, key: str) -> Any:
    """
    Given an instrument and key, return the specification value
    :param instrument: The instrument for which specification to check
    :param key: The key for the rule
    :return: The rule value
    """
    path = Path(f"rundetection/specifications/{instrument.lower()}_specification.json")
    with path.open(encoding="utf-8") as fle:
        spec = json.load(fle)
        return spec[key]


def consume_all_messages(consumer_channel: BlockingChannel) -> list[dict[str, Any]]:
    """Consume all messages from the queue"""
    recieved_messages = []
    for mf, _, body in consumer_channel.consume("scheduled-jobs", inactivity_timeout=1):
        if mf is None:
            break

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


@pytest.mark.parametrize(
    ("messages", "expected_requests"),
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
                        "ei": "'auto'",
                        "sam_mass": 0.0,
                        "sam_rmm": 0.0,
                        "monovan": 0,
                        "remove_bkg": False,
                        "sum_runs": False,
                        "runno": 27031,
                        "mask_file_link": EXPECTED_MARI_MASK,
                        "wbvan": EXPECTED_MARI_WBVAN,
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
                        "ei": "'auto'",
                        "sam_mass": 0.0,
                        "sam_rmm": 0.0,
                        "monovan": 0,
                        "remove_bkg": False,
                        "sum_runs": False,
                        "runno": 27030,
                        "mask_file_link": EXPECTED_MARI_MASK,
                        "wbvan": EXPECTED_MARI_WBVAN,
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
                        "ei": "'auto'",
                        "sam_mass": 0.0,
                        "sam_rmm": 0.0,
                        "monovan": 0,
                        "remove_bkg": False,
                        "sum_runs": True,
                        "runno": [27031, 27030],
                        "mask_file_link": EXPECTED_MARI_MASK,
                        "wbvan": EXPECTED_MARI_WBVAN,
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
                        "ei": "'auto'",
                        "sam_mass": 0.0,
                        "sam_rmm": 0.0,
                        "monovan": 0,
                        "remove_bkg": False,
                        "sum_runs": False,
                        "runno": 25581,
                        "mask_file_link": EXPECTED_MARI_MASK,
                        "wbvan": EXPECTED_MARI_WBVAN,
                    },
                },
            ],
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
                        "calibration_run_number": "00148587",
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
                        "calibration_run_number": "00148587",
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
                        "calibration_run_number": "00148587",
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
                        "calibration_run_number": "00148587",
                        "spectroscopy_reduction": "true",
                        "diffraction_reduction": "false",
                        "analyser": "graphite",
                        "input_runs": [98933],
                    },
                },
            ],
        ),
        (["/archive/NDXIMAT/Instrument/data/cycle_18_03/IMAT00004217.nxs"], []),
    ],
)
def test_e2e(producer_channel, consumer_channel, messages, expected_requests):
    """Test expected messages are consumed from the scheduled jobs queue
    When the given messages are sent to the watched-files queue"""
    for message in messages:
        produce_message(message, producer_channel)
    if len(expected_requests) > 0:
        recieved_runs = consume_all_messages(consumer_channel)
        for request in expected_requests:
            assert_run_in_recieved(request, recieved_runs)
    else:
        assert not consume_all_messages(consumer_channel)
