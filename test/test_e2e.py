"""
End-to-end tests
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from pika import BlockingConnection
from pika.adapters.blocking_connection import BlockingChannel


# pylint: disable=redefined-outer-name, no-name-in-module


@pytest.fixture
def producer_channel() -> BlockingChannel:
    """Producer channel fixture"""
    connection = BlockingConnection()
    channel = connection.channel()
    channel.exchange_declare("watched-files", exchange_type="direct", durable=True)
    channel.queue_declare("watched-files", durable=True, arguments={"x-queue-type": "quorum"})
    channel.queue_bind("watched-files", "watched-files", routing_key="")
    return channel


@pytest.fixture
def consumer_channel() -> BlockingChannel:
    """Consumer channel fixture"""
    connection = BlockingConnection()
    channel = connection.channel()
    channel.exchange_declare("scheduled-jobs", exchange_type="direct", durable=True)
    channel.queue_declare("scheduled-jobs", durable=True, arguments={"x-queue-type": "quorum"})
    channel.queue_bind("scheduled-jobs", "scheduled-jobs", routing_key="")
    return channel


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
    :param instrument: The instrument for which specificaiton to check
    :param key: The key for the rule
    :return: The rule value
    """
    with open(f"rundetection/specifications/{instrument.lower()}_specification.json", "r", encoding="utf-8") as fle:
        spec = json.load(fle)
        return spec[key]


# pylint: disable=too-many-locals
def test_e2e(producer_channel: BlockingChannel, consumer_channel):
    """
    Produce 3 files to the ingress station, one that should reduce, one that shouldn't and one that doesnt exist. Verify
    that the scheduled job metadata is sent to the egress station only
    :return: None
    """

    expected_wbvan = get_specification_value("mari", "mariwbvan")
    expected_mask = get_specification_value("mari", "marimaskfile")

    # Produce file that should reduce
    produce_message("/archive/NDXMAR/Instrument/data/cycle_22_04/MAR25581.nxs", producer_channel)

    # Produce MARI runs that should stitch
    produce_message("/archive/NDXMAR/Instrument/data/cycle_19_4/MAR27030.nxs", producer_channel)
    produce_message("/archive/NDXMAR/Instrument/data/cycle_19_4/MAR27031.nxs", producer_channel)

    # Produce file that should not reduce
    produce_message("/archive/NDXIMAT/Instrument/data/cycle_18_03/IMAT00004217.nxs", producer_channel)

    # Produce file that does not exist
    produce_message("/archive/foo/bar/baz.nxs", producer_channel)

    # Produce 3 TOSCA runs that should result in 5 reductions
    produce_message("/archive/NDXTOSCA/Instrument/data/cycle_19_4/TSC25234.nxs", producer_channel)
    produce_message("/archive/NDXTOSCA/Instrument/data/cycle_19_4/TSC25235.nxs", producer_channel)
    produce_message("/archive/NDXTOSCA/Instrument/data/cycle_19_4/TSC25236.nxs", producer_channel)

    # Produce Osiris runs
    # OSIRIS_108538 is a spectroscopy
    # OSIRIS_108539 Should sum from above
    produce_message("/archive/NDXOSIRIS/Instrument/data/cycle_14_1/OSIRIS00108538.nxs", producer_channel)
    produce_message("/archive/NDXOSIRIS/Instrument/data/cycle_14_1/OSIRIS00108539.nxs", producer_channel)

    # 98933 for diffraction
    produce_message("/archive/NDXOSIRIS/Instrument/data/cycle_12_2/OSI98933.nxs", producer_channel)

    expected_tosca_requests = [
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
    ]

    expected_mari_stitch_individual_1 = {
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
            "mask_file_link": expected_mask,
            "wbvan": expected_wbvan,
        },
    }
    expected_mari_stitch_individual_2 = {
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
            "mask_file_link": expected_mask,
            "wbvan": expected_wbvan,
        },
    }

    expected_mari_stitch_request = {
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
            "mask_file_link": expected_mask,
            "wbvan": expected_wbvan,
        },
    }

    expected_mari_request = {
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
            "mask_file_link": expected_mask,
            "wbvan": expected_wbvan,
        },
    }

    expected_osiris_spec_request = {
        "additional_values": {
            "analyser": 2,
            "freq10": 50.0,
            "freq6": 50.0,
            "input_runs": [108539],
            "mode": "spectroscopy",
            "panadium": 12345,
            "phase10": 12500.0,
            "phase6": 7700.0,
            "tcb_detector_max": 65500.0,
            "tcb_detector_min": 45500.0,
            "tcb_monitor_max": 60700.0,
            "tcb_monitor_min": 40700.0,
        },
        "experiment_number": "1410511",
        "experiment_title": "H2O 002_off QENS Cyl",
        "filepath": "/archive/NDXOSIRIS/Instrument/data/cycle_14_1/OSIRIS00108539.nxs",
        "good_frames": 299820,
        "instrument": "OSIRIS",
        "raw_frames": 375754,
        "run_end": "2014-05-18T07:01:38",
        "run_number": 108539,
        "run_start": "2014-05-18T04:55:46",
        "users": "Dicko Dr I C",
    }

    expected_osiris_diff_request_108538 = {
        "additional_values": {
            "freq10": 25.0,
            "freq6": 25.0,
            "mode": "diffraction",
            "panadium": 12345,
            "phase10": 12500.0,
            "phase6": 7700.0,
            "sum_runs": False,
            "tcb_detector_max": 65500.0,
            "tcb_detector_min": 45500.0,
            "tcb_monitor_max": 60700.0,
            "tcb_monitor_min": 40700.0,
        },
        "experiment_number": "1410511",
        "experiment_title": "H2O 002_off QENS Cyl",
        "filepath": "/archive/NDXOSIRIS/Instrument/data/cycle_14_1/OSIRIS00108538.nxs",
        "good_frames": 299728,
        "instrument": "OSIRIS",
        "raw_frames": 374740,
        "run_end": "2014-05-18T04:55:39",
        "run_number": 108538,
        "run_start": "2014-05-18T02:50:47",
        "users": "Dicko Dr I C",
    }

    expected_osiris_diff_request_98933 = {
        "additional_values": {
            "freq10": 25.0,
            "freq6": 25.0,
            "mode": "diffraction",
            "panadium": 12345,
            "phase10": 1569.0,
            "phase6": 1014.0,
            "sum_runs": False,
            "tcb_detector_max": 51700.0,
            "tcb_detector_min": 11700.0,
            "tcb_monitor_max": 51700.0,
            "tcb_monitor_min": 11700.0,
        },
        "experiment_number": "12345",
        "experiment_title": "d1 12/2 Van Rod",
        "filepath": "/archive/NDXOSIRIS/Instrument/data/cycle_12_2/OSI98933.nxs",
        "good_frames": 28136,
        "instrument": "OSIRIS",
        "raw_frames": 35169,
        "run_end": "2012-07-11T16:46:50",
        "run_number": 98933,
        "run_start": "2012-07-11T16:23:24",
        "users": " ",
    }
    expected_osiris_sum_run = {
        "additional_values": {
            "analyser": 2,
            "freq10": 50.0,
            "freq6": 50.0,
            "input_runs": [108539, 108538],
            "mode": "spectroscopy",
            "panadium": 12345,
            "phase10": 12500.0,
            "phase6": 7700.0,
            "tcb_detector_max": 65500.0,
            "tcb_detector_min": 45500.0,
            "tcb_monitor_max": 60700.0,
            "tcb_monitor_min": 40700.0,
        },
        "experiment_number": "1410511",
        "experiment_title": "H2O 002_off QENS Cyl",
        "filepath": "/archive/NDXOSIRIS/Instrument/data/cycle_14_1/OSIRIS00108539.nxs",
        "good_frames": 299820,
        "instrument": "OSIRIS",
        "raw_frames": 375754,
        "run_end": "2014-05-18T07:01:38",
        "run_number": 108539,
        "run_start": "2014-05-18T04:55:46",
        "users": "Dicko Dr I C",
    }

    recieved_messages = []

    for mf, _, body in consumer_channel.consume("scheduled-jobs", inactivity_timeout=1):
        if mf is None:
            break

        consumer_channel.basic_ack(mf.delivery_tag)
        recieved_messages.append(json.loads(body.decode()))

    def assert_run_in_recieved(run, recieved):
        assert run in recieved, f"{run} not in {recieved}"

    assert_run_in_recieved(expected_mari_request, recieved_messages)
    assert_run_in_recieved(expected_mari_stitch_request, recieved_messages)
    assert_run_in_recieved(expected_mari_stitch_individual_1, recieved_messages)
    assert_run_in_recieved(expected_mari_stitch_individual_2, recieved_messages)
    for request in expected_tosca_requests:
        assert_run_in_recieved(request, recieved_messages)

    assert_run_in_recieved(expected_osiris_diff_request_98933, recieved_messages)
    assert_run_in_recieved(expected_osiris_spec_request, recieved_messages)
    assert_run_in_recieved(expected_osiris_diff_request_108538, recieved_messages)
    assert_run_in_recieved(expected_osiris_sum_run, recieved_messages)
    assert_run_in_recieved(expected_osiris_diff_request_108538, recieved_messages)

    assert len(recieved_messages) == 13
