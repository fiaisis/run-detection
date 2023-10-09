"""
End-to-end tests
"""
# pylint: disable=redefined-outer-name, no-name-in-module

import json
import time
from typing import Any

import pytest
from pika import BlockingConnection
from pika.adapters.blocking_connection import BlockingChannel


@pytest.fixture
def producer_channel() -> BlockingChannel:
    connection = BlockingConnection()
    channel = connection.channel()
    channel.exchange_declare("detected-runs", exchange_type="direct", durable=True)
    channel.queue_declare("detected-runs")
    channel.queue_bind("detected-runs", "detected-runs", routing_key="")
    return channel


@pytest.fixture
def consumer_channel() -> BlockingChannel:
    connection = BlockingConnection()
    channel = connection.channel()
    channel.exchange_declare("scheduled-jobs", exchange_type="direct", durable=True)
    channel.queue_declare("scheduled-jobs")
    channel.queue_bind("scheduled-jobs", "scheduled-jobs", routing_key="")
    return channel


def produce_message(message: str, channel: BlockingChannel) -> None:
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

    time.sleep(10)

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

    recieved_messages = []

    for mf, props, body in consumer_channel.consume("scheduled-jobs", inactivity_timeout=30):
        if mf is None:
            break

        consumer_channel.basic_ack(mf.delivery_tag)
        recieved_messages.append(body)

    assert expected_mari_request in recieved_messages
    assert expected_mari_stitch_request in recieved_messages
    assert expected_mari_stitch_individual_1 in recieved_messages
    assert expected_mari_stitch_individual_2 in recieved_messages
    for request in expected_tosca_requests:
        assert request in recieved_messages
    assert len(recieved_messages) == 9
