"""
End-to-end tests
"""
# pylint: disable=redefined-outer-name, no-name-in-module

import asyncio
import json
from typing import Any

import pytest

from rundetection.run_detection import create_and_get_memphis


async def produce_message(message: str, memphis: Any) -> None:
    """
    Post a message to memphis
    :param message: the message to send
    :return: None
    """
    await memphis.produce(
        station_name="watched-files",
        producer_name="e2e-submission-producer",
        message=message,
        generate_random_suffix=True,
    )


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


@pytest.mark.asyncio
async def test_e2e():
    """
    Produce 3 files to the ingress station, one that should reduce, one that shouldn't and one that doesnt exist. Verify
    that the scheduled job metadata is sent to the egress station only
    :return: None
    """

    expected_wbvan = get_specification_value("mari", "mariwbvan")
    expected_mask = get_specification_value("mari", "marimaskfile")
    memphis = await create_and_get_memphis()
    # Produce file that should reduce
    await produce_message("/archive/NDXMAR/Instrument/data/cycle_22_04/MAR25581.nxs", memphis)

    # Produce file that should not reduce
    await produce_message("/archive/NDXIMAT/Instrument/data/cycle_18_03/IMAT00004217.nxs", memphis)

    # Produce file that does not exist
    await produce_message("/archive/foo/bar/baz.nxs", memphis)

    # Produce 3 TOSCA runs that should result in 5 reductions
    await produce_message("/archive/NDXTOSCA/Instrument/data/cycle_19_4/TSC25234.nxs", memphis)
    await produce_message("/archive/NDXTOSCA/Instrument/data/cycle_19_4/TSC25235.nxs", memphis)
    await produce_message("/archive/NDXTOSCA/Instrument/data/cycle_19_4/TSC25236.nxs", memphis)

    await asyncio.sleep(30)

    recieved = await memphis.fetch_messages("scheduled-jobs", "e2e-consumer")

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
    try:
        await asyncio.sleep(3)
        recieved_messages = [json.loads(message.get_data().decode("utf-8")) for message in recieved]
        assert expected_mari_request in recieved_messages
        for request in expected_tosca_requests:
            assert request in recieved_messages
        assert len(recieved_messages) == 6
    finally:
        for message in recieved:
            await message.ack()
        await memphis.close()
