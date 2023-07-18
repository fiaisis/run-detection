"""
End-to-end tests
"""
# pylint: disable=redefined-outer-name, no-name-in-module
import asyncio
import json

import pytest

from rundetection.run_detection import create_and_get_memphis


async def produce_message(message: str) -> None:
    """
    Post a message to memphis
    :param message: the message to send
    :return: None
    """
    memphis = await create_and_get_memphis()
    await memphis.produce(
        station_name="watched-files",
        producer_name="e2e-submission-producer",
        message=message,
        generate_random_suffix=True,
    )


@pytest.mark.asyncio
async def test_e2e():
    """
    Produce 3 files to the ingress station, one that should reduce, one that shouldn't and one that doesnt exist. Verify
    that the scheduled job metadata is sent to the egress station only
    :return: None
    """

    # Produce file that should reduce
    await produce_message("/archive/NDXMAR/Instrument/data/cycle_22_04/MAR25581.nxs")

    # Produce file that should not reduce
    await produce_message("/archive/NDXIMAT/Instrument/data/cycle_18_03/IMAT00004217.nxs")

    # Produce file that does not exist
    await produce_message("/archive/foo/bar/baz.nxs")

    await asyncio.sleep(3)

    memphis = await create_and_get_memphis()
    recieved = await memphis.fetch_messages("scheduled-jobs", "e2e-consumer")

    expected = {
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
            "mask_file_link": "https://raw.githubusercontent.com/pace-neutrons/InstrumentFiles/"
            "964733aec28b00b13f32fb61afa363a74dd62130/mari/mari_mask2023_1.xml",
            "wbvan": 28580,
        },
    }
    try:
        await asyncio.sleep(5)
        assert expected == json.loads(recieved[0].get_data().decode("utf-8"))
        assert len(recieved) == 1
    finally:
        for message in recieved:
            await message.ack()
        await memphis.close()
