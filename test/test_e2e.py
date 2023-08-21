"""
End-to-end tests
"""
import json

# pylint: disable=redefined-outer-name, no-name-in-module
import unittest
from typing import Any

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic, KafkaError
from stomp import Connection


@pytest.fixture
def amq_connection() -> Connection:
    """
    Setup and return stomp connection
    :return: stomp connection
    """
    conn = Connection()
    conn.connect("admin", "admin")
    return conn


@pytest.fixture
def kafka_consumer() -> Consumer:
    """
    Setup and return the kafka consumer
    :return: kafka consumer
    """
    admin_client = AdminClient({"bootstrap.servers": "localhost:29092"})
    topic = NewTopic("detected-runs", 1, 1)
    admin_client.create_topics([topic])
    consumer = Consumer({"bootstrap.servers": "localhost:29092", "group.id": "test", "auto.offset.reset": "earliest"})
    consumer.subscribe(["detected-runs"])
    return consumer


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


def test_end_to_end(amq_connection: Connection, kafka_consumer: Consumer) -> None:
    """
    Test message that is sent to activemq is processed and arrives at kafka instance
    :return: None
    """
    amq_connection.send("Interactive-Reduction", r"\\isis\inst$\Cycles$\cycle_18_03\NDXIMAT\IMAT00004217.nxs")
    amq_connection.send("Interactive-Reduction", r"\\isis\inst$\Cycles$\cycle_20_01\NDXENGINX\ENGINX00241391.nxs")
    amq_connection.send("Interactive-Reduction", r"\\isis\inst$\Cycles$\cycle_19_03\NDXALF\ALF82301.nxs")
    amq_connection.send("Interactive-Reduction", r"\\isis\inst$\Cycles$\cycle_22_04\NDXMAR\MAR25581.nxs")

    expected_wbvan = get_specification_value("mari", "mariwbvan")
    expected_mask = get_specification_value("mari", "marimaskfile")

    received = []
    for _ in range(30):
        msg = kafka_consumer.poll(timeout=1.0)
        if msg is None:
            continue
        try:
            if msg.error():
                pytest.fail(f"Failed to consume from broker: {msg.error()}")
            received.append(msg.value())

        except KafkaError as exc:
            kafka_consumer.close()
            pytest.fail("Problem with kafka consumer", exc)

    kafka_consumer.close()

    assert received == [
        b'{"run_number": 25581, "instrument": "MARI", "experiment_title": "Whitebeam - vanadium - detector tests - '
        b'vacuum bad - HT on not on all LAB", "experiment_number": "1820497", "filepath": '
        b'"/archive/NDXMAR/Instrument/data/cycle_22_04/MAR25581.nxs", '
        b'"run_start": "2019-03-22T10:15:44", "run_end": "2019-03-22T10:18:26", '
        b'"raw_frames": 8067, "good_frames": 6452, '
        b'"users": "Wood,Guidi,Benedek,Mansson,Juranyi,Nocerino,Forslund,Matsubara", '
        b'"additional_values": {"ei": "\'auto\'", "sam_mass": 0.0, '
        b'"sam_rmm": 0.0, "monovan": 0, "remove_bkg": false, "sum_runs": false, "runno": 25581, '
        b'"mask_file_link": "' + expected_mask.encode() + b'", ' + b'"wbvan": ' + str(expected_wbvan).encode() + b"}}",
    ]
    assert len(received) == 1


if __name__ == "__main__":
    unittest.main()
