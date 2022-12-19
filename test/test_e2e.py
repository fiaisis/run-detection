"""
End to end tests
"""
# pylint: disable=redefined-outer-name
import unittest

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
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
    consumer = Consumer({
        "bootstrap.servers": "localhost:29092",
        "group.id": "test",
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(["detected-runs"])
    return consumer


def test_end_to_end_run_should_be_processed(amq_connection: Connection, kafka_consumer: Consumer) -> None:
    """
    Test message that is sent to activemq is processed and arrives at kafka instance
    :return: None
    """

    amq_connection.send("Interactive-Reduction", r"\\isis\inst$\cycle_22_4\NDXGEM\GEM92450.nxs")

    for _ in range(60):

        msg = kafka_consumer.poll(timeout=1.0)
        if msg is None:
            continue
        try:
            if msg.error():
                pytest.fail(f"Failed to consume from broker: {msg.error()}")
            assert msg.value() == br"\\isis\inst$\cycle_22_4\NDXGEM\GEM92450.nxs"
        finally:
            kafka_consumer.close()
        break
    else:
        kafka_consumer.close()
        pytest.fail("No message could be consumed")


def test_end_to_end_run_should_not_be_processed() -> None:
    """
    Test message that is sent to activemq does not arrive at kafka instance
    :return: None
    """
    # This needs to be implemented when rules and specifications are implemented


if __name__ == '__main__':
    unittest.main()
