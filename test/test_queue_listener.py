"""
Unit tests for queue listener
"""
# pylint: disable=protected-access, redefined-outer-name
import os
import unittest
from queue import SimpleQueue
from unittest.mock import MagicMock, patch, Mock

import pytest
from stomp.exception import ConnectFailedException  # type: ignore

from rundetection.queue_listener import QueueListener, Message


@pytest.fixture
def listener() -> QueueListener:
    """
    Setup and return the queue listener for each test
    :return:
    """
    message_queue: SimpleQueue[Message] = SimpleQueue()
    return QueueListener(message_queue)


def test_on_message_creates_message_and_puts_to_queue(listener: QueueListener) -> None:
    """
    Test message creation and queue addition on activeMQ message received.
    :param listener: QueueListener fixture
    :return: None
    """
    frame = MagicMock()
    frame.body = "body text"
    frame.headers = {"message-id": "1"}
    listener.on_message(frame)
    assert listener._message_queue.get() == Message(value="body text", id="1")


def test_will_attempt_reconnect_on_disconnect(listener: QueueListener) -> None:
    """
    Tests that reconnection is attempted on disconnect
    :param listener: Queue Listener Fixture
    :return: None
    """
    listener._connection = Mock()
    listener.on_disconnected()
    assert_connect_and_subscribe(listener)


@patch("rundetection.queue_listener.time")
def test_will_wait_30_seconds_on_failure_to_reconnect(mock_time: Mock, listener: QueueListener) -> None:
    """
    Tests will attempt to reconnect after 30 seconds on connection failure
    :param mock_time: patched time mock
    :param listener: QueueListener fixture
    :return: None
    """
    listener._connection = Mock()
    listener._connection.connect.side_effect = [ConnectFailedException, None]
    listener.on_disconnected()
    mock_time.sleep.assert_called_once_with(30)


def test_acknowledge_sends_acknowledgment(listener: QueueListener) -> None:
    """
    Tests the queue listener uses connection to send acknowledgement of given message
    :param listener: QueueListener fixture
    :return: None
    """
    listener._connection = Mock()
    message = Message(value="value", id="id")
    listener.acknowledge(message)
    listener._connection.ack.assert_called_once_with(message.id, listener._subscription_id)


def test_run_connects_queue_listener(listener: QueueListener) -> None:
    """
    Tests the queue listener will attempt to connect on run
    :param listener: QueueListener fixture
    :return: None
    """
    listener._connection = Mock()
    listener.run()
    assert_connect_and_subscribe(listener)


@patch("rundetection.queue_listener.Connection")
def test_connection_default_with_no_environment_vars_is_setup_with_localhost(connection: Mock) -> None:
    """
    Ensure that localhost is the default to connect to when no env variable is set
    """
    os.environ.pop('ACTIVEMQ_IP', None)

    message_queue: SimpleQueue[Message] = SimpleQueue()
    listener = QueueListener(message_queue)

    assert listener._connection == connection.return_value
    connection.assert_called_once_with([("localhost", 61613)])


@patch("rundetection.queue_listener.Connection")
def test_connection_ip_is_setup_with_environment_variables(connection: Mock) -> None:
    """
    Ensure that the environment variable is used to connect to when environment variable is set
    """
    os.environ['ACTIVEMQ_IP'] = "192.168.0.1"

    message_queue: SimpleQueue[Message] = SimpleQueue()
    listener = QueueListener(message_queue)

    assert listener._connection == connection.return_value
    connection.assert_called_once_with([("192.168.0.1", 61613)])


def test_connection_username_and_password_defaults_are_set() -> None:
    """
    Test that the queue listener has defaults that are set when no environment variable is set
    """
    os.environ.pop('ACTIVEMQ_USER', None)
    os.environ.pop('ACTIVEMQ_PASS', None)

    message_queue: SimpleQueue[Message] = SimpleQueue()
    listener = QueueListener(message_queue)

    assert listener._user == "admin"
    assert listener._password == "admin"

    listener._connection = Mock()
    listener.run()
    assert_connect_and_subscribe(listener, username="admin", password="admin")


def test_connection_username_and_password_can_be_set_by_environment_variable() -> None:
    """
    Test that the queue listener sets the username and password for connecting to activemq using the environment
    variables
    """
    os.environ["ACTIVEMQ_USER"] = "great_username"
    os.environ["ACTIVEMQ_PASS"] = "great_password"

    message_queue: SimpleQueue[Message] = SimpleQueue()
    listener = QueueListener(message_queue)

    assert listener._user == "great_username"
    assert listener._password == "great_password"

    listener._connection = Mock()
    listener.run()
    assert_connect_and_subscribe(listener, username="great_username", password="great_password")


def assert_connect_and_subscribe(listener: QueueListener, username: str = "admin", password: str = "admin") -> None:
    """
    Assert the given queue listener attempted to connect
    :return: None
    """
    listener._connection.connect.assert_called_once_with(username=username, password=password)
    listener._connection.set_listener.assert_called_once_with(listener=listener, name="run-detection-listener")
    listener._connection.subscribe.assert_called_once_with(destination="Interactive-Reduction",
                                                           id=listener._subscription_id,
                                                           ack="client")


if __name__ == '__main__':
    unittest.main()
