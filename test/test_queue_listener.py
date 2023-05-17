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


@patch("rundetection.queue_listener.QueueListener._create_connection")
def test_will_attempt_reconnect_on_disconnect(mock_create_connection, listener: QueueListener) -> None:
    """
    Tests that reconnection is attempted on disconnect
    :param listener: Queue Listener Fixture
    :return: None
    """
    listener.on_disconnected()
    mock_create_connection.return_value.connect.assert_called_once()
    mock_create_connection.return_value.set_listener.assert_called_once()
    mock_create_connection.return_value.subscribe.assert_called_once()


@patch("rundetection.queue_listener.QueueListener._create_connection")
def test_connect_aborts_when_stopping(mock_create_connection, listener) -> None:
    """Test that connect and subscriber aborts when listener is stopping"""
    listener.stopping = True
    listener._connect_and_subscribe()
    mock_create_connection.assert_not_called()


@patch("rundetection.queue_listener.time")
@patch("rundetection.queue_listener.QueueListener._create_connection")
def test_will_wait_30_seconds_on_failure_to_reconnect(
    mock_create_connection, mock_time: Mock, listener: QueueListener
) -> None:
    """
    Tests will attempt to reconnect after 30 seconds on connection failure
    :param mock_create_connection: create connection mock
    :param mock_time: patched time mock
    :param listener: QueueListener fixture
    :return: None
    """

    mock_create_connection.return_value.connect.side_effect = [ConnectFailedException, None]
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


@patch("rundetection.queue_listener.QueueListener._create_connection")
def test_run_connects_queue_listener(mock_create_connection, listener: QueueListener) -> None:
    """
    Tests the queue listener will attempt to connect on run
    :param listener: QueueListener fixture
    :return: None
    """
    listener.run()
    assert_connect_and_subscribe(mock_create_connection.return_value, listener)


@patch("rundetection.queue_listener.Connection")
def test_connection_default_with_no_environment_vars_is_setup_with_localhost(connection: Mock) -> None:
    """
    Ensure that localhost is the default to connect to when no env variable is set
    """
    os.environ.pop("ACTIVEMQ_IP", None)

    message_queue: SimpleQueue[Message] = SimpleQueue()
    listener = QueueListener(message_queue)
    listener._connect_and_subscribe()

    assert listener._connection == connection.return_value
    connection.assert_called_once_with([("localhost", 61613)], heartbeats=(30000, 30000))


@patch("rundetection.queue_listener.Connection")
def test_connection_ip_is_setup_with_environment_variables(connection: Mock) -> None:
    """
    Ensure that the environment variable is used to connect to when environment variable is set
    """
    os.environ["ACTIVEMQ_IP"] = "192.168.0.1"

    message_queue: SimpleQueue[Message] = SimpleQueue()
    listener = QueueListener(message_queue)
    listener._connect_and_subscribe()
    assert listener._connection == connection.return_value
    connection.assert_called_once_with([("192.168.0.1", 61613)], heartbeats=(30000, 30000))


def test_connection_username_and_password_defaults_are_set() -> None:
    """
    Test that the queue listener has defaults that are set when no environment variable is set
    """
    os.environ.pop("ACTIVEMQ_USER", None)
    os.environ.pop("ACTIVEMQ_PASS", None)

    message_queue: SimpleQueue[Message] = SimpleQueue()
    listener = QueueListener(message_queue)

    assert listener._user == "admin"
    assert listener._password == "admin"


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

    os.environ.pop("ACTIVEMQ_USER", None)
    os.environ.pop("ACTIVEMQ_PASS", None)


def assert_connect_and_subscribe(
    connection: Mock, listener: QueueListener, username: str = "admin", password: str = "admin"
) -> None:
    """
    Assert the given queue listener attempted to connect
    :return: None
    """
    connection.connect.assert_called_once_with(username=username, passcode=password, wait=True)
    connection.set_listener.assert_called_once_with(listener=listener, name="run-detection-listener")
    connection.subscribe.assert_called_once_with(
        destination="Interactive-Reduction", id=listener._subscription_id, ack="client"
    )


@patch("time.sleep")
@patch("rundetection.queue_listener.QueueListener._create_connection")
def test_connect_and_subscribe_with_queue_var(mock_create_connection, _) -> None:
    """
    Assert the given queue listener attempted to connect
    :return: None
    """
    os.environ["ACTIVEMQ_QUEUE"] = "fancy_queue"
    message_queue: SimpleQueue[Message] = SimpleQueue()
    listener = QueueListener(message_queue)
    mock_connection = Mock()
    mock_create_connection.return_value = mock_connection
    listener.run()

    mock_connection.connect.assert_called_once_with(username="admin", passcode="admin", wait=True)
    mock_connection.set_listener.assert_called_once_with(listener=listener, name="run-detection-listener")
    mock_connection.subscribe.assert_called_once_with(
        destination="fancy_queue", id=listener._subscription_id, ack="client"
    )
    os.environ.pop("ACTIVEMQ_QUEUE", None)


def test_is_connected(listener) -> None:
    """Test that is_connected returns connection status"""
    listener._connection = Mock()
    listener.is_connected()
    listener._connection.is_connected.assert_called_once()


if __name__ == "__main__":
    unittest.main()
