"""
Tests for run detection module
"""

import logging
import re
import time
import unittest
from pathlib import Path
from queue import SimpleQueue
from unittest.mock import MagicMock, Mock, patch

import pytest

from rundetection.exceptions import ReductionMetadataError
from rundetection.ingestion.ingest import JobRequest
from rundetection.run_detection import (
    get_channel,
    process_message,
    process_messages,
    process_notifications,
    producer,
    start_run_detection,
    verify_archive_access,
    write_readiness_probe_file,
)


@patch("rundetection.run_detection.ingest")
@patch("rundetection.run_detection.InstrumentSpecification")
def test_process_message(
    mock_instrument_spec,
    mock_ingest,
):
    """
    Test that process message loads the correct spec and calls ingest
    :param mock_instrument_spec: Mock Specification class
    :param mock_ingest: Mock ingest function
    :return: None
    """
    notification_queue = SimpleQueue()
    mock_additional_request = MagicMock(spec=JobRequest)
    mock_request = JobRequest(
        1,
        "inst",
        "title",
        "num",
        Path(),
        "start",
        "end",
        1,
        1,
        "users",
        additional_requests=[mock_additional_request],
    )
    mock_request.additional_requests = [mock_additional_request]
    mock_ingest.return_value = mock_request
    mock_spec = Mock()
    mock_instrument_spec.return_value = mock_spec

    process_message("some/path/nexus.nxs", notification_queue)

    assert notification_queue.get() == mock_request
    assert notification_queue.get() == mock_additional_request


@patch("rundetection.run_detection.ingest")
@patch("rundetection.run_detection.InstrumentSpecification")
def test_process_message_no_notification(mock_instrument_spec, mock_ingest):
    """
    Test process message does not update notification queue if spec fails to verify
    :param mock_instrument_spec: Mock Spec class
    :param mock_ingest: Mock ingest function
    :return: None
    """
    notification_queue = SimpleQueue()
    mock_additional_request = MagicMock(spec=JobRequest)
    mock_request = JobRequest(
        1,
        "inst",
        "title",
        "num",
        Path(),
        "start",
        "end",
        1,
        1,
        "users",
        additional_requests=[mock_additional_request],
    )
    mock_request.will_reduce = False
    mock_request.additional_runs = [mock_additional_request]
    mock_ingest.return_value = mock_request
    mock_spec = Mock()
    mock_instrument_spec.return_value = mock_spec

    process_message("some/path/nexus.nxs", notification_queue)

    assert notification_queue.empty()


@patch("rundetection.run_detection.process_message")
def test_process_messages(mock_process):
    """
    Test each message is processed and acked
    :param mock_process: Mock process messages function
    :return: None
    """
    channel = MagicMock()
    method_frame = MagicMock()
    body = b"message_body"
    channel.consume.return_value = [(method_frame, None, body)]

    notification_queue = Mock()

    process_messages(channel, notification_queue)

    channel.consume.assert_called_once()
    mock_process.assert_called_once_with(body.decode(), notification_queue)
    channel.basic_ack.assert_called_once_with(method_frame.delivery_tag)


@patch("rundetection.run_detection.process_message")
def test_process_messages_raises_exception_nacks(mock_process):
    """
    Test messages are still acked after exception in processing
    :param mock_process: Mock Process messages function
    :return: None
    """
    channel = MagicMock()
    method_frame = MagicMock()
    body = b"message_body"
    channel.consume.return_value = [(method_frame, None, body)]
    notification_queue = SimpleQueue()
    mock_process.side_effect = RuntimeError

    process_messages(channel, notification_queue)

    channel.consume.assert_called_once()
    mock_process.assert_called_once_with(body.decode(), notification_queue)
    channel.basic_nack.assert_called_once_with(method_frame.delivery_tag)


@patch("rundetection.run_detection.process_message")
def test_process_messages_raises_metadataerror_still_acks(mock_process):
    """
    Test messages are still acked after exception in processing
    :param mock_process: Mock Process messages function
    :return: None
    """
    channel = MagicMock()
    method_frame = MagicMock()
    body = b"message_body"
    channel.consume.return_value = [(method_frame, None, body)]
    notification_queue = SimpleQueue()
    mock_process.side_effect = ReductionMetadataError

    process_messages(channel, notification_queue)

    channel.consume.assert_called_once()
    mock_process.assert_called_once_with(body.decode(), notification_queue)
    channel.basic_ack.assert_called_once_with(method_frame.delivery_tag)


def test_process_messages_does_not_ack_attribute_error():
    """
    Test messages are not acked after AttributeError in processing. As this should only occur when no message is
    consumed.
    :param mock_process: Mock Process messages function
    :return: None
    """
    channel = MagicMock()
    channel.consume.return_value = [(None, None, None)]

    notification_queue = Mock()

    with patch("rundetection.run_detection.process_message"):
        process_messages(channel, notification_queue)

    channel.consume.assert_called_once()
    channel.basic_ack.assert_not_called()


@patch("rundetection.run_detection.producer")
def test_process_notifications(mock_producer):
    """
    Tests messages in the notification queue are produced by the producer
    :param mock_byte: Mock bytearray class
    :return: None
    """
    detected_run_1 = MagicMock()
    detected_run_1.run_number = "1"
    detected_run_1.to_json_string.return_value = '{"run_number": "1"}'

    detected_run_2 = MagicMock()
    detected_run_2.run_number = "2"
    detected_run_2.to_json_string.return_value = '{"run_number": "2"}'

    notification_queue = SimpleQueue()
    notification_queue.put(detected_run_1)
    notification_queue.put(detected_run_2)

    channel = MagicMock()
    mock_producer.return_value.__enter__.return_value = channel

    # Call function
    process_notifications(notification_queue)

    channel.basic_publish.assert_any_call("scheduled-jobs", "", b'{"run_number": "1"}')
    channel.basic_publish.assert_any_call("scheduled-jobs", "", b'{"run_number": "2"}')

    # Assert the queue is empty
    assert notification_queue.empty()


@patch("rundetection.run_detection.process_messages")
@patch("rundetection.run_detection.process_notifications")
@patch("rundetection.run_detection.SimpleQueue")
@patch("rundetection.run_detection.time.sleep", side_effect=InterruptedError)
def test_start_run_detection(mock_queue, mock_proc_notifications, mock_proc_messages, mock_get_channel):
    """
    Mock run detection start up
    :param _: Mock sleep
    :param mock_queue: Mock notification queue
    :param mock_proc_notifications: mock process notification function
    :param mock_proc_messages: Mock process messages function
    :return:  None
    """
    mock_channel = Mock()
    mock_get_channel.return_value = mock_channel

    with pytest.raises(InterruptedError) and patch("rundetection.run_detection.get_channel"):
        start_run_detection()

    mock_get_channel.assert_called_once_with("watched-files", "watched-files")

    mock_proc_messages.assert_called_with(mock_channel, mock_queue.return_value)
    mock_proc_notifications.assert_called_with(mock_queue.return_value)


@patch("rundetection.run_detection.Path")
def test_verify_archive_access_accessible(mock_path, caplog):
    """
    Test logging when archive is accessible
    :param mock_path: mock Path class
    :param caplog: log capture fixture
    :return: None
    """
    mock_path.return_value.exists.return_value = True
    with caplog.at_level(logging.INFO):
        verify_archive_access()

        assert "The archive has been mounted correctly, and can be accessed." in caplog.messages


@patch("rundetection.run_detection.Path")
def test_verify_archive_access_not_accessible(mock_path, caplog):
    """
    Test logging when archive not accessible
    :param mock_path: Mock path class
    :param caplog: Log capture fixture
    :return: None
    """
    mock_path.return_value.exists.return_value = False
    with caplog.at_level(logging.INFO):
        verify_archive_access()

        assert "The archive has not been mounted correctly, and cannot be accessed." in caplog.text


@patch("rundetection.run_detection.PlainCredentials")
@patch("rundetection.run_detection.ConnectionParameters")
@patch("rundetection.run_detection.BlockingConnection")
def test_get_channel(mock_blocking_connection, mock_connection_parameters, mock_plain_credentials):
    """Test channel is created and returned"""
    mock_channel = MagicMock()
    mock_blocking_connection.return_value.channel.return_value = mock_channel

    exchange_name = "test_exchange"
    queue_name = "test_queue"

    # Call function
    channel = get_channel(exchange_name, queue_name)

    # Assert
    mock_plain_credentials.assert_called_once_with(username="guest", password="guest")  # noqa: S106
    mock_connection_parameters.assert_called_once_with(
        "localhost", 5672, credentials=mock_plain_credentials.return_value
    )
    mock_blocking_connection.assert_called_once_with(mock_connection_parameters.return_value)

    mock_channel.exchange_declare.assert_called_once_with(exchange_name, exchange_type="direct", durable=True)
    mock_channel.queue_declare.assert_called_once_with(queue_name, durable=True, arguments={"x-queue-type": "quorum"})
    mock_channel.queue_bind.assert_called_once_with(queue_name, exchange_name, routing_key="")

    assert channel == mock_channel


@patch("rundetection.run_detection.get_channel")  # Replace with actual module name
def test_producer(mock_get_channel):
    """Test the producer context manager"""
    mock_channel = MagicMock()
    mock_get_channel.return_value = mock_channel

    with producer() as channel:
        mock_get_channel.assert_called_once_with("scheduled-jobs", "scheduled-jobs")
        assert channel == mock_channel

    mock_channel.close.assert_called_once()
    mock_channel.connection.close.assert_called_once()


def test_write_readiness_probe_file():
    """
    Test the write_readiness_probe
    :return: None
    """
    # Call the function to create the file
    write_readiness_probe_file()

    path = Path("/tmp/heartbeat")  # noqa: S108
    assert path.exists(), "Heartbeat file does not exist."

    with path.open(encoding="utf-8") as file:
        content = file.read()

        # Check if the content matches the timestamp format
        timestamp_format = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
        assert timestamp_format.match(content), f"Unexpected content format: {content}"

        # Assert a reasonable time difference
        timestamp = time.strptime(content, "%Y-%m-%d %H:%M:%S")
        current_time = time.localtime()
        time_difference = time.mktime(current_time) - time.mktime(timestamp)
        assert abs(time_difference) < 5, f"The timestamp difference is too large: {time_difference} seconds."  # noqa: PLR2004


if __name__ == "__main__":
    unittest.main()
