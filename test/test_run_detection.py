"""
Tests for run detection module
"""
import logging
import unittest
from pathlib import Path
from queue import SimpleQueue
from unittest.mock import patch, AsyncMock, Mock, MagicMock

import pytest

from rundetection.ingest import JobRequest
from rundetection.run_detection import (
    create_and_get_memphis,
    process_message,
    process_messages,
    process_notifications,
    start_run_detection,
    verify_archive_access,
)


# pylint: disable=protected-access, redefined-outer-name


@pytest.mark.asyncio
@patch("rundetection.run_detection.Memphis")
async def test_create_and_get_memphis(mock_memphis):
    """
    Test that the memphis object is given the correct parameters and returned
    :param mock_memphis: Mock memphis class
    :return: None
    """
    expected_memphis = AsyncMock()
    mock_memphis.return_value = expected_memphis

    actual_memphis = await create_and_get_memphis()

    mock_memphis.assert_called_once()
    assert expected_memphis == actual_memphis
    expected_memphis.connect.assert_called_once_with(host="localhost", username="root", password="memphis")


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
        1, "inst", "title", "num", Path("."), "start", "end", 1, 1, "users", additional_runs=[mock_additional_request]
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
        1, "inst", "title", "num", Path("."), "start", "end", 1, 1, "users", additional_runs=[mock_additional_request]
    )
    mock_request.will_reduce = False
    mock_request.additional_runs = [mock_additional_request]
    mock_ingest.return_value = mock_request
    mock_spec = Mock()
    mock_instrument_spec.return_value = mock_spec

    process_message("some/path/nexus.nxs", notification_queue)

    assert notification_queue.empty()


@pytest.mark.asyncio
@patch("rundetection.run_detection.process_message")
async def test_process_messages(mock_process):
    """
    Test each message is processed and acked
    :param mock_process: Mock process messages function
    :return: None
    """
    mock_message = Mock()
    mock_message.ack = AsyncMock()
    mock_message.get_data.return_value.decode.return_value = "some path"
    messages = [mock_message]
    notification_queue = SimpleQueue()

    await process_messages(messages, notification_queue)

    mock_process.assert_called_once_with("some path", notification_queue)
    mock_message.ack.assert_called_once()


@pytest.mark.asyncio
@patch("rundetection.run_detection.process_message")
async def test_process_messages_raises_still_acks(mock_process):
    """
    Test messages are still acked after exception in processing
    :param mock_process: Mock Process messages function
    :return: None
    """
    mock_message = Mock()
    mock_message.ack = AsyncMock()
    mock_message.get_data.return_value.decode.return_value = "some path"
    messages = [mock_message]
    notification_queue = SimpleQueue()
    mock_process.side_effect = RuntimeError

    await process_messages(messages, notification_queue)

    mock_process.assert_called_once_with("some path", notification_queue)
    mock_message.ack.assert_called_once()


@pytest.mark.asyncio
@patch("rundetection.run_detection.bytearray")
async def test_process_notifications(mock_byte):
    """
    Tests messages in the notification queue are produced by the producer
    :param mock_byte: Mock bytearray class
    :return: None
    """
    producer = Mock()
    producer.produce = AsyncMock()
    notification_queue = SimpleQueue()
    run = Mock()
    notification_queue.put(run)

    await process_notifications(producer, notification_queue)
    mock_byte.assert_called_once_with(run.to_json_string.return_value, "utf-8")
    producer.produce.assert_called_once_with(mock_byte.return_value)


@pytest.mark.asyncio
@patch("rundetection.run_detection.create_and_get_memphis")
@patch("rundetection.run_detection.process_messages")
@patch("rundetection.run_detection.process_notifications")
@patch("rundetection.run_detection.asyncio.sleep", side_effect=InterruptedError)
@patch("rundetection.run_detection.SimpleQueue")
async def test_start_run_detection(mock_queue, _, mock_proc_notifications, mock_proc_messages, mock_get_memphis):
    """
    Mock run detection start up
    :param mock_queue: Mock notification queue
    :param _: Mock sleep
    :param mock_proc_notifications: mock process notification function
    :param mock_proc_messages: Mock process messages function
    :param mock_get_memphis: Mock memphis factory
    :return:  None
    """
    mock_memphis = Mock()
    mock_memphis.consumer = AsyncMock()
    mock_memphis.producer = AsyncMock()
    mock_get_memphis.return_value = mock_memphis

    with pytest.raises(InterruptedError):
        await start_run_detection()

    mock_proc_messages.assert_called_with(
        mock_memphis.consumer.return_value.fetch.return_value, mock_queue.return_value
    )
    mock_proc_notifications.assert_called_with(mock_memphis.producer.return_value, mock_queue.return_value)


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


if __name__ == "__main__":
    unittest.main()
