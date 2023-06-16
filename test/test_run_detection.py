"""
Tests for run detection module
"""
import unittest
from pathlib import Path
from queue import SimpleQueue
from unittest.mock import patch, AsyncMock, Mock, MagicMock

import pytest

from rundetection.ingest import DetectedRun
from rundetection.run_detection import (
    create_and_get_memphis,
    process_message,
    process_messages,
    process_notifications,
    start_run_detection,
)


# pylint: disable=protected-access, redefined-outer-name


@pytest.mark.asyncio
@patch("rundetection.run_detection.Memphis")
async def test_create_and_get_memphis(mock_memphis):
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
    notification_queue = SimpleQueue()
    mock_additional_run = MagicMock(spec=DetectedRun)
    mock_run = DetectedRun(
        1, "inst", "title", "num", Path("."), "start", "end", 1, 1, "users", additional_runs=[mock_additional_run]
    )
    mock_run.additional_runs = [mock_additional_run]
    mock_ingest.return_value = mock_run
    mock_spec = Mock()
    mock_instrument_spec.return_value = mock_spec

    process_message("some/path/nexus.nxs", notification_queue)

    assert notification_queue.get() == mock_run
    assert notification_queue.get() == mock_additional_run


@patch("rundetection.run_detection.ingest")
@patch("rundetection.run_detection.InstrumentSpecification")
def test_process_message_no_notification(mock_instrument_spec, mock_ingest):
    notification_queue = SimpleQueue()
    mock_additional_run = MagicMock(spec=DetectedRun)
    mock_run = DetectedRun(
        1, "inst", "title", "num", Path("."), "start", "end", 1, 1, "users", additional_runs=[mock_additional_run]
    )
    mock_run.will_reduce = False
    mock_run.additional_runs = [mock_additional_run]
    mock_ingest.return_value = mock_run
    mock_spec = Mock()
    mock_instrument_spec.return_value = mock_spec

    process_message("some/path/nexus.nxs", notification_queue)

    assert notification_queue.empty()


@pytest.mark.asyncio
@patch("rundetection.run_detection.process_message")
async def test_process_messages(mock_process):
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
async def test_start_run_detection(
        mock_queue, mock_sleep, mock_proc_notifications, mock_proc_messages, mock_get_memphis
):
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


if __name__ == "__main__":
    unittest.main()
