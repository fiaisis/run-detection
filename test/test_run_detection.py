"""
Tests for run detection module
"""
# pylint: disable=protected-access, redefined-outer-name

import unittest
from unittest.mock import patch, Mock

import pytest

from rundetection.notifications import Notification
from rundetection.queue_listener import Message
from rundetection.run_detection import RunDetector


@pytest.fixture
def detector() -> RunDetector:
    """
    Sets up and returns a run detector
    :return: RunDetector
    """
    return RunDetector()


def test__process_message(detector: RunDetector) -> None:
    """
    Testing for message processing
    :param detector: RunDetector Fixture
    :return: None
    """
    detector._queue_listener = Mock()
    detector._notifier = Mock()
    message = Message(id="id", value="value")
    detector._process_message(message)
    detector._notifier.notify.assert_called_with(Notification(message.value))
    assert message.processed is True
    detector._queue_listener.acknowledge.assert_called_once_with(message)


@patch("rundetection.run_detection.time.sleep", side_effect=InterruptedError)
def test_run(_: Mock, detector: RunDetector) -> None:
    """
    Test that run is processing messages and queue listener is started
    :param _: time.sleep patched mock
    :param detector:
    :return: None
    """
    detector._queue_listener = Mock()
    detector._process_message = Mock()  # type: ignore
    detector._message_queue = Mock()
    mock_message = Mock()
    detector._message_queue.get.return_value = mock_message
    try:
        detector.run()
    except InterruptedError:  # Throw Interrupt to break loop, then check state
        detector._message_queue.get.assert_called_once()
        detector._process_message.assert_called_once_with(mock_message)
        detector._queue_listener.run.assert_called_once()


if __name__ == '__main__':
    unittest.main()
