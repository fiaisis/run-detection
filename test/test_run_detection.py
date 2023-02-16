"""
Tests for run detection module
"""
# pylint: disable=protected-access, redefined-outer-name

import unittest
from pathlib import Path
from unittest.mock import patch, Mock

import pytest
from _pytest.logging import LogCaptureFixture

from rundetection.ingest import DetectedRun
from rundetection.notifications import Notification
from rundetection.queue_listener import Message
from rundetection.run_detection import RunDetector

MESSAGE = Message(id="id", value="value")


@pytest.fixture
def detector() -> RunDetector:
    """
    Sets up and returns a run detector
    :return: RunDetector
    """
    detector_ = RunDetector()
    detector_._queue_listener = Mock()
    detector_._message_queue = Mock()
    detector_._notifier = Mock()
    return detector_


@patch("rundetection.run_detection.InstrumentSpecification")
@patch("rundetection.run_detection.ingest")
def test__process_message_specification_met(mock_ingest, mock_specification, detector):
    """
    Test process message and specification is met
    :param mock_ingest: mock ingest function
    :param mock_specification: mock specification
    :param detector: RunDetector fixture
    :return: None
    """
    run = DetectedRun(
        run_number=123,
        instrument="mari",
        experiment_number="32131",
        experiment_title="title",
        filepath="/archive/mari/32131/123.nxs",
    )
    mock_ingest.return_value = run
    detector._process_message(MESSAGE)
    detector._notifier.notify.assert_called_once_with(Notification(run.to_json_string()))
    detector._queue_listener.acknowledge.assert_called_once_with(MESSAGE)


@patch(
    "rundetection.run_detection.InstrumentSpecification.verify", side_effect=lambda x: x.setattr("will_reduce", False)
)
@patch("rundetection.run_detection.ingest")
def test__process_message_specification_not_met(mock_ingest, _, detector):
    """
    Test message processed when specification is not met
    :param mock_ingest: Mock ingest function
    :param _: Throwaway mock specification
    :param detector: RunDetector fixture
    :return: None
    """
    run = DetectedRun(
        run_number=123,
        instrument="mari",
        experiment_number="32131",
        experiment_title="title",
        filepath="/archive/mari/32131/123.nxs",
    )
    mock_ingest.return_value = run
    detector._process_message(MESSAGE)

    detector._notifier.notify.assert_not_called()
    detector._queue_listener.acknowledge.assert_called_once_with(MESSAGE)


@patch("rundetection.run_detection.ingest", side_effect=FileNotFoundError)
def test__process_message_exception_logged(_: Mock, caplog: LogCaptureFixture, detector: RunDetector):
    """
    Test that any exception raise by the RunDetector is caught and logged and the message is still acknowledged
    :param _: Throwaway ingest mock
    :param caplog: LogCaptureFixture
    :param detector: RunDetector Fixture
    :return: None
    """
    detector._process_message(MESSAGE)
    assert "Problem processing message: value" in caplog.text
    detector._queue_listener.acknowledge.assert_called_once_with(MESSAGE)


@patch("rundetection.run_detection.time.sleep", side_effect=InterruptedError)
def test_run(_: Mock, detector: RunDetector) -> None:
    """
    Test that run is processing messages and queue listener is started
    :param _: time.sleep patched mock
    :param detector: RunDetector Fixture
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


def test__map_path(detector) -> None:
    """
    Test that the given path is mapped to use the expected /archive
    :param detector: The run detector fixture
    :return: None
    """
    initial_path_string = r"\\isis\foo\bar\baz.nxs"
    expected_path = Path("/archive/foo/bar/baz.nxs")
    assert detector._map_path(initial_path_string) == expected_path


if __name__ == "__main__":
    unittest.main()
