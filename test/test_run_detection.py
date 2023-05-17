"""
Tests for run detection module
"""
# pylint: disable=protected-access, redefined-outer-name

import unittest
from pathlib import Path
from queue import Empty
from unittest.mock import patch, Mock

import pytest
from _pytest.logging import LogCaptureFixture

from rundetection.ingest import DetectedRun
from rundetection.notifications import Notification
from rundetection.queue_listener import Message
from rundetection.run_detection import RunDetector

MESSAGE = Message(id="id", value=r"\\isis\inst$\Cycles$\cycle_22_04\NDXGEM\GEM12345.nxs")


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
def test__process_message_specification_met(mock_ingest, _, detector):
    """
    Test process message and specification is met
    :param mock_ingest: mock ingest function
    :param _: mock specification
    :param detector: RunDetector fixture
    :return: None
    """
    run = DetectedRun(
        run_number=123,
        instrument="mari",
        experiment_number="32131",
        experiment_title="title",
        filepath=Path("/archive/mari/32131/123.nxs"),
        run_start="start time",
        run_end="end time",
        users="Keiran",
        raw_frames=1,
        good_frames=1,
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
        filepath=Path("/archive/mari/32131/123.nxs"),
        run_start="start time",
        run_end="end time",
        users="bill, ben",
        raw_frames=0,
        good_frames=0,
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
    assert r"Problem processing message: \\isis\inst$\Cycles$\cycle_22_04\NDXGEM\GEM12345.nxs" in caplog.text
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


@patch("rundetection.run_detection.time.sleep")
def test_run_leaves_main_loop_if_stopping(_: Mock, detector: RunDetector) -> None:
    """
    Test that run will exit if stopping
    :param _: time.sleep patched mock
    :param detector: RunDetector Fixture
    :return: None
    """

    # If this test enters an infinite loop, it is failing as the loop has not broken
    detector._queue_listener = Mock()
    detector._queue_listener.stopping = True
    detector.restart_listener = Mock()
    detector._process_message = Mock()  # type: ignore
    detector._process_message.side_effect = Empty
    detector._message_queue = Mock()
    mock_message = Mock()
    detector._message_queue.get.return_value = mock_message

    detector.run()

    detector._message_queue.get.assert_called_once()
    detector._process_message.assert_called_once_with(mock_message)
    detector._queue_listener.run.assert_called_once()
    detector.restart_listener.assert_not_called()


@patch("rundetection.run_detection.time.sleep", side_effect=[None])
def test_run_will_restart_listener_if_not_connected(_: Mock, detector: RunDetector) -> None:
    """
    Test that run will exit if stopping
    :param _: time.sleep patched mock
    :param detector: RunDetector Fixture
    :return: None
    """

    # If this test enters an infinite loop, it is failing as the loop has not broken
    detector._queue_listener = Mock()
    detector.restart_listener = Mock()
    detector._process_message = Mock()  # type: ignore
    detector._process_message.side_effect = Empty
    detector._message_queue = Mock()
    mock_message = Mock()
    detector._message_queue.get.return_value = mock_message
    detector.restart_listener.side_effect = InterruptedError
    detector._queue_listener.is_connected.return_value = False
    detector._queue_listener.stopping = False
    try:
        detector.run()
    except InterruptedError:
        detector._message_queue.get.assert_called_once()
        detector._process_message.assert_called_once_with(mock_message)
        detector._queue_listener.run.assert_called_once()
        detector.restart_listener.assert_called_once()


def test__map_path(detector) -> None:
    """
    Test that the given path is mapped to use the expected /archive
    :param detector: The run detector fixture
    :return: None
    """

    input_path = r"\\isis\inst$\Cycles$\cycle_22_04\NDXGEM\GEM12345.nxs"
    expected_output = Path("/archive/NDXGEM/Instrument/data/cycle_22_04/GEM12345.nxs")
    result = detector._map_path(input_path)
    assert result == expected_output


def test__map_path_short_name_used(detector) -> None:
    """
    Test that the given path is mapped correctly using a short name
    :param detector:
    :return:
    """
    input_path = r"\\isis\inst$\Cycles$\cycle_23_01\NDXMARI\MAR28573.nxs"
    expected_output = Path("/archive/NDXMARI/Instrument/data/cycle_23_01/MAR28573.nxs")
    result = detector._map_path(input_path)
    assert result == expected_output


def test__map_path_raises_for_bad_path(detector) -> None:
    """
    Test that value error if bad path is given
    :param detector: run detector fixture
    :return: None
    """
    input_path = "foo"
    with pytest.raises(ValueError):
        detector._map_path(input_path)


@patch("rundetection.run_detection.time.sleep")
def test_restart_listener(mock_sleep, detector) -> None:
    """
    Test restart listener attempts to restart
    :return: None
    """
    detector.restart_listener()

    detector._queue_listener.stop.assert_called_once()
    mock_sleep.assert_called_once_with(30)
    detector._queue_listener.run.assert_called_once()


def test_shutdown_listener(detector):
    """
    Test listener shutdown is called
    :return: None
    """
    detector.shutdown_listener()
    detector._queue_listener.stop.assert_called_once()


def test_shutdown(detector):
    """
    Test shutdown calls are made when detector is shutdown
    :param detector: detector fixture
    :return: None
    """
    detector.shutdown(1, None)
    detector._queue_listener.stop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
