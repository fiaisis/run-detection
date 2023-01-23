"""
Unit tests for notification and notifier
"""
import os
import socket
from unittest.mock import Mock, patch

from rundetection.notifications import Notification, Notifier


@patch("rundetection.notifications.Producer", return_value=Mock())
def test_notify(mock_producer: Mock) -> None:
    """
    Test that notify producer produces a message for kafka
    :return: None
    """
    notification = Notification("foo")
    notifier = Notifier()
    notifier.notify(notification)
    mock_producer.return_value\
        .produce.assert_called_once_with("detected-runs", value=notification.value,
                                         callback=notifier._delivery_callback)  # pylint: disable=W0212


@patch("rundetection.notifications.Producer")
def test_notify_calls_producer_with_a_sensible_default_when_kafka_not_in_env(mock_producer: Mock) -> None:
    """
    Test that notify gets the correct expected IP address when kafka IP is not in the environment
    :return: None
    """
    os.environ.pop("KAFKA_IP", None)

    Notifier()
    mock_producer.assert_called_once_with({'bootstrap.servers': "broker", 'client.id': socket.gethostname()})


@patch("rundetection.notifications.Producer", return_value=Mock())
def test_notify_calls_producer_with_kafka_ip_when_kafka_in_env(mock_producer: Mock) -> None:
    """
    Test that notify gets the correct expected IP address when kafka IP is in the environment
    :return: None
    """
    os.environ["KAFKA_IP"] = "kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local"

    Notifier()
    mock_producer.assert_called_once_with({
        'bootstrap.servers': "kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local",
        'client.id': socket.gethostname()
    })
