"""
Unit tests for notification and notifier
"""
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
    mock_producer.return_value.produce.assert_called_once_with("detected-runs", value=notification.value)
