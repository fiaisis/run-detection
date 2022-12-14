"""
Unit tests for notification and notifier
"""
from unittest.mock import Mock

from rundetection.notifications import Notification, Notifier


def test_notify() -> None:
    """
    Test that notify producer produces a message for kafka
    :return: None
    """
    notification = Notification("foo")
    notifier = Notifier()
    notifier._producer = Mock()
    notifier.notify(notification)
    notifier._producer.produce.assert_called_once_with("detected-runs", value=notification.value)
