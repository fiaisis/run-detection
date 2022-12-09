"""
Unit tests for notification and notifier
"""
from unittest.mock import patch, Mock

from rundetection.notifications import Notification, Notifier


@patch("src.notifications.print")
def test_notify(mock_print: Mock) -> None:
    """
    Test that notify prints to console
    :param mock_print: patched print
    :return: None
    """
    notification = Notification("foo")
    notifier = Notifier()
    notifier.notify(notification)
    mock_print.assert_called_once_with(notification)
