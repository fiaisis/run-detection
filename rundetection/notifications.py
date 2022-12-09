"""
Notifications module contains the Notification class and the Notifier class. Notifications to be consumed by a Notifier
instance to send detected runs downstream.
"""
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class Notification:
    """
    Notification dataclass to store information about runs ready for reduction to be passed to notifier to be sent.
    """
    value: str


class Notifier:
    """
    Notifier is used to send notifications downstream.
    """

    # This could be static currently, but not once this does more than print
    def notify(self, notification: Notification) -> None:
        """
        Sends the given notification downstream
        :param notification: The notification to be sent
        :return: None
        """
        logger.info("Sending notification: %s", notification)
        print(notification)
