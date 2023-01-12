"""
Notifications module contains the Notification class and the Notifier class. Notifications to be consumed by a Notifier
instance to send detected runs downstream.
"""
import logging
import os
import socket
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Producer  # type: ignore

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

    def __init__(self) -> None:
        broker_ip = os.environ.get("KAFKA_IP", "broker")
        config = {'bootstrap.servers': broker_ip, 'client.id': socket.gethostname()}
        logger.info("Connecting to kafka using the ip: %s", broker_ip)
        self._producer = Producer(config)

    def notify(self, notification: Notification) -> None:
        """
        Sends the given notification downstream
        :param notification: The notification to be sent
        :return: None
        """
        logger.info("Sending notification: %s", notification)
        self._producer.produce("detected-runs", value=notification.value, callback=self._delivery_callback)

    @staticmethod
    def _delivery_callback(err: Any, msg: Any):
        if err:
            logger.error("Delivery failed for message %s: %s", msg.value(), err)
        else:
            logger.info("Delivered message to %s [%s]", msg.topic(), msg.partition())
