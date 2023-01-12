"""queue listener module containing classes relating to consuming messages from ICAT pre queue on activemq message
broker """
import logging
import os
import time
from dataclasses import dataclass
from queue import SimpleQueue

from stomp import Connection  # type: ignore
from stomp.exception import ConnectFailedException  # type: ignore
from stomp.listener import ConnectionListener  # type: ignore
from stomp.utils import Frame  # type: ignore

logger = logging.getLogger(__name__)


@dataclass
class Message:
    """
    Message Dataclass representing a message consumed from icat-prequeue
    """

    id: str
    value: str
    processed: bool = False


class TopicListener(ConnectionListener):  # type: ignore # No Library stub
    """
    TopicListener wraps stomp.py ConnectionListener. Handles Connection and disconnection from ActiveMQ,
    incoming messages and message acknowledgements
    """

    def __init__(self, message_queue: SimpleQueue[Message]) -> None:
        self._message_queue = message_queue
        self._ip: str = os.environ.get("ACTIVEMQ_IP", "localhost")
        self._user: str = os.environ.get("ACTIVEMQ_USER", "admin")
        self._password: str = os.environ.get("ACTIVEMQ_PASS", "admin")
        self._connection: Connection = Connection([(self._ip, 61613)])
        self._subscription_id = "1"
        super().__init__()

    def on_message(self, frame: Frame) -> None:
        """
        Called on message received, Creates the message object and passes into the internal message queue
        :param frame: The frame recieved by the queue Listener
        """
        message = Message(value=frame.body, id=frame.headers["message-id"])
        logger.info("Received message: %s", message)
        self._message_queue.put(message)

    def on_disconnected(self) -> None:
        """
        Called on disconnection from message broker. Will attempt to reconnect.
        """

        logger.warning("Disconnected, attempting reconnect...")
        self._connect_and_subscribe()

    def _connect_and_subscribe(self) -> None:
        try:
            logger.info("Attempting connection")
            self._connection.connect(username=self._user, password=self._password)
            self._connection.set_listener(listener=self, name="run-detection-listener")
            self._connection.subscribe(destination="/topic/Interactive-Reduction", id=self._subscription_id,
                                       ack="client")
        except ConnectFailedException:
            logger.warning("Failed to reconnect, attempting again in 30 seconds")
            time.sleep(30)
            self._connect_and_subscribe()

    def run(self) -> None:
        """
        Connect to activemq and start listening for messages. The queue listener is non blocking and runs
        asynchronously.
        :return: None
        """
        logger.info("Starting queue listener")
        self._connect_and_subscribe()

    def acknowledge(self, message: Message) -> None:
        """
        Sends acknowledgement to the message broker that the was consumed
        :param message: The message to acknowledge
        :return: None
        """
        logger.info("Acknowledging message: %s", message)
        self._connection.ack(message.id, self._subscription_id)
