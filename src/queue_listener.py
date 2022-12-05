"""queue listener module containing classes relating to consuming messages from ICAT pre queue on activemq message
broker """
from dataclasses import dataclass
from multiprocessing import SimpleQueue

from stomp import Connection  # type: ignore
from stomp.listener import ConnectionListener  # type: ignore
from stomp.utils import Frame  # type: ignore


@dataclass
class Message:
    """
    Message Dataclass representing a message consumed from icat-prequeue
    """

    id: str
    value: str
    processed: bool = False


class QueueListener(ConnectionListener):  # type: ignore # No Library stub
    """
    QueueListener wraps stomp.py ConnectionListener. Handles Connection and disconnection from ActiveMQ,
    incoming messages and message acknowledgements
    """

    # pylint: disable=(unsubscriptable-object)
    def __init__(self, message_queue: SimpleQueue[Message], ack_queue: SimpleQueue[Message]) -> None:
        self.message_queue = message_queue
        self.ack_queue = ack_queue
        super().__init__()

    def on_message(self, frame: Frame) -> None:
        """
        Called on message received, Creates the message object and passes into the internal message queue
        :param frame: The frame recieved by the queue Listener
        """
        self.message_queue.put(Message(value=frame.body, id=frame.headers["message-id"]))

    def run(self) -> None:
        """
        Connect to activemq and start listening for messages
        :return: None
        """
        connection = Connection()
        subscription_id = "1"
        connection.connect('admin', 'admin')
        connection.set_listener(listener=self, name="run-detection-queue-listener")
        connection.subscribe(destination="Interactive-Reduction", id=subscription_id, ack="client")
        while True:
            message = self.ack_queue.get()
            if message:
                connection.ack(message.id, subscription_id)
