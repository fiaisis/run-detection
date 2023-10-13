"""
Main module for run detection
"""
import logging
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from queue import SimpleQueue
from typing import Generator, Any

from pika import BlockingConnection, ConnectionParameters, PlainCredentials  # type: ignore
from pika.adapters.blocking_connection import BlockingChannel  # type: ignore

from rundetection.ingest import ingest, JobRequest
from rundetection.specifications import InstrumentSpecification

file_handler = logging.FileHandler(filename="run-detection.log")
stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(
    handlers=[file_handler, stdout_handler],
    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
logging.getLogger("pika").setLevel(logging.WARNING)

INGRESS_QUEUE_NAME = os.environ.get("INGRESS_QUEUE_NAME", "watched-files")
EGRESS_QUEUE_NAME = os.environ.get("EGRESS_QUEUE_NAME", "scheduled-jobs")


def get_channel(exchange_name: str, queue_name: str) -> BlockingChannel:
    """
    Given an exchange and queue name, return a blocking channel to the exchange and quque
    :param exchange_name: The exchange name
    :param queue_name: The queue name
    :return: The Blocking Channel
    """
    credentials = PlainCredentials(
        username=os.environ.get("QUEUE_USER", "guest"), password=os.environ.get("QUEUE_PASSWORD", "guest")
    )
    connection_parameters = ConnectionParameters(
        os.environ.get("QUEUE_HOST", "localhost"), 5672, credentials=credentials
    )
    connection = BlockingConnection(connection_parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange_name, exchange_type="direct", durable=True)
    channel.queue_declare(queue_name, durable=True, arguments={"x-queue-type": "quorum"})
    channel.queue_bind(queue_name, exchange_name, routing_key="")
    return channel


@contextmanager
# pylint: disable = unsupported-binary-operation
def producer() -> Generator[BlockingChannel | BlockingChannel, Any, None]:
    """
    Return a context managed pika producer channel
    :return: BlockingChannel
    """
    logger.info("Creating producer...")
    channel = get_channel("scheduled-jobs", "scheduled-jobs")
    yield channel
    logger.info("Closing producer channel and connection...")
    channel.close()
    channel.connection.close()
    logger.info("Producer closed.")


def process_message(message: str, notification_queue: SimpleQueue[JobRequest]) -> None:
    """
    Process the incoming message. If the message should result in an upstream notification, it will put the message on
    the given notification queue
    :param message: The message to process
    :param notification_queue: The notification queue to update
    :return: None
    """
    logger.info("Proccessing message: %s", message)
    data_path = Path(message)
    run = ingest(data_path)
    specification = InstrumentSpecification(run.instrument)
    specification.verify(run)
    if run.will_reduce:
        logger.info("specification met for run: %s", run)
        notification_queue.put(run)
        for request in run.additional_requests:
            notification_queue.put(request)
    else:
        logger.info("Specification not met, skipping run: %s", run)


def process_messages(channel: BlockingChannel, notification_queue: SimpleQueue[JobRequest]) -> None:
    """
    Given a list of messages and the notification queue, process each message, adding those which meet specifications to
    the notification queue
    :param messages: The list of messages
    :param notification_queue: The notification queue
    :return: None
    """
    logger.info("Listening for messages")
    for method_frame, _, body in channel.consume(INGRESS_QUEUE_NAME):
        try:
            process_message(body.decode(), notification_queue)
        # pylint: disable = broad-exception-caught
        except Exception as exc:
            logger.exception("Problem processing message: %s", body, exc_info=exc)
        finally:
            logger.info("Acking message %s", method_frame.delivery_tag)
            channel.basic_ack(method_frame.delivery_tag)
        logger.info("Pausing listener...")
        break


def process_notifications(notification_queue: SimpleQueue[JobRequest]) -> None:
    """
    Produce messages until the notification queue is empty
    :param notification_queue: The notification queue
    :return: None
    """
    logger.info("Checking notification queue...")
    while not notification_queue.empty():
        detected_run = notification_queue.get()
        logger.info("Sending notification for run: %s", detected_run.run_number)

        with producer() as channel:
            channel.basic_publish(EGRESS_QUEUE_NAME, "", detected_run.to_json_string().encode())
    logger.info("Notification queue empty. Continuing...")


def start_run_detection() -> None:
    """
    Main Coroutine starts the producer and consumer in a loop
    :return: None
    """

    logger.info("Starting Run Detection")
    logger.info("Creating consumer...")
    consumer_channel = get_channel(INGRESS_QUEUE_NAME, INGRESS_QUEUE_NAME)
    logger.info("Consumer created")
    notification_queue: SimpleQueue[JobRequest] = SimpleQueue()
    logger.info("Starting loop...")
    try:
        while True:
            process_messages(consumer_channel, notification_queue)
            process_notifications(notification_queue)
            time.sleep(0.1)

    # pylint: disable = broad-except
    except Exception:
        logger.exception("Uncaught error occurred in main loop. Restarting in 30 seconds...")
        time.sleep(30)
        start_run_detection()


def verify_archive_access() -> None:
    """Log archive access"""
    if Path("/archive", "NDXALF").exists():
        logger.info("The archive has been mounted correctly, and can be accessed.")
    else:
        logger.error("The archive has not been mounted correctly, and cannot be accessed.")


def main() -> None:
    """
    Entry point for run detection
    :return: None
    """
    verify_archive_access()
    start_run_detection()


if __name__ == "__main__":
    main()
