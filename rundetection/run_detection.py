"""Main module for run detection."""

from __future__ import annotations

import logging
import os
import sys
import time
import typing
from contextlib import contextmanager
from pathlib import Path
from queue import SimpleQueue

from pika import BlockingConnection, ConnectionParameters, PlainCredentials  # type: ignore

from rundetection.exceptions import ReductionMetadataError
from rundetection.health import Heartbeat
from rundetection.ingestion.ingest import ingest
from rundetection.rules.enginx_rules import build_enginx_run_number_cycle_map
from rundetection.specifications import InstrumentSpecification

if typing.TYPE_CHECKING:
    from collections.abc import Generator
    from typing import Any

    from pika.adapters.blocking_connection import BlockingChannel  # type: ignore

    from rundetection.job_requests import JobRequest

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
    Given an exchange and queue name, return a blocking channel to the exchange and queue
    :param exchange_name: The exchange name
    :param queue_name: The queue name
    :return: The Blocking Channel.
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
def producer() -> Generator[BlockingChannel | BlockingChannel, Any, None]:
    """
    Return a context managed pika producer channel
    :return: BlockingChannel.
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
    :return: None.
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
    :param channel: The channel for consuming from
    :param notification_queue: The notification queue
    :return: None.
    """
    for method_frame, _, body in channel.consume(INGRESS_QUEUE_NAME, inactivity_timeout=5):
        try:
            process_message(body.decode(), notification_queue)
            logger.info("Acking message %s", method_frame.delivery_tag)
            channel.basic_ack(method_frame.delivery_tag)
        except ReductionMetadataError as exc:
            logger.exception("Problem with metadata, cannot reduce, skipping message", exc_info=exc)
            channel.basic_ack(method_frame.delivery_tag)
        except AttributeError:  # If the message frame or body is missing attributes required e.g. the delivery tag
            pass
        except Exception as exc:
            logger.exception("Problem processing message: %s", body, exc_info=exc)
            logger.info("Nacking message %s", method_frame.delivery_tag)
            channel.basic_nack(method_frame.delivery_tag)
        break


def process_notifications(notification_queue: SimpleQueue[JobRequest]) -> None:
    """
    Produce messages until the notification queue is empty
    :param notification_queue: The notification queue
    :return: None.
    """
    while not notification_queue.empty():
        detected_run = notification_queue.get()
        logger.info("Sending notification for run: %s", detected_run.run_number)

        with producer() as channel:
            channel.basic_publish(EGRESS_QUEUE_NAME, "", detected_run.to_json_string().encode())


def start_run_detection() -> None:
    """
    Start the producer and consumer in a loop.
    :return: None.
    """
    logger.info("Starting Run Detection")
    logger.info("Creating consumer...")
    consumer_channel = get_channel(INGRESS_QUEUE_NAME, INGRESS_QUEUE_NAME)
    consumer_channel.basic_qos(prefetch_count=1)
    logger.info("Consumer created")
    notification_queue: SimpleQueue[JobRequest] = SimpleQueue()
    logger.info("Starting loop...")
    try:
        while True:
            process_messages(consumer_channel, notification_queue)
            process_notifications(notification_queue)
            time.sleep(0.1)
    except Exception:
        logger.exception("Uncaught error occurred in main loop. Restarting in 30 seconds...")
        time.sleep(30)
        start_run_detection()


def verify_archive_access() -> None:
    """Log archive access."""
    if Path("/archive", "NDXALF").exists():
        logger.info("The archive has been mounted correctly, and can be accessed.")
    else:
        logger.error("The archive has not been mounted correctly, and cannot be accessed.")


def pre_build_enginx_cycle_mapping() -> None:
    """
    Make an initial call to the enginx run_number_cycle_map to ensure it is cached ahead of time.
    :return: None
    """
    build_enginx_run_number_cycle_map()


def main() -> None:
    """
    Entry point for run detection
    :return: None.
    """
    verify_archive_access()
    heart_beat = Heartbeat()
    heart_beat.start()
    build_enginx_run_number_cycle_map()
    try:
        start_run_detection()
    finally:
        heart_beat.stop()


if __name__ == "__main__":
    main()
