"""
Main module for run detection
"""
import asyncio
import logging
import os
import sys
from pathlib import Path
from queue import SimpleQueue
from typing import List, Optional, Any

from memphis import Memphis  # type: ignore
from memphis.message import Message  # type: ignore

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


async def create_and_get_memphis() -> Memphis:
    """
    Create and return the connected Memphis Object
    :return: Memphis instance
    """
    logger.info("Creating memphis object...")
    memphis = Memphis()
    logger.info("Connecting...")
    host = os.environ.get("MEMPHIS_HOST", "localhost")
    user = os.environ.get("MEMPHIS_USER", "root")
    password = os.environ.get("MEMPHIS_PASS", "memphis")
    await memphis.connect(host=host, username=user, password=password)
    logger.info("Connected to memphis")
    return memphis


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


async def process_messages(messages: Optional[List[Message]], notification_queue: SimpleQueue[JobRequest]) -> None:
    """
    Given a list of messages and the notification queue, process each message, adding those which meet specifications to
    the notification queue
    :param messages: The list of messages
    :param notification_queue: The notification queue
    :return: None
    """
    if messages:
        for message in messages:
            message_value = message.get_data().decode("utf-8")
            try:
                process_message(message_value, notification_queue)
            # pylint: disable = broad-except
            except Exception:
                logger.exception("problem proscessing message")
            finally:
                logger.info("acking message")
                await message.ack()


async def process_notifications(producer: Any, notification_queue: SimpleQueue[JobRequest]) -> None:
    """
    Produce messages until the notification queue is empty
    :param producer: The producer
    :param notification_queue: The notification queue
    :return: None
    """
    while not notification_queue.empty():
        detected_run = notification_queue.get()
        logger.info("Sending notification for run: %s", detected_run.run_number)
        await producer.produce(bytearray(detected_run.to_json_string(), "utf-8"))


async def start_run_detection() -> None:
    """
    Main Coroutine starts the producer and consumer in a loop
    :return: None
    """

    logger.info("Starting Run Detection")
    memphis = await create_and_get_memphis()
    logger.info("Creating consumer")
    ingress_station = os.environ.get("MEMPHIS_INGRESS_NAME", "watched-files")
    consumer = await memphis.consumer(
        station_name=ingress_station,
        consumer_group="rundetection",
        consumer_name="rundetection",
        generate_random_suffix=True,
    )
    logger.info("Creating producer")
    egress_station = os.environ.get("MEMPHIS_EGRESS_NAME", "scheduled-jobs")
    producer = await memphis.producer(
        station_name=egress_station, producer_name="rundetection", generate_random_suffix=True
    )
    notification_queue: SimpleQueue[JobRequest] = SimpleQueue()
    logger.info("Starting loop...")
    try:
        while True:
            recieved: Optional[List[Message]] = await consumer.fetch()
            await process_messages(recieved, notification_queue)
            await process_notifications(producer, notification_queue)
            await asyncio.sleep(0.1)
    # pylint: disable = broad-except
    except Exception:
        logger.exception("Uncaught error occurred in main loop. Restarting in 30 seconds...")
        await asyncio.sleep(30)
        await start_run_detection()


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
    asyncio.run(start_run_detection())


if __name__ == "__main__":
    main()
