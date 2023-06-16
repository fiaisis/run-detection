import asyncio
import logging
import sys
from pathlib import Path
from queue import SimpleQueue

from memphis import Memphis

from rundetection.ingest import ingest, DetectedRun
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
    memphis = Memphis()
    await memphis.connect(host="localhost", username="root", password="memphis")
    return memphis


def process_message(message: str, notification_queue: SimpleQueue[DetectedRun]):
    logger.info("Proccessing message: %s", message)
    # data_path = map_path(message)
    data_path = Path(message)
    run = ingest(data_path)
    specification = InstrumentSpecification(run.instrument)
    specification.verify(run)
    if run.will_reduce:
        logger.info("specification met for run: %s", run)
        notification_queue.put(run)
        for additional_run in run.additional_runs:
            notification_queue.put(additional_run)
    else:
        logger.info("Specification not met, skipping run: %s", run)


async def main():
    memphis = await create_and_get_memphis()
    consumer = await memphis.consumer(station_name="watched-files", consumer_name="rundetection")
    producer = await memphis.producer(station_name="scheduled-jobs", producer_name="rundetection")
    notification_queue: SimpleQueue[DetectedRun] = SimpleQueue()
    while True:
        recieved = await consumer.fetch()
        if recieved:
            for message in recieved:
                message_value = message.get_data().decode("utf-8")
                try:
                    process_message(message_value, notification_queue)
                except Exception as exc:
                    logger.exception("problem proscessing message")
                finally:
                    await message.ack()

        while not notification_queue.empty():
            await producer.produce(bytearray(notification_queue.get().to_json_string(), "utf-8"))
        await asyncio.sleep(0.1)


asyncio.run(main())
