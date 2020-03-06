import os

from publisher.models import Station, StationStatus
from publisher.utils import model_builder
from typing import Any, Iterator, Dict

import asyncio
from aiohttp import ClientSession
import time
from google.cloud import pubsub_v1
import json
BASE_URL = "https://gbfs.citibikenyc.com/gbfs/en"


async def fetch(url) -> Dict:
    async with ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            return data


async def run(loop, url, model, run_rate: int, publisher: pubsub_v1, topic_name):
    raw_data = asyncio.Queue()
    tasks = []
    for _ in range(run_rate):
        tasks.append(loop.create_task(fetch(url)))
    for task in tasks:
        data: Dict = await task
        await raw_data.put(data)

    while not raw_data.empty():
        data = await raw_data.get()
        for item in data_parser(data, model):
            publisher.publish(topic_name, str(json.dumps(item.as_dict())).encode())


def data_parser(data: Dict, model: Any) -> Iterator[Any]:
    for station_data in data["data"]["stations"]:
        model_obj = model_builder(station_data, model)
        yield model_obj


def main():
    while True:
        run_rate = 10

        start_async = time.time()
        publisher = pubsub_v1.PublisherClient()
        topic_name = "projects/{project_id}/topics/{topic}".format(
            project_id=os.getenv("DEVSHELL_PROJECT_ID"),
            topic="station_ingestion_v1",
        )
        # try:
        #     publisher.create_topic(topic_name)
        # except:
        #     print("topic already exists")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            run(
                loop=loop,
                url=f"{BASE_URL}/station_status.json",
                model=StationStatus,
                run_rate=run_rate,
                publisher=publisher, topic_name=topic_name
            )
        )
        end_async = time.time() - start_async

        print(f"async runtime:{end_async}")
        time.sleep(30)


if __name__ == "__main__":
    main()
