import os

from models import Station, StationStatus
from utils import model_builder
from typing import Any, List, Iterator, Dict

import asyncio
from aiohttp import ClientSession
import time
from google.cloud import pubsub_v1

BASE_URL = "https://gbfs.citibikenyc.com/gbfs/en"


async def fetch(url) -> Dict:
    async with ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            return data


async def run(loop, url, model, run_rate: int, publisher: pubsub_v1) -> List[Any]:
    raw_data = asyncio.Queue()
    tasks = []
    for _ in range(run_rate):
        tasks.append(loop.create_task(fetch(url)))
    for task in tasks:
        data: Dict = await task
        await raw_data.put(data)

    built_models = []
    while not raw_data.empty():
        data = await raw_data.get()
        for item in data_parser(data, model):
            publisher.publish(topic='')


def data_parser(data: Dict, model: Any) -> Iterator[Any]:
    for station_data in data['data']['stations']:
        model_obj = model_builder(station_data, model)
        yield model_obj



def main():
    run_rate = 10
    start_sync = time.time()
    # sync_models = []
    # for _ in range(run_rate):
    #     r = requests.get(url=f"{BASE_URL}/station_information.json")
    #     data = r.json()
    #     sync_models.append(item for item in data_parser(data, Station))
    # end_sync = time.time() - start_sync
    # r = requests.get(url=f"{BASE_URL}/station_status.json")
    # data = r.json()
    # station_status = data_parser(data, StationStatus)
    # print(station_status[0])

    start_async = time.time()
    publisher = pubsub_v1.PublisherClient()
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id=os.getenv('DEVSHELL_PROJECT_ID'),
        topic='stations_ingestion',  # Set this to something appropriate.
    )
    publisher.create_topic(topic_name)
    loop = asyncio.get_event_loop()
    models = loop.run_until_complete(run(loop=loop, url=f"{BASE_URL}/station_information.json",
                                         model=Station, run_rate=run_rate, publisher=publisher))
    end_async = time.time() - start_async

    #
    # print(f'sync models:{len(sync_models)}')
    # print(f'sync runtime:{end_sync}')

    print(f'async models:{len(models)}')
    print(f'async runtime:{end_async}')


if __name__ == "__main__":
    main()
