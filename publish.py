import requests
from models import Station, StationStatus
from utils import model_builder
from typing import Any, List

import asyncio
from aiohttp import ClientSession
import json


BASE_URL = "https://gbfs.citibikenyc.com/gbfs/en"


async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()


async def run(url, delay):
    tasks = []

    async with ClientSession as session:
        data = await fetch(url, session)

        data = json.loads(data.decode("utf-8"))

        # for i in range(r):
        #     task = asyncio.ensure_future(fetch(url, session))
        #     tasks.append(task)

        responses = asyncio.gather(*tasks)
        await responses


async def data_parser(data, model) -> List[Any]:
    models = []
    for station_data in data["data"]["stations"]:
        model_obj = model_builder(station_data, model)
        models.append(model_obj)
    return models


if __name__ == "__main__":
    r = requests.get(url=f"{BASE_URL}/station_information.json")
    data = r.json()
    stations = data_parser(data, Station)

    r = requests.get(url=f"{BASE_URL}/station_status.json")
    data = r.json()
    station_status = data_parser(data, StationStatus)
    print(station_status[0])
