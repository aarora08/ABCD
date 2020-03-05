from dataclasses import dataclass, InitVar, field, asdict
from typing import List, Dict
from datetime import datetime


@dataclass
class Location:
    name: str
    short_name: str
    lat: float
    lon: float
    region_id: int


@dataclass
class Station(Location):
    station_id: int
    external_id: str
    rental_methods: List[str]
    capacity: int

    def as_dict(self):
        return asdict(self)


@dataclass
class StationStatus:
    station_id: int
    eightd_has_available_keys: InitVar[List[Dict[str, str]]]
    num_bikes_available: InitVar[int]
    num_bikes_disabled: InitVar[int]
    num_docks_available: InitVar[int]
    num_docks_disabled: InitVar[int]
    is_renting: InitVar[int]
    available_key_id: str = field(init=False)
    available_bikes: int = field(init=False)
    disabled_bikes: int = field(init=False)
    available_docks: int = field(init=False)
    disabled_docks: int = field(init=False)
    rent_status: bool = field(init=False)
    last_reported: int

    def __post_init__(
        self,
        eightd_has_available_keys: Dict[str, str],
        num_bikes_available: int,
        num_bikes_disabled: int,
        num_docks_available: int,
        num_docks_disabled: int,
        is_renting: int,
    ):
        self.available_key_id = eightd_has_available_keys
        self.available_bikes = num_bikes_available
        self.disabled_bikes = num_bikes_disabled
        self.available_docks = num_docks_available
        self.disabled_docks = num_docks_disabled
        self.rent_status = bool(is_renting)

    def as_dict(self):
        return asdict(self)
