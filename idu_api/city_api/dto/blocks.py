from dataclasses import dataclass
from typing import Optional
from shapely.geometry import Polygon, MultiPolygon, Point

from idu_api.city_api.dto.territory_base import TerritoryBase


@dataclass()
class BlocksDTO(TerritoryBase):
    id: int = None
    population: Optional[int] = None
    geometry: Optional[Polygon | MultiPolygon | Point] = None
    center: Point = None
