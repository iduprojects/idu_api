from dataclasses import dataclass
from typing import Optional
from shapely.geometry import Polygon, MultiPolygon, Point

from idu_api.city_api.dto.territory_base import TerritoryBase


@dataclass()
class MunicipalitiesDTO(TerritoryBase):
    id: int = None
    name: str = None
    population: int = None
    geometry: Optional[Polygon | MultiPolygon | Point] = None
    center: Point = None
    type: str = None


@dataclass()
class MunicipalitiesWithoutGeometryDTO(TerritoryBase):
    id: int = None
    name: str = None
    population: int = None
    center: Point = None
    type: str = None
