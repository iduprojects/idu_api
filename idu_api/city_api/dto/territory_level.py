from dataclasses import dataclass
from typing import Optional

from shapely import Polygon, MultiPolygon, Point

from idu_api.city_api.dto.base import Base


@dataclass()
class TerritoryLevelDTO(Base):
    id: int = None
    name: str = None
    population: Optional[int] = None
    geometry: Optional[Polygon | MultiPolygon | Point] = None
    center: Point = None
    type: str = None


@dataclass()
class TerritoryLevelWithoutGeometryDTO(Base):
    id: int = None
    name: str = None
    population: Optional[int] = None
    type: str = None
