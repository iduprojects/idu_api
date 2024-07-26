from dataclasses import dataclass
from typing import Optional
from shapely.geometry import Polygon, MultiPolygon, Point

from idu_api.city_api.dto.base import Base


@dataclass()
class BlocksDTO(Base):
    id: int = None
    population: Optional[int] = None
    geometry: Optional[Polygon | MultiPolygon | Point] = None
    center: Point = None
