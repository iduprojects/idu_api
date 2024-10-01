from dataclasses import dataclass
from typing import Optional

from shapely.geometry import MultiPolygon, Point, Polygon

from idu_api.city_api.dto.base import Base


@dataclass()
class BlocksDTO(Base):
    id: int = None
    population: Optional[int] = None
    geometry: Optional[Polygon | MultiPolygon | Point] = None
    center: Point = None

    @classmethod
    async def from_service(
        cls, id: int, population: int | None, geometry: Optional[Polygon | MultiPolygon | Point], center: Point = None
    ):
        return cls(id=id, population=population, geometry=geometry, center=center)


@dataclass()
class BlocksWithoutGeometryDTO(Base):
    id: int = None
    population: Optional[int] = None
    center: Point = None

    @classmethod
    async def from_service(cls, id: int, population: int | None, center: Point = None):
        return cls(id=id, population=population, center=center)
