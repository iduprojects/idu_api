from dataclasses import dataclass
from typing import Optional

from shapely.geometry import Polygon, MultiPolygon, Point

from idu_api.city_api.dto.base import Base


@dataclass()
class AdministrativeUnitsDTO(Base):
    """Administrative unit DTO"""

    id: int = None
    name: str = None
    center: Point = None
    type: str = None
    population: Optional[int] = None
    geometry: Optional[Polygon | MultiPolygon | Point] = None


@dataclass()
class AdministrativeUnitsWithoutGeometryDTO(Base):
    """Administrative unit DTO"""

    id: int = None
    name: str = None
    center: Point = None
    type: str = None
    population: Optional[int] = None
