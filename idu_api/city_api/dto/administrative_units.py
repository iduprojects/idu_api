from dataclasses import dataclass
from typing import Optional

from shapely.geometry import Polygon, MultiPolygon, Point

from idu_api.city_api.dto.territory_base import TerritoryBase


@dataclass()
class AdministrativeUnitsDTO(TerritoryBase):
    """Administrative unit DTO"""

    id: int = None
    name: str = None
    center: Point = None
    type: str = None
    population: Optional[int] = None
    geometry: Optional[Polygon | MultiPolygon | Point] = None
