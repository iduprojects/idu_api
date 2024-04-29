"""
Territories DTO are defined here.
"""
import shapely.geometry as geom

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class TerritoryTypeDTO:
    """
    Territory type DTO used to transfer territory type data
    """
    territory_type_id: Optional[int]
    name: str


@dataclass(frozen=True)
class TerritoryDTO:
    """
    Territory DTO used to transfer territory data
    """

    territory_id: int
    territory_type_id: int
    parent_id: int
    name: str
    geometry: geom.Polygon | geom.MultiPolygon
    level: int
    properties: dict
    centre_point: geom.Point
    admin_center: int
    okato_code: str
