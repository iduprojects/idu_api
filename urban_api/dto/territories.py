"""
Territories DTO are defined here.
"""

from dataclasses import dataclass
from typing import Dict, Optional

import shapely.geometry as geom


@dataclass(frozen=True)
class TerritoryTypeDTO:
    """
    Territory type DTO used to transfer territory type data
    """

    territory_type_id: Optional[int]
    name: str


@dataclass()
class TerritoryDTO:
    """
    Territory DTO used to transfer territory data
    """

    territory_id: int
    territory_type_id: int
    parent_id: int
    name: str
    geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    level: int
    properties: Optional[Dict[str, str]]
    centre_point: geom.Point
    admin_center: Optional[int]
    okato_code: Optional[str]

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)


@dataclass(frozen=True)
class TerritoryWithoutGeometryDTO:
    """
    Territory DTO used to transfer territory data
    """

    territory_id: int
    territory_type_id: int
    parent_id: int
    name: str
    level: int
    properties: Dict[str, str]
    admin_center: int
    okato_code: str
