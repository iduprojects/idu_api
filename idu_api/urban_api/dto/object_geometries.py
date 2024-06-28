"""
Object geometries DTO are defined here.
"""

from dataclasses import dataclass
from typing import Optional

import shapely.geometry as geom


@dataclass()
class ObjectGeometryDTO:
    object_geometry_id: int
    territory_id: int
    address: Optional[str]
    geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    centre_point: geom.Point

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)
