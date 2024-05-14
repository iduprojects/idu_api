"""
Physical objects DTO are defined here.
"""

from dataclasses import dataclass
from typing import Optional

import shapely.geometry as geom


@dataclass
class PhysicalObjectsDataDTO:
    physical_object_id: int
    physical_object_type_id: int
    name: Optional[str]
    address: Optional[str]
    properties: dict[str, str]


@dataclass
class PhysicalObjectWithGeometryDTO:
    physical_object_id: int
    physical_object_type_id: int
    name: Optional[str]
    address: Optional[str]
    properties: dict[str, str]
    geometry: geom.Polygon | geom.MultiPolygon
    centre_point: geom.Point
