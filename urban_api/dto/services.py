"""
Services DTO are defined here.
"""

from dataclasses import dataclass
from typing import Dict

import shapely.geometry as geom


@dataclass(frozen=True)
class ServiceDTO:
    service_id: int
    service_type_id: int
    territory_type_id: int
    name: str
    list_label: str
    capacity_real: int
    properties: Dict[str, str]


@dataclass(frozen=True)
class ServiceWithGeometryDTO:
    service_id: int
    service_type_id: int
    territory_type_id: int
    name: str
    list_label: str
    capacity_real: int
    properties: Dict[str, str]
    geometry: geom.Polygon | geom.MultiPolygon
    centre_point: geom.Point
