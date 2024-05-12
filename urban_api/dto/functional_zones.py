"""
Functional zones DTO are defined here.
"""

from dataclasses import dataclass

from shapely import geometry as geom


@dataclass
class FunctionalZoneDataDTO:
    functional_zone_id: int
    territory_id: int
    functional_zone_type_id: int
    geometry: geom.Polygon | geom.MultiPolygon
