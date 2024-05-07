from shapely import geometry as geom
from dataclasses import dataclass


@dataclass
class FunctionalZoneDataDTO:
    functional_zone_id: int
    territory_id: int
    functional_zone_type_id: int
    geometry: geom.Polygon | geom.MultiPolygon
