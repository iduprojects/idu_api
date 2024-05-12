"""
Living buildings DTO are defined here.
"""

from dataclasses import dataclass
from typing import Dict

import shapely.geometry as geom


@dataclass
class LivingBuildingsWithGeometryDTO:
    living_building_real_id: int
    physical_object_id: int
    residents_number: int
    living_area: float
    properties: Dict[str, str]
    geometry: geom.Polygon | geom.MultiPolygon
