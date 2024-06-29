"""
Living buildings DTO are defined here.
"""

from dataclasses import dataclass
from typing import Dict, Optional

import shapely.geometry as geom


@dataclass()
class LivingBuildingsWithGeometryDTO:  # pylint: disable=too-many-instance-attributes
    living_building_id: int
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    physical_object_name: Optional[str]
    physical_object_address: Optional[str]
    physical_object_properties: Dict[str, str]
    residents_number: Optional[int]
    living_area: Optional[float]
    properties: Dict[str, str]
    geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    centre_point: geom.Point

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)


@dataclass(frozen=True)
class LivingBuildingsDTO:
    living_building_id: int
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    physical_object_name: Optional[str]
    physical_object_address: Optional[str]
    physical_object_properties: Dict[str, str]
    residents_number: Optional[int]
    living_area: Optional[float]
    properties: Dict[str, str]
