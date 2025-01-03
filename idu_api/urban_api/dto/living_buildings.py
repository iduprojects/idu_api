"""Living buildings DTOs are defined here."""

from dataclasses import dataclass
from typing import Any

import shapely.geometry as geom


@dataclass
class LivingBuildingWithGeometryDTO:  # pylint: disable=too-many-instance-attributes
    living_building_id: int
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    physical_object_name: str | None
    physical_object_properties: dict[str, Any]
    living_area: float | None
    properties: dict[str, Any]
    object_geometry_id: int
    address: str | None
    osm_id: str | None
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
class LivingBuildingDTO:
    living_building_id: int
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    physical_object_name: str | None
    physical_object_properties: dict[str, Any]
    living_area: float | None
    properties: dict[str, Any]
