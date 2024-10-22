"""Living buildings DTO are defined here."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import shapely.geometry as geom


@dataclass
class LivingBuildingsWithGeometryDTO:  # pylint: disable=too-many-instance-attributes
    living_building_id: int
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    physical_object_function_id: int | None
    physical_object_function_name: str | None
    physical_object_name: str | None
    physical_object_properties: dict[str, str]
    physical_object_created_at: datetime
    physical_object_updated_at: datetime
    residents_number: int | None
    living_area: float | None
    properties: dict[str, Any]
    geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    centre_point: geom.Point
    physical_object_address: str | None
    object_geometry_osm_id: str | None

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)


@dataclass(frozen=True)
class LivingBuildingsDTO:  # pylint: disable=too-many-instance-attributes
    living_building_id: int
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    physical_object_function_id: int
    physical_object_function_name: str
    physical_object_name: str | None
    physical_object_properties: dict[str, Any]
    physical_object_created_at: datetime
    physical_object_updated_at: datetime
    residents_number: int | None
    living_area: float | None
    properties: dict[str, Any]
