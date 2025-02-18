"""Living buildings DTOs are defined here."""

from dataclasses import dataclass
from typing import Any

import shapely.geometry as geom
from shapely.wkb import loads as wkb_loads


@dataclass
class BuildingWithGeometryDTO:  # pylint: disable=too-many-instance-attributes
    building_id: int
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    physical_object_name: str | None
    physical_object_properties: dict[str, Any]
    properties: dict[str, Any]
    floors: int | None
    building_area_official: float | None
    building_area_modeled: float | None
    project_type: str | None
    floor_type: str | None
    wall_material: str | None
    built_year: int | None
    exploitation_start_year: int | None
    object_geometry_id: int
    address: str | None
    osm_id: str | None
    geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    centre_point: geom.Point

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, bytes):
            self.centre_point = wkb_loads(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, bytes):
            self.geometry = wkb_loads(self.geometry)


@dataclass(frozen=True)
class BuildingDTO:
    building_id: int
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    physical_object_name: str | None
    physical_object_properties: dict[str, Any]
    properties: dict[str, Any]
    floors: int | None
    building_area_official: float | None
    building_area_modeled: float | None
    project_type: str | None
    floor_type: str | None
    wall_material: str | None
    built_year: int | None
    exploitation_start_year: int | None
