"""Urban objects DTOs are defined here."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import shapely.geometry as geom
from shapely.wkb import loads as wkb_loads

Geom = geom.Polygon | geom.MultiPolygon | geom.Point | geom.LineString | geom.MultiLineString


@dataclass
class UrbanObjectDTO:  # pylint: disable=too-many-instance-attributes
    urban_object_id: int
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    physical_object_function_id: int | None
    physical_object_function_name: str | None
    physical_object_name: str | None
    building_id: int | None
    floors: int | None
    building_area_official: float | None
    building_area_modeled: float | None
    project_type: str | None
    floor_type: str | None
    wall_material: str | None
    built_year: int | None
    exploitation_start_year: int | None
    building_properties: dict[str, Any] | None
    physical_object_properties: dict[str, Any]
    physical_object_created_at: datetime
    physical_object_updated_at: datetime
    object_geometry_id: int
    territory_id: int
    territory_name: str
    address: str | None
    osm_id: str | None
    geometry: Geom
    centre_point: geom.Point
    object_geometry_created_at: datetime
    object_geometry_updated_at: datetime
    service_id: int | None
    service_type_id: int | None
    urban_function_id: int | None
    urban_function_name: str | None
    service_type_name: str | None
    service_type_capacity_modeled: int | None
    service_type_code: str | None
    infrastructure_type: str | None
    service_type_properties: dict[str, Any] | None
    territory_type_id: int | None
    territory_type_name: str | None
    service_name: str | None
    capacity: int | None
    is_capacity_real: bool | None
    service_properties: dict[str, Any] | None
    service_created_at: datetime | None
    service_updated_at: datetime | None

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, bytes):
            self.centre_point = wkb_loads(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, bytes):
            self.geometry = wkb_loads(self.geometry)


@dataclass
class ScenarioUrbanObjectDTO:  # pylint: disable=too-many-instance-attributes
    urban_object_id: int
    scenario_id: int
    public_urban_object_id: int | None
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    physical_object_function_id: int | None
    physical_object_function_name: str | None
    physical_object_name: str | None
    physical_object_properties: dict[str, Any]
    physical_object_created_at: datetime
    physical_object_updated_at: datetime
    building_id: int | None
    floors: int | None
    building_area_official: float | None
    building_area_modeled: float | None
    project_type: str | None
    floor_type: str | None
    wall_material: str | None
    built_year: int | None
    exploitation_start_year: int | None
    building_properties: dict[str, Any] | None
    is_scenario_physical_object: bool
    object_geometry_id: int
    territory_id: int
    territory_name: str
    address: str | None
    osm_id: str | None
    geometry: Geom
    centre_point: geom.Point
    object_geometry_created_at: datetime
    object_geometry_updated_at: datetime
    is_scenario_geometry: bool
    service_id: int | None
    service_type_id: int | None
    urban_function_id: int | None
    urban_function_name: str | None
    service_type_name: str | None
    service_type_capacity_modeled: int | None
    service_type_code: str | None
    infrastructure_type: str | None
    service_type_properties: dict[str, Any] | None
    territory_type_id: int | None
    territory_type_name: str | None
    service_name: str | None
    capacity: int | None
    is_capacity_real: bool | None
    service_properties: dict[str, Any] | None
    service_created_at: datetime | None
    service_updated_at: datetime | None
    is_scenario_service: bool | None

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, bytes):
            self.centre_point = wkb_loads(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, bytes):
            self.geometry = wkb_loads(self.geometry)
