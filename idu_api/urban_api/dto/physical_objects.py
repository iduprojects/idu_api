"""Physical objects DTOs are defined here."""

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import shapely.geometry as geom

Geom = geom.Polygon | geom.MultiPolygon | geom.Point | geom.LineString | geom.MultiLineString


@dataclass(frozen=True)
class PhysicalObjectDataDTO:
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    physical_object_function_id: int | None
    physical_object_function_name: str | None
    name: str | None
    properties: dict[str, Any]
    living_building_id: int | None
    living_area: float | None
    living_building_properties: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime


@dataclass
class PhysicalObjectWithGeometryDTO:
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    physical_object_function_id: int
    physical_object_function_name: str
    name: str | None
    address: str | None
    osm_id: str | None
    living_building_id: int | None
    living_area: float | None
    living_building_properties: dict[str, Any] | None
    properties: dict[str, Any]
    geometry: Geom
    centre_point: geom.Point
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)

    def to_geojson_dict(self) -> dict[str, Any]:
        physical_object = asdict(self)
        physical_object_type = {
            "physical_object_type_id": physical_object.pop("physical_object_type_id", None),
            "name": physical_object.pop("physical_object_type_name", None),
            "physical_object_function": None,
        }
        physical_object_function = {
            "id": physical_object.pop("physical_object_function_id", None),
            "name": physical_object.pop("physical_object_function_name", None),
        }
        physical_object["physical_object_type"] = physical_object_type
        if physical_object_function["id"] is not None:
            physical_object["physical_object_type"]["physical_object_function"] = physical_object_function
        living_building = {
            "id": physical_object.pop("living_building_id"),
            "living_area": physical_object.pop("living_area"),
            "properties": physical_object.pop("living_building_properties"),
        }
        if living_building["id"] is not None:
            physical_object["living_building"] = living_building

        return physical_object


@dataclass(frozen=True)
class PhysicalObjectWithTerritoryDTO:
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    physical_object_function_id: int | None
    physical_object_function_name: str | None
    name: str | None
    living_building_id: int | None
    living_area: float | None
    living_building_properties: dict[str, Any] | None
    properties: dict[str, Any]
    territories: list[dict[str, Any]]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ShortPhysicalObjectDTO:
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    name: str | None
    properties: dict[str, Any]
    living_building_id: int | None
    living_area: float | None
    living_building_properties: dict[str, Any] | None


@dataclass(frozen=True)
class ShortScenarioPhysicalObjectDTO(ShortPhysicalObjectDTO):
    is_scenario_object: bool


@dataclass(frozen=True)
class ScenarioPhysicalObjectDTO(PhysicalObjectDataDTO):
    is_scenario_object: bool
