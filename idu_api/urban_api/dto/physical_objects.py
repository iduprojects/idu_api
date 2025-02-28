"""Physical objects DTOs are defined here."""

from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import shapely.geometry as geom
from shapely.wkb import loads as wkb_loads

# pylint: disable=too-many-instance-attributes


Geom = geom.Polygon | geom.MultiPolygon | geom.Point | geom.LineString | geom.MultiLineString


@dataclass(frozen=True)
class PhysicalObjectDTO:
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    physical_object_function_id: int | None
    physical_object_function_name: str | None
    name: str | None
    properties: dict[str, Any]
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
    territories: list[dict[str, Any]]
    created_at: datetime
    updated_at: datetime

    @classmethod
    def fields(cls) -> Iterable[str]:
        return cls.__annotations__.keys()


@dataclass
class PhysicalObjectWithGeometryDTO:
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    physical_object_function_id: int
    physical_object_function_name: str
    territory_id: int
    territory_name: str
    name: str | None
    properties: dict[str, Any]
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
    object_geometry_id: int
    address: str | None
    osm_id: str | None
    geometry: Geom
    centre_point: geom.Point
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, bytes):
            self.centre_point = wkb_loads(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, bytes):
            self.geometry = wkb_loads(self.geometry)

    def to_geojson_dict(self) -> dict[str, Any]:
        physical_object = asdict(self)

        physical_object_type = {
            "physical_object_type_id": physical_object.pop("physical_object_type_id"),
            "name": physical_object.pop("physical_object_type_name"),
        }
        physical_object_function = (
            {
                "id": physical_object.pop("physical_object_function_id"),
                "name": physical_object.pop("physical_object_function_name"),
            }
            if physical_object["physical_object_function_id"] is not None
            else None
        )
        physical_object["physical_object_type"] = physical_object_type
        physical_object["physical_object_type"]["physical_object_function"] = physical_object_function

        building = {
            "id": physical_object.pop("building_id"),
            "properties": physical_object.pop("building_properties"),
            "floors": physical_object.pop("floors"),
            "building_area_official": physical_object.pop("building_area_official"),
            "building_area_modeled": physical_object.pop("building_area_modeled"),
            "project_type": physical_object.pop("project_type"),
            "floor_type": physical_object.pop("floor_type"),
            "wall_material": physical_object.pop("wall_material"),
            "built_year": physical_object.pop("built_year"),
            "exploitation_start_year": physical_object.pop("exploitation_start_year"),
        }
        if building["id"] is not None:
            physical_object["building"] = building
        else:
            physical_object["building"] = None

        physical_object["territories"] = [
            {
                "id": physical_object.pop("territory_id"),
                "name": physical_object.pop("territory_name"),
            }
        ]

        return physical_object


@dataclass(frozen=True)
class ShortPhysicalObjectDTO:
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    name: str | None
    properties: dict[str, Any]
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


@dataclass(frozen=True)
class ShortScenarioPhysicalObjectDTO(ShortPhysicalObjectDTO):
    is_scenario_object: bool


@dataclass(frozen=True)
class ScenarioPhysicalObjectDTO(PhysicalObjectDTO):
    is_scenario_object: bool

    @classmethod
    def fields(cls) -> Iterable[str]:
        return cls.__annotations__.keys() | super().__annotations__.keys()
