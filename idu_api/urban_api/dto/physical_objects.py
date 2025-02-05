"""Physical objects DTOs are defined here."""

from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import shapely.geometry as geom

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
    living_building_id: int | None
    living_area: float | None
    living_building_properties: dict[str, Any] | None
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
    living_building_id: int | None
    living_area: float | None
    living_building_properties: dict[str, Any] | None
    object_geometry_id: int
    address: str | None
    osm_id: str | None
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
        else:
            physical_object["living_building"] = None

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
    living_building_id: int | None
    living_area: float | None
    living_building_properties: dict[str, Any] | None


@dataclass(frozen=True)
class ShortScenarioPhysicalObjectDTO(ShortPhysicalObjectDTO):
    is_scenario_object: bool


@dataclass(frozen=True)
class ScenarioPhysicalObjectDTO(PhysicalObjectDTO):
    is_scenario_object: bool

    @classmethod
    def fields(cls) -> Iterable[str]:
        return cls.__annotations__.keys() | super().__annotations__.keys()
