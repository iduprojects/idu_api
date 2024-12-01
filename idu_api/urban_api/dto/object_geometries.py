"""Object geometries DTOs are defined here."""

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import shapely.geometry as geom

Geom = geom.Polygon | geom.MultiPolygon | geom.Point | geom.LineString | geom.MultiLineString


@dataclass
class ObjectGeometryDTO:
    object_geometry_id: int
    territory_id: int
    territory_name: str
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
        geometry = asdict(self)
        territory = {
            "id": geometry.pop("territory_id"),
            "name": geometry.pop("territory_name"),
        }
        geometry["territory"] = territory
        return geometry


@dataclass
class ScenarioGeometryDTO(ObjectGeometryDTO):
    is_scenario_object: bool


@dataclass
class GeometryWithAllObjectsDTO:
    object_geometry_id: int
    territory_id: int
    territory_name: str
    address: str | None
    osm_id: str | None
    geometry: Geom
    centre_point: geom.Point
    physical_objects: list[dict[str, Any]]
    services: list[dict[str, Any]]

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)

    def to_geojson_dict(self) -> dict[str, Any]:
        object = asdict(self)
        object["territory"] = {"id": object.pop("territory_id"), "name": object.pop("territory_name")}
        return object


@dataclass
class ScenarioGeometryWithAllObjectsDTO(GeometryWithAllObjectsDTO):
    is_scenario_object: bool
    physical_objects: list[dict[str, Any]]
    services: list[dict[str, Any]]
